#!/usr/bin/env python3
import httpx
import psycopg2
import os
from datetime import datetime, timezone
import time
import traceback

# ─── CONFIGURATION ─────────────────────────────────────────────
DB_NAME         = "darkpool_data"
DB_USER         = "trader"
DB_PASS         = "Deltuhdarkpools!7"
DB_HOST         = "localhost"
DB_PORT         = "5432"
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')

# ─── DB CONNECT ────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

# ─── BACKFILL FUNCTION ─────────────────────────────────────────
def backfill_data(ticker: str, start_date: str, end_date: str):
    conn = get_db_connection()
    print("Successfully connected to the database.")
    print(f"Starting backfill for {ticker} from {start_date} to {end_date}...\n")

    # initial URL (will be replaced by next_url on pagination)
    next_url = (
        f"https://api.polygon.io/v3/trades/{ticker.upper()}"
        f"?timestamp.gte={start_date}&timestamp.lte={end_date}&limit=50000"
    )

    total_downloaded = 0
    total_saved      = 0
    page_count       = 1

    while next_url:
        print(f"--- Fetching Page {page_count} ---")
        try:
            paginated = f"{next_url}&apiKey={POLYGON_API_KEY}"
            resp      = httpx.get(paginated, timeout=60.0)
            resp.raise_for_status()
            data      = resp.json()
        except Exception as e:
            print(f"Error fetching data from Polygon: {e}")
            break

        results = data.get("results", [])
        if not results:
            print("No more trades for this range.\n")
            break

        total_downloaded += len(results)

        # sample-print first 5
        print(f"  → Downloaded {len(results)} raw records; samples:")
        for i, trade in enumerate(results[:5]):
            ts_ns    = trade.get('participant_timestamp')
            time_str = (
                datetime.fromtimestamp(ts_ns/1e9, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                if ts_ns else "NO_TIMESTAMP"
            )
            print(f"    [{i+1}] Qty={trade.get('size')}, Price={trade.get('price')}, Time={time_str}, "
                  f"Exch={trade.get('exchange')}, TRF={'YES' if 'trf_id' in trade else 'NO'}, "
                  f"Conds={trade.get('conditions',[])}")

        # insert loop
        saved_on_page = 0
        with conn.cursor() as cur:
            for trade in results:
                quantity   = trade.get('size')
                price      = trade.get('price')
                ts_ns      = trade.get('participant_timestamp')
                conditions = trade.get('conditions')
                exchange   = trade.get('exchange')
                trf_id     = trade.get('trf_id')
                trf_ts     = trade.get('trf_timestamp') or ts_ns

                # skip bad data
                if not all([quantity, price, ts_ns, exchange, trf_ts]):
                    continue

                trade_value = quantity * price
                # your block-trade filter
                if quantity < 10000 and trade_value < 200000:
                    continue

                trade_time = datetime.fromtimestamp(ts_ns/1e9, tz=timezone.utc)

                cur.execute(
                    """
                    INSERT INTO block_trades
                      (trade_time, ticker, price, quantity,
                       trade_value, conditions, exchange,
                       trf_id, trf_timestamp)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (
                        trade_time,
                        ticker.upper(),
                        price,
                        quantity,
                        trade_value,
                        conditions,
                        exchange,
                        trf_id,
                        trf_ts
                    )
                )
                if cur.rowcount:
                    saved_on_page += 1

        conn.commit()
        total_saved += saved_on_page

        print(f"  → Processed {len(results)} records, saved {saved_on_page} new block trades "
              f"(total saved so far: {total_saved}).\n")

        next_url = data.get("next_url")
        if next_url:
            page_count += 1
            time.sleep(1)  # throttle between pages

    print(f"Backfill complete for {ticker}:")
    print(f"  • Downloaded {total_downloaded} raw records")
    print(f"  • Saved     {total_saved} valid block trades")
    conn.close()


# ─── MAIN ENTRYPOINT ────────────────────────────────────────────
if __name__ == "__main__":
    ticker_to_backfill = "QQQ"
    start_date         = "2025-06-20"
    end_date           = "2025-06-27"

    if not POLYGON_API_KEY:
        raise ValueError("Polygon API Key not found in environment.")

    # ─ Clear only the window you’re about to backfill ─────────────
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            print(f"Clearing {ticker_to_backfill.upper()} trades from {start_date} (inclusive) to {end_date} (exclusive)…")
            cur.execute("""
                DELETE FROM block_trades
                 WHERE ticker     = %s
                   AND trade_time >= %s::date
                   AND trade_time <  (%s::date + INTERVAL '1 day');
            """, (
                ticker_to_backfill.upper(),
                start_date,
                end_date
            ))
        conn.commit()
        print("Window cleared.\n")

    backfill_data(ticker_to_backfill, start_date, end_date)
