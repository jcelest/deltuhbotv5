#!/usr/bin/env python3
import argparse
import httpx
import psycopg2
import os
from datetime import datetime, timezone
import time
import traceback

# --- CONFIGURATION -----------------------------------------------
DB_NAME         = "darkpool_data"
DB_USER         = "trader"
DB_PASS         = "Deltuhdarkpools!7"
DB_HOST         = "localhost"
DB_PORT         = "5432"
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
TICKERS_FILE    = "tickers.txt"
LIT_MIN_BACKFILL_VALUE = 10_000_000 

# --- DB CONNECT --------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

# --- BACKFILL FUNCTION -------------------------------------------
def backfill_data(ticker: str, start_date: str, end_date: str, mode: str):
    conn = get_db_connection()
    print(f"Connected for {mode} backfill of {ticker}: {start_date} → {end_date}")

    base_url = (
        f"https://api.polygon.io/v3/trades/{ticker}"
        f"?timestamp.gte={start_date}&timestamp.lte={end_date}&limit=50000"
    )
    url = f"{base_url}&apiKey={POLYGON_API_KEY}"

    total_downloaded = 0
    total_saved      = 0
    page_count       = 1

    while url:
        print(f"--- Fetching page {page_count} for {ticker} ---")
        try:
            resp = httpx.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"● Fetch error for {ticker}: {e}")
            traceback.print_exc()
            break

        results = data.get("results", [])
        if not results:
            print("● No more data.\n")
            break

        total_downloaded += len(results)

        for trade in results[:3]:
            symbol = trade.get('sym') or trade.get('symbol') or ticker
            ts_ns  = trade.get('participant_timestamp')
            ts_str = (
                datetime.fromtimestamp(ts_ns/1e9, tz=timezone.utc).strftime('%H:%M:%S')
                if ts_ns else "NO_TS"
            )
            size  = trade.get('size')
            price = trade.get('price')
            print(f"  • {symbol} {size}@{price} at {ts_str}")
        print()

        saved_this_page = 0
        with conn.cursor() as cur:
            for trade in results:
                qty   = trade.get('size')
                pr    = trade.get('price')
                ts_ns = trade.get('participant_timestamp')
                exch  = trade.get('exchange')
                trf   = trade.get('trf_id')
                conds = trade.get('conditions', [])
                # ✅ FIX: Get the TRF timestamp from the API response
                trf_ts = trade.get('trf_timestamp')

                if not all([qty, pr, ts_ns, exch is not None]):
                    continue

                val = qty * pr
                dt  = datetime.fromtimestamp(ts_ns/1e9, tz=timezone.utc)

                if mode == 'block':
                    if exch != 4 or trf is None or (qty < 10000 and val < 200000):
                        continue
                    # ✅ FIX: Add the trf_timestamp column and its value to the INSERT statement
                    cur.execute(
                        """
                        INSERT INTO block_trades
                          (trade_time, ticker, price, quantity, trade_value,
                           conditions, exchange, trf_id, trf_timestamp)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (dt, ticker, pr, qty, val, conds, exch, trf, trf_ts)
                    )
                else:  # lit mode
                    if (exch == 4 and trf is not None) or val < LIT_MIN_BACKFILL_VALUE:
                        continue
                    cur.execute(
                        """
                        INSERT INTO lit_trades
                          (trade_time, ticker, price, quantity, trade_value,
                           conditions, exchange)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (dt, ticker, pr, qty, val, conds, exch)
                    )

                if cur.rowcount:
                    saved_this_page += 1

        conn.commit()
        total_saved += saved_this_page
        print(f"→ Saved {saved_this_page} new rows (total {total_saved}).\n")

        next_url = data.get('next_url')
        if next_url:
            url = f"{next_url}&apiKey={POLYGON_API_KEY}"
            page_count += 1
            time.sleep(1)
        else:
            break

    print(f"Finished {mode} backfill for {ticker}: downloaded {total_downloaded}, saved {total_saved}.\n")
    conn.close()

# --- MAIN ENTRYPOINT ---------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill lit or block trades")
    parser.add_argument('--mode', choices=['block','lit'], required=True,
                        help="Which table to backfill: 'block' or 'lit'")
    args = parser.parse_args()
    mode = args.mode

    # Note: You may want to make these command-line arguments as well
    start_date = "2025-08-24"
    end_date   = "2025-08-25"

    if not POLYGON_API_KEY:
        raise ValueError("Polygon API Key not set")

    try:
        with open(TICKERS_FILE) as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
    except FileNotFoundError:
        raise SystemExit(f"Tickers file '{TICKERS_FILE}' not found")

    for ticker in tickers:
        if not ticker.isalnum():
            print(f"Skipping invalid ticker '{ticker}'")
            continue

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                table = 'block_trades' if mode == 'block' else 'lit_trades'
                print(f"Clearing {table} for {ticker} {start_date}-{end_date}")
                cur.execute(
                    f"""
                    DELETE FROM {table}
                     WHERE ticker     = %s
                       AND trade_time >= %s::date
                       AND trade_time <  (%s::date + INTERVAL '1 day');
                    """,
                    (ticker, start_date, end_date)
                )
            conn.commit()
            print("Window cleared.\n")

        backfill_data(ticker, start_date, end_date, mode)
