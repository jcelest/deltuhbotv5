#!/usr/bin/env python
import sys
import time
import traceback
import json
import websocket
import psycopg2
from datetime import datetime, timezone

# ─── CONFIG ─────────────────────────────────────────────────────
POLY_KEY   = "29k_KtZDxzDgsNlnfUyutIa2ibYCTIpD"
DB_NAME    = "darkpool_data"
DB_USER    = "trader"
DB_PASS    = "Deltuhdarkpools!7"
DB_HOST    = "localhost"
DB_PORT    = "5432"

SOCKET_URL       = "wss://delayed.polygon.io/stocks"
MIN_VALUE        = 1_000_000   # only keep trades ≥ $1 000 000
RAW_SAMPLE_EVERY = 25_000      # only print raw JSON every N batches

# ─── DB ────────────────────────────────────────────────────────
def get_db():
    try:
        return psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
        )
    except psycopg2.OperationalError as e:
        print(f"‼ Unable to connect to Postgres as {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        print(f"   {e}")
        sys.exit(1)

# ─── HANDLER ──────────────────────────────────────────────────
class Handler:
    def __init__(self, api_key):
        if not api_key:
            raise RuntimeError("POLY_KEY is missing")
        self.api_key   = api_key
        self.conn      = None
        self.raw_count = 0

    def _connect_db(self):
        if not self.conn or self.conn.closed:
            self.conn = get_db()
        return self.conn

    def on_open(self, ws):
        # authenticate & subscribe
        ws.send(json.dumps({"action": "auth",      "params": self.api_key}))
        ws.send(json.dumps({"action": "subscribe", "params": "T.*"}))
        print("▶ WS open; subscribed to T.*")

    def on_message(self, ws, message):
        self.raw_count += 1
        if self.raw_count % RAW_SAMPLE_EVERY == 0:
            print(f"\n[RAW #{self.raw_count}] {message}\n")

        try:
            batch = json.loads(message)
        except json.JSONDecodeError:
            print("‼ Couldn't parse JSON")
            return

        for trade in batch:
            if trade.get("ev") != "T":
                continue

            size  = trade.get("s", 0)
            price = trade.get("p", 0.0)
            value = size * price

            # only keep trades ≥ MIN_VALUE
            if value < MIN_VALUE:
                continue

            # dark‐pool filter: exchange==4 + has a TRF facility
            if trade.get("x") != 4 or trade.get("trfi") is None:
                continue

            # use the official TRF timestamp (trft) as your trade_time
            ts_ms = trade.get("trft")
            if ts_ms is None:
                continue
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

            try:
                conn = self._connect_db()
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO block_trades
                          (trade_time, ticker, price, quantity, trade_value,
                           conditions, exchange, trf_id, trf_timestamp)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        dt,
                        trade.get("sym"),
                        price,
                        size,
                        value,
                        trade.get("c", []),
                        trade.get("x"),
                        trade.get("trfi"),
                        ts_ms,
                    ))
                conn.commit()

                print(
                    f"✔ Saved {trade['sym']} "
                    f"{size}@{price:.2f} (value={value:.0f}) "
                    f"exchange={trade['x']} trf_id={trade['trfi']} "
                    f"at {dt.isoformat()}"
                )
            except Exception:
                traceback.print_exc()
                if self.conn:
                    self.conn.rollback()

    def on_error(self, ws, error):
        print("‼ WS error:", error)

    def on_close(self, ws, code, msg):
        print(f"⏹ WS closed: code={code} msg={msg}")
        if self.conn:
            self.conn.close()

    def run(self):
        while True:
            ws = websocket.WebSocketApp(
                SOCKET_URL,
                on_open    = self.on_open,
                on_message = self.on_message,
                on_error   = self.on_error,
                on_close   = self.on_close
            )
            ws.run_forever()
            print("🔄 Disconnected—reconnecting in 10s…")
            time.sleep(10)

# ─── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    Handler(POLY_KEY).run()
