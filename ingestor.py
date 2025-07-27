#!/usr/bin/env python
import sys
import time
import traceback
import json
import websocket
import psycopg2

from datetime import datetime, timezone
from threading import Thread
from queue import Queue

# --- CONFIG ─────────────────────────────────────────────────────
POLY_KEY          = "29k_KtZDxzDgsNlnfUyutIa2ibYCTIpD"
DB_NAME           = "darkpool_data"
DB_USER           = "trader"
DB_PASS           = "Deltuhdarkpools!7"
DB_HOST           = "localhost"
DB_PORT           = "5432"
SOCKET_URL        = "wss://delayed.polygon.io/stocks"

# only keep trades ≥ $1,000,000 for block
MIN_VALUE         = 1_000_000
# only keep trades ≥ $10,000,000 for lit
LIT_MIN_VALUE     = 10_000_000

RAW_SAMPLE_EVERY  = 25_000      # print raw JSON every N batches
WORKER_COUNT      = 4           # number of parallel DB-writer threads
QUEUE_MAXSIZE     = 100_000     # buffer size for in-memory queue

# --- DB CONNECTION ──────────────────────────────────────────────
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
        print(f"  {e}")
        sys.exit(1)

# --- WORKER ─────────────────────────────────────────────────────
def worker(queue: Queue, worker_id: int):
    conn = None
    raw_count = 0
    while True:
        message = queue.get()
        try:
            raw_count += 1
            if raw_count % RAW_SAMPLE_EVERY == 0:
                print(f"\n[RAW #{raw_count} @ worker {worker_id}] {message}\n")
            batch = json.loads(message)
        except Exception as e:
            print(f"[worker {worker_id}] JSON parse error: {e!r}")
            queue.task_done()
            continue

        if conn is None or getattr(conn, 'closed', True):
            conn = get_db()

        for trade in batch:
            if trade.get("ev") != "T":
                continue

            size     = trade.get("s", 0)
            price    = trade.get("p", 0.0)
            value    = size * price
            exchange = trade.get("x")
            trf_id   = trade.get("trfi")
            trf_ts   = trade.get("trft")

            ts_field = trf_ts if trf_ts is not None else trade.get("t")
            if ts_field is None:
                continue
            dt = datetime.fromtimestamp(ts_field / 1000, tz=timezone.utc)

            try:
                with conn.cursor() as cur:
                    # Dark-pool block trades
                    if exchange == 4 and trf_id is not None and value >= MIN_VALUE:
                        cur.execute(
                            """
                            INSERT INTO block_trades
                              (trade_time, ticker, price, quantity, trade_value,
                               conditions, exchange, trf_id, trf_timestamp)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING;
                            """,
                            (
                                dt, trade.get("sym"), price, size, value,
                                trade.get("c", []), exchange, trf_id, trf_ts,
                            )
                        )
                    # ✅ CORRECTED LOGIC: This now perfectly matches your backfill script's logic.
                    # It explicitly excludes dark pool trades AND checks the minimum value.
                    elif not (exchange == 4 and trf_id is not None) and value >= LIT_MIN_VALUE:
                        cur.execute(
                            """
                            INSERT INTO lit_trades
                              (trade_time, ticker, price, quantity, trade_value,
                               conditions, exchange)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING;
                            """,
                            (
                                dt, trade.get("sym"), price, size, value,
                                trade.get("c", []), exchange,
                            )
                        )
                conn.commit()
            except Exception:
                print(f"[worker {worker_id}] DB error on {trade.get('sym')}:")
                traceback.print_exc()
                if conn:
                    conn.rollback()
        queue.task_done()

# --- HANDLER ────────────────────────────────────────────────────
class Handler:
    def __init__(self, api_key: str, queue: Queue):
        if not api_key:
            raise RuntimeError("POLY_KEY is missing")
        self.api_key   = api_key
        self.queue     = queue
        self.raw_count = 0

    def on_open(self, ws):
        ws.send(json.dumps({"action": "auth",      "params": self.api_key}))
        ws.send(json.dumps({"action": "subscribe", "params": "T.*"}))
        print("▶ WS open; subscribed to T.*")

    def on_message(self, ws, message):
        self.raw_count += 1
        self.queue.put(message)

    def on_error(self, ws, error):
        print("‼ WS error:", error)

    def on_close(self, ws, code, msg):
        print(f"⏹ WS closed: code={code} msg={msg}")

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

# --- MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    q = Queue(maxsize=QUEUE_MAXSIZE)
    for i in range(WORKER_COUNT):
        t = Thread(target=worker, args=(q, i+1), daemon=True)
        t.start()
    Handler(POLY_KEY, q).run()
