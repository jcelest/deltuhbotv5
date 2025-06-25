import websocket
import json
import os
import time
import psycopg2
from datetime import datetime

# --- CONFIGURATION ---
DB_NAME = "darkpool_data"
DB_USER = "trader"
DB_PASS = "Deltuhdarkpools!7" # Your password
DB_HOST = "localhost"
DB_PORT = "5432"
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
SOCKET_URL = "wss://delayed.polygon.io/stocks"


class TradeHandler:
    """A class to handle WebSocket events and database interactions."""

    def __init__(self, api_key):
        self._api_key = api_key
        self._db_conn = None

    def _connect_db(self):
        """Establishes and returns a database connection."""
        if self._db_conn is None or self._db_conn.closed:
            print("Connecting to database...")
            self._db_conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
            )
        return self._db_conn

    def _insert_trade(self, trade_data):
        """Inserts a single trade into the block_trades table."""
        trade_value = trade_data.get('s', 0) * trade_data.get('p', 0)
        if trade_data.get('s', 0) < 10000 and trade_value < 200000:
            return

        sql = """
            INSERT INTO block_trades (ticker, trade_time, price, quantity, trade_value, conditions)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        trade_timestamp = datetime.fromtimestamp(trade_data.get('t') / 1000.0)

        try:
            conn = self._connect_db()
            with conn.cursor() as cur:
                cur.execute(sql, (
                    trade_data.get('sym'), trade_timestamp, trade_data.get('p'),
                    trade_data.get('s'), trade_value, trade_data.get('c')
                ))
            conn.commit()
            print(f"SAVED >> Ticker: {trade_data.get('sym')}, Value: ${trade_value:,.2f}")
        except Exception as e:
            print(f"Database insert error: {e}")
            if self._db_conn:
                self._db_conn.rollback()

    # --- WebSocket Event Handler Methods ---

    def on_message(self, ws, message):
        data = json.loads(message)
        for trade in data:
            if trade.get('ev') == 'T':
                self._insert_trade(trade)

    def on_error(self, ws, error):
        print(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket connection closed.")
        if self._db_conn:
            self._db_conn.close()
            print("Database connection closed on exit.")

    def on_open(self, ws):
        print("WebSocket connection opened.")
        auth_data = {"action": "auth", "params": self._api_key}
        ws.send(json.dumps(auth_data))
        
        # Change this line to subscribe to the full feed when you have a paid plan
        sub_data = {"action": "subscribe", "params": "T.SPY,T.AAPL,T.MSFT"}
        ws.send(json.dumps(sub_data))
        print(f"Authenticated and subscribed to: {sub_data['params']}")

    def run(self):
        """Starts the WebSocket connection and runs forever."""
        print("Starting Data Ingestor...")
        ws = websocket.WebSocketApp(
            SOCKET_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        ws.run_forever()


if __name__ == "__main__":
    if not POLYGON_API_KEY:
        raise ValueError("Polygon API Key not found. Set the POLYGON_API_KEY environment variable.")

    # Loop to auto-reconnect if the connection drops
    while True:
        try:
            handler = TradeHandler(api_key=POLYGON_API_KEY)
            handler.run()
        except Exception as e:
            print(f"Main connection loop failed: {e}. Retrying in 10 seconds...")
            time.sleep(10)