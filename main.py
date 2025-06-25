from fastapi import FastAPI, HTTPException
from datetime import datetime, time
import psycopg2
from psycopg2.extras import RealDictCursor # This is useful for getting JSON-like results

# --- Initialize the FastAPI app ---
app = FastAPI(
    title="Dark Pool & Block Trade API",
    description="An API to serve filtered dark pool and block trade data.",
    version="1.0.0"
)

# --- DATABASE CONNECTION DETAILS (from your ingestor.py) ---
DB_NAME = "darkpool_data"
DB_USER = "trader"
DB_PASS = "Deltuhdarkpools!7" # Use the same password
DB_HOST = "localhost"
DB_PORT = "5432"

def get_db_connection():
    """Establishes a connection to the database."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

# --- ABNORMAL TRADE LOGIC ---
# This is where we define the "abnormal" value for different stocks
def get_abnormal_threshold(ticker: str) -> int:
    """Returns the abnormal trade value threshold for a given ticker."""
    ticker = ticker.upper()
    if ticker == "SPY":
        return 700_000_000
    if ticker == "QQQ":
        return 400_000_000
    if ticker in ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]:
        return 250_000_000
    # Default threshold for all other stocks
    return 20_000_000


# --- API ENDPOINTS ---

@app.get("/")
def read_root():
    return {"status": "API is running. Welcome to the Whale Hunter API."}


@app.get("/dp/allblocks/{ticker}")
def get_all_blocks(ticker: str):
    """
    Endpoint for the /dp allblocks command.
    Gets trades that are AFTER-HOURS or ABNORMALLY LARGE mid-day.
    """
    conn = get_db_connection()
    abnormal_threshold = get_abnormal_threshold(ticker)
    
    # This SQL query implements our special logic
    sql_query = """
        SELECT ticker, quantity, price, trade_value, trade_time
        FROM block_trades
        WHERE ticker = %s AND (
            trade_time::time NOT BETWEEN '09:30:00' AND '16:00:00' OR
            (trade_time::time BETWEEN '09:30:00' AND '16:00:00' AND trade_value >= %s)
        )
        ORDER BY trade_time DESC
        LIMIT 15;
    """
    try:
        # RealDictCursor returns each row as a dictionary, perfect for APIs
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql_query, (ticker.upper(), abnormal_threshold))
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/dp/alldp/{ticker}")
def get_all_dark_pool(ticker: str):
    """
    Endpoint for the /dp alldp command.
    Gets all significant trades that are DURING market hours.
    """
    conn = get_db_connection()
    sql_query = """
        SELECT ticker, quantity, price, trade_value, trade_time
        FROM block_trades
        WHERE ticker = %s AND trade_time::time BETWEEN '09:30:00' AND '16:00:00'
        ORDER BY trade_time DESC
        LIMIT 15;
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql_query, (ticker.upper(),))
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/dp/bigprints")
def get_big_prints():
    """
    Endpoint for the /dp bigprints command.
    Gets the largest trades across the entire market for the current day.
    """
    conn = get_db_connection()
    # Note: CURRENT_DATE works based on the server's date.
    sql_query = """
        SELECT ticker, quantity, price, trade_value, trade_time
        FROM block_trades
        WHERE trade_time >= CURRENT_DATE AND trade_value >= 10000000
        ORDER BY trade_value DESC
        LIMIT 20;
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql_query)
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()