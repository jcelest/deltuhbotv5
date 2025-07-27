#!/usr/bin/env python3
import traceback
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# --- CONFIGURATION -----------------------------------------------
DB_NAME               = "darkpool_data"
DB_USER               = "trader"
DB_PASS               = "Deltuhdarkpools!7"
DB_HOST               = "localhost"
DB_PORT               = "5432"
RECENT_TRADES_FOR_PCT = 5000
DEFAULT_MIN_VALUE     = 1_000_000.0
NY_TZ = ZoneInfo("America/New_York")

# --- FASTAPI APP -------------------------------------------------
app = FastAPI(
    title="Dark Pool & Block Trade API",
    description="Serves block trade data from a local PostgreSQL database.",
    version="6.0" # Final, working version
)

# --- Pydantic Model ----------------------------------------------
class Trade(BaseModel):
    ticker: str
    quantity: int
    price: float
    trade_value: float
    trade_time: datetime
    conditions: Optional[List[int]] = Field(default=None)

# --- Connection Pool ---------------------------------------------
db_pool: Optional[pool.SimpleConnectionPool] = None

@app.on_event("startup")
def startup():
    global db_pool
    db_pool = pool.SimpleConnectionPool(
        minconn=1, maxconn=10,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT
    )

@app.on_event("shutdown")
def shutdown():
    if db_pool:
        db_pool.closeall()

def get_db_connection():
    if not db_pool:
        raise RuntimeError("Database connection pool not initialized")
    return db_pool.getconn()

def release_db_connection(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

# --- Reusable Helper for Dynamic Threshold -------------------------
def get_dynamic_threshold(ticker: str, percentile: float, table_name: str) -> float:
    if table_name not in ('block_trades', 'lit_trades'):
        raise ValueError("Invalid table name provided for threshold calculation")
    conn = get_db_connection()
    try:
        sql = f"""
            WITH recent AS (
                SELECT trade_value::float FROM {table_name}
                WHERE ticker = %s
                ORDER BY trade_time DESC
                LIMIT {RECENT_TRADES_FOR_PCT}
            )
            SELECT percentile_cont(%s) WITHIN GROUP (ORDER BY trade_value) FROM recent;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (ticker.upper(), percentile))
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0])
    except Exception:
        traceback.print_exc()
    finally:
        release_db_connection(conn)
    return DEFAULT_MIN_VALUE

# --- API Endpoints --------------------------------------------------
@app.get("/dp/allblocks/{ticker}", response_model=List[Trade], summary="Block trades outside market hours")
def get_all_blocks(ticker: str, percentile: float = Query(0.98, ge=0, le=1)):
    floor = get_dynamic_threshold(ticker, percentile, 'block_trades')
    conn = get_db_connection()
    try:
        # This query is correct and will display NY time
        sql = """
            SELECT ticker, quantity, price::float, trade_value::float, 
                   (trade_time AT TIME ZONE 'America/New_York') as trade_time, 
                   conditions
            FROM block_trades
            WHERE ticker = %s AND trade_value >= %s
              AND (trade_time AT TIME ZONE 'America/New_York')::time NOT BETWEEN '09:30:00' AND '16:00:00'
            ORDER BY trade_time DESC LIMIT 500;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (ticker.upper(), floor))
            return cur.fetchall()
    finally:
        release_db_connection(conn)

@app.get("/dp/alldp/{ticker}", response_model=List[Trade], summary="Block trades during market hours")
def get_all_dark_pool(ticker: str, percentile: float = Query(0.98, ge=0, le=1)):
    floor = get_dynamic_threshold(ticker, percentile, 'block_trades')
    conn = get_db_connection()
    try:
        # This query is correct and will display NY time
        sql = """
            SELECT ticker, quantity, price::float, trade_value::float, 
                   (trade_time AT TIME ZONE 'America/New_York') as trade_time, 
                   conditions
            FROM block_trades
            WHERE ticker = %s AND trade_value >= %s
              AND (trade_time AT TIME ZONE 'America/New_York')::time BETWEEN '09:30:00' AND '16:00:00'
            ORDER BY trade_time DESC LIMIT 500;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (ticker.upper(), floor))
            return cur.fetchall()
    finally:
        release_db_connection(conn)

def big_prints_query(table_name: str, days: int):
    # Calculate timezone-aware start/end times in Python
    now_ny = datetime.now(NY_TZ)
    end_of_today_ny = now_ny.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_date_ny = (now_ny - timedelta(days=days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Convert to UTC for the database query, as all our data is stored in UTC
    start_utc = start_date_ny.astimezone(timezone.utc)
    end_utc = end_of_today_ny.astimezone(timezone.utc)

    conn = get_db_connection()
    try:
        # Use a simple BETWEEN on the indexed trade_time column. This is fast.
        sql = f"""
            SELECT ticker, quantity, price::float, trade_value::float, 
                   (trade_time AT TIME ZONE 'America/New_York') as trade_time, 
                   conditions
            FROM {table_name}
            WHERE trade_time BETWEEN %s AND %s
            ORDER BY trade_value DESC LIMIT 100;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (start_utc, end_utc))
            return cur.fetchall()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"DB error in {table_name} bigprints")
    finally:
        release_db_connection(conn)

@app.get("/dp/bigprints", response_model=List[Trade], summary="Top 100 dark pool prints by value")
def get_dp_big_prints(days: int = Query(1, ge=1, le=30)):
    return big_prints_query('block_trades', days)

@app.get("/lit/all/{ticker}", response_model=List[Trade], summary="All lit-market trades for a ticker")
def get_all_lit(ticker: str, percentile: Optional[float] = Query(None, ge=0, le=1)):
    floor = DEFAULT_MIN_VALUE
    if percentile is not None:
        floor = get_dynamic_threshold(ticker, percentile, 'lit_trades')
    
    conn = get_db_connection()
    try:
        # This query is correct and will display NY time
        sql = """
            SELECT ticker, quantity, price::float, trade_value::float, 
                   (trade_time AT TIME ZONE 'America/New_York') as trade_time, 
                   conditions
            FROM lit_trades
            WHERE ticker = %s AND trade_value >= %s
            ORDER BY trade_time DESC LIMIT 500;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (ticker.upper(), floor))
            return cur.fetchall()
    finally:
        release_db_connection(conn)

@app.get("/lit/bigprints", response_model=List[Trade], summary="Top 100 lit prints by value")
def get_lit_big_prints(days: int = Query(1, ge=1, le=30)):
    return big_prints_query('lit_trades', days)
