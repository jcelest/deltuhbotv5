#!/usr/bin/env python3
import traceback
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# ─── CONFIGURATION ───────────────────────────────────────────────
DB_NAME               = "darkpool_data"
DB_USER               = "trader"
DB_PASS               = "Deltuhdarkpools!7"
DB_HOST               = "localhost"
DB_PORT               = "5432"

# If we can’t compute a percentile (too few trades), fall back here
DEFAULT_MIN_VALUE     = 1_000_000.0
# How many of the most-recent trades to sample when computing percentile
RECENT_TRADES_FOR_PCT = 5000

# ─── FASTAPI APP ─────────────────────────────────────────────────
app = FastAPI(
    title="Dark Pool & Block Trade API",
    description="Serves live‐updated block trade data (dark‐pool only) from local DB",
    version="1.4"
)

# ─── Pydantic model ──────────────────────────────────────────────
class BlockTrade(BaseModel):
    ticker:       str
    quantity:     int
    price:        float
    trade_value:  float
    trade_time:   datetime
    conditions:   Optional[List[int]] = None

# ─── CONNECTION POOL ─────────────────────────────────────────────
_db_pool: Optional[pool.SimpleConnectionPool] = None

@app.on_event("startup")
def startup():
    global _db_pool
    _db_pool = pool.SimpleConnectionPool(
        minconn=1, maxconn=10,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT
    )

@app.on_event("shutdown")
def shutdown():
    global _db_pool
    if _db_pool:
        _db_pool.closeall()


def get_db_connection():
    if not _db_pool:
        raise RuntimeError("Connection pool not initialized")
    return _db_pool.getconn()

def release_db_connection(conn):
    if _db_pool and conn:
        _db_pool.putconn(conn)

# ─── DYNAMIC FLOOR HELPER ────────────────────────────────────────
def get_dynamic_threshold(ticker: str, percentile: float) -> float:
    """
    Compute the `percentile` (0–1) floor of trade_value
    over the most recent RECENT_TRADES_FOR_PCT trades for `ticker`.
    Falls back to DEFAULT_MIN_VALUE if not enough data or on error.
    """
    conn = get_db_connection()
    try:
        sql = f"""
            WITH recent AS (
              SELECT trade_value::float
                FROM block_trades
               WHERE ticker   = %s
                 AND exchange = 4
                 AND trf_id IS NOT NULL
               ORDER BY trade_time DESC
               LIMIT {RECENT_TRADES_FOR_PCT}
            )
            SELECT
              percentile_cont(%s) WITHIN GROUP (ORDER BY trade_value)
            FROM recent;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (ticker.upper(), percentile))
            row = cur.fetchone()
            if row and row[0] is not None:
                return row[0]
    except Exception:
        traceback.print_exc()
    finally:
        release_db_connection(conn)

    return DEFAULT_MIN_VALUE

# ─── allblocks: trades BEFORE 09:30 ET or AFTER 16:00 ET ─────────
@app.get(
    "/dp/allblocks/{ticker}",
    response_model=List[BlockTrade],
    summary="Block trades outside NYSE hours"
)
def get_all_blocks(
    ticker: str,
    percentile: float = Query(
        0.99,
        title="Lower percentile floor",
        description="Compute the Xᵗʰ percentile (0–1) of recent trade_value to use as minimum",
        ge=0, le=1
    )
):
    floor = get_dynamic_threshold(ticker, percentile)
    conn  = get_db_connection()
    try:
        sql = """
            SELECT
              ticker,
              quantity,
              price::float       AS price,
              trade_value::float AS trade_value,
              (trade_time AT TIME ZONE 'America/New_York') AS trade_time,
              conditions
            FROM block_trades
            WHERE ticker       = %s
              AND exchange     = 4
              AND trf_id     IS NOT NULL
              AND trade_value  >= %s
              AND (
                    (trade_time AT TIME ZONE 'America/New_York')::time <  '09:30:00'
                 OR (trade_time AT TIME ZONE 'America/New_York')::time >  '16:00:00'
                  )
            ORDER BY trade_time DESC
            LIMIT 500;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (ticker.upper(), floor))
            return cur.fetchall()
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="DB error in allblocks")
    finally:
        release_db_connection(conn)

# ─── alldp: trades BETWEEN 09:30–16:00 ET ────────────────────────
@app.get(
    "/dp/alldp/{ticker}",
    response_model=List[BlockTrade],
    summary="Block trades during NYSE hours"
)
def get_all_dark_pool(
    ticker: str,
    percentile: float = Query(
        0.99,
        title="Lower percentile floor",
        description="Compute the Xᵗʰ percentile (0–1) of recent trade_value to use as minimum",
        ge=0, le=1
    )
):
    floor = get_dynamic_threshold(ticker, percentile)
    conn  = get_db_connection()
    try:
        sql = """
            SELECT
              ticker,
              quantity,
              price::float       AS price,
              trade_value::float AS trade_value,
              (trade_time AT TIME ZONE 'America/New_York') AS trade_time,
              conditions
            FROM block_trades
            WHERE ticker       = %s
              AND exchange     = 4
              AND trf_id     IS NOT NULL
              AND trade_value  >= %s
              AND (trade_time AT TIME ZONE 'America/New_York')::time
                    BETWEEN '09:30:00' AND '16:00:00'
            ORDER BY trade_time DESC
            LIMIT 500;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (ticker.upper(), floor))
            return cur.fetchall()
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="DB error in alldp")
    finally:
        release_db_connection(conn)

# ─── bigprints: TOP-100 LAST X DAYS BY ET DATE ───────────────────────────
@app.get(
    "/dp/bigprints",
    response_model=List[BlockTrade],
    summary="Top 100 block trades by value over the last X days"
)
def get_big_prints(
    days: int = Query(
        1,
        title="Days to look back",
        description="Number of days to include in big prints (1–30)",
        ge=1, le=30
    )
):
    conn = get_db_connection()
    try:
        sql = """
            SELECT
              ticker,
              quantity,
              price::float       AS price,
              trade_value::float AS trade_value,
              (trade_time AT TIME ZONE 'America/New_York') AS trade_time,
              conditions
            FROM block_trades
            WHERE exchange      = 4
              AND trf_id     IS NOT NULL
              AND (trade_time AT TIME ZONE 'America/New_York')::date
                    >= (now() AT TIME ZONE 'America/New_York')::date - %s
            ORDER BY trade_value DESC
            LIMIT 100;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (days,))
            return cur.fetchall()
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="DB error in bigprints")
    finally:
        release_db_connection(conn)
