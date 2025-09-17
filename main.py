#!/usr/bin/env python3
"""
UNIFIED API - Both lit/dp and SD functionality in one server
ENHANCED VERSION - Unlimited API calls, job segments, correct date handling
INCLUDES: Segmented absorption timeline visualization with supply/demand colors
"""

import os
import asyncio
import uuid
import traceback
import json
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

import httpx
import asyncpg
import psycopg2
import psycopg2.pool
import psycopg2.extras
from psycopg2.extras import RealDictCursor
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── CONFIGURATION ───────────────────────────────────────────────

# Original darkpool database (UNCHANGED)
DARKPOOL_DB_NAME = "darkpool_data"
DARKPOOL_DB_USER = "trader"
DARKPOOL_DB_PASS = "Deltuhdarkpools!7"
DARKPOOL_DB_HOST = "localhost"
DARKPOOL_DB_PORT = "5432"

# Separate SD database
SD_DB_NAME = "supply_demand_data"
SD_DB_USER = "trader"
SD_DB_PASS = "Deltuhdarkpools!7"
SD_DB_HOST = "localhost"
SD_DB_PORT = "5432"

# Other configs
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
RECENT_TRADES_FOR_PCT = 5000
DEFAULT_MIN_VALUE = 1_000_000.0
NY_TZ = ZoneInfo("America/New_York")

# Connection pools - SEPARATE for each database
darkpool_db_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
sd_db_pool: Optional[asyncpg.Pool] = None
sd_sync_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
jobs_db: Dict[str, Dict] = {}  # In-memory job tracking

# ─── HELPER FUNCTIONS ────────────────────────────────────────────

def parse_date_string(date_str: str) -> date:
    """Convert date string to date object"""
    if isinstance(date_str, date):
        return date_str
    if isinstance(date_str, str):
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    raise ValueError(f"Invalid date format: {date_str}")

# ─── MODELS ──────────────────────────────────────────────────────

class BlockTrade(BaseModel):
    ticker: str
    quantity: int
    price: float
    trade_value: float
    trade_time: datetime
    conditions: Optional[List[int]] = None

class Level(BaseModel):
    ticker: str
    level_price: float
    level_type: str  # 'supply' or 'demand'
    level_name: Optional[str] = None

class LinkJobRequest(BaseModel):
    level_id: int

class MarketVolumeResponse(BaseModel):
    total_volume: int
    total_value: float
    total_trades: int
    price_range: str
    data_source: str
    processing_method: Optional[str] = "enhanced"
    api_calls_made: Optional[int] = 0

class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: int = 0
    result: Optional[Dict] = None
    error: Optional[str] = None

class JobSegment(BaseModel):
    job_id: str
    level_id: int
    volume: int
    value: float
    trades: int
    date_start: str
    date_end: str
    created_at: datetime

# ─── ENHANCED JOB PERSISTENCE FUNCTIONS ──────────────────────────

async def save_job_to_db(job_id: str, job_data: Dict):
    """Save job to database with enhanced tracking"""
    if sd_db_pool is None:
        return
        
    async with sd_db_pool.acquire() as conn:
        # Convert result to JSON string if it exists
        result_json = None
        if job_data.get('result'):
            result_json = json.dumps(job_data.get('result'))
        
        # Enhanced job saving with new fields
        await conn.execute(
            """
            INSERT INTO background_jobs 
            (job_id, ticker, level_price, level_id, date_range, status, progress, 
             created_at, result_data, enhancement, api_calls_used, is_absorption)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (job_id) DO UPDATE SET
            status = $6, progress = $7, result_data = $9, enhancement = $10,
            api_calls_used = $11, is_absorption = $12,
            completed_at = CASE WHEN $6 = 'completed' THEN NOW() ELSE background_jobs.completed_at END
            """,
            job_id,
            job_data.get('ticker'),
            job_data.get('level_price'),
            job_data.get('level_id'),
            job_data.get('date_range'),
            job_data.get('status'),
            job_data.get('progress', 0),
            datetime.fromisoformat(job_data.get('created_at', datetime.now().isoformat())),
            result_json,
            job_data.get('enhancement', 'standard'),
            job_data.get('api_calls_used', 0),
            job_data.get('is_absorption', False)
        )

async def load_jobs_from_db():
    """Load existing jobs from database into memory"""
    if sd_db_pool is None:
        return
        
    async with sd_db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM background_jobs ORDER BY created_at DESC LIMIT 100")
        
        for row in rows:
            job_data = {
                'job_id': row['job_id'],
                'ticker': row['ticker'],
                'level_price': float(row['level_price']) if row['level_price'] else None,
                'level_id': row['level_id'],
                'date_range': row['date_range'],
                'status': row['status'],
                'progress': row['progress'],
                'created_at': row['created_at'].isoformat() if row['created_at'] else None,
                'enhancement': row.get('enhancement', 'standard'),
                'api_calls_used': row.get('api_calls_used', 0),
                'is_absorption': row.get('is_absorption', False)
            }
            
            # Parse result_data JSON if it exists
            if row['result_data']:
                try:
                    job_data['result'] = json.loads(row['result_data'])
                except (json.JSONDecodeError, TypeError):
                    job_data['result'] = None
            
            if row['error_message']:
                job_data['error'] = row['error_message']
                
            jobs_db[row['job_id']] = job_data
        
        logger.info(f"Loaded {len(rows)} jobs from database")

async def update_job_in_db(job_id: str):
    """Update job status in database"""
    if job_id in jobs_db:
        await save_job_to_db(job_id, jobs_db[job_id])

# ─── DATABASE CONNECTION POOLS ───────────────────────────────────

async def init_darkpool_db_pool():
    """Initialize darkpool database connection pool (psycopg2)"""
    global darkpool_db_pool
    try:
        darkpool_db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=10,
            dbname=DARKPOOL_DB_NAME,
            user=DARKPOOL_DB_USER,
            password=DARKPOOL_DB_PASS,
            host=DARKPOOL_DB_HOST,
            port=DARKPOOL_DB_PORT
        )
        logger.info(f"Darkpool DB pool initialized: {DARKPOOL_DB_NAME}")
    except Exception as e:
        logger.error(f"Failed to initialize darkpool DB pool: {e}")
        raise

async def init_sd_db_pool():
    """Initialize enhanced SD database connection pool"""
    global sd_db_pool
    try:
        sd_db_pool = await asyncpg.create_pool(
            host=SD_DB_HOST,
            port=SD_DB_PORT,
            user=SD_DB_USER,
            password=SD_DB_PASS,
            database=SD_DB_NAME,
            min_size=5,
            max_size=25  # Increased for enhanced functionality
        )
        logger.info(f"Enhanced SD DB pool initialized: {SD_DB_NAME}")
        
        # Load existing jobs into memory
        await load_jobs_from_db()
        
    except Exception as e:
        logger.error(f"Failed to initialize enhanced SD DB pool: {e}")
        raise

async def init_sd_sync_pool():
    """Initialize SD sync pool for batch operations"""
    global sd_sync_pool
    try:
        sd_sync_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=3, maxconn=12,  # Increased for better performance
            dbname=SD_DB_NAME,
            user=SD_DB_USER,
            password=SD_DB_PASS,
            host=SD_DB_HOST,
            port=SD_DB_PORT
        )
        logger.info(f"Enhanced SD sync pool initialized for batch operations")
    except Exception as e:
        logger.error(f"Failed to initialize SD sync pool: {e}")
        raise

async def close_db_pools():
    """Close all database connection pools"""
    global darkpool_db_pool, sd_db_pool, sd_sync_pool
    
    if darkpool_db_pool:
        darkpool_db_pool.closeall()
        logger.info("Darkpool DB pool closed")
    
    if sd_db_pool:
        await sd_db_pool.close()
        logger.info("Enhanced SD DB pool closed")
    
    if sd_sync_pool:
        sd_sync_pool.closeall()
        logger.info("SD sync pool closed")

def get_darkpool_connection():
    """Get connection from darkpool database pool"""
    if not darkpool_db_pool:
        raise RuntimeError("Darkpool connection pool not initialized")
    return darkpool_db_pool.getconn()

def release_darkpool_connection(conn):
    """Release connection back to darkpool pool"""
    if darkpool_db_pool and conn:
        darkpool_db_pool.putconn(conn)

def get_sd_sync_connection():
    """Get synchronous connection for batch operations"""
    if not sd_sync_pool:
        raise RuntimeError("SD sync connection pool not initialized")
    return sd_sync_pool.getconn()

def release_sd_sync_connection(conn):
    """Release connection back to SD sync pool"""
    if sd_sync_pool and conn:
        sd_sync_pool.putconn(conn)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Enhanced Unified FastAPI server starting up...")
    await init_darkpool_db_pool()
    await init_sd_db_pool()
    await init_sd_sync_pool()
    yield
    # Shutdown
    logger.info("🔄 Enhanced Unified FastAPI server shutting down...")
    await close_db_pools()

# ─── ORIGINAL DARKPOOL FUNCTIONS (UNCHANGED) ─────────────────────

def get_dynamic_threshold(ticker: str, percentile: float, table_name: str) -> float:
    """Get dynamic threshold from darkpool database (UNCHANGED)"""
    if table_name not in ('block_trades', 'lit_trades'):
        raise ValueError("Invalid table name")
    
    conn = get_darkpool_connection()
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
                return row[0]
    finally:
        release_darkpool_connection(conn)
    return DEFAULT_MIN_VALUE

def big_prints_query(table_name: str, days: int, under_400m: bool = False, market_hours_only: bool = False):
    """Big prints query from darkpool database (UNCHANGED)"""
    now_ny = datetime.now(NY_TZ)
    end_of_today_ny = now_ny.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_date_ny = (now_ny - timedelta(days=days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_date_ny.astimezone(timezone.utc)
    end_utc = end_of_today_ny.astimezone(timezone.utc)
    
    additional_filter = ""
    if under_400m:
        additional_filter += " AND trade_value < 400000000"
    
    if market_hours_only:
        additional_filter += " AND (trade_time AT TIME ZONE 'America/New_York')::time BETWEEN '09:30:00' AND '16:00:00'"
    
    conn = get_darkpool_connection()
    try:
        sql = f"""
            SELECT
              ticker, quantity, price::float, trade_value::float,
              (trade_time AT TIME ZONE 'America/New_York') as trade_time,
              conditions
            FROM {table_name}
            WHERE trade_time BETWEEN %s AND %s {additional_filter}
            ORDER BY trade_value DESC LIMIT 300;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (start_utc, end_utc))
            return cur.fetchall()
    finally:
        release_darkpool_connection(conn)

# ─── ENHANCED SD DATABASE FUNCTIONS ─────────────────────────────

async def unlimited_fast_calculate_level_volume(ticker: str, level_price: float, 
                                              start_date: str, end_date: str, 
                                              tolerance: float = 0.025) -> Dict:
    """
    ENHANCED: Unlimited API calls for maximum data coverage
    """
    if not POLYGON_API_KEY:
        raise ValueError("POLYGON_API_KEY not set")
    
    # Convert dates to timestamps
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
    
    start_timestamp = int(start_dt.timestamp() * 1000000000)
    end_timestamp = int(end_dt.timestamp() * 1000000000)
    
    min_price = level_price - tolerance
    max_price = level_price + tolerance
    
    # Build URL
    base_url = (
        f"https://api.polygon.io/v3/trades/{ticker.upper()}"
        f"?timestamp.gte={start_timestamp}&timestamp.lte={end_timestamp}&limit=50000"
    )
    url = f"{base_url}&apiKey={POLYGON_API_KEY}"
    
    # Initialize counters for direct calculation
    total_volume = 0
    total_value = 0.0
    total_trades = 0
    api_calls = 0
    min_actual_price = float('inf')
    max_actual_price = 0.0
    
    logger.info(f"🚀 UNLIMITED calculation for {ticker} at ${level_price:.2f} ±${tolerance:.3f}")
    
    async with httpx.AsyncClient(timeout=200.0) as client:  # Extended timeout
        while url:
            api_calls += 1
            logger.info(f"⚡ UNLIMITED API call {api_calls} for {ticker}")
            
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"API error on call {api_calls}: {e}")
                # Continue with partial data instead of breaking
                await asyncio.sleep(1.0)
                continue
            
            results = data.get("results", [])
            if not results:
                logger.info(f"No more results after {api_calls} calls")
                break
            
            # Process trades in memory
            for trade in results:
                qty = trade.get('size')
                price = trade.get('price')
                
                if not (qty and price):
                    continue
                
                # Apply price filter immediately
                if min_price <= price <= max_price:
                    total_volume += qty
                    total_value += (qty * price)
                    total_trades += 1
                    
                    # Track actual price range
                    min_actual_price = min(min_actual_price, price)
                    max_actual_price = max(max_actual_price, price)
            
            # Check for next page
            next_url = data.get('next_url')
            if next_url:
                url = f"{next_url}&apiKey={POLYGON_API_KEY}"
                await asyncio.sleep(0.03)  # Reduced delay for faster processing
            else:
                logger.info(f"Reached end of data after {api_calls} calls")
                break
            
            # Progress logging every 100 calls
            if api_calls % 100 == 0:
                logger.info(f"📊 UNLIMITED Progress: {api_calls} calls, {total_trades:,} trades found")
    
    # Format price range
    if total_trades > 0 and min_actual_price != float('inf'):
        price_range = f"${min_actual_price:.2f} - ${max_actual_price:.2f}"
    else:
        price_range = f"${level_price:.2f} (no trades found)"
    
    logger.info(f"✅ UNLIMITED calculation complete: {total_trades:,} trades, {total_volume:,} volume, {api_calls} API calls")
    
    return {
        'total_volume': total_volume,
        'total_value': total_value,
        'total_trades': total_trades,
        'price_range': price_range,
        'api_calls_made': api_calls,
        'data_source': 'unlimited_direct_api_calculation',
        'processing_method': 'unlimited_fast_memory_only'
    }

async def create_absorption_job_segment(level_id: int, job_id: str, volume_data: Dict, 
                                       start_date: str, end_date: str) -> None:
    """Create a new absorption job segment for timeline tracking"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    async with sd_db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO absorption_job_segments 
            (job_id, level_id, volume, value, trades, date_start, date_end, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            """,
            job_id, level_id, volume_data['total_volume'], volume_data['total_value'],
            volume_data['total_trades'], parse_date_string(start_date), 
            parse_date_string(end_date)
        )

async def get_absorption_segments_for_level(level_id: int) -> List[Dict]:
    """Get all absorption job segments for a specific level"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    async with sd_db_pool.acquire() as conn:
        segments = await conn.fetch(
            """
            SELECT job_id, volume, value, trades, date_start, date_end, created_at
            FROM absorption_job_segments 
            WHERE level_id = $1 
            ORDER BY date_start ASC
            """,
            level_id
        )
        
        return [
            {
                'job_id': seg['job_id'],
                'volume': seg['volume'],
                'value': float(seg['value']),
                'trades': seg['trades'],
                'date_start': seg['date_start'].isoformat(),
                'date_end': seg['date_end'].isoformat(),
                'created_at': seg['created_at'].isoformat()
            }
            for seg in segments
        ]

async def get_enhanced_sd_levels_for_timeline(ticker: str) -> List[Dict]:
    """Get SD levels with enhanced data including job segments"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    async with sd_db_pool.acquire() as conn:
        levels = await conn.fetch(
            """
            SELECT l.id, l.ticker, l.level_price, l.level_type, l.level_name, 
                   l.date_created, l.is_active,
                   v.original_volume, v.absorbed_volume, v.absorption_percentage,
                   v.original_value, v.absorbed_value, v.last_updated,
                   v.original_date_start, v.original_date_end, v.absorption_start_date
            FROM supply_demand_levels l
            LEFT JOIN level_volume_tracking v ON l.id = v.level_id
            WHERE l.ticker = $1 AND l.is_active = true
            ORDER BY l.level_price DESC
            """,
            ticker.upper()
        )
        
        enhanced_levels = []
        for level in levels:
            original_vol = level['original_volume'] or 0
            absorbed_vol = level['absorbed_volume'] or 0
            absorption_pct = float(level['absorption_percentage'] or 0)
            
            # Enhanced status calculation
            if original_vol > 0 and absorbed_vol > 0:
                status = "Active with Absorption Data"
            elif original_vol > 0:
                status = "Ready for Absorption Analysis"
            else:
                status = "Needs Volume Data"
            
            # Get job segments for this level
            job_segments = await get_absorption_segments_for_level(level['id'])
            
            # FIXED: Use end date for absorption display (correct semantic)
            last_absorption_date = None
            if level['original_date_end']:
                last_absorption_date = level['original_date_end'].isoformat()
            elif job_segments:
                # Use the latest segment end date
                last_absorption_date = max(seg['date_end'] for seg in job_segments)
            
            level_data = {
                'level': {
                    'id': level['id'],
                    'ticker': level['ticker'],
                    'level_price': float(level['level_price']),
                    'level_type': level['level_type'],
                    'level_name': level['level_name'],
                    'date_created': level['date_created'].isoformat() if level['date_created'] else None,
                    'is_active': level['is_active'],
                    'status': status
                },
                'volume': {
                    'original_volume': original_vol,
                    'absorbed_volume': absorbed_vol,
                    'original_value': float(level['original_value'] or 0),
                    'absorbed_value': float(level['absorbed_value'] or 0),
                    'absorption_percentage': absorption_pct,
                    'last_updated': level['last_updated'].isoformat() if level['last_updated'] else None
                },
                'dates': {
                    'original_start': level['original_date_start'].isoformat() if level['original_date_start'] else None,
                    'original_end': level['original_date_end'].isoformat() if level['original_date_end'] else None,
                    'last_absorption_date': last_absorption_date  # FIXED: Shows correct end date
                },
                'job_segments': job_segments
            }
            
            enhanced_levels.append(level_data)
        
        return enhanced_levels

async def check_data_availability(ticker: str, start_date: str, end_date: str) -> Dict:
    """Check if we have data for this ticker/date range in SD database"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    # Convert string dates to date objects
    start_date_obj = parse_date_string(start_date)
    end_date_obj = parse_date_string(end_date)
    
    async with sd_db_pool.acquire() as conn:
        # Check if we have a completed fetch session for this range
        session = await conn.fetchrow(
            """
            SELECT id, status, total_trades_fetched, total_api_calls, completed_at
            FROM fetch_sessions 
            WHERE ticker = $1 AND start_date = $2 AND end_date = $3 
            AND status = 'completed'
            ORDER BY completed_at DESC 
            LIMIT 1
            """,
            ticker.upper(), start_date_obj, end_date_obj
        )
        
        if session:
            # Check actual trade count in cache
            trade_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM market_data_cache 
                WHERE ticker = $1 AND date_range_start = $2 AND date_range_end = $3
                """,
                ticker.upper(), start_date_obj, end_date_obj
            )
            
            return {
                'has_data': True,
                'session_id': session['id'],
                'total_trades': trade_count,
                'api_calls_used': session['total_api_calls'],
                'fetched_at': session['completed_at'].isoformat(),
                'status': 'available'
            }
        
        return {
            'has_data': False,
            'status': 'not_available'
        }

async def calculate_level_volume_from_cache(ticker: str, level_price: float, 
                                          start_date: str, end_date: str, 
                                          tolerance: float = 0.025) -> Dict:
    """Calculate volume at price level from cached data in SD database"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    # Convert string dates to date objects
    start_date_obj = parse_date_string(start_date)
    end_date_obj = parse_date_string(end_date)
    
    min_price = level_price - tolerance
    max_price = level_price + tolerance
    
    async with sd_db_pool.acquire() as conn:
        result = await conn.fetchrow(
            """
            SELECT 
                COUNT(*) as total_trades,
                COALESCE(SUM(quantity), 0) as total_volume,
                COALESCE(SUM(trade_value), 0) as total_value,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM market_data_cache
            WHERE ticker = $1 
            AND date_range_start = $2 
            AND date_range_end = $3
            AND price BETWEEN $4 AND $5
            """,
            ticker.upper(), start_date_obj, end_date_obj, min_price, max_price
        )
        
        total_volume = result['total_volume'] or 0
        total_value = result['total_value'] or 0
        total_trades = result['total_trades'] or 0
        
        if total_trades > 0 and result['min_price'] and result['max_price']:
            price_range = f"${float(result['min_price']):.2f} - ${float(result['max_price']):.2f}"
        else:
            price_range = f"${level_price:.2f} (no trades found)"
        
        return {
            'total_volume': int(total_volume),
            'total_value': float(total_value),
            'total_trades': int(total_trades),
            'price_range': price_range,
            'level_price': level_price,
            'tolerance': tolerance,
            'data_source': 'market_data_cache'
        }

async def create_sd_level(ticker: str, level_price: float, level_type: str, 
                         level_name: Optional[str] = None) -> int:
    """Create SD level in SD database"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    async with sd_db_pool.acquire() as conn:
        level_id = await conn.fetchval(
            """
            INSERT INTO supply_demand_levels 
            (ticker, level_price, level_type, level_name, date_created, is_active)
            VALUES ($1, $2, $3, $4, $5, true)
            RETURNING id
            """,
            ticker.upper(), level_price, level_type, level_name, datetime.now().date()
        )
        return level_id

async def get_sd_levels(ticker: str) -> List[Dict]:
    """Get SD levels from SD database"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    async with sd_db_pool.acquire() as conn:
        levels = await conn.fetch(
            """
            SELECT l.id, l.ticker, l.level_price, l.level_type, l.level_name, 
                   l.date_created, l.is_active,
                   v.original_volume, v.absorbed_volume, v.absorption_percentage,
                   v.last_updated
            FROM supply_demand_levels l
            LEFT JOIN level_volume_tracking v ON l.id = v.level_id
            WHERE l.ticker = $1 AND l.is_active = true
            ORDER BY l.level_price
            """,
            ticker.upper()
        )
        
        result = []
        for level in levels:
            result.append({
                'level': {
                    'id': level['id'],
                    'ticker': level['ticker'],
                    'level_price': float(level['level_price']),
                    'level_type': level['level_type'],
                    'level_name': level['level_name'],
                    'date_created': level['date_created'].isoformat() if level['date_created'] else None,
                    'is_active': level['is_active']
                },
                'absorption': {
                    'original_volume': level['original_volume'] or 0,
                    'absorbed_volume': level['absorbed_volume'] or 0,
                    'absorption_percentage': float(level['absorption_percentage'] or 0),
                    'last_updated': level['last_updated'].isoformat() if level['last_updated'] else None
                }
            })
        
        return result

async def delete_sd_level(level_id: int) -> Dict:
    """Delete SD level and all associated data from SD database"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    async with sd_db_pool.acquire() as conn:
        # Check if level exists and get info
        level_info = await conn.fetchrow(
            "SELECT ticker, level_price, level_type, level_name FROM supply_demand_levels WHERE id = $1",
            level_id
        )
        
        if not level_info:
            raise ValueError(f"Level {level_id} not found")
        
        # Delete absorption segments first
        deleted_segments = await conn.execute(
            "DELETE FROM absorption_job_segments WHERE level_id = $1",
            level_id
        )
        
        # Delete from level_volume_tracking
        deleted_tracking = await conn.execute(
            "DELETE FROM level_volume_tracking WHERE level_id = $1",
            level_id
        )
        
        # Delete the level itself
        deleted_level = await conn.execute(
            "DELETE FROM supply_demand_levels WHERE id = $1",
            level_id
        )
        
        return {
            'level_id': level_id,
            'ticker': level_info['ticker'],
            'level_price': float(level_info['level_price']),
            'level_type': level_info['level_type'],
            'level_name': level_info['level_name'],
            'deleted_tracking_records': deleted_tracking.split()[1] if deleted_tracking else "0",
            'deleted_segments': deleted_segments.split()[1] if deleted_segments else "0",
            'deleted': True
        }

async def delete_job_and_data(job_id: str) -> Dict:
    """Delete background job and associated data"""
    if job_id not in jobs_db:
        raise ValueError(f"Job {job_id} not found")
    
    job_info = jobs_db[job_id].copy()
    
    # Remove from memory
    del jobs_db[job_id]
    
    # Remove from database
    if sd_db_pool is not None:
        async with sd_db_pool.acquire() as conn:
            deleted = await conn.execute(
                "DELETE FROM background_jobs WHERE job_id = $1",
                job_id
            )
    
    return {
        'job_id': job_id,
        'ticker': job_info.get('ticker'),
        'status_before_deletion': job_info.get('status'),
        'deleted': True
    }

async def find_level_by_ticker_and_price(ticker: str, level_price: float, tolerance: float = 0.05) -> Optional[int]:
    """Find existing level by ticker and price (with tolerance)"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    async with sd_db_pool.acquire() as conn:
        level_id = await conn.fetchval(
            """
            SELECT id FROM supply_demand_levels 
            WHERE ticker = $1 
            AND ABS(level_price - $2) <= $3
            AND is_active = true
            ORDER BY ABS(level_price - $2)
            LIMIT 1
            """,
            ticker.upper(), level_price, tolerance
        )
        return level_id

async def update_level_volume_tracking(level_id: int, ticker: str, level_price: float, 
                                     volume_data: Dict, tolerance: float, 
                                     start_date: str, end_date: str, 
                                     is_absorption: bool = False) -> None:
    """Update level volume tracking with calculated data"""
    if sd_db_pool is None:
        raise RuntimeError("SD database pool not initialized")
    
    start_date_obj = parse_date_string(start_date)
    end_date_obj = parse_date_string(end_date)
    
    async with sd_db_pool.acquire() as conn:
        if is_absorption:
            # Get current original volume to calculate absorption percentage
            current_data = await conn.fetchrow(
                "SELECT original_volume FROM level_volume_tracking WHERE level_id = $1",
                level_id
            )
            
            if current_data and current_data['original_volume']:
                original_volume = current_data['original_volume']
                absorbed_volume = volume_data['total_volume']
                absorbed_value = volume_data['total_value']
                
                # Calculate absorption percentage
                if original_volume > 0:
                    absorption_percentage = (absorbed_volume / original_volume) * 100
                else:
                    absorption_percentage = 0.0
                
                # FIXED: Update absorption_start_date to be the END date
                await conn.execute(
                    """
                    UPDATE level_volume_tracking 
                    SET absorbed_volume = $1, absorbed_value = $2, 
                        absorption_percentage = $3, absorption_start_date = $4,
                        last_updated = NOW()
                    WHERE level_id = $5
                    """,
                    absorbed_volume, absorbed_value, absorption_percentage, end_date_obj, level_id
                )
            else:
                raise ValueError(f"Level {level_id} has no original volume data. Set original volume first.")
        else:
            # Standard volume tracking (original volume)
            await conn.execute(
                """
                INSERT INTO level_volume_tracking 
                (level_id, ticker, level_price, price_range_low, price_range_high,
                 original_volume, original_value, absorbed_volume, absorbed_value,
                 absorption_percentage, original_date_start, original_date_end,
                 absorption_start_date, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
                ON CONFLICT (level_id) DO UPDATE SET
                original_volume = $6, original_value = $7, 
                original_date_start = $11, original_date_end = $12,
                last_updated = NOW()
                """,
                level_id, ticker.upper(), level_price,
                level_price - tolerance, level_price + tolerance,
                volume_data['total_volume'], volume_data['total_value'],
                0, 0.0, 0.0,  # Keep existing absorption data
                start_date_obj, end_date_obj, end_date_obj  # Use end_date for absorption_start_date
            )

# ─── ENHANCED BACKGROUND JOB PROCESSING ─────────────────────────

async def enhanced_absorption_job(job_id: str, ticker: str, level_price: float,
                                start_date: str, end_date: str, tolerance: float,
                                level_id: int):
    """Enhanced absorption job with segments and unlimited API calls"""
    try:
        logger.info(f"🔥 Starting ENHANCED absorption job {job_id} for {ticker} at ${level_price:.2f}")
        
        # Update job status
        jobs_db[job_id]['status'] = 'starting_enhanced_absorption'
        jobs_db[job_id]['progress'] = 10
        await update_job_in_db(job_id)
        
        # Use unlimited API calculation
        logger.info(f"🌐 Fetching unlimited data for absorption period")
        jobs_db[job_id]['status'] = 'unlimited_fetch_in_progress'
        jobs_db[job_id]['progress'] = 30
        await update_job_in_db(job_id)
        
        try:
            result = await unlimited_fast_calculate_level_volume(
                ticker, level_price, start_date, end_date, tolerance
            )
            logger.info(f"⚡ UNLIMITED absorption calculation: {result['total_trades']:,} trades, {result['api_calls_made']} API calls")
            
        except Exception as e:
            logger.warning(f"Unlimited method failed for absorption: {e}")
            # Fall back to cache if available
            availability = await check_data_availability(ticker, start_date, end_date)
            if availability['has_data']:
                result = await calculate_level_volume_from_cache(
                    ticker, level_price, start_date, end_date, tolerance
                )
            else:
                raise e
        
        jobs_db[job_id]['status'] = 'updating_level_and_segments'
        jobs_db[job_id]['progress'] = 85
        await update_job_in_db(job_id)
        
        # Update level tracking with absorption data
        await update_level_volume_tracking(
            level_id, ticker, level_price, result, tolerance, 
            start_date, end_date, is_absorption=True
        )
        
        # CREATE ABSORPTION JOB SEGMENT
        await create_absorption_job_segment(
            level_id, job_id, result, start_date, end_date
        )
        
        result['level_id'] = level_id
        result['level_updated'] = True
        result['segment_created'] = True
        result['analysis_type'] = 'absorption'
        result['job_segment_id'] = job_id
        result['absorption_end_date'] = end_date  # Track the correct end date
        
        # Mark as completed
        jobs_db[job_id]['status'] = 'completed'
        jobs_db[job_id]['progress'] = 100
        jobs_db[job_id]['api_calls_used'] = result['api_calls_made']
        jobs_db[job_id]['result'] = {
            'market_data': result,
            'message': f'ENHANCED Absorption analysis completed: {result["total_trades"]:,} trades analyzed with unlimited API calls'
        }
        await update_job_in_db(job_id)
        
        logger.info(f"✅ ENHANCED absorption job {job_id} completed: {result['total_trades']:,} trades, segment created")
        
    except Exception as e:
        logger.error(f"❌ ENHANCED absorption job {job_id} failed: {e}")
        jobs_db[job_id]['status'] = 'failed'
        jobs_db[job_id]['error'] = str(e)
        await update_job_in_db(job_id)

async def enhanced_volume_job(job_id: str, ticker: str, level_price: float,
                            start_date: str, end_date: str, tolerance: float,
                            level_id: Optional[int] = None):
    """Enhanced volume job with unlimited API calls"""
    try:
        logger.info(f"🎯 Starting ENHANCED volume job {job_id} for {ticker} at ${level_price:.2f}")
        
        # Update job status
        jobs_db[job_id]['status'] = 'starting_enhanced_volume'
        jobs_db[job_id]['progress'] = 10
        await update_job_in_db(job_id)
        
        # Use unlimited fast calculation
        jobs_db[job_id]['status'] = 'unlimited_calculation_in_progress'
        jobs_db[job_id]['progress'] = 30
        await update_job_in_db(job_id)
        
        try:
            result = await unlimited_fast_calculate_level_volume(
                ticker, level_price, start_date, end_date, tolerance
            )
            logger.info(f"⚡ UNLIMITED volume calculation: {result['total_trades']:,} trades, {result['api_calls_made']} API calls")
            
        except Exception as e:
            logger.warning(f"Unlimited volume method failed, checking cache: {e}")
            availability = await check_data_availability(ticker, start_date, end_date)
            if availability['has_data']:
                result = await calculate_level_volume_from_cache(
                    ticker, level_price, start_date, end_date, tolerance
                )
            else:
                raise e
        
        # Update level tracking
        target_level_id = level_id
        if not target_level_id:
            target_level_id = await find_level_by_ticker_and_price(ticker, level_price, 0.10)
        
        if target_level_id:
            logger.info(f"🔄 Updating level {target_level_id} with volume data")
            await update_level_volume_tracking(
                target_level_id, ticker, level_price, result, tolerance, 
                start_date, end_date, is_absorption=False
            )
            result['level_id'] = target_level_id
            result['level_updated'] = True
        else:
            logger.warning(f"⚠️ No matching level found for {ticker} ${level_price:.2f}")
            result['level_updated'] = False
        
        result['analysis_type'] = 'volume'
        
        # Mark as completed
        jobs_db[job_id]['status'] = 'completed'
        jobs_db[job_id]['progress'] = 100
        jobs_db[job_id]['api_calls_used'] = result['api_calls_made']
        jobs_db[job_id]['result'] = {
            'market_data': result,
            'message': f'ENHANCED Volume calculation completed: {result["total_trades"]:,} trades analyzed with unlimited API calls'
        }
        await update_job_in_db(job_id)
        
        logger.info(f"✅ ENHANCED volume job {job_id} completed: {result['total_trades']:,} trades")
        
    except Exception as e:
        logger.error(f"❌ ENHANCED volume job {job_id} failed: {e}")
        jobs_db[job_id]['status'] = 'failed'
        jobs_db[job_id]['error'] = str(e)
        await update_job_in_db(job_id)

# ─── FASTAPI APP CREATION ────────────────────────────────────────

app = FastAPI(
    title="ENHANCED Unified Dark Pool & SD API",
    description="Enhanced with unlimited API calls, job segments, and segmented timeline visualization",
    version="20.0.0-ENHANCED",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── API ENDPOINTS ───────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "message": "ENHANCED Unified API with Segmented Timeline Visualization",
        "version": "20.0.0-ENHANCED",
        "enhancements": [
            "Unlimited Polygon API calls for complete data coverage",
            "Job segments for detailed absorption timeline tracking",
            "Supply/demand color differentiation in visualizations",
            "Correct end date display for absorption periods",
            "Enhanced database schema with job segments"
        ],
        "databases": {
            "darkpool_data": "Original lit/dp data (UNCHANGED)",
            "supply_demand_data": "Enhanced SD analysis with segments"
        }
    }

@app.get("/health")
async def health_check():
    return {
        "status": "OK", 
        "darkpool_db": DARKPOOL_DB_NAME,
        "sd_db": SD_DB_NAME,
        "unified": True,
        "enhanced": True,
        "features": [
            "unlimited_api_calls",
            "job_segments",
            "segmented_timeline_visualization",
            "supply_demand_color_coding",
            "correct_date_semantics"
        ],
        "version": "20.0.0-ENHANCED"
    }

# ─── ORIGINAL DARKPOOL ENDPOINTS (UNCHANGED) ─────────────────────

@app.get("/dp/allblocks/{ticker}", response_model=List[BlockTrade], summary="Block trades outside NYSE hours")
def get_all_blocks(ticker: str, percentile: float = Query(0.98, ge=0, le=1)):
    floor = get_dynamic_threshold(ticker, percentile, 'block_trades')
    conn = get_darkpool_connection()
    try:
        sql = """
            SELECT
              ticker, quantity, price::float, trade_value::float,
              (trade_time AT TIME ZONE 'America/New_York') AS trade_time,
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
        release_darkpool_connection(conn)

@app.get("/dp/alldp/{ticker}", response_model=List[BlockTrade], summary="Block trades during market hours")
def get_all_dark_pool(ticker: str, percentile: float = Query(0.98, ge=0, le=1)):
    floor = get_dynamic_threshold(ticker, percentile, 'block_trades')
    conn = get_darkpool_connection()
    try:
        sql = """
            SELECT
              ticker, quantity, price::float, trade_value::float,
              (trade_time AT TIME ZONE 'America/New_York') AS trade_time,
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
        release_darkpool_connection(conn)

@app.get("/dp/bigprints", response_model=List[BlockTrade], summary="Top block trades by value")
def get_dp_big_prints(
    days: int = Query(1, ge=1, le=30),
    market_hours_only: bool = Query(False)
):
    return big_prints_query('block_trades', days, market_hours_only=market_hours_only)

@app.get("/lit/all/{ticker}", response_model=List[BlockTrade], summary="All lit-market trades for a ticker")
def get_all_lit(ticker: str, percentile: Optional[float] = Query(None, ge=0, le=1)):
    floor = DEFAULT_MIN_VALUE
    if percentile is not None:
        floor = get_dynamic_threshold(ticker, percentile, 'lit_trades')
    conn = get_darkpool_connection()
    try:
        sql = """
            SELECT
              ticker, quantity, price::float, trade_value::float,
              (trade_time AT TIME ZONE 'America/New_York') AS trade_time,
              conditions
            FROM lit_trades
            WHERE ticker = %s AND trade_value >= %s
            ORDER BY trade_time DESC LIMIT 500;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (ticker.upper(), floor))
            return cur.fetchall()
    finally:
        release_darkpool_connection(conn)

@app.get("/lit/bigprints", response_model=List[BlockTrade], summary="Top lit trades by value")
def get_lit_big_prints(
    days: int = Query(1, ge=1, le=30), 
    under_400m: bool = False,
    market_hours_only: bool = Query(False)
):
    return big_prints_query('lit_trades', days, under_400m, market_hours_only)

# ─── ENHANCED SD ENDPOINTS ────────────────────────────────────────

@app.post("/levels/create")
async def create_level(level: Level):
    """Create a new supply/demand level in SD database"""
    try:
        level_id = await create_sd_level(
            level.ticker, level.level_price, level.level_type, level.level_name
        )
        
        return {
            'message': 'Enhanced level created successfully',
            'level_id': level_id,
            'ticker': level.ticker.upper(),
            'level_price': level.level_price,
            'level_type': level.level_type,
            'features': ['unlimited_api_support', 'job_segments', 'timeline_visualization']
        }
        
    except Exception as e:
        if "unique" in str(e).lower():
            raise HTTPException(
                status_code=400, 
                detail=f"Level at ${level.level_price} already exists for {level.ticker.upper()}"
            )
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/levels/{ticker}")
async def get_levels(ticker: str):
    """Get all levels for a ticker from SD database"""
    try:
        levels = await get_sd_levels(ticker)
        return levels
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/levels/{ticker}/enhanced-timeline")
async def get_enhanced_levels_for_timeline(ticker: str):
    """Get SD levels with enhanced data including job segments for timeline visualization"""
    try:
        levels = await get_enhanced_sd_levels_for_timeline(ticker)
        
        return {
            'ticker': ticker.upper(),
            'levels': levels,
            'level_count': len(levels),
            'supply_count': len([l for l in levels if l['level']['level_type'] == 'supply']),
            'demand_count': len([l for l in levels if l['level']['level_type'] == 'demand']),
            'enhanced_features': [
                'Job segments for absorption tracking',
                'Correct end date display',
                'Unlimited API call support',
                'Supply/demand color schemes'
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Keep the existing timeline endpoint for backward compatibility
@app.get("/levels/{ticker}/timeline")
async def get_levels_for_timeline_legacy(ticker: str):
    """Legacy timeline endpoint - redirects to enhanced version"""
    return await get_enhanced_levels_for_timeline(ticker)

@app.delete("/levels/{level_id}")
async def delete_level(level_id: int):
    """Delete a supply/demand level and all associated data"""
    try:
        result = await delete_sd_level(level_id)
        return {
            'message': f'Enhanced level {level_id} deleted successfully',
            'deleted_level': result
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete enhanced level: {str(e)}")

@app.put("/levels/{level_id}/deactivate")
async def deactivate_level(level_id: int):
    """Deactivate a level (soft delete - keeps data but hides from lists)"""
    if sd_db_pool is None:
        raise HTTPException(status_code=500, detail="SD database pool not initialized")
    
    try:
        async with sd_db_pool.acquire() as conn:
            level_info = await conn.fetchrow(
                "SELECT ticker, level_price, level_type FROM supply_demand_levels WHERE id = $1",
                level_id
            )
            
            if not level_info:
                raise HTTPException(status_code=404, detail=f"Level {level_id} not found")
            
            await conn.execute(
                "UPDATE supply_demand_levels SET is_active = false WHERE id = $1",
                level_id
            )
            
            return {
                'message': f'Enhanced level {level_id} deactivated successfully',
                'level_id': level_id,
                'ticker': level_info['ticker'],
                'level_price': float(level_info['level_price']),
                'status': 'deactivated'
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to deactivate level: {str(e)}")


@app.get("/market-volume-job-enhanced/{ticker}")
async def start_enhanced_market_volume_job(
    background_tasks: BackgroundTasks,
    ticker: str,
    level_price: float = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    price_tolerance: float = Query(0.025),
    level_id: Optional[int] = Query(None, description="Level ID to update (required for absorption)"),
    is_absorption: bool = Query(False, description="Whether this is absorption analysis")
):
    """Enhanced market volume job with unlimited API calls and job segments"""
    
    # Validate that level_id is provided for absorption analysis
    if is_absorption and level_id is None:
        raise HTTPException(
            status_code=400, 
            detail="level_id is required for absorption analysis. Please provide a valid level_id."
        )
    
    job_id = str(uuid.uuid4())
    
    # Check data availability for estimates
    availability = await check_data_availability(ticker, start_date, end_date)
    
    # Enhanced time estimates for unlimited API calls
    if not is_absorption:
        estimated_time = "30 seconds - 10 minutes (unlimited API calls for maximum coverage)"
        complexity = "enhanced_unlimited_volume"
    else:
        estimated_time = "30 seconds - 10 minutes (unlimited absorption analysis with segment creation)"
        complexity = "enhanced_unlimited_absorption"
    
    analysis_type = "absorption" if is_absorption else "volume"
    
    # Initialize enhanced job status
    jobs_db[job_id] = {
        'job_id': job_id,
        'status': 'starting_enhanced',
        'progress': 0,
        'created_at': datetime.now().isoformat(),
        'ticker': ticker.upper(),
        'level_price': level_price,
        'level_id': level_id,
        'date_range': f"{start_date} to {end_date}",
        'data_availability': availability,
        'estimated_time': estimated_time,
        'analysis_type': analysis_type,
        'is_absorption': is_absorption,
        'enhancement': 'unlimited_api_calls_with_segments',
        'api_calls_used': 0
    }
    
    # Save job to database
    await save_job_to_db(job_id, jobs_db[job_id])
    
    # Start enhanced background task
    if is_absorption:
        # At this point, level_id is guaranteed to be not None due to validation above
        background_tasks.add_task(
            enhanced_absorption_job, 
            job_id, ticker, level_price, start_date, end_date, price_tolerance, level_id  # type: ignore
        )
    else:
        background_tasks.add_task(
            enhanced_volume_job, 
            job_id, ticker, level_price, start_date, end_date, price_tolerance, level_id
        )
    
    return {
        'job_id': job_id,
        'status': 'started_enhanced',
        'estimated_time': estimated_time,
        'complexity': complexity,
        'data_availability': availability,
        'analysis_type': analysis_type,
        'enhancement': 'Unlimited Polygon API calls + Job segments + Correct date handling + Supply/demand visualization'
    }

@app.get("/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """Get status of an enhanced background job"""
    if job_id not in jobs_db:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return jobs_db[job_id]

@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a background job"""
    try:
        result = await delete_job_and_data(job_id)
        return {
            'message': f'Enhanced job {job_id} deleted successfully',
            'deleted_job': result
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")

@app.post("/jobs/{job_id}/link-to-level")
async def link_job_to_level(job_id: str, request: LinkJobRequest):
    """Link a completed job to a level (for retroactive updates)"""
    if job_id not in jobs_db:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs_db[job_id]
    if job['status'] != 'completed':
        raise HTTPException(status_code=400, detail="Job must be completed to link to level")
    
    # Get the volume data from the completed job
    result = job['result']['market_data']
    is_absorption = job.get('is_absorption', False)
    
    # Update level tracking
    await update_level_volume_tracking(
        request.level_id, job['ticker'], job['level_price'], result, 
        0.025, job['date_range'].split(' to ')[0], job['date_range'].split(' to ')[1],
        is_absorption
    )
    
    analysis_type = "absorption" if is_absorption else "volume"
    
    return {
        'message': f'Enhanced job {job_id} successfully linked to level {request.level_id} as {analysis_type} data',
        'level_id': request.level_id,
        'volume_data': result,
        'analysis_type': analysis_type,
        'enhancement': 'Processed with unlimited API calls and enhanced features'
    }

@app.get("/data-availability/{ticker}")
async def check_ticker_data_availability(
    ticker: str,
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    """Check what data is available for a ticker/date range"""
    availability = await check_data_availability(ticker, start_date, end_date)
    return availability

@app.get("/db/status")
async def get_database_status():
    """Get status of both databases"""
    status = {
        'darkpool_db': {
            'name': DARKPOOL_DB_NAME,
            'connected': darkpool_db_pool is not None,
            'type': 'psycopg2'
        },
        'sd_db': {
            'name': SD_DB_NAME,
            'connected': sd_db_pool is not None,
            'type': 'asyncpg_enhanced'
        },
        'sd_sync_db': {
            'name': SD_DB_NAME + '_sync',
            'connected': sd_sync_pool is not None,
            'type': 'psycopg2_batch_enhanced'
        },
        'enhancements': [
            'Unlimited API call support',
            'Job segments for timeline tracking',
            'Supply/demand color differentiation',
            'Correct absorption date semantics',
            'Enhanced timeline visualization',
            'Improved connection pooling'
        ]
    }
    
    # Get darkpool stats
    if darkpool_db_pool:
        try:
            conn = get_darkpool_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM block_trades")
                    block_count = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM lit_trades")
                    lit_count = cur.fetchone()[0]
                    
                status['darkpool_db']['stats'] = {
                    'block_trades': block_count,
                    'lit_trades': lit_count
                }
            finally:
                release_darkpool_connection(conn)
        except Exception as e:
            status['darkpool_db']['error'] = str(e)
    
    # Get enhanced SD stats
    if sd_db_pool:
        try:
            async with sd_db_pool.acquire() as conn:
                level_count = await conn.fetchval("SELECT COUNT(*) FROM supply_demand_levels WHERE is_active = true")
                cache_count = await conn.fetchval("SELECT COUNT(*) FROM market_data_cache")
                job_count = await conn.fetchval("SELECT COUNT(*) FROM background_jobs")
                segment_count = await conn.fetchval("SELECT COUNT(*) FROM absorption_job_segments")
                
                status['sd_db']['stats'] = {
                    'active_levels': level_count,
                    'cached_trades': cache_count,
                    'tracked_jobs': job_count,
                    'absorption_segments': segment_count
                }
        except Exception as e:
            status['sd_db']['error'] = str(e)
    
    return status

# ─── SERVER STARTUP ──────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8001,
        reload=True,
        log_level="info"
    )