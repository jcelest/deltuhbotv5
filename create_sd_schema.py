#!/usr/bin/env python3
"""
Create Supply/Demand Database Schema - COMPLETELY SEPARATE
This creates a new database: supply_demand_data
Your original darkpool_data database remains untouched
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Database connection details
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "trader"
DB_PASS = "Deltuhdarkpools!7"

# SEPARATE database name for SD (completely separate)
SD_DB_NAME = "supply_demand_data"

def create_sd_database():
    """Create separate database for supply/demand analysis"""
    # Connect to postgres database to create new database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT, 
        user=DB_USER,
        password=DB_PASS,
        database="postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    cur = conn.cursor()
    
    # Check if database exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (SD_DB_NAME,))
    exists = cur.fetchone()
    
    if not exists:
        print(f"Creating database: {SD_DB_NAME}")
        cur.execute(f"CREATE DATABASE {SD_DB_NAME}")
        print("✅ Database created successfully")
    else:
        print(f"📁 Database {SD_DB_NAME} already exists")
    
    cur.close()
    conn.close()

def create_sd_tables():
    """Create all tables for SD analysis in separate database"""
    
    # Connect to the new SD database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=SD_DB_NAME
    )
    
    cur = conn.cursor()
    
    # Supply/Demand Levels Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS supply_demand_levels (
            id BIGSERIAL PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            level_price DECIMAL(12,4) NOT NULL,
            level_type VARCHAR(10) NOT NULL CHECK (level_type IN ('supply', 'demand')),
            level_name VARCHAR(100),
            date_created DATE NOT NULL DEFAULT CURRENT_DATE,
            created_by VARCHAR(50),
            notes TEXT,
            is_active BOOLEAN DEFAULT true,
            UNIQUE(ticker, level_price)
        )
    """)
    print("✅ Created supply_demand_levels table")
    
    # Level Volume Tracking Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS level_volume_tracking (
            id BIGSERIAL PRIMARY KEY,
            level_id BIGINT REFERENCES supply_demand_levels(id) ON DELETE CASCADE,
            ticker VARCHAR(20) NOT NULL,
            level_price DECIMAL(12,4) NOT NULL,
            price_range_low DECIMAL(12,4) NOT NULL,
            price_range_high DECIMAL(12,4) NOT NULL,
            original_volume BIGINT DEFAULT 0,
            original_value DECIMAL(20,4) DEFAULT 0,
            absorbed_volume BIGINT DEFAULT 0,
            absorbed_value DECIMAL(20,4) DEFAULT 0,
            absorption_percentage DECIMAL(5,2) DEFAULT 0,
            original_date_start DATE,
            original_date_end DATE,
            absorption_start_date DATE,
            last_updated TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(level_id)
        )
    """)
    print("✅ Created level_volume_tracking table")
    
    # Market Data Cache Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_data_cache (
            id BIGSERIAL PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            trade_time TIMESTAMPTZ NOT NULL,
            price DECIMAL(12,4) NOT NULL,
            quantity BIGINT NOT NULL,
            trade_value DECIMAL(20,4) NOT NULL,
            conditions INTEGER[],
            exchange INTEGER,
            participant_timestamp BIGINT,
            -- Metadata for SD analysis
            date_range_start DATE NOT NULL,
            date_range_end DATE NOT NULL,
            fetch_session_id UUID NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    print("✅ Created market_data_cache table")
    
    # Data Fetch Sessions Tracking
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fetch_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            ticker VARCHAR(20) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            total_trades_fetched BIGINT DEFAULT 0,
            total_api_calls INTEGER DEFAULT 0,
            status VARCHAR(20) DEFAULT 'processing' CHECK (status IN ('processing', 'completed', 'failed')),
            error_message TEXT,
            started_at TIMESTAMPTZ DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            UNIQUE(ticker, start_date, end_date)
        )
    """)
    print("✅ Created fetch_sessions table")
    
    # Daily Level Volume Snapshots
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_level_volume (
            id BIGSERIAL PRIMARY KEY,
            level_id BIGINT REFERENCES supply_demand_levels(id) ON DELETE CASCADE,
            ticker VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            daily_volume BIGINT DEFAULT 0,
            daily_value DECIMAL(20,4) DEFAULT 0,
            daily_trade_count INTEGER DEFAULT 0,
            cumulative_volume BIGINT DEFAULT 0,
            last_updated TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(level_id, trade_date)
        )
    """)
    print("✅ Created daily_level_volume table")
    
    # Create indexes for performance
    indexes = [
        # SD Levels indexes
        ("CREATE INDEX IF NOT EXISTS idx_sd_levels_ticker ON supply_demand_levels(ticker)", "SD levels ticker"),
        ("CREATE INDEX IF NOT EXISTS idx_sd_levels_active ON supply_demand_levels(ticker, is_active)", "SD levels active"),
        ("CREATE INDEX IF NOT EXISTS idx_sd_levels_price ON supply_demand_levels(ticker, level_price)", "SD levels price"),
        
        # Volume tracking indexes
        ("CREATE INDEX IF NOT EXISTS idx_volume_tracking_level ON level_volume_tracking(level_id)", "Volume tracking level"),
        ("CREATE INDEX IF NOT EXISTS idx_volume_tracking_ticker ON level_volume_tracking(ticker)", "Volume tracking ticker"),
        
        # Market data cache indexes
        ("CREATE INDEX IF NOT EXISTS idx_market_data_ticker_time ON market_data_cache(ticker, trade_time)", "Market data ticker/time"),
        ("CREATE INDEX IF NOT EXISTS idx_market_data_price_range ON market_data_cache(ticker, price, date_range_start, date_range_end)", "Market data price range"),
        ("CREATE INDEX IF NOT EXISTS idx_market_data_session ON market_data_cache(fetch_session_id)", "Market data session"),
        ("CREATE INDEX IF NOT EXISTS idx_market_data_ticker_date_range ON market_data_cache(ticker, date_range_start, date_range_end)", "Market data ticker/date"),
        
        # Fetch sessions indexes
        ("CREATE INDEX IF NOT EXISTS idx_fetch_sessions_ticker_dates ON fetch_sessions(ticker, start_date, end_date)", "Fetch sessions ticker/dates"),
        ("CREATE INDEX IF NOT EXISTS idx_fetch_sessions_status ON fetch_sessions(status)", "Fetch sessions status"),
        
        # Daily volume indexes
        ("CREATE INDEX IF NOT EXISTS idx_daily_volume_level_date ON daily_level_volume(level_id, trade_date)", "Daily volume level/date"),
        ("CREATE INDEX IF NOT EXISTS idx_daily_volume_ticker_date ON daily_level_volume(ticker, trade_date)", "Daily volume ticker/date")
    ]
    
    for index_sql, description in indexes:
        cur.execute(index_sql)
        print(f"✅ Created index: {description}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("✅ All SD tables and indexes created successfully")

def cleanup_old_tables():
    """Remove any SD tables from original darkpool_data database if they exist"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database="darkpool_data"  # Original database
        )
        
        cur = conn.cursor()
        
        # Check and remove SD tables that might have been created there
        sd_tables = [
            'market_data_cache',
            'fetch_sessions', 
            'level_volume_tracking',
            'supply_demand_levels',
            'daily_level_volume'
        ]
        
        removed_tables = []
        for table in sd_tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table,))
            
            result = cur.fetchone()
            if result and result[0]:
                print(f"🧹 Removing {table} from darkpool_data...")
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                removed_tables.append(table)
        
        if removed_tables:
            conn.commit()
            print(f"✅ Cleaned up {len(removed_tables)} SD tables from darkpool_data")
        else:
            print("✅ No SD tables found in darkpool_data - already clean")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"⚠️  Could not clean darkpool_data: {e}")
        print("   This is OK if the tables don't exist there")

def test_connection():
    """Test connection to both databases"""
    try:
        # Test darkpool_data connection
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database="darkpool_data"
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM block_trades")
        result = cur.fetchone()
        block_count = result[0] if result else 0
        cur.close()
        conn.close()
        print(f"✅ darkpool_data connection OK - {block_count} block trades")
    except Exception as e:
        print(f"❌ darkpool_data connection failed: {e}")
    
    try:
        # Test supply_demand_data connection
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=SD_DB_NAME
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM supply_demand_levels")
        result = cur.fetchone()
        level_count = result[0] if result else 0
        cur.close()
        conn.close()
        print(f"✅ {SD_DB_NAME} connection OK - {level_count} SD levels")
    except Exception as e:
        print(f"❌ {SD_DB_NAME} connection failed: {e}")

if __name__ == "__main__":
    print("🚀 Setting up SEPARATE Supply/Demand database...")
    print("📋 This will NOT affect your existing darkpool_data database")
    print()
    
    # Step 1: Create separate database
    print("🔧 Step 1: Creating separate database...")
    create_sd_database()
    print()
    
    # Step 2: Create SD tables in separate database
    print("🔧 Step 2: Creating SD tables in separate database...")
    create_sd_tables()
    print()
    
    # Step 3: Clean up any SD tables from original database
    print("🧹 Step 3: Cleaning any SD tables from original darkpool_data...")
    cleanup_old_tables()
    print()
    
    # Step 4: Test connections
    print("🔍 Step 4: Testing database connections...")
    test_connection()
    print()
    
    print("✅ Setup complete!")
    print()
    print("📊 Databases:")
    print(f"   • darkpool_data (unchanged) - your lit/dp data")
    print(f"   • {SD_DB_NAME} (new) - SD analysis data")
    print()
    print("🚀 Next steps:")
    print("   1. Run: python main.py")
    print("   2. Run: python bot.py")
    print()
    print("🔒 Complete isolation guaranteed - no interference!")