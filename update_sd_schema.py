#!/usr/bin/env python3
"""
Update Supply/Demand Database Schema - ADD ENHANCEMENT FEATURES
This updates your existing supply_demand_data database with:
1. Absorption job segments table
2. Enhanced background jobs tracking
3. Correct date semantics
4. Performance indexes

Fixed version with proper None handling for type safety
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Optional, Tuple, Any

# Database connection details (same as your create_sd_schema.py)
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "trader"
DB_PASS = "Deltuhdarkpools!7"
SD_DB_NAME = "supply_demand_data"

def update_background_jobs_table() -> None:
    """Update existing background_jobs table with enhancement features"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=SD_DB_NAME
    )
    
    cur = conn.cursor()
    
    # Check if background_jobs table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'background_jobs'
        )
    """)
    
    result = cur.fetchone()
    if not result or not result[0]:
        # Create table if it doesn't exist
        print("Creating background_jobs table...")
        cur.execute("""
            CREATE TABLE background_jobs (
                job_id VARCHAR(255) PRIMARY KEY,
                ticker VARCHAR(10),
                level_price DECIMAL(10,2),
                level_id INTEGER,
                date_range VARCHAR(50),
                status VARCHAR(50),
                progress INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP,
                result_data JSONB,
                error_message TEXT
            )
        """)
        print("✅ Created background_jobs table")
    else:
        print("📁 background_jobs table already exists")
    
    # Add enhancement columns if they don't exist
    enhancement_columns = [
        ("enhancement", "VARCHAR(100)", "Track enhanced vs legacy jobs"),
        ("api_calls_used", "INTEGER DEFAULT 0", "Track unlimited API usage"),
        ("is_absorption", "BOOLEAN DEFAULT FALSE", "Track job type")
    ]
    
    for column_name, column_type, description in enhancement_columns:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'background_jobs' AND column_name = %s
            )
        """, (column_name,))
        
        result = cur.fetchone()
        if not result or not result[0]:
            print(f"Adding column: {column_name}")
            cur.execute(f"ALTER TABLE background_jobs ADD COLUMN {column_name} {column_type}")
            print(f"✅ Added {column_name} - {description}")
        else:
            print(f"📁 Column {column_name} already exists")
    
    conn.commit()
    cur.close()
    conn.close()

def create_absorption_segments_table() -> None:
    """Create new absorption job segments table"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=SD_DB_NAME
    )
    
    cur = conn.cursor()
    
    # Check if table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'absorption_job_segments'
        )
    """)
    
    result = cur.fetchone()
    if not result or not result[0]:
        print("Creating absorption_job_segments table...")
        cur.execute("""
            CREATE TABLE absorption_job_segments (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(255) NOT NULL,
                level_id INTEGER NOT NULL,
                volume BIGINT NOT NULL,
                value DECIMAL(15,2) NOT NULL,
                trades INTEGER NOT NULL,
                date_start DATE NOT NULL,
                date_end DATE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (level_id) REFERENCES supply_demand_levels(id) ON DELETE CASCADE
            )
        """)
        print("✅ Created absorption_job_segments table")
        
        # Add indexes for performance
        indexes = [
            ("CREATE INDEX idx_absorption_segments_level_id ON absorption_job_segments(level_id)", "Level ID index"),
            ("CREATE INDEX idx_absorption_segments_date_range ON absorption_job_segments(date_start, date_end)", "Date range index"),
            ("CREATE INDEX idx_absorption_segments_job_id ON absorption_job_segments(job_id)", "Job ID index"),
            ("CREATE INDEX idx_absorption_segments_level_dates ON absorption_job_segments(level_id, date_start, date_end)", "Composite level/dates index")
        ]
        
        for index_sql, description in indexes:
            cur.execute(index_sql)
            print(f"✅ Created index: {description}")
            
    else:
        print("📁 absorption_job_segments table already exists")
    
    conn.commit()
    cur.close()
    conn.close()

def create_enhanced_timeline_view() -> None:
    """Create optimized view for enhanced timeline data"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=SD_DB_NAME
    )
    
    cur = conn.cursor()
    
    print("Creating enhanced_timeline_view...")
    cur.execute("""
        CREATE OR REPLACE VIEW enhanced_timeline_view AS
        SELECT 
            l.id,
            l.ticker,
            l.level_price,
            l.level_type,
            l.level_name,
            l.date_created,
            l.is_active,
            v.original_volume,
            v.absorbed_volume,
            v.absorption_percentage,
            v.original_value,
            v.absorbed_value,
            v.last_updated,
            v.original_date_start,
            v.original_date_end,
            v.absorption_start_date as absorption_end_date,
            COUNT(s.id) as segment_count,
            COALESCE(
                ARRAY_AGG(
                    json_build_object(
                        'job_id', s.job_id,
                        'volume', s.volume,
                        'value', s.value,
                        'trades', s.trades,
                        'date_start', s.date_start,
                        'date_end', s.date_end,
                        'created_at', s.created_at
                    ) ORDER BY s.date_start
                ) FILTER (WHERE s.id IS NOT NULL),
                '{}'::json[]
            ) as job_segments
        FROM supply_demand_levels l
        LEFT JOIN level_volume_tracking v ON l.id = v.level_id
        LEFT JOIN absorption_job_segments s ON l.id = s.level_id
        WHERE l.is_active = true
        GROUP BY l.id, l.ticker, l.level_price, l.level_type, l.level_name, 
                 l.date_created, l.is_active, v.original_volume, v.absorbed_volume, 
                 v.absorption_percentage, v.original_value, v.absorbed_value, 
                 v.last_updated, v.original_date_start, v.original_date_end, v.absorption_start_date
        ORDER BY l.level_price DESC
    """)
    print("✅ Created enhanced_timeline_view")
    
    conn.commit()
    cur.close()
    conn.close()

def add_column_comments() -> None:
    """Add comments to clarify the corrected date semantics"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=SD_DB_NAME
    )
    
    cur = conn.cursor()
    
    print("Adding column comments for clarity...")
    
    comments = [
        ("level_volume_tracking", "absorption_start_date", "End date of the absorption period (semantic correction for timeline display)"),
        ("absorption_job_segments", "date_end", "End date of absorption job - used as absorption completion date in timeline"),
        ("absorption_job_segments", "date_start", "Start date of absorption job period"),
        ("background_jobs", "enhancement", "Tracks whether job used enhanced unlimited API calls"),
        ("background_jobs", "api_calls_used", "Number of Polygon API calls made (unlimited for enhanced jobs)")
    ]
    
    for table_name, column_name, comment in comments:
        try:
            cur.execute(f"""
                COMMENT ON COLUMN {table_name}.{column_name} IS %s
            """, (comment,))
            print(f"✅ Added comment to {table_name}.{column_name}")
        except Exception as e:
            print(f"⚠️  Could not add comment to {table_name}.{column_name}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()

def verify_schema_updates() -> None:
    """Verify all updates were applied correctly"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=SD_DB_NAME
    )
    
    cur = conn.cursor()
    
    print("🔍 Verifying schema updates...")
    
    # Check tables exist
    tables_to_check = ['supply_demand_levels', 'level_volume_tracking', 'background_jobs', 'absorption_job_segments']
    
    for table in tables_to_check:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (table,))
        
        result = cur.fetchone()
        exists = result[0] if result else False
        status = "✅" if exists else "❌"
        print(f"{status} Table {table}: {'EXISTS' if exists else 'MISSING'}")
    
    # Check background_jobs enhancement columns
    enhancement_columns = ['enhancement', 'api_calls_used', 'is_absorption']
    for column in enhancement_columns:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'background_jobs' AND column_name = %s
            )
        """, (column,))
        
        result = cur.fetchone()
        exists = result[0] if result else False
        status = "✅" if exists else "❌"
        print(f"{status} Column background_jobs.{column}: {'EXISTS' if exists else 'MISSING'}")
    
    # Check view exists
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.views 
            WHERE table_name = 'enhanced_timeline_view'
        )
    """)
    
    result = cur.fetchone()
    view_exists = result[0] if result else False
    status = "✅" if view_exists else "❌"
    print(f"{status} View enhanced_timeline_view: {'EXISTS' if view_exists else 'MISSING'}")
    
    # Count existing data with safe None handling
    cur.execute("SELECT COUNT(*) FROM supply_demand_levels")
    result = cur.fetchone()
    level_count = result[0] if result else 0
    
    cur.execute("SELECT COUNT(*) FROM background_jobs")
    result = cur.fetchone()
    job_count = result[0] if result else 0
    
    cur.execute("SELECT COUNT(*) FROM absorption_job_segments")
    result = cur.fetchone()
    segment_count = result[0] if result else 0
    
    print(f"📊 Current data:")
    print(f"   • SD Levels: {level_count}")
    print(f"   • Background Jobs: {job_count}")
    print(f"   • Absorption Segments: {segment_count}")
    
    cur.close()
    conn.close()

def create_sample_data() -> None:
    """Create sample data for testing (optional)"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=SD_DB_NAME
    )
    
    cur = conn.cursor()
    
    # Check if we already have levels
    cur.execute("SELECT COUNT(*) FROM supply_demand_levels")
    result = cur.fetchone()
    existing_levels = result[0] if result else 0
    
    if existing_levels == 0:
        response = input("No existing levels found. Create sample data for testing? (y/N): ")
        if response.lower() == 'y':
            print("Creating sample data...")
            
            # Create sample level
            cur.execute("""
                INSERT INTO supply_demand_levels (ticker, level_price, level_type, level_name, date_created)
                VALUES ('TSLA', 356.43, 'supply', 'Key Level', '2025-01-01')
                RETURNING id
            """)
            result = cur.fetchone()
            level_id = result[0] if result else None
            
            if level_id:
                print(f"✅ Created sample level with ID: {level_id}")
                
                # Create sample volume tracking
                cur.execute("""
                    INSERT INTO level_volume_tracking 
                    (level_id, ticker, level_price, price_range_low, price_range_high, 
                     original_volume, original_value, absorbed_volume, absorbed_value, 
                     absorption_percentage, original_date_start, original_date_end, absorption_start_date)
                    VALUES (%s, 'TSLA', 356.43, 356.405, 356.455,
                            26660000, 9500000000.00, 9460000, 3500000000.00,
                            35.5, '2024-01-01', '2024-01-31', '2025-09-11')
                """, (level_id,))
                print("✅ Created sample volume tracking")
                
                # Create sample absorption segments
                segments = [
                    ('job_1', level_id, 5000000, 1850000000.00, 2500, '2025-05-26', '2025-07-15'),
                    ('job_2', level_id, 4460000, 1650000000.00, 2200, '2025-07-16', '2025-09-11')
                ]
                
                for segment in segments:
                    cur.execute("""
                        INSERT INTO absorption_job_segments 
                        (job_id, level_id, volume, value, trades, date_start, date_end)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, segment)
                
                print("✅ Created sample absorption segments")
            else:
                print("❌ Failed to create sample level")
            
            conn.commit()
        else:
            print("Skipping sample data creation")
    else:
        print(f"📊 Found {existing_levels} existing levels - skipping sample data creation")
    
    cur.close()
    conn.close()

def main() -> None:
    """Run all schema updates"""
    print("🚀 Updating Supply/Demand database schema for enhanced features...")
    print("📋 This will add tables and columns for:")
    print("   • Absorption job segments")
    print("   • Enhanced background job tracking")
    print("   • Unlimited API call metrics")
    print("   • Correct absorption date semantics")
    print()
    
    try:
        # Test connection first
        print("🔍 Testing database connection...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=SD_DB_NAME
        )
        conn.close()
        print("✅ Database connection successful")
        print()
        
        # Step 1: Update background jobs table
        print("🔧 Step 1: Updating background_jobs table...")
        update_background_jobs_table()
        print()
        
        # Step 2: Create absorption segments table
        print("🔧 Step 2: Creating absorption_job_segments table...")
        create_absorption_segments_table()
        print()
        
        # Step 3: Create enhanced view
        print("🔧 Step 3: Creating enhanced_timeline_view...")
        create_enhanced_timeline_view()
        print()
        
        # Step 4: Add column comments
        print("🔧 Step 4: Adding column comments...")
        add_column_comments()
        print()
        
        # Step 5: Verify updates
        print("🔧 Step 5: Verifying schema updates...")
        verify_schema_updates()
        print()
        
        # Step 6: Optional sample data
        print("🔧 Step 6: Sample data creation...")
        create_sample_data()
        print()
        
        print("✅ Schema updates complete!")
        print()
        print("🎯 Enhanced features now available:")
        print("   • Unlimited Polygon API calls")
        print("   • Job segmentation for absorption tracking")
        print("   • Supply/demand color differentiation")
        print("   • Correct absorption end date display")
        print("   • Enhanced timeline visualization")
        print()
        print("🚀 Next steps:")
        print("   1. Update your main.py with the enhanced code")
        print("   2. Update your bot.py with the enhanced commands")
        print("   3. Test with /sd enhanced_volume_job and /sd enhanced_absorption_job")
        
    except Exception as e:
        print(f"❌ Schema update failed: {e}")
        print("   Make sure your supply_demand_data database exists")
        print("   Run create_sd_schema.py first if needed")

if __name__ == "__main__":
    main()