import psycopg2
from datetime import datetime

# --- DATABASE CONNECTION DETAILS ---
DB_NAME = "darkpool_data"
DB_USER = "trader"
DB_PASS = "Deltuhdarkpools!7" # Your password
DB_HOST = "localhost"
DB_PORT = "5432"

# A list of realistic, sample block trades to insert
sample_trades = [
    # Ticker,  Time,                   Price,   Quantity, Value,        Conditions
    ('SPY',   '2025-06-25 10:15:00',  545.50,  20000,    10910000.00,  [4]),
    ('SPY',   '2025-06-25 11:30:00',  545.75,  35000,    19101250.00,  [4, 12]),
    ('AAPL',  '2025-06-25 09:45:00',  210.20,  50000,    10510000.00,  [4]),
    ('MSFT',  '2025-06-25 12:00:00',  450.00,  15000,    6750000.00,   [4, 15]),
    ('SPY',   '2025-06-24 17:30:00',  544.10,  100000,   54410000.00,  [7, 9]) # An after-hours trade
]

def insert_test_data():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        print("Successfully connected to the database.")

        with conn.cursor() as cur:
            sql = """
                INSERT INTO block_trades (ticker, trade_time, price, quantity, trade_value, conditions)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            for trade in sample_trades:
                # Unpack the tuple for insertion
                ticker, time_str, price, qty, val, conds = trade
                
                # Convert string time to datetime object
                trade_time = datetime.fromisoformat(time_str)

                cur.execute(sql, (ticker, trade_time, price, qty, val, conds))
                print(f"Inserted trade for {ticker} with value ${val:,.2f}")
        
        conn.commit()
        print("\nSuccessfully inserted all test data.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    insert_test_data()
    