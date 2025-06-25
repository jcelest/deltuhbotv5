-- This first line will delete the old, broken table if it exists,
-- ensuring we can re-run this script cleanly.
DROP TABLE IF EXISTS block_trades;

-- This is the corrected table definition
CREATE TABLE block_trades (
    id BIGSERIAL,
    trade_time TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL, -- Using TEXT as recommended by the warning you saw
    price NUMERIC(15, 5) NOT NULL,
    quantity BIGINT NOT NULL,
    trade_value NUMERIC(20, 2) NOT NULL,
    conditions INTEGER[],
    -- This composite primary key satisfies the TimescaleDB requirement
    PRIMARY KEY (id, trade_time)
);

-- This will now succeed without error
SELECT create_hypertable('block_trades', 'trade_time');

-- This index is still useful for fast lookups by ticker
CREATE INDEX idx_ticker_time ON block_trades (ticker, trade_time DESC);