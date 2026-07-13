CREATE TABLE IF NOT EXISTS stocks (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(16) UNIQUE NOT NULL,
    name VARCHAR(100),
    sector VARCHAR(100),
    industry VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_prices (
    id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL CHECK (volume >= 0),
    UNIQUE(stock_id, date)
);

CREATE TABLE IF NOT EXISTS indicators (
    id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    sma_20 DOUBLE PRECISION,
    sma_50 DOUBLE PRECISION,
    sma_200 DOUBLE PRECISION,
    rsi_14 DOUBLE PRECISION,
    macd_line DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_histogram DOUBLE PRECISION,
    bb_upper DOUBLE PRECISION,
    bb_middle DOUBLE PRECISION,
    bb_lower DOUBLE PRECISION,
    UNIQUE(stock_id, date)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    tickers_attempted INTEGER NOT NULL DEFAULT 0,
    tickers_succeeded INTEGER NOT NULL DEFAULT 0,
    rows_affected INTEGER NOT NULL DEFAULT 0,
    errors TEXT
);

-- Upgrade databases created by earlier project versions.
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS sector VARCHAR(100);
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS industry VARCHAR(100);
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'running';
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS rows_affected INTEGER DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date);
CREATE INDEX IF NOT EXISTS idx_indicators_date ON indicators(date);

CREATE OR REPLACE VIEW daily_returns_view AS
SELECT
    stock_id,
    date,
    close,
    LAG(close) OVER (PARTITION BY stock_id ORDER BY date) AS prev_close,
    close / NULLIF(LAG(close) OVER (PARTITION BY stock_id ORDER BY date), 0) - 1
        AS daily_return_pct
FROM daily_prices;

CREATE OR REPLACE VIEW rolling_volatility_view AS
SELECT
    stock_id,
    date,
    close,
    daily_return_pct,
    STDDEV(daily_return_pct) OVER (
        PARTITION BY stock_id
        ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS rolling_20d_volatility
FROM daily_returns_view;
