CREATE TABLE IF NOT EXISTS stocks (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_prices (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER REFERENCES stocks(id),
    date DATE NOT NULL,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT,
    UNIQUE(stock_id, date)
);


CREATE TABLE IF NOT EXISTS ingestion_runs
(
    id                SERIAL PRIMARY KEY,
    run_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tickers_attempted INTEGER,
    tickers_succeeded INTEGER,
    rows_inserted     INTEGER,
    errors            TEXT
)


CREATE OR REPLACE VIEW daily_returns_view AS
SELECT
    stock_id,
    date,
    close,
    LAG(close) OVER (PARTITION BY stock_id ORDER BY date) AS prev_close,
    ROUND(
        (close - LAG(close) OVER (PARTITION BY stock_id ORDER BY date))
        / LAG(close) OVER (PARTITION BY stock_id ORDER BY date) * 100
    , 2) AS daily_return_pct
FROM daily_prices;

CREATE OR REPLACE VIEW rolling_volatility_view AS
SELECT
    stock_id,
    date,
    close,
    ROUND(
        STDDEV(close) OVER (
            PARTITION BY stock_id
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )::numeric
    , 4) AS rolling_20d_volatility
FROM daily_prices;
