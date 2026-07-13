-- Representative pre-hardening schema, including the Pandas-created indicators table.
CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sector VARCHAR(100),
    industry VARCHAR(100)
);

CREATE TABLE daily_prices (
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

CREATE TABLE ingestion_runs (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tickers_attempted INTEGER,
    tickers_succeeded INTEGER,
    rows_inserted INTEGER,
    errors TEXT
);

CREATE TABLE indicators (
    id BIGINT,
    stock_id BIGINT,
    date DATE,
    sma_20 DOUBLE PRECISION,
    sma_50 DOUBLE PRECISION,
    sma_200 DOUBLE PRECISION,
    rsi_14 DOUBLE PRECISION,
    macd_line DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_histogram DOUBLE PRECISION,
    bb_upper DOUBLE PRECISION,
    bb_middle DOUBLE PRECISION,
    bb_lower DOUBLE PRECISION
);

CREATE VIEW daily_returns_view AS
SELECT
    stock_id,
    date,
    close,
    LAG(close) OVER (PARTITION BY stock_id ORDER BY date) AS prev_close,
    ROUND(
        (close - LAG(close) OVER (PARTITION BY stock_id ORDER BY date))
        / LAG(close) OVER (PARTITION BY stock_id ORDER BY date) * 100,
        2
    ) AS daily_return_pct
FROM daily_prices;

CREATE VIEW rolling_volatility_view AS
SELECT
    stock_id,
    date,
    close,
    ROUND(
        STDDEV(close) OVER (
            PARTITION BY stock_id ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )::numeric,
        4
    ) AS rolling_20d_volatility
FROM daily_prices;

INSERT INTO stocks (ticker) VALUES ('AAPL');
INSERT INTO daily_prices (stock_id, date, open, high, low, close, volume)
VALUES (1, '2025-01-02', 100, 102, 99, 101, 1000);
INSERT INTO indicators (id, stock_id, date, sma_200)
VALUES (1, 1, '2025-01-02', 100);
INSERT INTO ingestion_runs (tickers_attempted, tickers_succeeded, rows_inserted)
VALUES (1, 1, 1);
