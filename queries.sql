-- 1. Top 5 gainers last month
SELECT
    s.ticker,
    ROUND(((last.close - first.close) / first.close * 100)::numeric, 2) AS pct_gain
FROM stocks s
JOIN (
    SELECT stock_id, close
    FROM daily_prices
    WHERE date = (SELECT MAX(date) FROM daily_prices)
) last ON s.id = last.stock_id
JOIN (
    SELECT stock_id, close
    FROM daily_prices
    WHERE date = (SELECT MIN(date) FROM daily_prices WHERE date >= CURRENT_DATE - INTERVAL '30 days')
) first ON s.id = first.stock_id
ORDER BY pct_gain DESC
LIMIT 5;

-- 2. Most volatile stocks (last 30 days)
SELECT
    s.ticker,
    ROUND(STDDEV(dp.close)::numeric, 4) AS price_stddev
FROM stocks s
JOIN daily_prices dp ON s.id = dp.stock_id
WHERE dp.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY s.ticker
ORDER BY price_stddev DESC
LIMIT 5;

-- 3. Highest average volume (all time)
SELECT
    s.ticker,
    ROUND(AVG(dp.volume)) AS avg_volume
FROM stocks s
JOIN daily_prices dp ON s.id = dp.stock_id
GROUP BY s.ticker
ORDER BY avg_volume DESC
LIMIT 5;

--4.A classification query on bull bear and flat stock.
SELECT
    date,
    close,
    (close - open) / open * 100 AS pct_change,
    CASE
        WHEN (close - open) / open * 100 > 1 THEN 'Bull'
        WHEN (close - open) / open * 100 < -1 THEN 'Bear'
        ELSE 'Flat'
    END AS classification
FROM daily_prices
WHERE stock_id = 1
ORDER BY date;


-- 5. daily review of stocks
CREATE VIEW daily_returns_view AS
SELECT stock_id,
       date,
       close,
       LAG(close) OVER (PARTITION BY stock_id ORDER BY date) AS prev_close,
       ROUND(
               (close - LAG(close) OVER (PARTITION BY stock_id ORDER BY date))
                   / LAG(close) OVER (PARTITION BY stock_id ORDER BY date) * 100
           , 2)                                              AS daily_return_pct
FROM daily_prices;


CREATE VIEW rolling_volatility_view AS
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

SELECT * FROM rolling_volatility_view WHERE stock_id = 2 LIMIT 25;

--indicators sheet
CREATE TABLE IF NOT EXISTS indicators (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER REFERENCES stocks(id),
    date DATE NOT NULL,
    sma_20 NUMERIC(10,4),
    sma_50 NUMERIC(10,4),
    sma_200 NUMERIC(10,4),
    rsi_14 NUMERIC(10,4),
    macd_line NUMERIC(10,4),
    macd_signal NUMERIC(10,4),
    macd_histogram NUMERIC(10,4),
    bb_upper NUMERIC(10,4),
    bb_middle NUMERIC(10,4),
    bb_lower NUMERIC(10,4),
    UNIQUE(stock_id, date)
);

SELECT i.date, i.rsi_14, s.ticker
FROM indicators i
JOIN stocks s ON s.id = i.stock_id
WHERE s.ticker = 'AAPL'
ORDER BY i.date DESC
LIMIT 5;


SELECT s.ticker, i.date, i.rsi_14, i.sma_20, i.sma_50, i.sma_200,
       dp.close
FROM indicators i
JOIN stocks s ON s.id = i.stock_id
JOIN daily_prices dp ON dp.stock_id = i.stock_id AND dp.date = i.date
WHERE i.date = (SELECT MAX(date) FROM indicators)
  AND i.rsi_14 < 40
  AND dp.close > i.sma_200
ORDER BY i.rsi_14 ASC;