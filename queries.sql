-- Top five gainers over each stock's latest available 30-day window.
WITH ranked AS (
    SELECT
        stock_id,
        date,
        close,
        FIRST_VALUE(close) OVER (
            PARTITION BY stock_id ORDER BY date
        ) AS first_close,
        LAST_VALUE(close) OVER (
            PARTITION BY stock_id ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_close,
        ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY date DESC) AS latest_row
    FROM daily_prices
    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT s.ticker, ROUND(((r.last_close / NULLIF(r.first_close, 0)) - 1)::numeric * 100, 2) AS pct_gain
FROM ranked r
JOIN stocks s ON s.id = r.stock_id
WHERE r.latest_row = 1
ORDER BY pct_gain DESC
LIMIT 5;

-- Most volatile stocks by standard deviation of daily returns over 30 days.
SELECT
    s.ticker,
    ROUND((STDDEV(dr.daily_return_pct) * SQRT(252) * 100)::numeric, 2)
        AS annualized_volatility_pct
FROM stocks s
JOIN daily_returns_view dr ON s.id = dr.stock_id
WHERE dr.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY s.ticker
ORDER BY annualized_volatility_pct DESC
LIMIT 5;

-- Highest average volume.
SELECT s.ticker, ROUND(AVG(dp.volume)) AS avg_volume
FROM stocks s
JOIN daily_prices dp ON s.id = dp.stock_id
GROUP BY s.ticker
ORDER BY avg_volume DESC
LIMIT 5;

-- Latest oversold stocks above their 200-session moving average.
SELECT s.ticker, i.date, i.rsi_14, i.sma_20, i.sma_50, i.sma_200, dp.close
FROM indicators i
JOIN stocks s ON s.id = i.stock_id
JOIN daily_prices dp ON dp.stock_id = i.stock_id AND dp.date = i.date
WHERE i.date = (SELECT MAX(date) FROM indicators)
  AND i.rsi_14 < 40
  AND dp.close > i.sma_200
ORDER BY i.rsi_14;

-- Sector return summary. Return values are decimal fractions.
SELECT
    s.sector,
    ROUND((AVG(dr.daily_return_pct) * 100)::numeric, 4) AS avg_daily_return_pct,
    COUNT(DISTINCT s.id) AS num_stocks
FROM daily_returns_view dr
JOIN stocks s ON s.id = dr.stock_id
WHERE dr.daily_return_pct IS NOT NULL AND s.sector IS NOT NULL
GROUP BY s.sector
ORDER BY avg_daily_return_pct DESC;
