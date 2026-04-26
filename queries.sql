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