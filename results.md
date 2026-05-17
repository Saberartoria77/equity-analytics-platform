# Backtest Results

## RSI Mean-Reversion Strategy (RSI < 30 buy, RSI > 70 sell)

**Does the strategy make money?**  
Yes, but it consistently underperforms buy & hold across all tested stocks. 
AAPL returned 20.2% vs buy & hold 101.1% over the same 5-year period.

**Why does it underperform?**  
The RSI strategy exits positions when RSI > 70, missing continued upside in 
trend-driven stocks. NVDA is the clearest example — the strategy returned 7.4% 
while buy & hold returned 870.5%, because NVDA's AI-driven rally meant 
"overbought" signals were followed by further gains, not reversals.

**Impact of transaction costs?**  
Applying Wealthsimple's 1.5% CAD account FX fee per trade reduced AAPL returns 
from 34.2% to 20.2% — a 41% reduction in gross returns. For low-frequency 
strategies with few trades, FX fees are the single largest cost driver.

## Conclusion
Mean-reversion strategies like RSI work best in range-bound markets, not 
trending ones. Real trading costs (FX fees, slippage) erode returns significantly 
and must be modeled before any strategy is considered viable.