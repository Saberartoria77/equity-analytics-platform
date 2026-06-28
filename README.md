# Equity Analytics Platform

A data engineering project that ingests equity market data into PostgreSQL, computes technical indicators, backtests trading strategies, and surfaces insights via an interactive dashboard.

**Tech stack**: Python · PostgreSQL · SQLAlchemy · yfinance · Pandas · NumPy · SciPy · Jupyter · Streamlit · Plotly · Railway - Neon

## Live Demo
🔗 [equity-analytics-platform.streamlit.app](https://saberartoria77-equity-analytics-platform-dashboard-yqyusj.streamlit.app/)

---

## Architecture

```
yfinance API ─┐
              ├─► Python pipeline ─► PostgreSQL (Neon) ─► Streamlit dashboard
Alpha Vantage ┘   retry · logging     tables · views          Price · Backtest
  (backup)        scheduling          indicators              · Sector
```

---

## Schema

**Tables**
- `stocks` — ticker, name, sector, industry
- `daily_prices` — stock_id, date, OHLCV (UNIQUE on stock_id + date)
- `indicators` — SMA 20/50/200, RSI 14, MACD, Bollinger Bands
- `ingestion_runs` — pipeline run tracking: timestamp, row count, errors

**Views**
- `daily_returns_view` — daily return % via LAG window function
- `rolling_volatility_view` — 20-day rolling standard deviation

**Indexes**
- `UNIQUE(stock_id, date)` on `daily_prices` — auto-created, primary query index

---

## Data

- Universe: 100 S&P 500 stocks (tech + finance sectors)
- History: 5 years daily OHLCV
- Source: Yahoo Finance via yfinance · Alpha Vantage as backup
- Hosted: Hosted: Neon (Postgres, serverless)

---

## Features

- Exponential backoff retry on API failures
- Python logging to file and console
- Ingestion run tracking for pipeline observability
- Automated daily scheduler
- 4 technical indicators: SMA (20/50/200), RSI (14), MACD (12/26/9), Bollinger Bands (20-day, 2σ)
- RSI mean-reversion backtest engine with Sharpe ratio, win rate, max drawdown
- Strategy significance testing (t-test + bootstrap)
- 3-page Streamlit dashboard: Price & Indicators · Backtest Results · Sector Analysis

---

## Backtest Results (RSI Strategy)

**Strategy**: Buy when RSI < 30, Sell when RSI > 70  
**Transaction cost**: 1.5% per trade (Wealthsimple CAD account FX fee)

| Ticker | Trades | RSI Return | Buy & Hold | Win% | Sharpe | Max Drawdown |
|--------|--------|------------|------------|------|--------|--------------|
| AAPL   | 5      | 20.2%      | 101.1%     | 100% | 1.17   | 0%           |
| NVDA   | 2      | 7.4%       | 870.5%     | 50%  | 0.07   | 33.1%        |
| TSLA   | 5      | 24.1%      | 70.1%      | 60%  | 0.23   | 23.3%        |
| MSFT   | 4      | 7.9%       | 41.9%      | 50%  | 0.14   | 13.8%        |

**Key finding**: RSI mean-reversion underperforms buy & hold cumulatively — though, tested across all 100 stocks, this underperformance is **not statistically significant** (see [Strategy Significance](#strategy-significance)). FX fees are the single largest cost driver — AAPL gross return dropped from 34% to 20% after applying Wealthsimple's 1.5% CAD account FX fee.

---

## Strategy Significance

Is the strategy's underperformance a real effect, or noise? Tested on the daily return difference (strategy − market) across 100 stocks, 2021–2026. Positions are set on *prior-day* RSI(14) to avoid look-ahead bias. (Gross daily returns; transaction costs would only widen the gap.)

| Method | n | mean daily diff | p-value |
|--------|---|-----------------|---------|
| Pooled stock-days (naive) | 115,743 | -0.000330 | 2e-09 |
| Daily cross-sectional mean (correct) | 1,166 | -0.000294 | 0.19 |

The naive pooled test treats ~100 stocks that move together each day as independent observations (**pseudo-replication**), understating variance and yielding a spuriously tiny p-value. Averaging the difference to one value per day removes the cross-sectional correlation; the honest test then shows **no statistically significant difference** (p = 0.19), consistent with a single-stock bootstrap whose 95% CI for the mean daily difference includes 0.

**Takeaway**: cumulative-return gaps can mislead without a significance test — the strategy's lower terminal return comes from reduced market exposure, not a statistically reliable edge.

→ Full analysis: [`notebooks/rsi_significance.ipynb`](notebooks/rsi_significance.ipynb)

---

## Setup

1. Clone the repo
2. Create venv: `python -m venv venv`
3. Install: `pip install -r requirements.txt`
4. Create `.env`: `DB_URL=postgresql://postgres:password@localhost:5432/equity_analytics`
5. Create DB: `psql -U postgres -c "CREATE DATABASE equity_analytics;"`
6. Run schema: `psql -U postgres -d equity_analytics -f schema.sql`
7. Run ingestion: `python ingest.py`
8. Compute indicators: `python indicators.py`
9. Launch dashboard: `streamlit run dashboard.py`

---

## Roadmap

- [x] Project setup
- [x] AAPL ingestion pipeline
- [x] Multi-stock ingestion (100 stocks)
- [x] Error handling + logging
- [x] Ingestion run tracking
- [x] Alpha Vantage backup API
- [x] Daily returns view
- [x] Rolling volatility view
- [x] Automated scheduler
- [x] Technical indicators (SMA, RSI, MACD, Bollinger Bands)
- [x] Backtesting engine with metrics
- [x] Strategy significance testing (t-test + bootstrap)
- [x] Streamlit dashboard (3 pages)
- [x] Cloud deployment (Railway + Streamlit Cloud)
- [ ] Predictive model (scikit-learn)
- [ ] Scheduled cloud pipeline (GitHub Actions)
- [ ] AI-powered market commentary (Claude API)
- [ ] Text-to-SQL natural language interface
- [ ] Docker containerization
- [ ] Airflow DAG scheduler
