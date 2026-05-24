# Equity Analytics Platform

A data engineering project that ingests equity market data into PostgreSQL, computes technical indicators, backtests trading strategies, and surfaces insights via an interactive dashboard.

**Tech stack**: Python · PostgreSQL · SQLAlchemy · yfinance · Pandas · Streamlit · Plotly · Railway

## Live Demo
🔗 [equity-analytics-platform.streamlit.app](https://saberartoria77-equity-analytics-platform-dashboard-yqyusj.streamlit.app/)

---

## Architecture

yfinance API ──────────────────────────────────────────┐
▼
Alpha Vantage (backup) ──────────► Python Pipeline ──► PostgreSQL (Railway)
│                      │
retry logic            daily_returns_view
logging                rolling_volatility_view
scheduling                  │
▼
Streamlit Dashboard
(Price · Backtest · Sector)


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
- Hosted: Railway PostgreSQL

---

## Features

- Exponential backoff retry on API failures
- Python logging to file and console
- Ingestion run tracking for pipeline observability
- Automated daily scheduler
- 4 technical indicators: SMA (20/50/200), RSI (14), MACD (12/26/9), Bollinger Bands (20-day, 2σ)
- RSI mean-reversion backtest engine with Sharpe ratio, win rate, max drawdown
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

**Key finding**: RSI mean-reversion consistently underperforms buy & hold. FX fees are the single largest cost driver — AAPL gross return dropped from 34% to 20% after applying Wealthsimple's 1.5% CAD account FX fee.

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
- [x] Streamlit dashboard (3 pages)
- [x] Cloud deployment (Railway + Streamlit Cloud)
- [ ] AI-powered market commentary (Claude API)
- [ ] Text-to-SQL natural language interface
- [ ] Docker containerization
- [ ] Airflow DAG scheduler
