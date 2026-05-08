# Equity Analytics Platform

A data engineering project that ingests equity market data into PostgreSQL and surfaces insights via SQL analytics and an interactive dashboard.

**Tech stack**: Python · PostgreSQL · yfinance · SQLAlchemy · Pandas · Streamlit

---

## Architecture
yfinance API → Python ingestion → PostgreSQL → SQL views → Streamlit dashboard
↑                  ↓
Alpha Vantage         daily_returns_view
(backup API)          rolling_volatility_view


## Schema

**Tables:**
- `stocks` — ticker, name, created_at
- `daily_prices` — stock_id, date, OHLCV data (UNIQUE on stock_id + date)
- `ingestion_runs` — tracks each pipeline run: timestamp, success count, row count, errors

**Views:**
- `daily_returns_view` — daily return % using LAG window function
- `rolling_volatility_view` — 20-day rolling standard deviation

**Indexes:**
- UNIQUE(stock_id, date) on daily_prices — auto-created, serves as primary query index

## Data

- Universe: 100 stocks (S&P 500 tech + finance sectors)
- History: 5 years daily OHLCV
- Source: Yahoo Finance via yfinance (Alpha Vantage as backup)

## Features

- Retry with exponential backoff on API failures
- Python logging to file and console
- Automated daily scheduler
- Ingestion run tracking for pipeline observability

## Setup

1. Clone the repo
2. Create venv: `python -m venv venv`
3. Install dependencies: `pip install -r requirements.txt`
4. Create `.env` file with `DB_URL=postgresql://postgres:yourpassword@localhost:5432/equity_analytics`
5. Create database: `psql -U postgres -c "CREATE DATABASE equity_analytics;"`
6. Run schema: `psql -U postgres -d equity_analytics -f schema.sql`
7. Run ingestion: `python ingest.py`

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
- [ ] Technical indicators (SMA, RSI, Bollinger Bands)
- [ ] Backtesting engine
- [ ] Streamlit dashboard
- [ ] Deployment
