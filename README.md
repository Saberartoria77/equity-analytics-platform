# Equity Analytics Platform

A data engineering project that ingests equity market data into PostgreSQL and surfaces insights via SQL analytics and an interactive dashboard.

**Tech stack**: Python · PostgreSQL · yfinance · SQLAlchemy · Pandas · Streamlit

---

## Architecture


yfinance API → Python ingestion → PostgreSQL → SQL analytics → Streamlit dashboard


## Setup

*Coming soon*

## Data

- Universe: S&P 500 stocks (expanding)
- History: 5 years daily OHLCV
- Source: Yahoo Finance via yfinance

## Roadmap

- [x] Project setup
- [ ] AAPL ingestion pipeline
- [ ] Multi-stock ingestion (S&P 500)
- [ ] Technical indicators (SMA, RSI, Bollinger Bands)
- [ ] SQL analytics layer
- [ ] Streamlit dashboard
- [ ] Deployment
