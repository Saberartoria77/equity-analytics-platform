# Equity Analytics Platform

A portfolio data-engineering project that ingests equity prices, maintains a PostgreSQL analytical model, computes technical indicators, evaluates an RSI strategy, and presents results in Streamlit.

**Stack:** Python 3.11 · PostgreSQL · SQLAlchemy · Pandas · yfinance · Alpha Vantage · Streamlit · Plotly · pytest · GitHub Actions

## Architecture

```text
Yahoo Finance ── retry ──┐
                         ├── price upserts ── PostgreSQL ── indicator upserts
Alpha Vantage ─ fallback ┘                                      │
                                                               ├── Streamlit
                                                               └── backtest/statistics
```

The daily workflow runs ingestion and indicator refresh sequentially. Provider corrections update historical rows, and every ingestion run records its real affected-row count and status.

## Analytical semantics

- SMA windows use 20, 50, and 200 sessions.
- RSI uses Wilder-style exponential smoothing with a 14-session period.
- The RSI strategy enters below 30 and exits above 70.
- A signal observed at close on day `t` controls exposure to the return from `t` to `t+1`.
- Returns compound on a daily equity curve; entry and exit costs are applied when position state changes.
- Sharpe is annualized from daily returns, and drawdown is measured from the running equity peak.
- Strategy significance uses daily cross-sectional differences with HAC standard errors and a moving-block bootstrap.

## Result integrity notice

The numerical results previously shown here were **historical, pre-correction results** from a same-close, additive-return implementation. They were invalidated when the engine was corrected and must not be compared with new output. Run `python backtesting.py` against a refreshed database to generate current results.

## Data model

`schema.sql` is the canonical, idempotent PostgreSQL schema:

- `stocks` stores ticker metadata;
- `daily_prices` stores unique stock/date OHLCV observations;
- `indicators` stores unique stock/date technical indicators;
- `ingestion_runs` records pipeline status and actual affected rows;
- `daily_returns_view` calculates decimal close-to-close returns;
- `rolling_volatility_view` calculates 20-session return volatility.

The ticker list is a curated technology and financial-services universe. It is not dynamically synchronized with current S&P 500 membership.

## Local setup

Prerequisites: Python 3.11+, PostgreSQL, and `psql`.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
createdb equity_analytics
psql equity_analytics -f schema.sql
export DB_URL=postgresql://localhost/equity_analytics
python ingest.py
python indicators.py
streamlit run dashboard.py
```

Set `ALPHA_VANTAGE_KEY` to enable fallback requests after Yahoo Finance retries are exhausted.
The fallback uses adjusted OHLC data and therefore requires a **premium Alpha Vantage** key;
a free-tier key will fail safely without overwriting Yahoo's adjusted history.

## Automated workflows

- `ci.yml` lints, tests, compiles Python, and applies the schema to PostgreSQL 16.
- `daily-pipeline.yml` supports manual and scheduled ingestion using the `DB_URL` repository secret and optional `ALPHA_VANTAGE_KEY`.
- The local scheduler supports `PIPELINE_TIME`, `PIPELINE_TIMEZONE`, and `PIPELINE_TIMEOUT_SECONDS`.
- `MIN_INGEST_SUCCESS_RATE` controls the production failure threshold and defaults to `0.8`; runs below it persist `failed` and exit non-zero.

## Verification

```bash
ruff check .
pytest -v
python -m compileall -q .
```

Tests do not call live providers or mutate the production database.

## Streamlit deployment

Configure `DB_URL` in Streamlit secrets. To use the dashboard as a public portfolio demo, set the deployed app visibility to public in Streamlit Community Cloud; the application URL may otherwise redirect visitors to authentication.

## Roadmap

- Production monitoring and alerting
- Predictive modeling
- Text-to-SQL exploration
- Containerized deployment
