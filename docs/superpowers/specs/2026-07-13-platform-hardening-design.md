# Equity Analytics Platform Hardening Design

## Objective

Make the existing Python, PostgreSQL, and Streamlit project reproducible, analytically correct, testable, and suitable for a public data-engineering portfolio without expanding it into a new product.

## Architecture

The project keeps its current script-oriented workflow but separates pure calculations from database and UI side effects. Database configuration is resolved only when an entry point needs it, allowing indicator and backtest logic to be imported and tested without credentials. The scheduled pipeline runs ingestion and indicator refresh in sequence and records truthful run results.

Data flows through these stages:

1. Yahoo Finance is the primary market-data provider.
2. Alpha Vantage is a bounded fallback for supported failures.
3. Ingestion normalizes provider data and upserts price corrections as well as new dates.
4. A canonical, idempotent PostgreSQL schema owns all tables, constraints, indexes, and views.
5. Indicator calculation recomputes deterministic values and upserts changed rows.
6. Backtesting uses prior-day RSI positions applied to subsequent daily returns.
7. Streamlit reads cached query results and handles empty data or connection failures visibly.

## Database and Migration Design

`schema.sql` becomes the complete canonical schema, including `indicators`. Foreign keys are non-null where the relationship is required, deletion behavior is explicit, and date uniqueness is enforced. Volatility views use daily returns rather than price-level standard deviation.

The migration utility applies the schema, copies rows with conflict-safe upserts, and resets PostgreSQL sequences after explicit IDs are loaded. Re-running migration must not duplicate data or weaken constraints.

## Ingestion and Providers

Provider requests use timeouts, status checks, structured error detection, and bounded exponential retry. Alpha Vantage is attempted only when the primary provider exhausts its retries and a key is configured.

Ingestion returns actual affected-row counts. Existing price rows are updated because providers can revise adjusted history. Each run records attempted and successful tickers, inserted or updated rows, failures, start time, completion time, and final status. Failures remain isolated by ticker while producing a non-zero process exit when the pipeline is materially unsuccessful.

## Indicator Semantics

SMA windows are exactly 20, 50, and 200 sessions. RSI uses a documented Wilder-style exponentially smoothed calculation. MACD and Bollinger Bands retain their conventional parameters. Indicator persistence uses `ON CONFLICT ... DO UPDATE` so corrected calculations replace stale values.

Pure calculation functions accept and return Pandas data frames without reading environment variables or opening database connections.

## Backtest Semantics

RSI observed at the end of day `t` determines the position held for the return from `t` to `t+1`. Strategy returns are daily returns multiplied by the lagged position. Entry and exit costs are applied when position changes, using a configurable rate expressed as a decimal.

Metrics derive from a compounded daily equity curve:

- total return is final equity minus one;
- annualized Sharpe uses daily mean and standard deviation with 252 sessions;
- maximum drawdown is the worst percentage decline from the running equity peak;
- win rate is the share of completed round trips with positive net return;
- open positions are marked to the final available close for reporting.

The dashboard and research notebook use the same signal semantics. Statistical inference uses daily cross-sectional mean differences with heteroskedasticity-and-autocorrelation-consistent standard errors. A moving-block bootstrap replaces independent daily resampling.

## Dashboard and Scheduling

The dashboard validates database configuration, caches the engine and query results, and shows actionable errors instead of crashing. Empty ticker, price, backtest, and sector result sets are handled explicitly.

The scheduler uses the active Python interpreter and repository-absolute script paths. Each daily run executes ingestion and indicators sequentially with timeouts and propagates failures. Schedule timezone and target time are configurable through environment variables.

The public deployment documentation will describe required Streamlit visibility and secrets configuration. Development configuration will not disable CORS or XSRF protection.

## Dependencies and Tooling

Python 3.11 is the supported runtime. `requirements.txt` contains only runtime dependencies with compatible version ranges; development and test dependencies live in `requirements-dev.txt`. UTF-8 is used throughout.

Pytest covers calculations, retry/fallback behavior, pipeline orchestration, configuration errors, and edge cases. Ruff provides linting. GitHub Actions runs lint, unit tests, compilation, and a PostgreSQL-backed schema smoke test. No live market-data or production-database access is required in CI.

## Documentation and Result Integrity

README setup, architecture, scheduler, provider fallback, and statistical language will match implemented behavior. Historical headline results will be labeled stale until recomputed by the corrected engine; unsupported numerical claims will not be presented as current validated results.

## Non-Goals

- Predictive models, text-to-SQL, Airflow, and AI commentary are not added.
- The dashboard is not visually redesigned.
- Production credentials or production database mutations are not required for verification.

## Acceptance Criteria

- A fresh PostgreSQL database can apply `schema.sql` and run every pipeline stage.
- Pure analytics import without `DB_URL`.
- Correctness tests fail against the reviewed defects and pass after implementation.
- The scheduled workflow updates both prices and indicators.
- Dependency installation succeeds on Python 3.11.
- CI configuration validates lint, tests, compilation, and schema creation.
- Repository documentation no longer overstates unverified results or deployment accessibility.
