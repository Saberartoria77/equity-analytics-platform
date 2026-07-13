# Equity Analytics Platform Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the existing equity analytics platform reproducible, analytically correct, testable, and ready for a public portfolio.

**Architecture:** Keep the script-oriented Python/PostgreSQL/Streamlit application, but move configuration and database creation behind functions so pure calculations are importable. Use one canonical idempotent schema, deterministic upserts, a sequential daily pipeline, and a shared bias-safe backtest model.

**Tech Stack:** Python 3.11, Pandas, NumPy, SQLAlchemy 2, PostgreSQL, yfinance, Requests, Streamlit, Plotly, SciPy, statsmodels, pytest, Ruff, GitHub Actions

## Global Constraints

- Support Python 3.11.
- Do not require live market data or production database access in tests.
- Do not mutate the production database during implementation verification.
- Preserve the current three-page dashboard and project scope.
- Use prior-day RSI positions for subsequent daily returns.
- Store corrected prices and indicators with conflict-safe updates.

---

### Task 1: Reproducible dependencies and test harness

**Files:**
- Modify: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `pyproject.toml`
- Create: `tests/conftest.py`

**Interfaces:**
- Produces: Python 3.11-compatible runtime and development environments; pytest and Ruff configuration used by all later tasks.

- [ ] **Step 1: Add a dependency-file validation test**

```python
def test_runtime_requirements_are_utf8_and_minimal(project_root):
    text = (project_root / "requirements.txt").read_text(encoding="utf-8")
    assert "streamlit" in text
    assert "ipykernel" not in text
```

- [ ] **Step 2: Run the test and verify it fails because the current file is UTF-16 and contains notebook packages**

Run: `python -m pytest tests/test_project_config.py -v`
Expected: FAIL while decoding `requirements.txt` or on the `ipykernel` assertion.

- [ ] **Step 3: Replace the environment dump with minimal runtime ranges and add development tooling**

```text
pandas>=2.2,<3
numpy>=1.26,<3
SQLAlchemy>=2.0,<3
psycopg2-binary>=2.9,<3
python-dotenv>=1.0,<2
yfinance>=0.2.54,<2
requests>=2.32,<3
schedule>=1.2,<2
streamlit>=1.42,<2
plotly>=5.24,<7
scipy>=1.13,<2
statsmodels>=0.14,<1
```

```text
-r requirements.txt
pytest>=8.3,<9
pytest-cov>=6,<7
ruff>=0.9,<1
```

- [ ] **Step 4: Run the configuration test and dependency dry-run**

Run: `python -m pytest tests/test_project_config.py -v && python -m pip install --dry-run -r requirements.txt`
Expected: PASS and dependency resolution succeeds on Python 3.11.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt requirements-dev.txt pyproject.toml tests
git commit -m "build: add reproducible Python toolchain"
```

### Task 2: Canonical schema and database configuration

**Files:**
- Create: `database.py`
- Modify: `schema.sql`
- Modify: `queries.sql`
- Create: `tests/test_database.py`
- Create: `tests/test_schema.py`

**Interfaces:**
- Produces: `get_database_url(explicit_url: str | None = None) -> str` and `create_db_engine(explicit_url: str | None = None) -> Engine`.
- Produces: a canonical schema containing `stocks`, `daily_prices`, `indicators`, `ingestion_runs`, and return-based views.

- [ ] **Step 1: Write failing configuration and schema tests**

```python
def test_missing_database_url_has_actionable_error(monkeypatch):
    monkeypatch.delenv("DB_URL", raising=False)
    with pytest.raises(RuntimeError, match="DB_URL"):
        get_database_url()

def test_schema_contains_indicator_uniqueness(schema_text):
    assert "CREATE TABLE IF NOT EXISTS indicators" in schema_text
    assert "UNIQUE(stock_id, date)" in schema_text

def test_volatility_view_uses_returns(schema_text):
    assert "STDDEV(daily_return_pct)" in schema_text
```

- [ ] **Step 2: Run tests and verify missing module/table failures**

Run: `python -m pytest tests/test_database.py tests/test_schema.py -v`
Expected: FAIL because `database.py` and the canonical indicator schema do not exist.

- [ ] **Step 3: Implement lazy configuration and the complete idempotent schema**

```python
def get_database_url(explicit_url=None):
    url = explicit_url or os.getenv("DB_URL")
    if not url:
        raise RuntimeError("DB_URL is required; set it in the environment or Streamlit secrets.")
    return url

def create_db_engine(explicit_url=None):
    return create_engine(get_database_url(explicit_url), pool_pre_ping=True)
```

Define all tables in dependency order, use non-null foreign keys with `ON DELETE CASCADE`, create unique stock/date constraints, track run status/timestamps, and calculate rolling volatility from `daily_return_pct`.

- [ ] **Step 4: Run schema and configuration tests**

Run: `python -m pytest tests/test_database.py tests/test_schema.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add database.py schema.sql queries.sql tests/test_database.py tests/test_schema.py
git commit -m "fix: make database schema canonical"
```

### Task 3: Correct and test indicators

**Files:**
- Modify: `indicators.py`
- Create: `tests/test_indicators.py`

**Interfaces:**
- Produces: `compute_indicators(df: pd.DataFrame) -> pd.DataFrame` without database side effects.
- Produces: `save_indicators(engine, stock_id: int, df: pd.DataFrame) -> int` with upsert semantics.

- [ ] **Step 1: Write failing calculation tests**

```python
def test_sma_200_requires_200_observations(price_frame):
    result = compute_indicators(price_frame.iloc[:199].copy())
    assert result["sma_200"].isna().all()
    result = compute_indicators(price_frame.iloc[:200].copy())
    assert result["sma_200"].iloc[-1] == pytest.approx(price_frame["close"].mean())

def test_compute_indicators_does_not_mutate_input(price_frame):
    original = price_frame.copy(deep=True)
    compute_indicators(price_frame)
    pd.testing.assert_frame_equal(price_frame, original)
```

- [ ] **Step 2: Run tests and verify the 100-session implementation fails**

Run: `python -m pytest tests/test_indicators.py -v`
Expected: FAIL on the 199-observation SMA assertion.

- [ ] **Step 3: Implement conventional calculations and conflict updates**

Copy the input frame, sort by date, use `rolling(window=200, min_periods=200)`, use Wilder-style `ewm(alpha=1/14, adjust=False, min_periods=14)`, and change persistence to `ON CONFLICT ... DO UPDATE SET` for every indicator column.

- [ ] **Step 4: Run indicator tests**

Run: `python -m pytest tests/test_indicators.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add indicators.py tests/test_indicators.py
git commit -m "fix: calculate and upsert indicators correctly"
```

### Task 4: Bias-safe backtesting and statistics

**Files:**
- Modify: `backtesting.py`
- Create: `statistics_analysis.py`
- Create: `tests/test_backtesting.py`
- Create: `tests/test_statistics_analysis.py`

**Interfaces:**
- Produces: `run_backtest(df: pd.DataFrame, transaction_cost: float = 0.015) -> BacktestResult`.
- Produces: `compute_metrics(strategy_returns: pd.Series, trades: Sequence[Trade]) -> dict[str, float]`.
- Produces: `hac_mean_test(values: pd.Series, max_lags: int | None = None) -> dict[str, float]` and `moving_block_bootstrap_mean(...)`.

- [ ] **Step 1: Write failing timing, compounding, cost, and drawdown tests**

```python
def test_signal_is_applied_to_next_return():
    df = frame(close=[100, 110, 121], rsi=[20, 50, 80])
    result = run_backtest(df, transaction_cost=0)
    assert result.daily["strategy_return"].tolist() == pytest.approx([0, 0.10, 0.10])

def test_total_return_compounds():
    metrics = compute_metrics(pd.Series([0.10, 0.10]), [])
    assert metrics["total_return"] == pytest.approx(0.21)

def test_max_drawdown_uses_equity_curve():
    metrics = compute_metrics(pd.Series([0.10, -0.20, 0.05]), [])
    assert metrics["max_drawdown"] == pytest.approx(-0.20)
```

- [ ] **Step 2: Run tests and verify failures against same-close, additive behavior**

Run: `python -m pytest tests/test_backtesting.py tests/test_statistics_analysis.py -v`
Expected: FAIL on missing API and incorrect existing semantics.

- [ ] **Step 3: Implement daily position/equity model and robust inference**

Generate persistent positions from RSI thresholds, shift position by one period before multiplying by close-to-close returns, subtract costs on position changes, compound with `(1 + returns).cumprod()`, compute annualized Sharpe and percentage drawdown, and derive completed/marked trades. Use statsmodels HAC covariance for the daily mean and a seeded moving-block bootstrap.

- [ ] **Step 4: Run analytics tests**

Run: `python -m pytest tests/test_backtesting.py tests/test_statistics_analysis.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backtesting.py statistics_analysis.py tests/test_backtesting.py tests/test_statistics_analysis.py
git commit -m "fix: remove backtest bias and correct metrics"
```

### Task 5: Provider fallback and truthful ingestion

**Files:**
- Modify: `alpha_vantage.py`
- Modify: `ingest.py`
- Create: `tests/test_providers.py`
- Create: `tests/test_ingest.py`

**Interfaces:**
- Produces: `fetch_daily(ticker: str, api_key: str, timeout: float = 15) -> pd.DataFrame`.
- Produces: `fetch_prices(ticker, primary_fetcher, fallback_fetcher=None, max_retries=3) -> pd.DataFrame`.
- Produces: `ingest_stock(engine, ticker: str, frame: pd.DataFrame | None = None) -> int`.

- [ ] **Step 1: Write failing provider and row-count tests**

```python
def test_fallback_runs_after_primary_retries():
    primary = FailingFetcher()
    result = fetch_prices("AAPL", primary, lambda _: sample_prices(), max_retries=3)
    assert primary.calls == 3
    assert len(result) == len(sample_prices())

def test_ingestion_reports_database_affected_rows(fake_engine):
    assert ingest_stock(fake_engine, "AAPL", sample_prices()) == len(sample_prices())
```

- [ ] **Step 2: Run tests and verify missing fallback/count behavior**

Run: `python -m pytest tests/test_providers.py tests/test_ingest.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement bounded requests, normalized fallback data, bulk upserts, and run status**

Use request timeouts and `raise_for_status`, reject Alpha Vantage `Error Message`/`Note`, normalize columns, perform PostgreSQL bulk upserts, count returned affected rows, and record run start/completion/status around the ticker loop.

- [ ] **Step 4: Run ingestion tests**

Run: `python -m pytest tests/test_providers.py tests/test_ingest.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add alpha_vantage.py ingest.py tests/test_providers.py tests/test_ingest.py
git commit -m "fix: add resilient providers and truthful ingestion"
```

### Task 6: Reliable scheduler and migration

**Files:**
- Modify: `scheduler.py`
- Modify: `migrate_to_railway.py`
- Create: `tests/test_scheduler.py`
- Create: `tests/test_migration.py`

**Interfaces:**
- Produces: `run_pipeline(runner=subprocess.run, timeout: int = 3600) -> bool`.
- Produces: `reset_sequences(engine) -> None` and idempotent `migrate(local_engine, remote_engine) -> MigrationSummary`.

- [ ] **Step 1: Write failing orchestration and sequence tests**

```python
def test_pipeline_runs_ingestion_then_indicators(recording_runner):
    assert run_pipeline(recording_runner)
    assert recording_runner.scripts == ["ingest.py", "indicators.py"]

def test_pipeline_stops_when_ingestion_fails(failing_runner):
    assert not run_pipeline(failing_runner)
    assert failing_runner.scripts == ["ingest.py"]
```

- [ ] **Step 2: Run tests and verify current ingestion-only behavior fails**

Run: `python -m pytest tests/test_scheduler.py tests/test_migration.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement sequential pipeline and idempotent migration**

Use `sys.executable`, paths relative to `__file__`, checked subprocesses, configurable timeout, target time, and timezone. Apply schema before migration, upsert tables in dependency order, and execute `setval(pg_get_serial_sequence(...), max(id), true)` for serial tables.

- [ ] **Step 4: Run orchestration and migration tests**

Run: `python -m pytest tests/test_scheduler.py tests/test_migration.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scheduler.py migrate_to_railway.py tests/test_scheduler.py tests/test_migration.py
git commit -m "fix: orchestrate complete daily pipeline"
```

### Task 7: Resilient dashboard and honest documentation

**Files:**
- Modify: `dashboard.py`
- Modify: `README.md`
- Modify: `results.md`
- Modify: `CLAUDE.md`
- Modify: `.devcontainer/devcontainer.json`
- Modify: `notebooks/rsi_significance.ipynb`
- Create: `tests/test_documentation.py`

**Interfaces:**
- Consumes: corrected backtest and database APIs.
- Produces: cached, empty-safe dashboard behavior and documentation aligned to verified semantics.

- [ ] **Step 1: Write failing documentation assertions**

```python
def test_readme_does_not_publish_stale_headline_results(readme):
    assert "870.5%" not in readme
    assert "historical, pre-correction" in readme.lower()

def test_devcontainer_keeps_streamlit_security_enabled(devcontainer_text):
    assert "enableXsrfProtection false" not in devcontainer_text
```

- [ ] **Step 2: Run tests and verify stale claims and unsafe flags fail**

Run: `python -m pytest tests/test_documentation.py -v`
Expected: FAIL.

- [ ] **Step 3: Add cached data access, empty/error guards, corrected metrics, and aligned docs/notebook**

Cache the engine as a resource, cache query functions with short TTLs, stop rendering after configuration/query errors, guard empty frames before `.iloc`, and display compounded metrics. Replace notebook independent bootstrap with shared HAC/block-bootstrap helpers. Label old results as invalidated pending recomputation, document Python 3.11 and public Streamlit settings, and remove disabled security flags.

- [ ] **Step 4: Run documentation tests and compile source**

Run: `python -m pytest tests/test_documentation.py -v && python -m compileall -q .`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dashboard.py README.md results.md CLAUDE.md .devcontainer notebooks tests/test_documentation.py
git commit -m "docs: align dashboard and project claims"
```

### Task 8: Continuous integration and final verification

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/daily-pipeline.yml`
- Create: `tests/test_ci_config.py`

**Interfaces:**
- Produces: PR/push CI and an opt-in scheduled production pipeline using repository secrets.

- [ ] **Step 1: Write failing CI configuration test**

```python
def test_ci_runs_lint_tests_compile_and_schema(ci_text):
    for command in ["ruff check .", "pytest", "compileall", "psql"]:
        assert command in ci_text
```

- [ ] **Step 2: Run the test and verify workflows are missing**

Run: `python -m pytest tests/test_ci_config.py -v`
Expected: FAIL.

- [ ] **Step 3: Add CI and scheduled pipeline workflows**

Use Python 3.11, dependency caching, PostgreSQL 16 service health checks, `ruff check .`, `pytest`, `compileall`, and `psql -f schema.sql` in CI. The daily workflow uses `DB_URL` and optional `ALPHA_VANTAGE_KEY` secrets, runs `ingest.py`, then `indicators.py`, and supports manual dispatch.

- [ ] **Step 4: Run the full verification suite**

Run: `ruff check . && pytest -v && python -m compileall -q . && git diff --check`
Expected: all commands exit 0.

- [ ] **Step 5: Review repository state and commit**

```bash
git add .github tests/test_ci_config.py
git commit -m "ci: validate and schedule analytics pipeline"
git status --short --branch
```
