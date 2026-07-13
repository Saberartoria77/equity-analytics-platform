from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA = (PROJECT_ROOT / "schema.sql").read_text(encoding="utf-8")


def test_schema_contains_all_pipeline_tables():
    for table in ["stocks", "daily_prices", "indicators", "ingestion_runs"]:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in SCHEMA


def test_indicator_rows_are_unique_by_stock_and_date():
    indicator_definition = SCHEMA.split("CREATE TABLE IF NOT EXISTS indicators", 1)[1]

    assert "UNIQUE(stock_id, date)" in indicator_definition


def test_required_stock_relations_are_not_nullable_and_cascade():
    assert "stock_id INTEGER NOT NULL REFERENCES stocks(id) ON DELETE CASCADE" in SCHEMA


def test_volatility_view_uses_return_dispersion():
    volatility_definition = SCHEMA.split("rolling_volatility_view", 1)[1]

    assert "STDDEV(daily_return_pct)" in volatility_definition
    assert "STDDEV(close)" not in volatility_definition


def test_existing_indicator_table_is_retrofitted_with_constraints():
    assert "ALTER TABLE indicators ALTER COLUMN stock_id SET NOT NULL" in SCHEMA
    assert "ALTER TABLE indicators ALTER COLUMN date SET NOT NULL" in SCHEMA
    assert "ADD CONSTRAINT uq_indicators_stock_date" in SCHEMA
    assert "ADD CONSTRAINT fk_indicators_stock" in SCHEMA
