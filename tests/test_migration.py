import pandas as pd

from migrate_to_railway import normalize_migration_frame, reset_sequences


class RecordingConnection:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(str(statement))


class BeginContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class RecordingEngine:
    def __init__(self):
        self.connection = RecordingConnection()

    def begin(self):
        return BeginContext(self.connection)


def test_reset_sequences_repairs_every_serial_table():
    engine = RecordingEngine()

    reset_sequences(engine)

    sql = "\n".join(engine.connection.statements)
    for table in ["stocks", "daily_prices", "indicators", "ingestion_runs"]:
        assert f"pg_get_serial_sequence('{table}', 'id')" in sql
        assert f"FROM {table}" in sql


def test_legacy_ingestion_runs_are_mapped_to_new_columns():
    legacy = pd.DataFrame(
        {
            "id": [1],
            "run_at": [pd.Timestamp("2025-01-02T03:04:05Z")],
            "tickers_attempted": [100],
            "tickers_succeeded": [99],
            "rows_inserted": [125_000],
            "errors": ["one failed"],
        }
    )

    normalized = normalize_migration_frame("ingestion_runs", legacy)

    assert "run_at" not in normalized
    assert "rows_inserted" not in normalized
    assert normalized.loc[0, "started_at"] == legacy.loc[0, "run_at"]
    assert normalized.loc[0, "completed_at"] == legacy.loc[0, "run_at"]
    assert normalized.loc[0, "rows_affected"] == 125_000
    assert normalized.loc[0, "status"] == "partial"
