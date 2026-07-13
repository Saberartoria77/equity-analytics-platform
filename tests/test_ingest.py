import pandas as pd

from ingest import ingest_stock, ingestion_status, is_material_failure


class FakeResult:
    def __init__(self, *, scalar_value=None, rowcount=0):
        self.scalar_value = scalar_value
        self.rowcount = rowcount

    def scalar_one(self):
        return self.scalar_value


class FakeConnection:
    def __init__(self):
        self.price_records = []

    def execute(self, statement, parameters=None):
        sql = str(statement)
        if "INSERT INTO stocks" in sql:
            return FakeResult(scalar_value=7, rowcount=1)
        if "INSERT INTO daily_prices" in sql:
            self.price_records = parameters
            return FakeResult(rowcount=len(parameters))
        raise AssertionError(f"Unexpected SQL: {sql}")


class BeginContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class FakeEngine:
    def __init__(self):
        self.connection = FakeConnection()

    def begin(self):
        return BeginContext(self.connection)


def sample_prices():
    return pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "High": [102.0, 103.0],
            "Low": [99.0, 100.0],
            "Close": [101.0, 102.0],
            "Volume": [1_000, 1_100],
        },
        index=pd.to_datetime(["2025-01-02", "2025-01-03"]),
    )


def test_ingestion_reports_database_affected_rows():
    engine = FakeEngine()

    affected = ingest_stock(engine, "AAPL", sample_prices())

    assert affected == 2
    assert len(engine.connection.price_records) == 2


def test_ingestion_returns_zero_for_empty_frame():
    assert ingest_stock(FakeEngine(), "AAPL", sample_prices().iloc[:0]) == 0


def test_partial_ticker_failure_is_not_material_pipeline_failure():
    assert is_material_failure(attempted=100, succeeded=99) is False
    assert is_material_failure(attempted=100, succeeded=80) is False
    assert is_material_failure(attempted=100, succeeded=79) is True
    assert is_material_failure(attempted=100, succeeded=1) is True
    assert is_material_failure(attempted=100, succeeded=0) is True


def test_ingestion_status_persists_failed_below_threshold():
    assert ingestion_status(100, 100, False) == "completed"
    assert ingestion_status(100, 99, True) == "partial"
    assert ingestion_status(100, 10, True) == "failed"
