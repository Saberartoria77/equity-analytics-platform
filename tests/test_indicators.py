import json

import numpy as np
import pandas as pd
import pytest

import indicators
from indicators import compute_indicators, run, save_indicators


@pytest.fixture
def price_frame():
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=250, freq="B"),
            "close": np.arange(1.0, 251.0),
        }
    )


def test_sma_200_requires_200_observations(price_frame):
    short_result = compute_indicators(price_frame.iloc[:199].copy())
    complete_result = compute_indicators(price_frame.iloc[:200].copy())

    assert short_result["sma_200"].isna().all()
    assert complete_result["sma_200"].iloc[-1] == pytest.approx(100.5)


def test_compute_indicators_does_not_mutate_input(price_frame):
    original = price_frame.copy(deep=True)

    compute_indicators(price_frame)

    pd.testing.assert_frame_equal(price_frame, original)


def test_indicators_are_sorted_by_date_before_rolling(price_frame):
    reversed_frame = price_frame.iloc[::-1].reset_index(drop=True)

    result = compute_indicators(reversed_frame)

    assert result["date"].is_monotonic_increasing
    assert result["sma_20"].iloc[19] == pytest.approx(10.5)


class FakeResult:
    def __init__(self, rowcount=0):
        self.rowcount = rowcount


class RecordingConnection:
    def __init__(self):
        self.statements = []

    def execute(self, statement, parameters=None):
        self.statements.append((str(statement), parameters))
        if "INSERT INTO indicators" in str(statement):
            return FakeResult(51)
        return FakeResult()


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


def test_indicator_refresh_deletes_legacy_rows_before_insert(price_frame):
    engine = RecordingEngine()
    calculated = compute_indicators(price_frame)

    affected = save_indicators(engine, 7, calculated)

    assert affected == 51
    assert "DELETE FROM indicators WHERE stock_id = :sid" in engine.connection.statements[0][0]
    insert_sql, insert_parameters = engine.connection.statements[1]
    assert "INSERT INTO indicators" in insert_sql
    assert "jsonb_to_recordset" in insert_sql
    assert isinstance(insert_parameters, dict)
    records = json.loads(insert_parameters["indicators_json"])
    assert len(records) == 51


def test_run_reports_per_stock_failures(monkeypatch, price_frame):
    monkeypatch.setattr(
        indicators.pd,
        "read_sql",
        lambda *args, **kwargs: pd.DataFrame({"id": [1, 2]}),
    )
    monkeypatch.setattr(
        indicators,
        "get_prices",
        lambda engine, stock_id: (_ for _ in ()).throw(RuntimeError("broken"))
        if stock_id == 2
        else price_frame,
    )
    monkeypatch.setattr(indicators, "save_indicators", lambda engine, stock_id, frame: 51)

    affected, errors = run(object())

    assert affected == 51
    assert errors == ["stock_id 2: broken"]
