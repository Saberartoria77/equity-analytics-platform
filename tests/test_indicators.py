import numpy as np
import pandas as pd
import pytest

from indicators import compute_indicators


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
