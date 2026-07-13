import numpy as np
import pandas as pd
import pytest

from statistics_analysis import hac_mean_test, moving_block_bootstrap_mean


def test_hac_mean_test_reports_mean_and_p_value():
    result = hac_mean_test(pd.Series([0.01, 0.02, 0.015, 0.01, 0.02]))

    assert result["mean"] == pytest.approx(0.015)
    assert 0 <= result["p_value"] <= 1
    assert result["n"] == 5


def test_moving_block_bootstrap_is_reproducible():
    values = pd.Series(np.sin(np.arange(30)) / 100)

    first = moving_block_bootstrap_mean(values, block_size=5, samples=200, seed=42)
    second = moving_block_bootstrap_mean(values, block_size=5, samples=200, seed=42)

    assert first == second
    assert first["lower"] <= first["mean"] <= first["upper"]


def test_statistics_reject_empty_input():
    with pytest.raises(ValueError, match="at least one"):
        hac_mean_test(pd.Series(dtype=float))
