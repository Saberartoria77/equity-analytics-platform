"""Robust statistical helpers for strategy-return analysis."""

from __future__ import annotations

from math import ceil

import numpy as np
import pandas as pd
import statsmodels.api as sm


def _clean_values(values: pd.Series) -> np.ndarray:
    cleaned = pd.Series(values, dtype=float).dropna().to_numpy()
    cleaned = cleaned[np.isfinite(cleaned)]
    if cleaned.size == 0:
        raise ValueError("values must contain at least one finite observation")
    return cleaned


def hac_mean_test(
    values: pd.Series, max_lags: int | None = None
) -> dict[str, float | int]:
    """Test whether a time-series mean differs from zero using HAC errors."""
    cleaned = _clean_values(values)
    if cleaned.size == 1:
        return {
            "mean": float(cleaned[0]),
            "standard_error": 0.0,
            "t_stat": 0.0,
            "p_value": 1.0,
            "n": 1,
        }
    if max_lags is None:
        max_lags = max(1, int(4 * (cleaned.size / 100) ** (2 / 9)))
    model = sm.OLS(cleaned, np.ones((cleaned.size, 1))).fit(
        cov_type="HAC", cov_kwds={"maxlags": min(max_lags, cleaned.size - 1)}
    )
    return {
        "mean": float(model.params[0]),
        "standard_error": float(model.bse[0]),
        "t_stat": float(model.tvalues[0]),
        "p_value": float(model.pvalues[0]),
        "n": int(cleaned.size),
    }


def moving_block_bootstrap_mean(
    values: pd.Series,
    block_size: int = 20,
    samples: int = 10_000,
    seed: int = 42,
) -> dict[str, float]:
    """Estimate a mean confidence interval while retaining local dependence."""
    cleaned = _clean_values(values)
    if block_size < 1 or block_size > cleaned.size:
        raise ValueError("block_size must be between 1 and the number of observations")
    if samples < 1:
        raise ValueError("samples must be positive")

    rng = np.random.default_rng(seed)
    circular = np.concatenate([cleaned, cleaned[: block_size - 1]])
    blocks_needed = ceil(cleaned.size / block_size)
    bootstrap_means = np.empty(samples)
    for sample_index in range(samples):
        starts = rng.integers(0, cleaned.size, size=blocks_needed)
        sample = np.concatenate(
            [circular[start : start + block_size] for start in starts]
        )[: cleaned.size]
        bootstrap_means[sample_index] = sample.mean()

    lower, upper = np.percentile(bootstrap_means, [2.5, 97.5])
    return {
        "mean": float(cleaned.mean()),
        "lower": float(lower),
        "upper": float(upper),
    }
