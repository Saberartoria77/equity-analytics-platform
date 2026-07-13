"""Alpha Vantage market-data fallback."""

from __future__ import annotations

import pandas as pd
import requests

API_URL = "https://www.alphavantage.co/query"


class AlphaVantageError(RuntimeError):
    """Raised when Alpha Vantage returns an API-level failure."""


def fetch_daily(
    ticker: str,
    api_key: str,
    timeout: float = 15,
    session=requests,
) -> pd.DataFrame:
    """Fetch and normalize daily OHLCV data for one ticker."""
    if not api_key:
        raise AlphaVantageError("ALPHA_VANTAGE_KEY is required for fallback requests")

    response = session.get(
        API_URL,
        params={
            "function": "TIME_SERIES_DAILY",
            "symbol": ticker,
            "outputsize": "full",
            "apikey": api_key,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    for error_key in ("Error Message", "Note", "Information"):
        if error_key in payload:
            raise AlphaVantageError(str(payload[error_key]))

    time_series = payload.get("Time Series (Daily)")
    if not time_series:
        raise AlphaVantageError(f"No daily time series returned for {ticker}")

    rows = []
    for date, values in time_series.items():
        rows.append(
            {
                "date": pd.Timestamp(date),
                "Open": float(values["1. open"]),
                "High": float(values["2. high"]),
                "Low": float(values["3. low"]),
                "Close": float(values["4. close"]),
                "Volume": float(values["5. volume"]),
            }
        )
    return pd.DataFrame(rows).set_index("date").sort_index()
