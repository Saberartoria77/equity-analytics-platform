import pandas as pd
import pytest

from alpha_vantage import AlphaVantageError, fetch_daily
from ingest import fetch_prices


class FailingFetcher:
    def __init__(self):
        self.calls = 0

    def __call__(self, ticker):
        self.calls += 1
        raise RuntimeError(f"primary failed for {ticker}")


def sample_prices():
    return pd.DataFrame(
        {
            "Open": [100.0],
            "High": [102.0],
            "Low": [99.0],
            "Close": [101.0],
            "Volume": [1_000],
        },
        index=pd.to_datetime(["2025-01-02"]),
    )


def test_fallback_runs_after_primary_retries():
    primary = FailingFetcher()

    result = fetch_prices(
        "AAPL",
        primary_fetcher=primary,
        fallback_fetcher=lambda ticker: sample_prices(),
        max_retries=3,
        sleep_fn=lambda _: None,
    )

    assert primary.calls == 3
    pd.testing.assert_frame_equal(result, sample_prices())


def test_primary_failure_is_raised_without_fallback():
    primary = FailingFetcher()

    with pytest.raises(RuntimeError, match="primary failed"):
        fetch_prices(
            "AAPL",
            primary_fetcher=primary,
            max_retries=2,
            sleep_fn=lambda _: None,
        )


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.status_checked = False

    def raise_for_status(self):
        self.status_checked = True

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.request = None

    def get(self, url, params, timeout):
        self.request = {"url": url, "params": params, "timeout": timeout}
        return self.response


def test_alpha_vantage_normalizes_daily_prices_and_uses_timeout():
    response = FakeResponse(
        {
            "Time Series (Daily)": {
                "2025-01-02": {
                    "1. open": "100",
                    "2. high": "102",
                    "3. low": "99",
                    "4. close": "101",
                    "5. volume": "1000",
                }
            }
        }
    )
    session = FakeSession(response)

    result = fetch_daily("AAPL", "secret", timeout=7, session=session)

    assert response.status_checked is True
    assert session.request["timeout"] == 7
    assert "secret" not in session.request["url"]
    assert result.iloc[0].to_dict() == {
        "Open": 100.0,
        "High": 102.0,
        "Low": 99.0,
        "Close": 101.0,
        "Volume": 1000.0,
    }


def test_alpha_vantage_rejects_rate_limit_payload():
    session = FakeSession(FakeResponse({"Note": "rate limit reached"}))

    with pytest.raises(AlphaVantageError, match="rate limit"):
        fetch_daily("AAPL", "secret", session=session)
