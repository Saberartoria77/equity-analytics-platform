"""Resilient and observable equity-price ingestion."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import text

from alpha_vantage import fetch_daily as fetch_alpha_vantage
from database import create_db_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "ASML",
    "AMD", "QCOM", "TXN", "MU", "AMAT", "INTC", "ADI", "KLAC", "LRCX", "MRVL",
    "CRM", "ADBE", "NOW", "UBER", "PANW", "CRWD", "ABNB", "SHOP", "ANET", "SNAP",
    "PLTR", "NET", "DDOG", "ZS", "MDB", "SNOW", "COIN", "DASH", "RBLX", "PINS",
    "TTD", "TEAM", "HUBS", "VEEV", "BILL", "OKTA", "ZM", "ROKU", "HOOD", "SOFI",
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW", "AXP", "USB", "PNC",
    "TFC", "COF", "BK", "STT", "FITB", "HBAN", "CFG", "RF", "KEY", "V", "MA",
    "PYPL", "FIS", "FI", "ICE", "CME", "MCO", "SPGI", "MSCI", "AIG", "MET", "PRU",
    "AFL", "TRV", "PGR", "ALL", "CB", "CINF", "AON", "BRO", "WTW", "RJF", "LPLA",
    "IBKR", "NDAQ", "CBOE", "FDS", "MKTX", "VIRT",
]


def fetch_yahoo(ticker: str) -> pd.DataFrame:
    """Fetch five years of adjusted daily data from Yahoo Finance."""
    frame = yf.Ticker(ticker).history(period="5y", auto_adjust=True, timeout=20)
    if frame.empty:
        raise ValueError(f"Empty Yahoo Finance data for {ticker}")
    return frame


def fetch_prices(
    ticker: str,
    primary_fetcher: Callable[[str], pd.DataFrame] = fetch_yahoo,
    fallback_fetcher: Callable[[str], pd.DataFrame] | None = None,
    max_retries: int = 3,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> pd.DataFrame:
    """Fetch prices with exponential retry and an optional final fallback."""
    if max_retries < 1:
        raise ValueError("max_retries must be positive")
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            frame = primary_fetcher(ticker)
            if frame.empty:
                raise ValueError(f"Empty primary data for {ticker}")
            return frame
        except Exception as error:
            last_error = error
            if attempt < max_retries - 1:
                delay = 2**attempt
                logger.warning(
                    "Primary attempt %s/%s failed for %s: %s; retrying in %ss",
                    attempt + 1,
                    max_retries,
                    ticker,
                    error,
                    delay,
                )
                sleep_fn(delay)

    if fallback_fetcher is not None:
        logger.warning("Primary provider exhausted for %s; using Alpha Vantage", ticker)
        return fallback_fetcher(ticker)
    assert last_error is not None
    raise last_error


def _price_records(stock_id: int, frame: pd.DataFrame) -> list[dict[str, object]]:
    required = {"Open", "High", "Low", "Close", "Volume"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing price columns: {', '.join(sorted(missing))}")
    records = []
    for date, row in frame.sort_index().iterrows():
        records.append(
            {
                "stock_id": stock_id,
                "date": pd.Timestamp(date).date(),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
            }
        )
    return records


def ingest_stock(engine: Engine, ticker: str, frame: pd.DataFrame) -> int:
    """Upsert one ticker's prices and return the database affected-row count."""
    if frame.empty:
        return 0
    with engine.begin() as connection:
        stock_id = connection.execute(
            text(
                """
                INSERT INTO stocks (ticker) VALUES (:ticker)
                ON CONFLICT (ticker) DO UPDATE SET ticker = EXCLUDED.ticker
                RETURNING id
                """
            ),
            {"ticker": ticker},
        ).scalar_one()
        records = _price_records(stock_id, frame)
        result = connection.execute(
            text(
                """
                INSERT INTO daily_prices (stock_id, date, open, high, low, close, volume)
                VALUES (:stock_id, :date, :open, :high, :low, :close, :volume)
                ON CONFLICT (stock_id, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """
            ),
            records,
        )
    return result.rowcount


def _start_run(engine: Engine, attempted: int) -> int:
    with engine.begin() as connection:
        return connection.execute(
            text(
                """
                INSERT INTO ingestion_runs (tickers_attempted, status)
                VALUES (:attempted, 'running') RETURNING id
                """
            ),
            {"attempted": attempted},
        ).scalar_one()


def _finish_run(
    engine: Engine,
    run_id: int,
    succeeded: int,
    affected: int,
    errors: list[str],
) -> None:
    status = "completed" if not errors else "partial"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE ingestion_runs
                SET completed_at = CURRENT_TIMESTAMP,
                    status = :status,
                    tickers_succeeded = :succeeded,
                    rows_affected = :affected,
                    errors = :errors
                WHERE id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "succeeded": succeeded,
                "affected": affected,
                "errors": "; ".join(errors) if errors else None,
            },
        )


def run_ingestion(engine: Engine, tickers: list[str] | None = None) -> tuple[int, list[str]]:
    """Run ingestion for a universe and return affected rows and errors."""
    selected = tickers or TICKERS
    run_id = _start_run(engine, len(selected))
    api_key = os.getenv("ALPHA_VANTAGE_KEY")
    fallback = (
        (lambda ticker: fetch_alpha_vantage(ticker, api_key)) if api_key else None
    )
    succeeded = 0
    affected = 0
    errors: list[str] = []
    for ticker in selected:
        try:
            frame = fetch_prices(ticker, fallback_fetcher=fallback)
            row_count = ingest_stock(engine, ticker, frame)
            succeeded += 1
            affected += row_count
            logger.info("Ingested %s: %s rows affected", ticker, row_count)
        except Exception as error:
            logger.exception("Failed to ingest %s", ticker)
            errors.append(f"{ticker}: {error}")
    _finish_run(engine, run_id, succeeded, affected, errors)
    return affected, errors


def main() -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    affected, errors = run_ingestion(create_db_engine())
    logger.info("Ingestion complete: %s rows affected, %s errors", affected, len(errors))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
