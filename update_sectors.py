"""Refresh stock sector and industry metadata."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import text

from database import create_db_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def update_sectors(engine: Engine) -> tuple[int, list[str]]:
    """Update metadata for all stocks and return successes and errors."""
    with engine.begin() as connection:
        tickers = connection.execute(text("SELECT id, ticker FROM stocks")).fetchall()

    succeeded = 0
    errors: list[str] = []
    for stock_id, ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            with engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        UPDATE stocks
                        SET sector = :sector, industry = :industry
                        WHERE id = :id
                        """
                    ),
                    {
                        "sector": info.get("sector"),
                        "industry": info.get("industry"),
                        "id": stock_id,
                    },
                )
            succeeded += 1
            logger.info("Updated metadata for %s", ticker)
        except Exception as error:
            logger.exception("Failed metadata refresh for %s", ticker)
            errors.append(f"{ticker}: {error}")
    return succeeded, errors


def main() -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    succeeded, errors = update_sectors(create_db_engine())
    logger.info("Metadata refresh complete: %s succeeded, %s errors", succeeded, len(errors))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
