"""Compute and persist technical indicators."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from database import create_db_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_prices(engine: Engine, stock_id: int) -> pd.DataFrame:
    """Load ordered closing prices for one stock."""
    return pd.read_sql(
        text("SELECT date, close FROM daily_prices WHERE stock_id = :sid ORDER BY date"),
        engine,
        params={"sid": stock_id},
    )


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Return a date-sorted copy with conventional technical indicators."""
    required = {"date", "close"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    result = df.copy(deep=True).sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(result["close"], errors="raise")

    result["sma_20"] = close.rolling(window=20, min_periods=20).mean()
    result["sma_50"] = close.rolling(window=50, min_periods=50).mean()
    result["sma_200"] = close.rolling(window=200, min_periods=200).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    relative_strength = avg_gain / avg_loss.replace(0, float("nan"))
    result["rsi_14"] = 100 - (100 / (1 + relative_strength))
    result.loc[(avg_loss == 0) & (avg_gain > 0), "rsi_14"] = 100.0
    result.loc[(avg_loss == 0) & (avg_gain == 0), "rsi_14"] = 50.0

    ema_12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema_26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    result["macd_line"] = ema_12 - ema_26
    result["macd_signal"] = result["macd_line"].ewm(
        span=9, adjust=False, min_periods=9
    ).mean()
    result["macd_histogram"] = result["macd_line"] - result["macd_signal"]

    rolling_std = close.rolling(window=20, min_periods=20).std()
    result["bb_middle"] = result["sma_20"]
    result["bb_upper"] = result["bb_middle"] + 2 * rolling_std
    result["bb_lower"] = result["bb_middle"] - 2 * rolling_std
    return result


def save_indicators(engine: Engine, stock_id: int, df: pd.DataFrame) -> int:
    """Transactionally replace one stock's complete valid indicator set."""
    ready = df.dropna(subset=["sma_200"]).copy()
    columns = [
        "sma_20",
        "sma_50",
        "sma_200",
        "rsi_14",
        "macd_line",
        "macd_signal",
        "macd_histogram",
        "bb_upper",
        "bb_middle",
        "bb_lower",
    ]
    records = [
        {
            "sid": stock_id,
            "date": pd.Timestamp(row["date"]).date().isoformat(),
            **{column: float(row[column]) for column in columns},
        }
        for _, row in ready.iterrows()
    ]
    assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns)
    record_columns = ", ".join(
        f"{column} DOUBLE PRECISION" for column in columns
    )
    statement = text(
        f"""
        INSERT INTO indicators (
            stock_id, date, {", ".join(columns)}
        )
        SELECT
            :sid,
            computed.date,
            {", ".join(f"computed.{column}" for column in columns)}
        FROM jsonb_to_recordset(CAST(:indicators_json AS jsonb)) AS computed(
            date DATE,
            {record_columns}
        )
        ON CONFLICT (stock_id, date) DO UPDATE SET {assignments}
        """
    )
    with engine.begin() as connection:
        connection.execute(
            text("DELETE FROM indicators WHERE stock_id = :sid"), {"sid": stock_id}
        )
        if ready.empty:
            return 0
        result = connection.execute(
            statement,
            {"sid": stock_id, "indicators_json": json.dumps(records)},
        )
    return result.rowcount


def run(engine: Engine) -> tuple[int, list[str]]:
    """Recompute every stock and return affected rows plus failures."""
    stock_ids = pd.read_sql(text("SELECT id FROM stocks ORDER BY id"), engine)["id"].tolist()
    total = 0
    errors: list[str] = []
    for stock_id in stock_ids:
        try:
            frame = compute_indicators(get_prices(engine, stock_id))
            affected = save_indicators(engine, stock_id, frame)
            total += affected
            logger.info("Saved %s indicator rows for stock_id %s", affected, stock_id)
        except Exception as error:
            logger.exception("Failed stock_id %s", stock_id)
            errors.append(f"stock_id {stock_id}: {error}")
    return total, errors


def main() -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    total, errors = run(create_db_engine())
    logger.info(
        "Indicator refresh complete: %s rows affected, %s errors", total, len(errors)
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
