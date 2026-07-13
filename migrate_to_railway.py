"""Idempotently migrate a local database into a schema-compatible remote database."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from database import create_db_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parent
TABLES = ("stocks", "daily_prices", "indicators", "ingestion_runs")


@dataclass(frozen=True)
class MigrationSummary:
    rows_by_table: dict[str, int]

    @property
    def total_rows(self) -> int:
        return sum(self.rows_by_table.values())


def apply_schema(engine: Engine) -> None:
    """Apply the complete PostgreSQL schema in one driver transaction."""
    schema = (PROJECT_ROOT / "schema.sql").read_text(encoding="utf-8")
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(schema)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _clean_record(record: dict[str, object]) -> dict[str, object]:
    return {
        key: None if pd.isna(value) else value
        for key, value in record.items()
    }


def normalize_migration_frame(table: str, frame: pd.DataFrame) -> pd.DataFrame:
    """Map legacy table columns onto the canonical schema."""
    normalized = frame.copy(deep=True)
    if table != "ingestion_runs":
        return normalized

    if "started_at" not in normalized and "run_at" in normalized:
        normalized["started_at"] = normalized["run_at"]
    if "completed_at" not in normalized:
        normalized["completed_at"] = normalized.get("run_at", normalized["started_at"])
    if "rows_affected" not in normalized:
        normalized["rows_affected"] = normalized.get("rows_inserted", 0)
    if "status" not in normalized:
        has_error = normalized["errors"].fillna("").astype(str).str.len() > 0
        normalized["status"] = has_error.map({True: "partial", False: "completed"})
    return normalized.drop(columns=["run_at", "rows_inserted"], errors="ignore")


def natural_key_for(table: str) -> tuple[str, ...]:
    """Return the stable conflict key used by a migrated market-data table."""
    keys = {
        "stocks": ("ticker",),
        "daily_prices": ("stock_id", "date"),
        "indicators": ("stock_id", "date"),
    }
    try:
        return keys[table]
    except KeyError:
        raise ValueError(f"No natural migration key for {table}") from None


def migrate_stocks(local_engine: Engine, remote_engine: Engine) -> tuple[int, dict[int, int]]:
    """Upsert stocks by ticker and return source-to-destination ID mapping."""
    frame = pd.read_sql(text("SELECT * FROM stocks ORDER BY id"), local_engine)
    if frame.empty:
        return 0, {}
    statement = text(
        """
        INSERT INTO stocks (ticker, name, sector, industry, created_at)
        VALUES (:ticker, :name, :sector, :industry, COALESCE(:created_at, CURRENT_TIMESTAMP))
        ON CONFLICT (ticker) DO UPDATE SET
            name = EXCLUDED.name,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry
        RETURNING id
        """
    )
    stock_id_map: dict[int, int] = {}
    with remote_engine.begin() as connection:
        for record in frame.to_dict(orient="records"):
            cleaned = _clean_record(record)
            source_id = int(cleaned.pop("id"))
            parameters = {
                "ticker": cleaned["ticker"],
                "name": cleaned.get("name"),
                "sector": cleaned.get("sector"),
                "industry": cleaned.get("industry"),
                "created_at": cleaned.get("created_at"),
            }
            stock_id_map[source_id] = int(
                connection.execute(statement, parameters).scalar_one()
            )
    return len(frame), stock_id_map


def migrate_market_table(
    local_engine: Engine,
    remote_engine: Engine,
    table: str,
    stock_id_map: dict[int, int],
) -> int:
    """Upsert prices or indicators by remapped stock/date natural key."""
    key_columns = natural_key_for(table)
    if table == "stocks":
        raise ValueError("Use migrate_stocks for the stocks table")
    frame = pd.read_sql(text(f"SELECT * FROM {table} ORDER BY id"), local_engine)
    if frame.empty:
        return 0
    frame = frame.drop(columns=["id"], errors="ignore")
    frame["stock_id"] = frame["stock_id"].map(stock_id_map)
    if frame["stock_id"].isna().any():
        raise ValueError(f"{table} contains a stock_id absent from the stocks migration")
    frame["stock_id"] = frame["stock_id"].astype(int)
    columns = frame.columns.tolist()
    assignments = ", ".join(
        f"{column} = EXCLUDED.{column}"
        for column in columns
        if column not in key_columns
    )
    statement = text(
        f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES ({", ".join(f":{column}" for column in columns)})
        ON CONFLICT ({", ".join(key_columns)}) DO UPDATE SET {assignments}
        """
    )
    records = [_clean_record(record) for record in frame.to_dict(orient="records")]
    with remote_engine.begin() as connection:
        connection.execute(statement, records)
    return len(records)


def replace_ingestion_runs(local_engine: Engine, remote_engine: Engine) -> int:
    """Replace remote run history explicitly; it has no stable natural key."""
    frame = normalize_migration_frame(
        "ingestion_runs",
        pd.read_sql(text("SELECT * FROM ingestion_runs ORDER BY id"), local_engine),
    )
    with remote_engine.begin() as connection:
        connection.execute(text("DELETE FROM ingestion_runs"))
        if frame.empty:
            return 0
        columns = frame.columns.tolist()
        statement = text(
            f"""
            INSERT INTO ingestion_runs ({", ".join(columns)})
            VALUES ({", ".join(f":{column}" for column in columns)})
            """
        )
        records = [_clean_record(record) for record in frame.to_dict(orient="records")]
        connection.execute(statement, records)
    return len(frame)


def reset_sequences(engine: Engine) -> None:
    """Advance serial sequences after rows with explicit IDs are copied."""
    with engine.begin() as connection:
        for table in TABLES:
            connection.execute(
                text(
                    f"""
                    SELECT setval(
                        pg_get_serial_sequence('{table}', 'id'),
                        COALESCE(MAX(id), 1),
                        MAX(id) IS NOT NULL
                    )
                    FROM {table}
                    """
                )
            )


def migrate(local_engine: Engine, remote_engine: Engine) -> MigrationSummary:
    """Apply schema and migrate by natural keys with explicit history replacement."""
    apply_schema(remote_engine)
    stock_count, stock_id_map = migrate_stocks(local_engine, remote_engine)
    counts = {
        "stocks": stock_count,
        "daily_prices": migrate_market_table(
            local_engine, remote_engine, "daily_prices", stock_id_map
        ),
        "indicators": migrate_market_table(
            local_engine, remote_engine, "indicators", stock_id_map
        ),
        "ingestion_runs": replace_ingestion_runs(local_engine, remote_engine),
    }
    reset_sequences(remote_engine)
    return MigrationSummary(counts)


def main() -> int:
    load_dotenv()
    remote_url = os.getenv("RAILWAY_URL")
    if not remote_url:
        raise RuntimeError("RAILWAY_URL is required for the migration destination")
    summary = migrate(create_db_engine(), create_db_engine(remote_url))
    print(f"Migration complete: {summary.total_rows} rows copied or updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
