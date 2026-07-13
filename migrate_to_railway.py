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


def migrate_table(local_engine: Engine, remote_engine: Engine, table: str) -> int:
    """Copy one known table by primary key, updating existing rows."""
    if table not in TABLES:
        raise ValueError(f"Unsupported migration table: {table}")
    frame = normalize_migration_frame(
        table,
        pd.read_sql(text(f"SELECT * FROM {table} ORDER BY id"), local_engine),
    )
    if frame.empty:
        return 0
    columns = frame.columns.tolist()
    assignments = ", ".join(
        f"{column} = EXCLUDED.{column}" for column in columns if column != "id"
    )
    statement = text(
        f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES ({", ".join(f":{column}" for column in columns)})
        ON CONFLICT (id) DO UPDATE SET {assignments}
        """
    )
    records = [_clean_record(record) for record in frame.to_dict(orient="records")]
    with remote_engine.begin() as connection:
        connection.execute(statement, records)
    return len(records)


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
    """Apply schema, copy tables in dependency order, and repair sequences."""
    apply_schema(remote_engine)
    counts = {
        table: migrate_table(local_engine, remote_engine, table) for table in TABLES
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
