"""Database configuration helpers with no import-time connection side effects."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def get_database_url(explicit_url: str | None = None) -> str:
    """Return a configured database URL or raise an actionable error."""
    url = explicit_url or os.getenv("DB_URL")
    if not url:
        raise RuntimeError(
            "DB_URL is required; set it in the environment or Streamlit secrets."
        )
    return url


def create_db_engine(explicit_url: str | None = None) -> Engine:
    """Create a resilient SQLAlchemy engine only when database access is needed."""
    from sqlalchemy import create_engine

    return create_engine(get_database_url(explicit_url), pool_pre_ping=True)
