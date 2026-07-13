import pytest

from database import get_database_url


def test_explicit_database_url_takes_precedence(monkeypatch):
    monkeypatch.setenv("DB_URL", "postgresql://environment")

    assert get_database_url("postgresql://explicit") == "postgresql://explicit"


def test_missing_database_url_has_actionable_error(monkeypatch):
    monkeypatch.delenv("DB_URL", raising=False)

    with pytest.raises(RuntimeError, match="DB_URL is required"):
        get_database_url()
