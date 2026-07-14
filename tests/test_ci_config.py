from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_ci_runs_lint_tests_compile_and_schema():
    ci = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    for expected in [
        "python-version: \"3.11\"",
        "postgres:16",
        "ruff check .",
        "python -m pytest",
        "compileall",
        "psql",
    ]:
        assert expected in ci
    assert "ON_ERROR_STOP=1" in ci
    assert "tests/fixtures/legacy_schema.sql" in ci


def test_daily_workflow_runs_the_complete_pipeline():
    workflow = (PROJECT_ROOT / ".github/workflows/daily-pipeline.yml").read_text(
        encoding="utf-8"
    )

    assert "workflow_dispatch:" in workflow
    assert "schedule:" in workflow
    assert "secrets.DB_URL" in workflow
    assert 'psql "$DB_URL" -v ON_ERROR_STOP=1 -f schema.sql' in workflow
    assert "python ingest.py" in workflow
    assert "python indicators.py" in workflow

    migration = workflow.index('psql "$DB_URL" -v ON_ERROR_STOP=1 -f schema.sql')
    ingestion = workflow.index("python ingest.py")
    assert migration < ingestion
