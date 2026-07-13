import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "alpha_vantage",
        "backtesting",
        "dashboard",
        "database",
        "indicators",
        "ingest",
        "migrate_to_railway",
        "scheduler",
        "statistics_analysis",
        "update_sectors",
    ],
)
def test_modules_import_without_database_credentials(module):
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        env={"PATH": ""},
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
