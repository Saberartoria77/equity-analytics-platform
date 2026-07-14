import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def read(path):
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def test_readme_labels_old_results_as_invalidated():
    readme = read("README.md").lower()

    assert "historical, pre-correction" in readme
    assert "870.5%" not in readme
    assert "python 3.11" in readme


def test_results_do_not_present_stale_numbers_as_current():
    results = read("results.md").lower()

    assert "invalidated" in results
    assert "101.1%" not in results


def test_devcontainer_keeps_streamlit_security_enabled():
    devcontainer = read(".devcontainer/devcontainer.json")

    assert "enableXsrfProtection false" not in devcontainer
    assert "enableCORS false" not in devcontainer


def test_notebook_uses_robust_shared_statistics():
    notebook = json.loads(read("notebooks/rsi_significance.ipynb"))
    source = "\n".join(
        "".join(cell.get("source", [])) for cell in notebook.get("cells", [])
    )

    assert "hac_mean_test" in source
    assert "moving_block_bootstrap_mean" in source
    assert "stats.ttest_1samp" not in source


def test_readme_documents_premium_adjusted_fallback():
    readme = read("README.md").lower()

    assert "premium alpha vantage" in readme
    assert "min_ingest_success_rate" in readme


def test_readme_separates_setup_from_everyday_dashboard_launch():
    readme = read("README.md").lower()

    assert "one-time setup" in readme
    assert "everyday dashboard shortcut" in readme
    assert "source .venv/bin/activate && db_url=" in readme
    assert "you do not need to recreate" in readme
