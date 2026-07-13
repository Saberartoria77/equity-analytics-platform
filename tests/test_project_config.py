from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_runtime_requirements_are_utf8_and_minimal():
    requirements = (PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8")

    assert "streamlit" in requirements
    assert "SQLAlchemy" in requirements
    assert "ipykernel" not in requirements
    assert "debugpy" not in requirements


def test_project_targets_python_311_or_newer():
    config = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert 'target-version = "py311"' in config
