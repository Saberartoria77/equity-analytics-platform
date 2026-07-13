from pathlib import Path

from scheduler import run_pipeline


class Result:
    def __init__(self, returncode=0, stderr=""):
        self.returncode = returncode
        self.stdout = ""
        self.stderr = stderr


class RecordingRunner:
    def __init__(self, returncodes=None):
        self.returncodes = list(returncodes or [0, 0])
        self.scripts = []
        self.commands = []

    def __call__(self, command, **kwargs):
        self.commands.append((command, kwargs))
        self.scripts.append(Path(command[1]).name)
        return Result(self.returncodes.pop(0))


def test_pipeline_runs_ingestion_then_indicators():
    runner = RecordingRunner()

    assert run_pipeline(runner=runner, timeout=30) is True
    assert runner.scripts == ["ingest.py", "indicators.py"]
    assert all(call[1]["timeout"] == 30 for call in runner.commands)


def test_pipeline_stops_when_ingestion_fails():
    runner = RecordingRunner(returncodes=[1])

    assert run_pipeline(runner=runner) is False
    assert runner.scripts == ["ingest.py"]


def test_pipeline_uses_active_python_interpreter():
    runner = RecordingRunner()

    run_pipeline(runner=runner)

    assert runner.commands[0][0][0].endswith("python") or runner.commands[0][0][0].endswith(
        "python3"
    )
