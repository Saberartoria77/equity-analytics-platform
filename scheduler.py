"""Daily orchestration for ingestion followed by indicator refresh."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

import schedule

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent
PIPELINE_SCRIPTS = ("ingest.py", "indicators.py")


def run_pipeline(
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    timeout: int = 3600,
) -> bool:
    """Run each pipeline stage sequentially, stopping on the first failure."""
    for script_name in PIPELINE_SCRIPTS:
        script = PROJECT_ROOT / script_name
        logger.info("Starting pipeline stage: %s", script_name)
        try:
            result = runner(
                [sys.executable, str(script)],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=PROJECT_ROOT,
            )
        except (OSError, subprocess.TimeoutExpired):
            logger.exception("Pipeline stage could not complete: %s", script_name)
            return False
        if result.returncode != 0:
            logger.error(
                "Pipeline stage failed: %s (exit %s)\n%s",
                script_name,
                result.returncode,
                result.stderr,
            )
            return False
        logger.info("Completed pipeline stage: %s", script_name)
    return True


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    target_time = os.getenv("PIPELINE_TIME", "02:00")
    timezone = os.getenv("PIPELINE_TIMEZONE", "UTC")
    timeout = int(os.getenv("PIPELINE_TIMEOUT_SECONDS", "3600"))
    schedule.every().day.at(target_time, timezone).do(run_pipeline, timeout=timeout)
    logger.info("Scheduler ready: daily at %s %s", target_time, timezone)
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
