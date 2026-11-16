"""Silver integration scenario: adds isolated retained data; no drops or kills."""

import json
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def test_silver_corrections_replay_and_reject_retry():
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "infra/compose/compose.yaml"),
            "--profile",
            "stream",
            "run",
            "--rm",
            "--no-deps",
            "-e",
            "PYTHONPATH=/app",
            "-v",
            f"{ROOT / 'tests'}:/app/tests:ro",
            "-v",
            f"{ROOT / 'streaming'}:/app/streaming:ro",
            "spark-silver",
            "python",
            "tests/integration/silver_scenario.py",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    line = next(ln for ln in result.stdout.splitlines() if ln.startswith("SILVER_SCENARIO "))
    summary = json.loads(line.removeprefix("SILVER_SCENARIO "))
    assert summary["current_rows"] == 1 and summary["revision_rows"] == 4
    assert summary["durable_rejects"] == 1 and summary["unique_dlq_keys"] == 1
    assert summary["checkpoint_restarts"] == 3
    print(line)
