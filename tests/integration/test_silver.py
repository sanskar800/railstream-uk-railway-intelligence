"""Silver integration scenario: adds isolated retained data; no drops or kills."""

import json
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def run_scenario(script, prefix):
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
            f"tests/integration/{script}",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    line = next(ln for ln in result.stdout.splitlines() if ln.startswith(prefix))
    print(line)
    return json.loads(line.removeprefix(prefix))


def test_silver_corrections_replay_and_reject_retry():
    summary = run_scenario("silver_scenario.py", "SILVER_SCENARIO ")
    assert summary["current_rows"] == 1 and summary["revision_rows"] == 4
    assert summary["durable_rejects"] == 1 and summary["unique_dlq_keys"] == 1
    assert summary["checkpoint_restarts"] == 3


def test_silver_watermark_and_locations():
    summary = run_scenario("silver_state_scenario.py", "SILVER_STATE_SCENARIO ")
    assert summary["current_rows"] == 5 and summary["revision_rows"] == 6
    assert summary["dropped_duplicate_rows"] >= 2
    assert summary["evicted_state_rows"] >= 1
