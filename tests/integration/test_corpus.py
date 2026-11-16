"""Run only the retained-data CORPUS scenario against the local Airflow profile."""

import json
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def test_corpus_workflow_and_version_idempotence():
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "infra/compose/compose.yaml"),
            "--profile",
            "airflow",
            "run",
            "--rm",
            "--no-deps",
            "-v",
            f"{ROOT / 'tests'}:/tests:ro",
            "airflow",
            "python",
            "/tests/integration/corpus_scenario.py",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    line = next(row for row in result.stdout.splitlines() if row.startswith("CORPUS_SCENARIO "))
    summary = json.loads(line.removeprefix("CORPUS_SCENARIO "))
    assert summary["rows"] == 4 and summary["repeat_inserted"] == 0
    print(line)
