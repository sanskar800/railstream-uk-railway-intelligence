"""Iceberg maintenance against the running stack: needs the airflow profile (make airflow)."""

import json
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def test_maintenance_dag_wiring_and_a_real_run():
    result = subprocess.run(
        ["docker", "compose", "-f", str(ROOT / "infra/compose/compose.yaml"), "--profile",
         "airflow", "run", "--rm", "--no-deps", "-v", f"{ROOT / 'tests'}:/tests:ro", "airflow",
         "python", "/tests/integration/maintenance_scenario.py"],
        cwd=ROOT, capture_output=True, text=True, timeout=600,
    )  # fmt: skip
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    line = next(r for r in result.stdout.splitlines() if r.startswith("MAINTENANCE_SCENARIO "))
    s = json.loads(line.removeprefix("MAINTENANCE_SCENARIO "))
    assert s["files_before"] == 15 and s["rows"] == 15  # nothing lost
    assert s["yesterday_files"] == 1 and s["rewritten"] == 12  # finished day compacted
    assert s["today_files"] == 3  # the partition still being written is untouched
    assert s["metadata_cleanup"] == "true" and s["recorded_runs"] == 1
    print(line)
