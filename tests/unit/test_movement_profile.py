import copy
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingest.stomp_bridge.capture import RawCaptureWriter
from scripts.profile_movements import profile, summarize


def test_profile_detects_correction_that_changes_actual_time(tmp_path):
    samples = [
        json.loads(line)
        for line in Path("tests/fixtures/trust_valid_samples.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    original = next(r["message"] for r in samples if r["case"] == "movement_on_route")
    original = copy.deepcopy(original)
    day = datetime(2026, 11, 8, tzinfo=UTC)
    epoch = int(day.timestamp() * 1000)
    original["header"]["msg_queue_timestamp"] = str(epoch)
    original["body"].update(actual_timestamp=str(epoch), planned_timestamp=str(epoch))
    corrected = copy.deepcopy(original)
    corrected["body"].update(correction_ind="true", actual_timestamp=str(epoch + 60000))
    writer = RawCaptureWriter(tmp_path)
    writer.write(day + timedelta(seconds=30), {}, json.dumps([original]))
    writer.write(day + timedelta(seconds=120), {}, json.dumps([corrected]))
    for hour in range(1, 24):
        writer.write(day + timedelta(hours=hour), {}, "[]")
    writer.close()
    result = profile(tmp_path, ["2026-11-08"])
    assert result["counts"]["matched_groups_with_changed_actual_time"] == 1
    assert result["counts"]["groups_with_ordinary_and_correction"] == 1
    assert result["lateness"]["all_movements"]["p50_seconds"] == 30
    assert result["lateness"]["all_movements"]["p95_seconds"] == 60


def test_lateness_thresholds_and_negative_times_are_counted():
    result = summarize(Counter({-120: 1, 30: 98, 3601: 1}))
    assert result["p99_seconds"] == 30
    assert result["future_over_60s"] == 1
    assert result["beyond_seconds"]["3600"] == 1
