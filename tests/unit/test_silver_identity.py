import copy
import json
from datetime import UTC, datetime
from pathlib import Path

from trust.silver import classify

BASE = next(
    r["message"]
    for r in map(
        json.loads, Path("tests/fixtures/trust_valid_samples.jsonl").read_text().splitlines()
    )
    if r["case"] == "movement_on_route"
)


def record(message, offset=0):
    return {
        "payload": json.dumps(message),
        "kafka_topic": "test",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "received_at": datetime(2026, 11, 16, tzinfo=UTC),
        "ingested_at": datetime(2026, 11, 16, tzinfo=UTC),
        "ingest_source": "replay",
    }


def test_actual_time_correction_is_a_new_revision_of_the_same_call():
    first = classify(record(BASE))["movement"]
    changed = copy.deepcopy(BASE)
    changed["body"]["actual_timestamp"] = str(int(changed["body"]["actual_timestamp"]) + 60000)
    changed["body"]["correction_ind"] = "true"
    correction = classify(record(changed, 1))["movement"]
    assert first["event_key"] == correction["event_key"]
    assert first["revision_id"] != correction["revision_id"]


def test_replay_lineage_cannot_change_revision_identity():
    first = classify(record(BASE))["movement"]
    replay = record(BASE, 100)
    replay["received_at"] = datetime(2026, 11, 17, tzinfo=UTC)
    assert classify(replay)["movement"]["revision_id"] == first["revision_id"]


def test_planned_time_separates_repeat_visits_and_runs_on_different_dates():
    changed = copy.deepcopy(BASE)
    changed["body"]["planned_timestamp"] = str(int(changed["body"]["planned_timestamp"]) + 86400000)
    assert (
        classify(record(BASE))["movement"]["event_key"]
        != classify(record(changed))["movement"]["event_key"]
    )


def test_offroute_identity_is_explicit_and_does_not_merge_all_null_plans():
    m = copy.deepcopy(BASE)
    m["body"].update(offroute_ind="true", variation_status="OFF ROUTE")
    m["body"].pop("planned_timestamp", None)
    first = classify(record(m))["movement"]
    m["body"]["actual_timestamp"] = str(int(m["body"]["actual_timestamp"]) + 60000)
    second = classify(record(m))["movement"]
    assert first["identity_unresolved"] and second["identity_unresolved"]
    assert first["event_key"] != second["event_key"]


def test_reject_identity_and_failure_time_survive_batch_retry():
    bad = record(BASE)
    bad["payload"] = "broken"
    assert classify(bad)["reject"] == classify(bad)["reject"]
    bad2 = dict(bad, kafka_offset=1)
    assert classify(bad)["reject"]["record_id"] != classify(bad2)["reject"]["record_id"]
