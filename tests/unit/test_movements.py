import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import FormatChecker

from trust.contract import validator
from trust.movements import parse_movement

SAMPLES = {
    r["case"]: r["message"]
    for r in map(
        json.loads,
        Path("tests/fixtures/trust_valid_samples.jsonl").read_text(encoding="utf-8").splitlines(),
    )
}


def parse(message):
    return parse_movement(
        json.dumps(message),
        topic="raw.trust.movement",
        partition=2,
        offset=42,
        received_at=datetime(2026, 11, 8, tzinfo=UTC),
        ingest_source="replay",
    )


@pytest.mark.parametrize("case", ["movement_on_route", "movement_off_route"])
def test_real_movements_are_typed_and_satisfy_silver_contract(case):
    original = copy.deepcopy(SAMPLES[case])
    result = parse(original)
    assert result.rejection is None
    row = result.movement
    assert row["actual_ts"].tzinfo == UTC
    assert isinstance(row["delay_min"], int)
    assert isinstance(row["is_correction"], bool)
    assert (row["source_partition"], row["source_offset"]) == (2, 42)
    assert original == SAMPLES[case]
    serialized = json.loads(json.dumps(row, default=lambda v: v.isoformat()))
    schema = validator("silver.trust_movement").schema
    checked = type(validator("silver.trust_movement"))(schema, format_checker=FormatChecker())
    checked.validate(serialized)


@pytest.mark.parametrize(("status", "expected"), [("LATE", 7), ("EARLY", -7), ("ON TIME", 0)])
def test_delay_sign_and_platform_trimming(status, expected):
    m = copy.deepcopy(SAMPLES["movement_on_route"])
    m["body"].update(variation_status=status, timetable_variation="7", platform=" 7")
    row = parse(m).movement
    assert row["delay_min"] == expected
    assert row["platform"] == "7"


def test_off_route_has_no_planned_time_and_no_delay():
    row = parse(SAMPLES["movement_off_route"]).movement
    assert row["is_offroute"] and row["delay_min"] == 0 and row["planned_ts"] is None


@pytest.mark.parametrize("case", [k for k in SAMPLES if not k.startswith("movement")])
def test_other_valid_message_types_are_distinguished_from_rejections(case):
    result = parse(SAMPLES[case])
    assert result.message_type == SAMPLES[case]["header"]["msg_type"]
    assert result.movement is None and result.rejection is None


def test_invalid_payload_is_a_rejection():
    result = parse_movement(b"not json", topic="test", partition=0, offset=0)
    assert result.rejection.reason == "not_json"
    assert result.movement is None


def test_summer_times_convert_at_the_edge_and_ambiguous_times_are_flagged():
    m = copy.deepcopy(SAMPLES["movement_on_route"])
    body = m["body"]
    for field in ("actual_timestamp", "planned_timestamp", "gbtt_timestamp"):
        body[field] = "1782901800000"  # 2026-07-01 10:30 encoded as UTC
    assert parse(m).movement["actual_ts"] == datetime(2026, 7, 1, 9, 30, tzinfo=UTC)
    body["actual_timestamp"] = str(
        int(datetime(2026, 10, 25, 1, 30, tzinfo=UTC).timestamp() * 1000)
    )
    assert "actual_timestamp:ambiguous" in parse(m).movement["timestamp_issues"]
