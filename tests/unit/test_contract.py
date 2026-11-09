"""Contract tests: real samples pass; known-bad samples are rejected with the right reason."""

import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from trust.contract import CONTRACTS, dlq_record, validate_raw, validator

SAMPLES = {
    row["case"]: row["message"]
    for row in map(
        json.loads,
        Path("tests/fixtures/trust_valid_samples.jsonl").read_text(encoding="utf-8").splitlines(),
    )
}
MOVEMENT = SAMPLES["movement_on_route"]


@pytest.mark.parametrize("case", sorted(SAMPLES))
def test_real_samples_of_every_message_type_satisfy_the_contract(case):
    message, rejection = validate_raw(json.dumps(SAMPLES[case]))
    assert rejection is None, rejection
    assert message == SAMPLES[case]


def mutate(change, base=MOVEMENT):
    message = copy.deepcopy(base)
    change(message)
    return json.dumps(message)


def drop(field):
    return lambda m: m["body"].pop(field)


def put(field, value):
    return lambda m: m["body"].__setitem__(field, value)


BAD = [
    ("not JSON", "{'header':", ("not_json", None, None)),
    ("JSON but not an object", "[1, 2]", ("not_an_object", None, None)),
    ("missing body", json.dumps({"header": MOVEMENT["header"]}), ("schema", "body", "required")),
    ("unknown msg_type", mutate(lambda m: m["header"].__setitem__("msg_type", "0009")),
     ("schema", "header.msg_type", "enum")),
    ("missing actual time", mutate(drop("actual_timestamp")),
     ("schema", "body.actual_timestamp", "required")),
    ("timestamp in seconds", mutate(put("actual_timestamp", "1794131400")),
     ("schema", "body.actual_timestamp", "pattern")),
    ("timestamp as a number", mutate(put("actual_timestamp", 1794131400000)),
     ("schema", "body.actual_timestamp", "type")),
    ("short STANOX", mutate(put("loc_stanox", "8720")), ("schema", "body.loc_stanox", "pattern")),
    ("new event type", mutate(put("event_type", "PASS")), ("schema", "body.event_type", "enum")),
    ("boolean not a string", mutate(put("offroute_ind", False)),
     ("schema", "body.offroute_ind", "enum")),
    ("negative variation", mutate(put("timetable_variation", "-3")),
     ("schema", "body.timetable_variation", "pattern")),
    ("on route without planned time", mutate(drop("planned_timestamp")),
     ("schema", "body.planned_timestamp", "required")),
    ("bad train id", mutate(put("train_id", "872L09")), ("schema", "body.train_id", "pattern")),
]  # fmt: skip


@pytest.mark.parametrize(("value", "expected"), [b[1:] for b in BAD], ids=[b[0] for b in BAD])
def test_known_bad_samples_are_rejected_with_the_right_reason(value, expected):
    message, rejection = validate_raw(value)
    assert message is None
    assert (rejection.reason, rejection.error_path, rejection.rule) == expected


def test_off_route_movement_may_omit_planned_time():
    assert "planned_timestamp" not in SAMPLES["movement_off_route"]["body"]
    assert validate_raw(json.dumps(SAMPLES["movement_off_route"]))[1] is None


def test_unknown_extra_fields_are_allowed_because_the_feed_is_external():
    value = mutate(put("new_upstream_field", "x"))
    assert validate_raw(value)[1] is None


def test_undecodable_bytes_are_rejected_not_raised():
    assert validate_raw(b"\xff\xfe{")[1].reason == "not_json"
    assert validate_raw(None)[1].reason == "not_json"


def test_dlq_record_satisfies_its_own_contract_and_keeps_the_payload():
    payload = mutate(drop("actual_timestamp")).encode()
    _, rejection = validate_raw(payload)
    record = dlq_record(
        rejection,
        payload=payload,
        topic="raw.trust.movement",
        partition=2,
        offset=41,
        received_at="2026-11-08T10:00:01+00:00",
        contract_version=1,
        failed_at=datetime(2026, 11, 9, 12, 0, tzinfo=UTC),
    )
    assert list(validator("dlq.trust.movement").iter_errors(record)) == []
    assert record["payload"] == payload.decode()  # verbatim, for re-processing
    assert (record["source_partition"], record["source_offset"]) == (2, 41)
    assert record["error_path"] == "body.actual_timestamp"


def test_every_contract_is_a_valid_draft7_schema():
    names = [p.name.removesuffix(".schema.json") for p in CONTRACTS.glob("*.schema.json")]
    assert sorted(names) == ["dlq.trust.movement", "raw.trust.movement", "silver.trust_movement"]
    for name in names:
        validator(name)  # check_schema raises if invalid
