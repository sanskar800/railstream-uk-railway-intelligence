"""Validating raw TRUST records against their contract, and building dead-letter records.

The contracts live in `contracts/*.schema.json` (JSON Schema, registered in the Schema Registry).
`validate_raw` never raises on bad input: every failure becomes a Rejection with a stable reason
category (for the DLQ-rate-by-reason metric), the failing path and rule, and a readable detail.

Two validators for one schema: fastjsonschema (the schema compiled to Python code) decides valid or
not, ~20x faster than jsonschema (52k vs 2.6k messages/s, identical verdicts on 50k real messages);
jsonschema runs only for the rare rejected message, to explain where and why it failed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

import fastjsonschema
from jsonschema import Draft7Validator
from jsonschema.exceptions import best_match

CONTRACTS = Path(__file__).resolve().parent.parent / "contracts"
RAW_SUBJECT = "raw.trust.movement"
DLQ_SUBJECT = "dlq.trust.movement"
DETAIL_MAX = 500


def _schema(name: str) -> dict:
    return json.loads((CONTRACTS / f"{name}.schema.json").read_text(encoding="utf-8"))


@cache
def validator(name: str) -> Draft7Validator:
    """The explaining validator (slow, detailed errors)."""
    schema = _schema(name)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


@cache
def fast_validator(name: str):
    """The deciding validator: compiled code; raises JsonSchemaException when invalid."""
    return fastjsonschema.compile(_schema(name))


@dataclass(frozen=True)
class Rejection:
    reason: str  # not_json | not_an_object | schema
    detail: str
    error_path: str | None = None  # e.g. body.actual_timestamp
    rule: str | None = None  # e.g. pattern, required, enum


def validate_raw(value: bytes | str | None) -> tuple[dict | None, Rejection | None]:
    """(message, None) if the record satisfies the raw contract, else (None, rejection)."""
    if value is None:
        return None, Rejection("not_json", "empty record value")
    try:
        message = json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return None, Rejection("not_json", str(exc)[:DETAIL_MAX])
    if not isinstance(message, dict):
        return None, Rejection("not_an_object", f"top level is {type(message).__name__}")
    try:
        fast_validator(RAW_SUBJECT)(message)
        return message, None
    except fastjsonschema.JsonSchemaException:
        pass
    error = best_match(validator(RAW_SUBJECT).iter_errors(message))
    if error is None:  # the two validators disagree: never seen, but dead-letter it, never crash
        detail = "fastjsonschema rejected a message that jsonschema accepts"
        return None, Rejection("schema", detail, None, "validator_disagreement")
    path = ".".join(str(p) for p in error.absolute_path)
    if error.validator == "required":
        # Point at the missing field itself, not at its parent object.
        missing = error.message.split("'")[1] if "'" in error.message else ""
        path = ".".join(p for p in (path, missing) if p)
    return None, Rejection("schema", error.message[:DETAIL_MAX], path or None, str(error.validator))


def dlq_record(
    rejection: Rejection,
    *,
    payload: bytes | str | None,
    topic: str,
    partition: int,
    offset: int,
    received_at: str | None = None,
    contract_version: int | None = None,
    failed_at: datetime | None = None,
) -> dict:
    """A record for dlq.trust.movement (contracts/dlq.trust.movement.schema.json)."""
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    record = {
        "reason": rejection.reason,
        "detail": rejection.detail,
        "source_topic": topic,
        "source_partition": partition,
        "source_offset": offset,
        "payload": payload or "",
        "failed_at": (failed_at or datetime.now(UTC)).isoformat(),
    }
    optional = {
        "error_path": rejection.error_path,
        "rule": rejection.rule,
        "received_at": received_at,
        "contract_version": contract_version,
    }
    record.update({k: v for k, v in optional.items() if v is not None})
    return record
