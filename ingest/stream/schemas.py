"""Register the contracts in `contracts/` with the Schema Registry (idempotent).

Subject per contract: `<topic>-value` for a contract that describes a Kafka topic's values
(TopicNameStrategy), the plain name otherwise (the silver table contract). Every subject gets
BACKWARD_TRANSITIVE compatibility (ADR-006): archived data from the first day is replayed
against the newest contract, so a new version must accept data valid under every older one.

Registering an unchanged schema returns its existing id; an incompatible change is refused by
the registry (HTTP 409) and this command fails, which is what the CI check relies on.

Run `uv run python -m ingest.stream.schemas` (part of `make up`); `--check` only tests
compatibility without registering.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import urllib.error
import urllib.request

from ingest.stream.topics import TOPICS
from trust.contract import CONTRACTS

log = logging.getLogger("schemas")
COMPATIBILITY = "BACKWARD_TRANSITIVE"
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


def subject_for(contract_name: str, topic_names: set[str]) -> str:
    return f"{contract_name}-value" if contract_name in topic_names else contract_name


def _call(method: str, url: str, body: dict | None = None) -> tuple[int, dict | list]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", CONTENT_TYPE)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")


def required_lists(schema, path: str = "#") -> dict[str, set[str]]:
    """Every `required` keyword in a schema, by JSON path."""
    found: dict[str, set[str]] = {}
    if isinstance(schema, dict):
        for key, value in schema.items():
            if key == "required" and isinstance(value, list):
                found[path] = set(value)
            else:
                found.update(required_lists(value, f"{path}/{key}"))
    elif isinstance(schema, list):
        for i, item in enumerate(schema):
            found.update(required_lists(item, f"{path}/{i}"))
    return found


def added_required(old: dict, new: dict) -> list[str]:
    """Fields that became required anywhere, including inside if/then branches.

    The registry's JSON Schema check does not look inside conditionals (findings F15), and a
    newly required field breaks BACKWARD compatibility (old data lacks it), so this closes the gap.
    A required list at a path that did not exist before counts as added in full (conservative).
    """
    before = required_lists(old)
    return [
        f"{path}: {sorted(fields - before.get(path, set()))}"
        for path, fields in required_lists(new).items()
        if fields - before.get(path, set())
    ]


def incompatibilities(registry: str, subject: str, schema: dict, versions: list[int]) -> list[str]:
    """Problems of `schema` against every registered version (transitive)."""
    new = json.loads(schema["schema"])
    problems = []
    for version in versions:
        # Redpanda has no "all versions" endpoint (404), so each version is checked explicitly.
        url = f"{registry}/compatibility/subjects/{subject}/versions/{version}?verbose=true"
        status, result = _call("POST", url, schema)
        if not (status == 200 and result.get("is_compatible")):
            problems.append(f"v{version} (registry): {result.get('messages', result)}")
        _, old = _call("GET", f"{registry}/subjects/{subject}/versions/{version}")
        for added in added_required(json.loads(old["schema"]), new):
            problems.append(f"v{version} (required added): {added}")
    return problems


def sync(registry: str, check_only: bool = False) -> list[str]:
    """Check every contract against all its registered versions, then register it unless
    check_only. Returns the subjects with problems (nothing is registered for those)."""
    topics = {t.name for t in TOPICS}
    failed = []
    for path in sorted(CONTRACTS.glob("*.schema.json")):
        name = path.name.removesuffix(".schema.json")
        subject = subject_for(name, topics)
        schema = {"schemaType": "JSON", "schema": path.read_text(encoding="utf-8")}
        status, versions = _call("GET", f"{registry}/subjects/{subject}/versions")
        versions = versions if status == 200 else []
        problems = incompatibilities(registry, subject, schema, versions)
        if problems:
            log.error("%-34s INCOMPATIBLE: %s", subject, " | ".join(problems)[:1500])
            failed.append(subject)
            continue
        if check_only:
            log.info("%-34s compatible with all %d version(s)", subject, len(versions))
            continue
        if not versions:
            _call("PUT", f"{registry}/config/{subject}", {"compatibility": COMPATIBILITY})
        status, result = _call("POST", f"{registry}/subjects/{subject}/versions", schema)
        if status != 200:
            log.error("%-34s refused (%s): %s", subject, status, result)
            failed.append(subject)
            continue
        _, versions = _call("GET", f"{registry}/subjects/{subject}/versions")
        log.info("%-34s id %s, version %s (%s)", subject, result["id"], max(versions),
                 COMPATIBILITY)  # fmt: skip
    return failed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="test compatibility only")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081")
    if sync(registry, check_only=args.check):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
