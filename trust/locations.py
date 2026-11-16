"""Deterministic CORPUS selection, independent of Spark and input row order."""

import gzip
import hashlib
import json
import re
from pathlib import Path


def select_locations(rows: list[dict]) -> list[dict]:
    candidates = {}
    for source in rows:
        row = {k: str(v).strip() if v is not None else "" for k, v in source.items()}
        stanox = row.get("STANOX", "")
        if not stanox:
            continue
        if not re.fullmatch(r"[0-9]{5}", stanox):
            raise ValueError(f"invalid CORPUS STANOX: {stanox!r}")
        # Prefer a passenger code and a usable name/TIPLOC; lexical tie-break is reproducible.
        rank = (
            not bool(row.get("3ALPHA")),
            not bool(row.get("NLCDESC")),
            not bool(row.get("TIPLOC")),
            json.dumps(row, sort_keys=True),
        )
        if stanox not in candidates or rank < candidates[stanox][0]:
            candidates[stanox] = (rank, row)
    return [
        {
            "stanox": key,
            "location_name": row.get("NLCDESC") or None,
            "crs": row.get("3ALPHA") or None,
            "tiploc": row.get("TIPLOC") or None,
        }
        for key, (_, row) in sorted(candidates.items())
    ]


def read_corpus(path: Path) -> tuple[str, list[dict]]:
    # Hash uncompressed content: gzip metadata must not create a new logical version.
    raw = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
    rows = json.loads(raw)["TIPLOCDATA"]
    selected = select_locations(rows)
    if not selected:
        raise ValueError("CORPUS contains no usable STANOX rows")
    return hashlib.sha256(raw).hexdigest(), selected
