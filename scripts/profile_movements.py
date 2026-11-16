"""Measure movement lateness and correction identities before choosing silver state rules.

Run from the repository root: python -m scripts.profile_movements --day YYYY-MM-DD
--day YYYY-MM-DD --output results/dev/<run>/movement-profile.json
This reads captures only; it does not publish, repair files or contact the feed.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingest.replay.replay import missing_hours, read_frames
from ingest.stomp_bridge.frames import split_frame
from trust.timestamps import utc_from_ms, wall_clock_to_utc


def planned_key(message):
    """Candidate planned-call identity, deliberately excluding mutable actual time."""
    b = message["body"]
    if not b.get("planned_timestamp") or b.get("offroute_ind") == "true":
        return None
    return tuple(
        b.get(k)
        for k in (
            "train_id",
            "loc_stanox",
            "planned_event_type",
            "planned_timestamp",
        )
    )


def summarize(histogram: Counter) -> dict:
    total = histogram.total()
    if not total:
        return {"count": 0}
    ordered = sorted(histogram.items())
    result = {"count": total, "min_seconds": ordered[0][0], "max_seconds": ordered[-1][0]}
    for label, fraction in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99), ("p999", 0.999)):
        target, cumulative = math.ceil(total * fraction), 0
        for seconds, count in ordered:
            cumulative += count
            if cumulative >= target:
                result[f"{label}_seconds"] = seconds
                break
    result["future_over_60s"] = sum(n for seconds, n in ordered if seconds < -60)
    result["beyond_seconds"] = {
        str(limit): sum(n for seconds, n in ordered if seconds > limit)
        for limit in (60, 300, 900, 1800, 3600, 86400)
    }
    return result


def profile(root: Path, days: list[str]) -> dict:
    windows = []
    for day in sorted(set(days)):
        start = datetime.fromisoformat(day).replace(tzinfo=UTC)
        end = start + timedelta(days=1)
        if missing := missing_hours(root, start, end):
            raise ValueError(f"missing hours for {day}: {missing}")
        windows.append((start, end))
    histograms = defaultdict(Counter)
    types, totals = Counter(), Counter()
    corrected = {}

    def messages():
        for start, end in windows:
            for frame in read_frames(root, start, end):
                for message in split_frame(frame.body):
                    yield frame.received_at, message

    for received, m in messages():
        types[m["header"]["msg_type"]] += 1
        if m["header"]["msg_type"] != "0003":
            continue
        b = m["body"]
        queue = utc_from_ms(int(m["header"]["msg_queue_timestamp"]))
        actual = wall_clock_to_utc(int(b["actual_timestamp"]), queue)
        lag = round((received - actual).total_seconds())
        correction = b["correction_ind"] == "true"
        histograms["all_movements"][lag] += 1
        histograms["corrections" if correction else "ordinary_reports"][lag] += 1
        totals["movements"] += 1
        if correction:
            totals["correction_messages"] += 1
            key = planned_key(m)
            if key:
                corrected.setdefault(key, {"actuals": set(), "ordinary": 0, "corrections": 0})
            else:
                totals["corrections_without_planned_identity"] += 1

    # Only keep groups involving a correction: bounded memory rather than storing millions of calls.
    for _, m in messages():
        if m["header"]["msg_type"] != "0003":
            continue
        group = corrected.get(planned_key(m))
        if group is not None:
            b = m["body"]
            group["actuals"].add(b["actual_timestamp"])
            group["corrections" if b["correction_ind"] == "true" else "ordinary"] += 1
    totals["corrected_planned_groups"] = len(corrected)
    totals["groups_with_ordinary_and_correction"] = sum(
        g["ordinary"] > 0 for g in corrected.values()
    )
    totals["matched_groups_with_changed_actual_time"] = sum(
        g["ordinary"] > 0 and len(g["actuals"]) > 1 for g in corrected.values()
    )
    return {
        "days": sorted(set(days)),
        "source_root": str(root),
        "method": "whole-second rounded receive-minus-actual lag; nearest-rank quantiles",
        "limitations": "Hour-file presence does not prove uninterrupted feed coverage. "
        "Planned-key groups are candidates, not proof of unique calls.",
        "message_types": dict(types),
        "counts": dict(totals),
        "lateness": {name: summarize(hist) for name, hist in histograms.items()},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path("data/raw/trust"))
    parser.add_argument("--day", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = profile(args.root, args.day)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
