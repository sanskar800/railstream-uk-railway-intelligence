"""Append-only raw capture of STOMP frames, one file per UTC hour.

Layout: <root>/date=YYYY-MM-DD/hour=HH.jsonl while the hour is being written, then
hour=HH.jsonl.gz once it is complete. One line per frame: received_at, broker headers and the
body exactly as received (a JSON array of TRUST messages, kept as a string).

Durability: each frame is flushed and fsynced before write() returns, so the caller can
acknowledge the frame to the broker knowing it is on disk. The open hour is plain JSONL (a
half-written gzip stream is unreadable after a crash); completed hours are compressed.
Hours are UTC so file names never depend on the BST clock change.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

log = logging.getLogger("capture")


def capture_files(root: Path) -> list[Path]:
    """One representation per hour; plain is authoritative until compression unlinks it.

    Exact suffixes exclude unfinished compression files and quarantined tails.
    """
    hours = {p.with_suffix("") for p in root.glob("date=*/hour=*.jsonl.gz")}
    hours.update(root.glob("date=*/hour=*.jsonl"))
    return [p if p.exists() else p.with_suffix(".jsonl.gz") for p in sorted(hours)]


def recover_tail(path: Path) -> None:
    """Repair only an unterminated final append; preserve damaged bytes before truncating.

    Newline-terminated or interior corruption is an error, never silently discarded.
    Call only on closed plain files, with a single capture writer owning the directory.
    """
    if not path.exists():
        return
    with path.open("r+b") as fh:
        while True:
            offset = fh.tell()
            line = fh.readline()
            if not line:
                return
            try:
                json.loads(line)
            except (ValueError, UnicodeDecodeError):
                if line.endswith(b"\n"):
                    raise ValueError(f"corrupt capture record in {path} at byte {offset}") from None
                backup = path.with_name(f"{path.name}.damaged-{uuid4().hex}")
                with backup.open("xb") as out:
                    out.write(line)
                    out.flush()
                    os.fsync(out.fileno())
                fh.seek(offset)
                fh.truncate()
                fh.flush()
                os.fsync(fh.fileno())
                log.warning("incomplete append preserved in %s; recovered %s", backup, path)
                return
            if not line.endswith(b"\n"):
                fh.seek(0, os.SEEK_END)
                fh.write(b"\n")
                fh.flush()
                os.fsync(fh.fileno())
                return


def hour_of(ts: datetime) -> datetime:
    """Truncate an aware timestamp to its UTC hour."""
    return ts.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def compress_file(path: Path) -> Path:
    """Compress hour=HH.jsonl to hour=HH.jsonl.gz atomically, then remove the plain file.

    Idempotent: if a crash happens after the replace but before the unlink, running it again
    rewrites the same .gz from the same plain file.
    """
    recover_tail(path)
    gz_path = path.with_suffix(".jsonl.gz")
    tmp_path = gz_path.with_name(gz_path.name + ".tmp")
    with path.open("rb") as src, gzip.open(tmp_path, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst)
    os.replace(tmp_path, gz_path)
    path.unlink()
    return gz_path


class RawCaptureWriter:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._hour: datetime | None = None
        self._path: Path | None = None
        self._fh = None

    def path_for(self, hour: datetime) -> Path:
        return self.root / f"date={hour:%Y-%m-%d}" / f"hour={hour:%H}.jsonl"

    def write(self, received_at: datetime, headers: dict[str, str], body: str) -> Path:
        """Append one frame durably; rotate (and compress the previous hour) on a new hour."""
        hour = hour_of(received_at)
        if hour != self._hour:
            self._rotate(hour)
        record = {"received_at": received_at.isoformat(), "headers": headers, "body": body}
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())
        return self._path

    def compress_completed(self, now: datetime) -> list[Path]:
        """Compress plain files left from earlier hours (e.g. after a crash or a stop)."""
        current = self.path_for(hour_of(now))
        done = []
        for path in sorted(self.root.glob("date=*/hour=*.jsonl")):
            recover_tail(path)
            if path != current:
                done.append(compress_file(path))
        return done

    def last_received_at(self) -> datetime | None:
        """Receive time of the newest captured frame, read back from disk (None if no capture).

        Lets a restarted bridge know how long it was away, so a gap across a process or host
        restart is detected, not only gaps within one run.
        """
        files = capture_files(self.root)
        for path in reversed(files):
            if path.suffix == ".jsonl":
                recover_tail(path)
            opener = gzip.open if path.suffix == ".gz" else open
            last = None
            with opener(path, "rt", encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        last = line
            if last is not None:
                return datetime.fromisoformat(json.loads(last)["received_at"])
        return None

    def close(self) -> None:
        """Close the open hour without compressing it: a restart in the same hour appends to it."""
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def _rotate(self, hour: datetime) -> None:
        previous = self._path
        self.close()
        if previous is not None and previous.exists():
            compress_file(previous)
        self._path = self.path_for(hour)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        recover_tail(self._path)
        self._fh = self._path.open("a", encoding="utf-8")
        self._hour = hour
