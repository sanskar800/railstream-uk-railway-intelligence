import gzip
import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from ingest.stomp_bridge.capture import RawCaptureWriter, compress_file, hour_of

T0 = datetime(2026, 11, 5, 14, 59, 58, tzinfo=UTC)


def read_lines(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def test_frame_is_written_verbatim_to_its_utc_hour_file(tmp_path):
    writer = RawCaptureWriter(tmp_path)
    body = '[{"header":{"msg_type":"0003"},"body":{"train_id":"872L09MX03"}}]'
    path = writer.write(T0, {"message-id": "1"}, body)
    writer.close()

    assert path == tmp_path / "date=2026-11-05" / "hour=14.jsonl"
    [record] = read_lines(path)
    assert record["body"] == body  # kept as the exact string, not re-serialised
    assert record["headers"] == {"message-id": "1"}
    assert datetime.fromisoformat(record["received_at"]) == T0


def test_new_hour_compresses_the_previous_hour(tmp_path):
    writer = RawCaptureWriter(tmp_path)
    writer.write(T0, {}, "[1]")
    writer.write(T0 + timedelta(seconds=1), {}, "[2]")
    writer.write(T0 + timedelta(seconds=3), {}, "[3]")  # 15:00:01 -> new hour
    writer.close()

    day = tmp_path / "date=2026-11-05"
    assert not (day / "hour=14.jsonl").exists()
    assert [r["body"] for r in read_lines(day / "hour=14.jsonl.gz")] == ["[1]", "[2]"]
    assert [r["body"] for r in read_lines(day / "hour=15.jsonl")] == ["[3]"]


def test_restart_in_the_same_hour_appends(tmp_path):
    first = RawCaptureWriter(tmp_path)
    first.write(T0, {}, "[1]")
    first.close()
    second = RawCaptureWriter(tmp_path)
    path = second.write(T0 + timedelta(seconds=1), {}, "[2]")
    second.close()

    assert [r["body"] for r in read_lines(path)] == ["[1]", "[2]"]


def test_startup_compresses_leftover_hours_but_not_the_current_one(tmp_path):
    writer = RawCaptureWriter(tmp_path)
    writer.write(T0, {}, "[old]")  # 14:xx, left uncompressed as if the process crashed
    writer.close()
    current = RawCaptureWriter(tmp_path)
    current.write(T0 + timedelta(hours=1), {}, "[now]")
    current.close()  # hour 15 is still open

    done = RawCaptureWriter(tmp_path).compress_completed(T0 + timedelta(hours=1))

    day = tmp_path / "date=2026-11-05"
    assert done == [day / "hour=14.jsonl.gz"]
    assert (day / "hour=15.jsonl").exists()


def test_compress_is_idempotent_if_interrupted_before_unlink(tmp_path):
    plain = tmp_path / "hour=14.jsonl"
    plain.write_text('{"body": "[1]"}\n', encoding="utf-8")
    compress_file(plain)
    plain.write_text('{"body": "[1]"}\n', encoding="utf-8")  # crash left the plain file behind

    gz = compress_file(plain)

    assert read_lines(gz) == [{"body": "[1]"}]
    assert not plain.exists()


def test_hours_are_utc_regardless_of_input_offset():
    bst = timezone(timedelta(hours=1))
    assert hour_of(datetime(2026, 7, 1, 10, 30, tzinfo=bst)) == datetime(2026, 7, 1, 9, tzinfo=UTC)


def test_last_received_at_reads_the_newest_frame_back_from_disk(tmp_path):
    writer = RawCaptureWriter(tmp_path)
    assert writer.last_received_at() is None  # nothing captured yet

    writer.write(T0, {}, "[1]")
    writer.write(T0 + timedelta(seconds=3), {}, "[2]")  # new hour; hour 14 compressed
    writer.close()

    restarted = RawCaptureWriter(tmp_path)
    assert restarted.last_received_at() == T0 + timedelta(seconds=3)


@pytest.mark.parametrize("previous_hour", [False, True])
def test_startup_preserves_partial_tail_and_continues_capture(tmp_path, previous_hour):
    writer = RawCaptureWriter(tmp_path)
    path = writer.write(T0, {}, "[]")
    writer.close()
    tail = b'{"received_at":"broken\xe2'
    with path.open("ab") as fh:
        fh.write(tail)
    restarted = RawCaptureWriter(tmp_path)
    now = T0 + timedelta(hours=1) if previous_hour else T0
    restarted.compress_completed(now)
    assert restarted.last_received_at() == T0
    assert [p.read_bytes() for p in path.parent.glob("*.damaged-*")] == [tail]
    new = restarted.write(now + timedelta(seconds=1), {}, "[]")
    restarted.close()
    assert read_lines(new)[-1]["received_at"] == (now + timedelta(seconds=1)).isoformat()


def test_complete_record_without_newline_is_kept_before_append(tmp_path):
    writer = RawCaptureWriter(tmp_path)
    path = writer.write(T0, {}, "[]")
    writer.close()
    path.write_bytes(path.read_bytes().rstrip(b"\n"))
    restarted = RawCaptureWriter(tmp_path)
    restarted.write(T0 + timedelta(seconds=1), {}, "[]")
    restarted.close()
    assert len(read_lines(path)) == 2
    assert not list(path.parent.glob("*.damaged-*"))


def test_interior_corruption_is_not_discarded(tmp_path):
    path = RawCaptureWriter(tmp_path).path_for(T0)
    path.parent.mkdir(parents=True)
    data = b'broken\n{"received_at":"2026-11-05T14:59:58+00:00"}\n'
    path.write_bytes(data)
    with pytest.raises(ValueError, match="corrupt capture record"):
        RawCaptureWriter(tmp_path).last_received_at()
    assert path.read_bytes() == data


def test_startup_ignores_incomplete_compression_files(tmp_path):
    writer = RawCaptureWriter(tmp_path)
    path = writer.write(T0, {}, "[]")
    writer.close()
    path.with_name("hour=15.jsonl.gz.tmp").write_bytes(b"incomplete gzip")
    assert RawCaptureWriter(tmp_path).last_received_at() == T0
