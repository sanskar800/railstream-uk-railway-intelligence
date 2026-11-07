import gzip
import json
from datetime import UTC, datetime, timedelta, timezone

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
