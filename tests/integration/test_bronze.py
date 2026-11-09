"""Integration tests for the bronze job against the running stack (make up, then make stream).

Each test uses its own topic, table and checkpoint, so the live pipeline is not touched.
Run with `make it` (deselected from `make test`: needs Docker, Kafka and the Spark image).

- Integration: N messages in, N rows out, every offset exactly once.
- Kill test: the job is killed with SIGKILL several times mid-run and restarted from its
  checkpoint; the table must still hold every offset exactly once (spec: "0 duplicates
  across N forced restarts").
"""

from __future__ import annotations

import json
import random
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(ROOT / "infra/compose/compose.yaml"),
    "--profile",
    "stream",
]
BOOTSTRAP = "127.0.0.1:19092"
SAMPLES = [
    json.loads(line)["message"]
    for line in (ROOT / "tests/fixtures/trust_valid_samples.jsonl").read_text().splitlines()
]


def synthetic_messages(n: int) -> list[dict]:
    """n distinct messages built from the real samples (bronze does not parse them)."""
    messages = []
    for i in range(n):
        m = json.loads(json.dumps(SAMPLES[i % len(SAMPLES)]))
        m["body"]["train_id"] = f"{i:06d}TEST"[:10]  # spreads keys over all partitions
        messages.append(m)
    return messages


@pytest.fixture
def run_id():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def topic(run_id):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    name = f"it.bronze.{run_id}"
    admin.create_topics([NewTopic(name, 3, 1)])[name].result()
    yield name
    admin.delete_topics([name])[name].result()


def produce(topic: str, n: int) -> None:

    from confluent_kafka import Producer

    from ingest.stream.producer import TrustProducer, producer_config

    producer = TrustProducer(
        Producer(producer_config(BOOTSTRAP, "it")), source="replay", topic=topic
    )
    messages = synthetic_messages(n)
    for start in range(0, n, 25):  # frames of ~25 messages, like the feed
        producer.send(datetime.now(UTC), messages[start : start + 25], block=True)
    assert producer.flush(30) == 0
    stats = producer.take_stats()
    assert (stats.delivered, stats.failed) == (n, 0)


def bronze_cmd(name: str, topic: str, table: str, checkpoint: str, batch: int) -> list[str]:
    return COMPOSE + [
        "run", "--rm", "--no-deps", "--name", name, "-e", "SPARK_DRIVER_MEMORY=768m",
        "spark-bronze", "python", "-m", "streaming.bronze", "--available-now",
        "--topic", topic, "--table", table, "--checkpoint", checkpoint,
        "--max-offsets-per-trigger", str(batch),
    ]  # fmt: skip


def audit_and_drop(table: str) -> dict:
    out = subprocess.run(
        COMPOSE + ["run", "--rm", "--no-deps", "spark-bronze", "python", "-m", "streaming.audit",
                   table, "--drop"],
        capture_output=True, text=True, timeout=600, check=True,
    ).stdout  # fmt: skip
    line = next(ln for ln in out.splitlines() if ln.startswith("AUDIT "))
    return json.loads(line.removeprefix("AUDIT "))


def assert_exactly_once(result: dict, n: int) -> None:
    assert result["rows"] == n, result
    assert result["distinct_offsets"] == n, result  # no duplicates
    assert all(p["no_gaps"] and p["lo"] == 0 for p in result["partitions"].values()), result
    assert sum(p["rows"] for p in result["partitions"].values()) == n


def test_every_message_lands_exactly_once(topic, run_id):
    n = 5_000
    produce(topic, n)
    table = f"bronze_it.t_{run_id}"
    subprocess.run(
        bronze_cmd(f"it-bronze-{run_id}", topic, table, f"/checkpoints/it-{run_id}", 100_000),
        capture_output=True,
        text=True,
        timeout=600,
        check=True,
    )
    result = audit_and_drop(table)
    assert_exactly_once(result, n)
    print("integration:", json.dumps(result))


def committed_batches(container: str) -> int:
    """Micro-batches the job has logged so far (ProgressLogger writes one line per batch)."""
    logs = subprocess.run(["docker", "logs", container], capture_output=True, text=True).stderr
    return sum(" bronze batch " in line for line in logs.splitlines())


def test_killed_mid_run_restarts_without_duplicates_or_loss(topic, run_id):
    n, batch, kills = 40_000, 500, 3  # 80 small batches: plenty of run to interrupt
    produce(topic, n)
    table = f"bronze_it.k_{run_id}"
    checkpoint = f"/checkpoints/it-kill-{run_id}"
    rng = random.Random(run_id)
    kill_log = []
    for attempt in range(kills):
        name = f"it-kill-{run_id}-{attempt}"
        proc = subprocess.Popen(bronze_cmd(name, topic, table, checkpoint, batch),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # fmt: skip
        # Kill after a few committed batches, at a random moment inside the next one
        # (a batch of 500 rows takes about a second), not on a fixed timer.
        wait_for = rng.randint(2, 5)
        deadline = time.monotonic() + 180
        while proc.poll() is None and time.monotonic() < deadline:
            if committed_batches(name) >= wait_for:
                break
            time.sleep(0.3)
        if proc.poll() is not None:
            break  # finished before the kill: nothing left to interrupt
        time.sleep(rng.uniform(0.0, 1.0))
        done = committed_batches(name)
        subprocess.run(["docker", "kill", "--signal", "KILL", name], capture_output=True)
        proc.wait(timeout=60)
        kill_log.append(f"run {attempt}: SIGKILL after {done} batches")
    # Final run to completion from the same checkpoint.
    subprocess.run(
        bronze_cmd(f"it-kill-{run_id}-final", topic, table, checkpoint, batch),
        capture_output=True, text=True, timeout=900, check=True,
    )  # fmt: skip
    result = audit_and_drop(table)
    print("kill test:", "; ".join(kill_log), "|", json.dumps(result))
    assert len(kill_log) == kills, f"the job finished before all kills: {kill_log}"
    assert_exactly_once(result, n)
