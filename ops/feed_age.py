"""Publish `feed_age_seconds` (newest record in raw.trust.movement) and `disk_used_percent`.

Runs every minute on the processing VM (systemd timer `railstream-feed-age`). One number watches
the whole ingestion path: it grows if the capture VM, the bridge, the private network, Kafka
publishing or the Network Rail feed stops. Oracle Monitoring alarms on it (age too high) and on
its absence (this VM or job down); both email the `railstream-alerts` topic (ADR-014).

Authentication: instance principal; the policy allows publishing to the `railstream` metric
namespace only. If Kafka cannot be read, nothing is published, so the absence alarm fires.

    uv run --group ops python -m ops.feed_age [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import urllib.request
from datetime import UTC, datetime

TOPIC = "raw.trust.movement"
NAMESPACE = "railstream"
METRIC = "feed_age_seconds"
DISK_METRIC = "disk_used_percent"
HOST = "railstream-core"
METADATA = "http://169.254.169.254/opc/v2/instance/"


def latest_record_time(consumer, topic: str = TOPIC, timeout_s: float = 10) -> datetime | None:
    """Kafka timestamp of the newest record across all partitions (None if the topic is empty)."""
    from confluent_kafka import TopicPartition

    partitions = consumer.list_topics(topic, timeout=timeout_s).topics[topic].partitions
    newest = None
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout_s)
        if high <= low:
            continue
        consumer.assign([TopicPartition(topic, p, high - 1)])
        msg = consumer.poll(timeout_s)
        if msg is None or msg.error():
            raise RuntimeError(f"could not read the newest record of partition {p}")
        ts = datetime.fromtimestamp(msg.timestamp()[1] / 1000, UTC)
        newest = ts if newest is None or ts > newest else newest
    return newest


def feed_age_seconds(latest: datetime | None, now: datetime) -> float | None:
    """Seconds since `latest`, never negative (clock skew); None if there is no record at all."""
    return None if latest is None else max(0.0, (now - latest).total_seconds())


def metric_payload(
    name: str, value: float, dimensions: dict, compartment_id: str, now: datetime
) -> dict:
    """One PostMetricData entry (plain data, so it is testable without the SDK)."""
    return {
        "namespace": NAMESPACE,
        "compartment_id": compartment_id,
        "name": name,
        "dimensions": dimensions,
        "datapoints": [{"timestamp": now, "value": round(value, 1)}],
    }


def disk_used_percent(path: str = "/") -> float:
    usage = shutil.disk_usage(path)
    return 100.0 * usage.used / usage.total


def publish(payloads: list[dict], region: str) -> None:
    import oci  # ops dependency group: installed on the processing VM only

    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client = oci.monitoring.MonitoringClient(
        config={},
        signer=signer,
        service_endpoint=f"https://telemetry-ingestion.{region}.oraclecloud.com",
    )
    details = [
        oci.monitoring.models.MetricDataDetails(
            **{**p, "datapoints": [oci.monitoring.models.Datapoint(**d) for d in p["datapoints"]]}
        )
        for p in payloads
    ]
    client.post_metric_data(oci.monitoring.models.PostMetricDataDetails(metric_data=details))


def instance_metadata() -> dict:
    request = urllib.request.Request(METADATA, headers={"Authorization": "Bearer Oracle"})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="measure and print, do not publish")
    args = parser.parse_args()

    from confluent_kafka import Consumer

    now = datetime.now(UTC)
    # Disk first and independently: a full disk is what breaks Kafka (findings F28), so this
    # number must still arrive when the feed age cannot be measured.
    disk = disk_used_percent("/")
    measurements = [(DISK_METRIC, disk, {"host": HOST, "mount": "/"})]
    print(f"{DISK_METRIC}={disk:.1f}", flush=True)
    failure = None
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
            "group.id": "railstream-feed-age",
            "enable.auto.commit": False,
        }  # fmt: skip
    )
    try:
        age = feed_age_seconds(latest_record_time(consumer), now)
        if age is None:
            failure = f"{TOPIC} is empty"
        else:
            measurements.append((METRIC, age, {"topic": TOPIC}))
            print(f"{METRIC}={age:.1f}", flush=True)
    except Exception as exc:  # Kafka down: publish no feed age, so the absence alarm fires
        failure = f"cannot read {TOPIC}: {exc}"
    finally:
        consumer.close()
    if not args.dry_run:
        meta = instance_metadata()
        payloads = [metric_payload(n, v, d, meta["compartmentId"], now) for n, v, d in measurements]
        publish(payloads, meta["canonicalRegionName"])
    if failure:
        sys.exit(failure)


if __name__ == "__main__":
    main()
