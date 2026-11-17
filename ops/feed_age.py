"""Publish `feed_age_seconds`: time since the newest record reached raw.trust.movement.

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
import sys
import urllib.request
from datetime import UTC, datetime

TOPIC = "raw.trust.movement"
NAMESPACE = "railstream"
METRIC = "feed_age_seconds"
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


def metric_payload(age: float, compartment_id: str, now: datetime) -> dict:
    """The PostMetricData body (as plain data, so it is testable without the SDK)."""
    return {
        "namespace": NAMESPACE,
        "compartment_id": compartment_id,
        "name": METRIC,
        "dimensions": {"topic": TOPIC},
        "datapoints": [{"timestamp": now, "value": round(age, 1)}],
    }


def publish(payload: dict, region: str) -> None:
    import oci  # ops dependency group: installed on the processing VM only

    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client = oci.monitoring.MonitoringClient(
        config={},
        signer=signer,
        service_endpoint=f"https://telemetry-ingestion.{region}.oraclecloud.com",
    )
    datapoints = [oci.monitoring.models.Datapoint(**d) for d in payload["datapoints"]]
    details = oci.monitoring.models.MetricDataDetails(**{**payload, "datapoints": datapoints})
    client.post_metric_data(oci.monitoring.models.PostMetricDataDetails(metric_data=[details]))


def instance_metadata() -> dict:
    request = urllib.request.Request(METADATA, headers={"Authorization": "Bearer Oracle"})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="measure and print, do not publish")
    args = parser.parse_args()

    from confluent_kafka import Consumer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "railstream-feed-age",
            "enable.auto.commit": False,
        }  # fmt: skip
    )
    try:
        now = datetime.now(UTC)
        age = feed_age_seconds(latest_record_time(consumer), now)
    finally:
        consumer.close()
    if age is None:
        sys.exit(f"{TOPIC} is empty; nothing to publish")
    print(f"{METRIC}={age:.1f}", flush=True)
    if not args.dry_run:
        meta = instance_metadata()
        publish(metric_payload(age, meta["compartmentId"], now), meta["canonicalRegionName"])


if __name__ == "__main__":
    main()
