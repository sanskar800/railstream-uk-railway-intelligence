# ADR-012: Run the pipeline on an Always Free cloud VM, fed by the capture VM

- **Status:** Accepted
- **Date:** 2026-11-17

## Context

Until now Kafka, Spark, Iceberg and Airflow ran on the laptop, with the always-on capture VM (1 GB, Always Free) as the raw archive and a second, laptop-side bridge for live data; gaps were copied down (`make pull-raw`) and replayed. The user's requirement: no laptop in the middle; everything hosted. Facts checked on 2026-11-17 (Oracle documentation): the $300 trial credit ends 30 days after sign-up (about 2026-12-05) and resources paid with it are permanently reclaimed after a further 30-day grace period unless the account is upgraded and paid; Always Free includes Ampere A1 (ARM) capacity equivalent to **2 OCPUs and 12 GB**, plus 200 GB of block storage, free indefinitely. Every image in the stack publishes an arm64 build (checked), and the stack's memory caps total ~7.5 GiB.

## Options considered

1. **Paid x86 VM on trial credits** (e.g. 4 OCPU / 32 GB): more headroom, but deleted in early January unless upgraded and paid (~£60–100/month).
2. **Always Free A1 VM for processing; the capture VM keeps the raw archive and feeds it** (chosen).
3. **A second live bridge on the processing VM** (its own subscription): no network change, but a second raw copy and a second subscription for the same feed.

## Decision

- A second Always Free VM, `railstream-core` (VM.Standard.A1.Flex, 2 OCPU / 12 GB, Ubuntu 24.04 ARM, 150 GB boot volume), runs the same Compose stack: Redpanda, SeaweedFS, Postgres, Lakekeeper, Airflow, Spark bronze and silver. Set up by `infra/cloud/setup-core.sh`.
- **One bridge, one subscription, one raw copy.** The capture VM's bridge writes its archive (unchanged: disk, then ACK) and also publishes to the processing VM's Kafka when `BRIDGE_KAFKA_BOOTSTRAP` is set. Kafka gets a third listener bound to the processing VM's **private** address (`10.0.0.231:29092`); the subnet security list allows TCP 29092 from `10.0.0.0/24` only, and everything else stays reachable only through SSH. Verified: reachable from the capture VM, closed from the internet.
- The processing VM keeps **no raw files**. Tables are (re)built by replaying from the capture VM into the processing VM's Kafka (`ingest.replay` run on the capture VM), so nothing is stored twice. CORPUS is downloaded by the Airflow DAG on the processing VM.
- The laptop is only for code: commit, push, then `git pull` and `make …` over SSH.

## Consequences

- £0 indefinitely, no dependence on the trial; the credit stays available for short experiments (e.g. a one-off throughput test on a bigger shape).
- The processing VM should stay running: a stopped A1 instance can fail to start again for lack of capacity, and an idle one may be reclaimed; a running pipeline is not idle.
- The capture VM's disk-before-Kafka order (ADR-005) is kept: Redpanda is a single broker, so the raw archive remains the only durable source if the processing VM is down; missed windows are replayed from it.
- Spec deviation (architecture.md §6): the spec assumed local Compose with an optional cloud run. The code and pipeline are unchanged; only where it runs.
- Wrong if: 2 OCPUs cannot keep silver within the 5-minute freshness target, or memory runs out once the API and monitoring are added (re-measured on Days 11–13).
