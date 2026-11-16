# RailStream project handover

Reviewed 2026-11-16. This is a migration audit, not a feature implementation.
The existing code is the implementation source of truth; the portfolio roadmap
section 4 defines the intended scope. Read the parent `AGENTS.md` / `CLAUDE.md`,
then this document and the local `TASKS.md` before resuming work.

**Recovery follow-up, 2026-11-16 (authorized after the audit):** milestone 1's
capture-tail and replay fixes are now implemented locally. Partial final appends
are quarantined before repair, compression temporary files are excluded, plain
and compressed hours replay once, and the replay CLI fails on incomplete input
or delivery. The kill test now checks progress, kill success and abnormal exit.
See `docs/recovery.md` for exit statuses and the expired-offset recovery plan.
The review findings below describe the audit baseline; issues A1–A3 are addressed
by this follow-up. Manual ACK-to-Kafka catch-up (A4) and expired-offset cutover
automation (A5) remain open. No VM redeploy or destructive tests were performed.

## 1. Repository and working state

- Repository: `railstream/`, a separate Git repository under `C:\dev\5projects`.
- Branch: `main`; HEAD `42a3e247172f7de4f8c4ad8495643de235b50ee1`.
- At audit start, tracked and untracked working changes were absent. `main`
  matched the locally cached `origin/main`; no fetch was performed to establish
  whether GitHub has changed since that cache was updated.
- Recent history implements contracts/CI (`158a58e`, `81bd5fc`), Spark bronze
  (`e829c09`), and its integration/recovery tests (`42a3e24`).
- `TASKS.md` is intentionally gitignored. Its Days 0–5 are marked complete;
  Days 6–14 remain planned. Some completed checkboxes cover preparation only:
  DLQ validation/format exists, but runtime routing does not yet exist.
- Secrets remain in `.env`, which is ignored. No secret values are included here.
- Only this handover and an appended local progress-log entry are changed by
  this audit. No code, contracts, infrastructure or dependencies were changed.

## 2. Architecture as implemented

```text
Network Rail TRUST STOMP topic TRAIN_MVT_ALL_TOC
  |
  +-- archive subscription --> Oracle VM capture container
  |                            write + fsync --> ACK
  |                            hourly raw JSONL; completed hours gzip
  |                                      |
  |                                manual pull-raw over SSH
  |                                      v
  |                                data/raw/trust/
  |                                      |
  |                                replay at N-times speed
  |                                      |
  +-- separate live subscription --> laptop bridge
                                    write + fsync --> ACK --> async producer
                                    data/raw/trust-live/       |
                                                              v
                                  Redpanda raw.trust.movement
                                  3 partitions, 7-day retention
                                                              |
                                  Spark Structured Streaming bronze
                                  checkpoint: named Docker volume
                                                              |
                                  Lakekeeper REST catalog --> Postgres metadata
                                                              |
                                  Iceberg bronze.trust_movement
                                  files on SeaweedFS (S3 API)

Prepared, not wired into a running processor:
  JSON Schema contracts + validation helper + dlq.trust.movement topic definition

Planned:
  silver validation/DLQ/dedupe/corrections/enrichment --> dbt gold --> API/dashboard
  Airflow reference loads/maintenance/backfill; Prometheus/Grafana/data quality
```

Important differences from the original stack description:

- SeaweedFS replaces MinIO (accepted deviation in architecture.md section 6).
- Lakekeeper is the Iceberg REST catalog; Postgres stores its metadata.
- JSON Schema replaces Avro/Protobuf (accepted deviation, ADR-006).
- Spark is 4.1.4 with Iceberg 1.12.0, Python 3.12 and Java 17 in a custom image.
- Bronze is partitioned by `days(kafka_ts)`, not railway event time. Checkpoints
  live on `spark-checkpoints`, not object storage (ADR-007).
- DuckDB/dbt is a proposed integration with a time-boxed spike and fallback
  (ADR-004). Neither DuckDB nor Trino serving is implemented.

## 3. Component status, with evidence boundaries

| Component | Status | Evidence / limit |
|---|---|---|
| Durable capture, hourly files, reconnect/backoff/quiet logs | Implemented; VM running | Source in `ingest/stomp_bridge/`; VM inspected today; crash-tail gaps below |
| Local live producer | Implemented | `make live`; separate subscription; async idempotent producer keyed by train ID |
| Archive copy + replay | Implemented, manual | `scripts/pull_raw.py`, `ingest/replay/`; no automatic catch-up ledger |
| Redpanda + Schema Registry | Configured | Raw/DLQ topics and schema registration; no local containers running today |
| SeaweedFS + Lakekeeper + Postgres | Configured; historical integration evidence | Compose and init scripts exist; not started during audit |
| Bronze Spark job | Implemented; historical recovery evidence | `streaming/bronze.py`; append-only payload/metadata, checkpointed Iceberg sink |
| Contracts, timestamp conversion, DLQ format | Helpers implemented | Unit-tested; not yet applied to a silver stream or runtime DLQ writer |
| Silver, watermarks, event dedupe, corrections | Not started | No silver job or stateful processing implementation |
| CORPUS enrichment and operator lookup | Input/docs only | Local CORPUS file exists; no reference-table loader or streaming join |
| Airflow and Iceberg maintenance | Not started | No DAGs or Airflow service |
| dbt gold and dbt tests | Not started | Metric definitions exist; no `transform/` project |
| DuckDB/Trino, FastAPI, user dashboard | Not started | No serving code/dependencies/services |
| Prometheus/Grafana/Soda | Not started | Log messages exist; monitoring services/rules do not |
| GitHub Actions | Configured | Unit/lint and contract compatibility jobs; integration tests excluded |

There is no complete user-facing application yet. Redpanda Console and Spark UI
are development interfaces available when their local services are started.

## 4. Diagnostics performed on 2026-11-16

### Verified now

- Existing Python environment: **95 unit tests passed, 2 integration tests
  deselected** using `.venv/Scripts/python.exe -m pytest -p no:cacheprovider -q`.
- Ruff check passed; formatting check passed for **47 files**.
- Local Compose configuration validates (`config --quiet`).
- Docker Desktop server responds as 29.1.3 in context `desktop-linux`.
- That context lists **no RailStream containers**. Existing Spark/capture images
  and four named data/checkpoint volumes are present. Volume existence does not
  establish current database/table contents; those were not opened or changed.
- Capture VM: host up **10 days 21 hours**, container up **8 days**, deployed Git
  commit **7656626**, raw directory **377 MB**, root disk **14% used / 39 GB free**.
  Latest inspected UTC log lines at 11:32 and 11:33 report 708 and 745 messages
  per minute with `unparseable=0`. This establishes recent activity, not complete
  capture over the entire uptime or an absence of historical gaps.
- Laptop has **94 completed archive files** spanning 2026-11-05 through 11-09.
  Newer VM hours were not pulled during this read-only audit.

Sandbox restrictions initially prevented Python/Docker access. Repeating the
diagnostics with authorized access succeeded; the environment was not repaired.

### Historical evidence, not re-run today

`results/dev/2026-11-09-bronze-kill-test/pytest-output.txt` records two passing
integration tests: 5,000 records into bronze and 40,000 records across three
reported SIGKILL/restart cycles, with row counts equal to distinct Kafka offsets
and contiguous per-partition offsets. This is development evidence, not a full
pipeline benchmark or proof of railway-event deduplication.

The current kill test waits for batch logs then delays randomly. It does not
prove a kill hit the exact Iceberg-commit/checkpoint boundary, and it does not
check the Docker kill command's return code. Preserve the historical evidence,
but qualify claims of guaranteed mid-commit failure coverage.

No integration/kill tests, starts/stops, image builds, database writes, archive
downloads, remote deployments or destructive Docker commands ran during review.

## 5. Known issues and correctness risks

### A. Recovery issues confirmed or directly visible in code

1. **Truncated capture tail blocks startup (reproduced).**
   `RawCaptureWriter.last_received_at()` parses the final nonempty line without
   handling partial JSON. A temporary capture with an interrupted final line
   raised `JSONDecodeError`. fsync protects completed writes, but does not repair
   an interrupted next append. Replay also assumes every JSONL line is complete.
   The `hour=*.jsonl*` startup glob also includes compression `.tmp` files.
   Recovery needs explicit valid-file selection and a policy preserving damaged
   bytes while recovering the last complete frame.

2. **Plain + compressed leftovers replay twice (reproduced).**
   `hour_files()` yields both representations if both exist. A compression
   interruption after replacement but before unlink is already an acknowledged
   scenario in the capture tests. One temporary original frame was replayed
   twice when both files existed. Select one authoritative representation.

3. **Replay can report success with incomplete delivery.**
   `ingest/replay/__main__.py` logs failures/undelivered records and returns
   normally; no-frame windows also only warn. Malformed frames are counted and
   skipped. Missing hours are silently absent from iteration. An automatic
   backfill must not mark a window complete solely from the process exit code.
   Define exit status, coverage, and skipped-record policy before scheduling it.

4. **ACK-to-Kafka recovery is manual.**
   The bridge ACKs after disk durability and before Kafka delivery (ADR-005).
   A process crash between those operations, or delivery timeout, leaves data on
   disk but not necessarily in Kafka. There is no durable publishing cursor or
   automated catch-up. Producer idempotence does not cover process restarts or
   separate replay runs. Frame splitting rejects a whole malformed frame before
   Kafka; these frames cannot currently reach an offset-based DLQ downstream.

5. **Expired Kafka offsets need a real recovery procedure.**
   Bronze deliberately leaves `failOnDataLoss=true`. Appending archive replay
   records creates new offsets; it does not restore offsets already expired from
   a saved checkpoint. The comment suggesting replay alone fixes this is
   incomplete. Any recovery must preserve existing tables/checkpoints and define
   a safe restart position plus duplicate reconciliation; do not delete volumes.

### B. Decisions required before silver

6. **Kafka-offset exactly-once is not event deduplication.**
   Replays and STOMP redeliveries get new Kafka offsets and therefore new bronze
   rows by design. Silver needs its own stable railway-event identity. The plan
   proposes actual timestamp in the dedupe key, while metrics.md calls for the
   latest report for a planned event. A correction changing actual time would
   bypass the former key. Repeated station visits, off-route calls, train identity
   changes and reuse across dates also need explicit treatment.

7. **No watermark or late-data policy exists yet.**
   Choose the delay from representative captured data, including corrections and
   clock anomalies. Live traffic can advance an event-time watermark past an old
   backfill and cause legitimate historical rows to be discarded. Future-dated
   events can advance it prematurely. Bounded dedupe state alone cannot guarantee
   permanent uniqueness or apply corrections arriving days later. Define an
   idempotent correction/backfill path and observable treatment beyond the watermark.

8. **DLQ is a contract/helper, not a connected runtime sink.**
   The registry does not enforce plain JSON producer values automatically. Silver
   must actually call validation and route failures. Writes to silver and Kafka
   DLQ are separate sinks: retries need a stable DLQ identity (source topic,
   partition, offset) and tests for failure between the two writes.

9. **CORPUS joins can multiply facts.**
   Existing profiling found 356 nonunique STANOX keys. Select deterministically,
   test row-count preservation, retain unknown locations, and define reference
   refresh/version behaviour. Operator mappings also contain shared sector codes.

10. **DST disambiguation is a policy, not uniquely recoverable truth.**
    `trust/timestamps.py` selects the autumn occurrence closest to queue time.
    Late amendments and planned future times can violate that heuristic. Field
    aliases and true-UTC assumptions have not been verified against summer data.
    Retain original values and expose ambiguity/nonexistent-time flags in silver.

### C. Integration, testing and documentation limitations

- CI seeds only the base commit's schema versions into an empty registry;
  local registration checks all stored versions, but CI does not reconstruct all
  historical versions. Contract removal is not checked. The required-field guard
  covers additions, not every possible change to conditional logic. HTTP errors
  listing schema versions are treated as an empty history rather than failing
  closed (`ingest/stream/schemas.py`).
- `scripts/check_contracts.sh` temporarily removes/replaces the working-tree
  contracts directory. Do not run it as a read-only diagnostic against local work.
- `streaming.audit` groups by partition/offset without topic and issues multiple
  reads rather than explicitly pinning one snapshot. Its assumptions fit the
  isolated, stopped, single-topic tests; a live/multi-topic audit needs stronger
  identity and snapshot consistency. Kill tests leave checkpoint directories.
- Host DuckDB/dbt/API cannot resolve the catalog's `seaweedfs:8333` endpoint;
  use a tested Docker-network deployment or resolve the endpoint design. No
  DuckDB-to-Iceberg/dbt materialization test has been performed in this review.
- Spark Dockerfile hardcodes an amd64 `JAVA_HOME`; the proposed Oracle Arm
  migration is not ready without a tested architecture-aware image.
- `make up` starts the core and registration commands, not the live bridge,
  Spark, Airflow or serving. `make stream` assumes warehouse initialization has
  already succeeded. End-to-end startup is not automated.
- Raw frame bodies are retained verbatim on disk; Kafka values are compact JSON
  reserializations (`to_records`). Documentation claiming byte-exact original
  JSON on Kafka conflates semantic preservation with byte preservation.
- README is a skeleton; architecture.md uses planned names such as
  `bronze.trust_messages`, while code writes `bronze.trust_movement`.
  Some delivery guarantees and metric descriptions are prospective.
- Historical Oracle cost/capacity and dependency-availability claims were not
  revalidated with provider documentation or billing during this audit. Healthy
  capture does not prove zero charges or availability of a larger free instance.

## 6. Implementation milestones and current approval state

### 1. Recovery baseline before extending the stream

Authorized and implemented locally on 2026-11-16: **111 unit tests pass**, lint
and formatting pass. See the follow-up note above. The revised destructive kill
test still needs explicit execution approval. Milestones 2–3 remain proposed.

Fix capture-tail/tmp recovery and duplicate hour representations; make replay
completion/failure/coverage observable and machine-checkable. Add focused tests
for each reproduced failure. Strengthen the kill test's checked outcomes and
document checkpoint-retention recovery. Preserve all existing raw data/volumes.

Acceptance: current 95 tests remain green; crash-tail recovery preserves complete
frames; a duplicate representation replays once; failed/incomplete replay cannot
be reported as a successful completed backfill. Service-mutating tests need
explicit approval before execution.

### 2. Day 6 silver correctness and reference enrichment

Agree on event identity versus revision identity, correction precedence, watermark
and historical backfill rules. Implement bronze-to-silver parsing with shared
timestamp logic, actual DLQ routing, dedupe/correction handling and CORPUS
enrichment. Keep evidence and measurable late/reject/duplicate counts.

Acceptance: repeated replay does not double-count silver; corrections update the
right event; late/future cases follow the declared policy; restart retries do not
lose rejects; location joins preserve rows, including unknown locations.

### 3. Airflow reference and maintenance jobs (Days 7–8)

Add the lightweight Airflow profile, repeatable CORPUS refresh and maintenance
DAGs. Automate archive catch-up only after milestones 1–2 make completion and
watermark-safe backfill reliable. Record reference versions, job status and
measured memory. After this, proceed to the ADR-004 dbt/DuckDB spike and gold.

Acceptance: DAG retries are idempotent, failed replay hours are not marked done,
maintenance respects active streaming snapshots/checkpoints, and the measured
stack fits the existing memory budget.

## 7. Safe resumption

- Keep the functioning VM capture running; do not redeploy it during this review.
- Treat named volumes as persistent assets. No `down -v`, prune, table drops,
  checkpoint deletion or destructive integration tests without explicit approval.
- Use `make test` and `make lint` for routine checks. `make it` is deliberately
  excluded: it creates/deletes topics and tables and force-kills test containers.
- The audit was read-only apart from documentation. The subsequently authorized
  recovery fixes are described in the follow-up note. Day 6 is the next milestone;
  its event identity, watermark and correction policy must be explicit before
  implementation.
