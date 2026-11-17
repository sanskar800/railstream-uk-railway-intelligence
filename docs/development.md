# Development guide

## 0. Machine inventory (checked 2026-11-03)

| Item | Value | Notes |
|---|---|---|
| OS | Windows 11 Pro | Commands run from Git Bash or PowerShell |
| CPU / RAM | i7-14650HX (24 logical CPUs), 15.7 GB RAM | ~3.4 GB free with the desktop in use |
| Docker | Docker Desktop 29.1.3, Compose v2.40.3 | WSL2 backend |
| Memory available to Docker | 8,170,668,032 bytes (7.6 GiB) | WSL2 default (no `.wslconfig`): the whole stack must fit in this |
| uv | 0.9.9 | Python 3.12 pinned in `.python-version` |
| make | GNU Make 4.4.1 | |
| Java | not installed | Not needed: Spark runs in a container |
| Disk | 194 GB free on C: | Raw captures grow daily; check before long runs |

**Memory budget rule:** services are split into Compose profiles (`core` always on; `airflow`, `monitoring`, `serving` started on demand), each with a memory limit. Measured usage is recorded here as services are added.

## 1. Tools

### uv
- Why: one tool for the Python version, virtual environment and lock file (`uv.lock`), so every machine installs identical versions.
- Check: `uv --version`
- Install: https://docs.astral.sh/uv/getting-started/installation/
- Verify: `make setup` creates `.venv`.

### Docker Desktop + Compose
- Why: Redpanda, SeaweedFS, Postgres, Spark and Airflow all run as containers.
- Check: `docker info --format '{{.MemTotal}}'` prints the memory available to Docker (fails if Docker Desktop is not running).
- Verify: `docker compose version`.

### make
- Why: one short, documented entry point per task (`make help` lists them).
- Check: `make --version`.

## 2. Network Rail data feeds account

- Why: the live TRUST movement feed and the CORPUS reference file both require a free account.
- Register: https://publicdatafeeds.networkrail.co.uk (places are limited; activation is not instant). The account has access to all feeds; no per-feed subscription is needed.
- Credentials: the STOMP login is the account email and password. Copy `.env.example` to `.env` and fill in `NR_USERNAME` / `NR_PASSWORD`. `.env` is git-ignored (`git check-ignore .env` prints `.env`).
- Check the network path: `Test-NetConnection publicdatafeeds.networkrail.co.uk -Port 61618` (PowerShell) reports `TcpTestSucceeded : True` (checked 2026-11-03).
- Verified 2026-11-03: a 60 s subscription to `/topic/TRAIN_MVT_ALL_TOC` received live messages (findings F1).

### Feeds used

| Feed | Use |
|---|---|
| MOVEMENT (`TRAIN_MVT_ALL_TOC`, STOMP) | Core stream: train movement events |
| CORPUS (static, HTTP) | Location reference: STANOX → name, TIPLOC, 3-letter station code |
| SCHEDULE (static, HTTP) | Planned timetable; later, for planned-vs-actual questions |

### Downloading CORPUS by hand

```bash
set -a && . ./.env && set +a
curl -L -u "$NR_USERNAME:$NR_PASSWORD" -o data/reference/CORPUSExtract.json.gz \
  "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS"
```

The endpoint redirects to a pre-signed S3 URL; the file is gzipped JSON with one key, `TIPLOCDATA` (findings F3). `data/` is git-ignored.

### Capturing the live feed

The STOMP bridge (`ingest/stomp_bridge`) holds a durable subscription to `TRAIN_MVT_ALL_TOC`, writes every frame to `<out>/date=YYYY-MM-DD/hour=HH.jsonl` (UTC hours; completed hours are compressed to `.jsonl.gz`) and acknowledges it only after it is on disk. A restart within 5 minutes loses nothing (findings F5).

**Exactly one bridge may run at a time.** A second bridge with the same `client-id` is not rejected: the broker splits the messages between them, so each archive silently holds about half (findings F6).

#### Always-on capture VM (the canonical archive)

The feed is captured 24/7 on an Oracle Cloud **Always Free** VM (`VM.Standard.E2.1.Micro`, Ubuntu 24.04, London; created 2026-11-05) because a laptop cannot run around the clock. Only Always Free resources are used: £0 during and after the trial.

| What | Where |
|---|---|
| Host | `CAPTURE_HOST` in `.env` (`ubuntu@<public-ip>`); SSH key `~/.ssh/railstream_oci` (key login only) |
| Setup (one-off, idempotent) | `scp infra/capture/setup-vm.sh` to the VM and run `bash setup-vm.sh`: 2 GB swap, security updates, Docker, clone of this repo |
| Credentials | `~/railstream/.env` on the VM, only the `NR_` lines, mode 600 |
| Run | `cd ~/railstream && docker compose -f infra/capture/compose.yaml up -d --build` (`restart: unless-stopped`, 256 MB memory cap, logs capped at 30 MB) |
| Logs | `docker compose -f infra/capture/compose.yaml logs -f` (per-minute `frames=… messages=…`; ~28 frames/min at peak) |
| Update code | `git pull` then the run command again |
| Get the data | on the laptop: `make pull-raw` copies completed hours into `data/raw/trust/` (never the open hour) |

Networking notes from setup: the instance needed the "Connect public subnet to internet" quick action (internet gateway + route) and an **ephemeral public IP** added under VNIC → IP administration; neither was created by the instance wizard after the shape was changed.

#### Live bridge on the laptop (`make live`)

Feeds the local pipeline. It uses **its own durable subscription** (`railstream-live`, client-id `<account>-railstream-live`), which receives a full copy of the feed (findings F10), so it runs alongside the VM without splitting the archive. Needs `make up` first.

- Each frame is written to `data/raw/trust-live/` and acknowledged as on the VM, then its messages are published to Kafka `raw.trust.movement` (one record per TRUST message, key = train id, idempotent producer). Publishing is asynchronous and after the ACK: if Kafka is down, capture continues and the bridge logs `NOT IN KAFKA: frames received <from>–<to>`; that window is refilled from disk with the replay tool.
- Every 60 s: `frames=… messages=… bytes=… last_frame_age_s=…` (a growing age = quiet feed; a sudden halving of `frames` = a second bridge on the same subscription, F6) and `kafka sent=… delivered=… failed=…`.
- Stop with Ctrl+C (outstanding Kafka deliveries are flushed). When the laptop is off, the broker keeps the subscription's messages for 5 minutes only; longer gaps are covered by the VM archive.
- Disk on the VM: check `du -sh data/raw` now and then; daily growth is measured from the capture, not estimated.

#### Replay (`make replay`)

Re-publishes captured frames to `raw.trust.movement` through the same producer as the live bridge, with the original receive time in the `received_at` header and `source=replay`. Reads `data/raw/trust/` (the VM archive; run `make pull-raw` first) unless `--root` says otherwise.

| Use | Command |
|---|---|
| Development: a day at 10× (2.4 h), with real bursts and quiet periods | `make replay DAY=2026-11-06` |
| Backfill after the laptop was off (no waiting; measured ~40k messages/s, so a day takes ~20 s) | `make replay DAY=2026-11-06 SPEED=0` |
| Refill a window the live bridge logged as `NOT IN KAFKA` | `make replay FROM=2026-11-07T15:00 TO=2026-11-07T15:05 SPEED=0` |

Times are UTC (a timestamp without an offset is read as UTC). Replaying a window that is already in Kafka publishes it again; silver dedupes. Measured 2026-11-07: the 08:00 UTC peak hour of 2026-11-06 (52,386 messages) in 1.3 s at full speed; 5 minutes at 10× in 29.97 s (expected 29.8 s).

## 3. Rail Data Marketplace account (validation data only)

- Why: the official **historic delay attribution** data, used once to sanity-check the gold delay models. It is not used by the running pipeline.
- Register: https://raildata.org.uk (separate, free account; the Network Rail feeds login does not work there).
- Dataset: search "NWR Historic Delay Attribution", subscribe (Open Government Licence), then download yearly `.zip` files from its "data files" tab by hand into `data/reference/delay_attribution/` (git-ignored).
- No credentials in `.env`: the dataset is a manual file download, not an API, so the code never logs in.

## 4. Local stack (`make up`)

`infra/compose/compose.yaml`, run through the Makefile. All ports bind to `127.0.0.1`; credentials are local-only defaults, overridable in `.env` (see `.env.example`).

| Service | Image (checked 2026-11-05) | Host port | Role |
|---|---|---|---|
| Redpanda | `redpandadata/redpanda:v26.1.20` | 19092 (Kafka), 18081 (Schema Registry) | ADR-001 |
| SeaweedFS | `chrislusf/seaweedfs:4.52` | 8333 (S3 API) | Object storage for Iceberg (replaces MinIO, findings F7) |
| Postgres | `postgres:18.6-alpine` | 5434 | Lakekeeper catalog database (Airflow later) |
| Lakekeeper | `quay.io/lakekeeper/catalog:v0.14.2` | 8181 | Iceberg REST catalog, ADR-003 |
| Redpanda Console (`make tools`) | `redpandadata/console:v3.13.1` | 8085 | Web UI: topics, messages, schemas, lag |
| Spark bronze job (`make stream`) | `railstream-spark:dev`, built from `infra/spark/Dockerfile` (Spark 4.1.4, Iceberg 1.12.0, Python 3.12, Java 17) | 4040 (Spark UI) | Kafka → Iceberg `bronze.trust_movement`, ADR-007; 1.5 GB limit, ~705 MiB in use |

- `make up` starts the services, waits until healthy, then runs `lakehouse-init` (idempotent): creates the `warehouse` bucket, bootstraps Lakekeeper and creates the `railstream` warehouse (Lakekeeper validates storage by writing a test object). Then it creates the Kafka topics declared in `ingest/stream/topics.py` (`make topics`; existing topics are kept). Safe to run repeatedly.
- Kafka from the laptop: **`127.0.0.1:19092`, not `localhost:19092`**. On Windows `localhost` resolves to IPv6 `::1` as well, where nothing listens, and librdkafka failed about 1 in 20 fresh connections when it tried `::1` first (30/30 succeeded with `127.0.0.1`). Redpanda also advertises `127.0.0.1:19092`, because clients reconnect to the advertised address, not the bootstrap one.
- `make tools` (Console), `make ps`, `make logs`, `make stats`, `make down` (keeps data volumes).
- `make stream` starts the bronze job (builds the image the first time, ~4 min); `make stream-logs` shows one line per micro-batch. Its checkpoint is in the `spark-checkpoints` volume: deleting that volume makes the next run start again from the earliest Kafka offset (duplicates in bronze), so do not, unless the table is dropped too.
- Remote signing: clients do not need S3 keys; Lakekeeper signs storage requests for them.
- **Open (Day 5/9):** Lakekeeper tells clients the storage endpoint is `http://seaweedfs:8333`, which resolves only inside the Docker network. Clients inside the network (Spark) are fine; clients on the laptop (DuckDB, dbt, the API) need either to run in the network or an endpoint name that resolves on both sides.
- Ports 5432/5433 (other local databases), 8000/8010/3000/3001/8080/8081 (other projects, Relay) are avoided.

**Memory baseline (2026-11-05, idle, no data):** Redpanda 177.5 MiB, Postgres 116.3 MiB, SeaweedFS 46.6 MiB, Console 40.9 MiB, Lakekeeper 31.0 MiB: **≈ 412 MiB** of ~7.6 GiB. Images ≈ 2.5 GB on disk. Re-measured with data flowing and with Spark (Day 5).

## 5. Python project

- Recovery behaviour, replay exit statuses, and Kafka retention/checkpoint
  recovery: [recovery.md](recovery.md). Do not reset checkpoints to bypass an
  expired-offset error.

- `make setup`: install from `uv.lock`.
- `make lint` / `make format`: ruff.
- `make it`: integration tests against the running stack (`make up`, then `make stream` once to build the Spark image). Bronze: N messages in = N rows out, and a kill test (three SIGKILLs mid-run, restart from the checkpoint, no duplicates or loss). Each test uses its own topic, table and checkpoint; about 2 minutes. Not part of `make test` or CI (needs Docker and the stack).
- The project is an application, not a library (`[tool.uv] package = false`); modules run with `python -m` from the repo root.
- `make schemas`: register `contracts/` in the Schema Registry (also part of `make up`). It first checks every contract against all registered versions (BACKWARD_TRANSITIVE plus the required-field guard, ADR-006) and registers nothing that fails. `uv run python -m ingest.stream.schemas --check` only checks.
- Changing a contract locally: `scripts/check_contracts.sh <base-commit>` against an **empty** registry checks the working tree (uncommitted edits are kept). A scratch registry: `docker run -d --rm --name rp-scratch -p 127.0.0.1:28081:8081 redpandadata/redpanda:v26.1.20 redpanda start --mode dev-container --smp 1 --schema-registry-addr 0.0.0.0:8081`, then `SCHEMA_REGISTRY_URL=http://127.0.0.1:28081`.

## 6. Continuous integration

`.github/workflows/ci.yml` runs on every push to `main` and every pull request:

| Job | What |
|---|---|
| Lint and unit tests | `uv sync --locked`, `make lint`, `make test` (no network or Docker needed by the tests) |
| Contract compatibility | Starts an empty Redpanda (same image as the local stack), registers the base commit's contracts, checks the changed ones (`scripts/check_contracts.sh`); an incompatible change fails the job |


## 7. Silver processor (Day 6)

After bronze exists (`make up`, then `make stream`), load the reference data once with
`make load-corpus` (`data/reference/CORPUSExtract.json.gz`; runs the same writer as the
`refresh_corpus` DAG, `orchestration/corpus.py`, in the Airflow image without starting Airflow).
Then `make silver` starts the
revision-aware processor and `make silver-logs` shows movements/rejects/other counts.
Its Spark UI is localhost:4041; its checkpoint is `/checkpoints/silver_trust_movement_v2`
in the existing checkpoint volume. One writer owns a namespace/checkpoint.

Tables: `silver.trust_movement` (latest planned-call row),
`silver.movement_revisions` (distinct revision history),
`silver.rejected_messages` (durable rejects published to Kafka's DLQ). Equal-time
conflicts and off-route unresolved identities are explicit flags, not hidden guesses.
See `silver-design.md` and ADRs 008/009 for guarantees and limitations. A guarded
15-minute live watermark bounds duplicate state; history/corrections bypass it.
The one-row-per-STANOX left join adds names, CRS, TIPLOC, a match flag and reference
version. The loader retains immutable versions and is safe to repeat. Loading a
new reference refreshes affected current rows on processing or replay, not all
historical rows immediately.

The stateful query uses a new v2 checkpoint because its plan differs from v1.
Keep v1 intact; v2 rereads retained bronze, with durable MERGEs preventing extra
current/revision rows. Kafka DLQ copies may repeat under their stable keys.
The silver heap is 768 MiB, shuffle partitions two, container cap 2 GiB;
batch materialization uses disk to leave room for Python and native memory.
The earlier stateless backlog was sampled near 1.4 GiB. The new plan sampled
1.462 GiB during the real reread, so its original 1.5 GiB cap was raised. Core,
tools, bronze and silver limits total 6 GiB of Docker's ~7.6 GiB; add later
profiles only after measuring their budget. Runtime measurements
for the updated job are recorded under `results/dev/2026-11-16-silver-state/`.

Focused integration test: `uv run pytest -m integration -s tests/integration/test_silver.py`.
It creates isolated retained tables/topic/checkpoint, with no drops or kills.
The test mounts `tests/` and `streaming/` read-only into the built Spark image;
shared `trust/` code must be current in the image. `make it` also includes the
older destructive bronze kill tests and needs explicit approval before execution.

## 8. Airflow reference refresh (Day 7)

Airflow runs in Docker, not the Windows Python environment. Docker Desktop and
Compose are the only prerequisites: check `docker info` and `docker compose version`.
From the project root in PowerShell, on this machine:

```powershell
$env:DOCKER_CONTEXT = "desktop-linux"
make airflow
```

The first run builds `railstream-airflow:dev` on `apache/airflow:3.4.0-python3.12`,
adding PyIceberg 0.12.0 and its S3 dependencies under Airflow's pinned constraints.
The target starts Postgres/catalog/storage, initializes the bucket and warehouse
idempotently, creates only missing Airflow database resources, then waits for
Airflow to become healthy.
No data volume is reset. `make up` still starts
the streaming core; it does not enable Airflow automatically.

Open **http://localhost:8086**. Local login: username `railstream`, default password
`railstream-local` (override `AIRFLOW_UI_PASSWORD` in `.env` if desired). These are
development defaults. Never reuse Network Rail credentials. Airflow's UI login
and database password are separate. Passwords containing reserved URI characters
are encoded before database connection. `_CMD` output must have no trailing
newline; a regression test covers that boundary.

`refresh_corpus` is the reference workflow (a setup-only smoke DAG used on
2026-11-16 was removed once this real DAG proved scheduling and execution):

```text
download (Network Rail HTTPS) -> validate (CORPUS + STANOX rules)
                            -> load (Lakekeeper -> Iceberg ref.locations)
```

It is enabled on a daily **04:00 UTC** schedule, with two retries, a one-minute
retry delay and one active run. You can also trigger it manually in the UI.
Open its latest successful run, then each task's logs. The load log reports the
content version, selected locations and inserted count. A repeated content
version reports `inserted=0`. Previous versions and their timestamps stay intact.
Silver uses the newest reference on its next micro-batch; new input or replay
refreshes affected existing current rows. Streaming services need not run for
the reference workflow to execute.

Always use `make airflow` to start/rebuild it: the target explicitly passes
`--env-file .env`. Direct Compose commands that omit that flag may miss the
Network Rail credentials with this Compose file's nested location. Only
`NR_USERNAME`/`NR_PASSWORD` are injected for the download; no credentials are
copied into the image. Redirects to signed storage lose the Basic Auth header,
and task results contain paths/hash/counts, not passwords or signed URLs.
Reference files live inside `airflow-home` at `/opt/airflow/reference/corpus/`,
with one directory per run. They are not automatically copied to the laptop's
`data/reference/` folder.

The laptop and Docker must be on for scheduled tasks. `catchup=False` avoids
replaying every missed daily reference interval; the latest due refresh may run
on startup. The cloud VM continues archiving movements independently. Automated
movement catch-up from that VM remains future work; it is not supplied by this DAG.

Proposed movement catch-up follow-up: a separate workflow pulls only completed
VM hour files, preflights coverage with the existing replay checks, then replays
the missing/overlapping UTC window into Kafka. Record an archive cursor only
after confirmed Kafka delivery; silver's durable keys handle overlap. Keep feed
gaps explicit instead of treating missing hours as success. This is a proposal,
not an implemented automatic restart/backfill feature.

```powershell
make airflow-logs
make ps
docker compose --env-file .env -f infra/compose/compose.yaml --profile airflow stop airflow
```

`make down` includes the Airflow profile and preserves named volumes. Airflow
metadata lives in its own `airflow` Postgres database/role; logs and runtime files
live in `airflow-home`. DAG code under `orchestration/dags/` is mounted read-only.
Rerunning bootstrap never drops a database or changes an existing role password.
If existing credentials differ from `.env`, resolve that explicitly rather than
resetting the database. Postgres initialization scripts run automatically only
on a new volume, so `make airflow` explicitly runs the idempotent Airflow script
for existing volumes too.

The profile uses LocalExecutor with one task, one API worker and one parser.
Health checks verify API access plus database/scheduler/parser/triggerer health.
Observed after the smoke task: Airflow about 1.01 GiB of its 1.5 GiB cap, Postgres
about 86 MiB of 256 MiB. These are sampled development observations, not capacity
benchmarks. All current core/tools/stream/Airflow limits total 7.5 GiB, near
Docker's ~7.6 GiB. Keep unused profiles stopped; larger tasks still need measurement.
The first setup started only Airflow/Postgres; reference workflows also need
Lakekeeper and SeaweedFS, now started as dependencies. Kafka, the streaming jobs
and Console remain stopped unless requested. Standalone is for local development,
with deployment changes deferred to the cloud hosting step (ADR-010).

During/after a repeated real CORPUS run, six samples reached 1.272 GiB under the
1.5 GiB cap with no observed OOM/restart. This remains a sampled observation;
the entire streaming stack was not running concurrently during this check.

Focused non-destructive workflow test:
`uv run pytest -m integration -s tests/integration/test_corpus.py` (after `make airflow`).
It imports/wires the real DAG and writes only an isolated retained reference table.
It verifies repeated versions create neither duplicate rows nor another snapshot,
old versions survive a changed file, and changed input cannot bypass validation.
The broader `make it` still contains destructive bronze tests; do not use it for
this check. Generated real-run and Spark interoperability evidence is under
`results/dev/2026-11-16-corpus-refresh/`.
