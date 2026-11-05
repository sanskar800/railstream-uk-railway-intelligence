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
- Why: Redpanda, MinIO, Postgres, Spark and Airflow all run as containers.
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

#### Local capture (`make capture`)

For development only, and **only while the VM bridge is stopped**: writes to `data/raw/trust-local/` so it can never collide with the VM archive in `data/raw/trust/`. Stop with Ctrl+C.

- Every 60 s the bridge logs `frames=… messages=… bytes=… last_frame_age_s=…`. A growing `last_frame_age_s` means the feed went quiet; a sudden halving of `frames` means a second bridge is connected (F6).
- Disk: check `du -sh data/raw` on the VM now and then; daily growth is measured from the capture, not estimated.

## 3. Rail Data Marketplace account (validation data only)

- Why: the official **historic delay attribution** data, used once to sanity-check the gold delay models. It is not used by the running pipeline.
- Register: https://raildata.org.uk (separate, free account; the Network Rail feeds login does not work there).
- Dataset: search "NWR Historic Delay Attribution", subscribe (Open Government Licence), then download yearly `.zip` files from its "data files" tab by hand into `data/reference/delay_attribution/` (git-ignored).
- No credentials in `.env`: the dataset is a manual file download, not an API, so the code never logs in.

## 4. Python project

- `make setup`: install from `uv.lock`.
- `make lint` / `make format`: ruff.
- The project is an application, not a library (`[tool.uv] package = false`); modules run with `python -m` from the repo root.
