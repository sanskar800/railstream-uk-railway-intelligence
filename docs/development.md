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

## 3. Python project

- `make setup`: install from `uv.lock`.
- `make lint` / `make format`: ruff.
- The project is an application, not a library (`[tool.uv] package = false`); modules run with `python -m` from the repo root.
