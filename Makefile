# Short aliases for the commands documented in docs/development.md.
# Recipes are plain `uv` / `docker compose` calls, so they work under sh (Linux, CI) and cmd (Windows).

# .env is optional for the local stack: every variable it can set has a local default.
COMPOSE = docker compose $(if $(wildcard .env),--env-file .env,) -f infra/compose/compose.yaml

.PHONY: help setup test lint format live replay topics schemas pull-raw up down ps logs tools stats

help:  ## List targets
	@uv run python -c "import re; [print(f\"{m[0]:<12} {m[1]}\") for m in re.findall(r\"^([a-z-]+):.*## (.*)$$\", open(\"Makefile\").read(), re.M)]"

setup:  ## Create .venv and install exact versions from uv.lock
	uv sync

test:  ## Run unit tests (no network or Docker needed)
	uv run pytest

live:  ## Live bridge: own subscription -> data/raw/trust-live + Kafka (needs `make up`; Ctrl+C stops)
	uv run python -m ingest.stomp_bridge --subscription railstream-live --out data/raw/trust-live --kafka 127.0.0.1:19092

SPEED ?= 10
replay:  ## Replay captured hours into Kafka: DAY=YYYY-MM-DD or FROM=.. TO=.. (UTC); SPEED=10, 0 = max
	uv run python -m ingest.replay $(if $(DAY),--day $(DAY),--from $(FROM) --to $(TO)) --speed $(SPEED)

pull-raw:  ## Copy completed hour files from the capture VM (CAPTURE_HOST in .env)
	uv run python scripts/pull_raw.py

lint:  ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .

format:  ## Auto-fix lint issues and format
	uv run ruff check --fix .
	uv run ruff format .

up:  ## Start the core stack (Redpanda, SeaweedFS, Postgres, Lakekeeper) and initialise the lakehouse
	$(COMPOSE) up -d --wait
	$(COMPOSE) --profile init run --rm lakehouse-init
	uv run python -m ingest.stream.topics
	uv run python -m ingest.stream.schemas

schemas:  ## Register contracts/ in the Schema Registry (idempotent; refused if incompatible)
	uv run python -m ingest.stream.schemas

topics:  ## Create missing Kafka topics (idempotent; also part of `up`)
	uv run python -m ingest.stream.topics

tools:  ## Also start Redpanda Console on http://localhost:8085
	$(COMPOSE) --profile tools up -d --wait console

down:  ## Stop the stack (data volumes are kept)
	$(COMPOSE) --profile tools --profile init down

ps:  ## Show stack status
	$(COMPOSE) --profile tools ps -a

logs:  ## Follow stack logs
	$(COMPOSE) logs -f --tail=100

stats:  ## Memory and CPU per container (one snapshot)
	docker stats --no-stream --format "table {{.Name}}	{{.MemUsage}}	{{.CPUPerc}}"
