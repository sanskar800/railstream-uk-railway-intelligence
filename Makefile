# Short aliases for the commands documented in docs/development.md.
# Recipes are plain `uv` / `docker compose` calls, so they work under sh (Linux, CI) and cmd (Windows).

# .env is optional for the local stack: every variable it can set has a local default.
COMPOSE = docker compose $(if $(wildcard .env),--env-file .env,) -f infra/compose/compose.yaml

.PHONY: help setup test lint format capture pull-raw up down ps logs tools stats

help:  ## List targets
	@uv run python -c "import re; [print(f\"{m[0]:<12} {m[1]}\") for m in re.findall(r\"^([a-z-]+):.*## (.*)$$\", open(\"Makefile\").read(), re.M)]"

setup:  ## Create .venv and install exact versions from uv.lock
	uv sync

test:  ## Run unit tests (no network or Docker needed)
	uv run pytest

capture:  ## Capture locally to data/raw/trust-local (not while the VM capture runs; Ctrl+C to stop)
	uv run python -m ingest.stomp_bridge --out data/raw/trust-local

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
