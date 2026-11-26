# Short aliases for the commands documented in docs/development.md.
# Recipes are plain `uv` / `docker compose` calls, so they work under sh (Linux, CI) and cmd (Windows).

# .env is optional for the local stack: every variable it can set has a local default.
COMPOSE = docker compose $(if $(wildcard .env),--env-file .env,) -f infra/compose/compose.yaml

.PHONY: verify-deploy attribution-check gold-backfill help setup test it tunnel sql dbt dbt-docs lint format replay topics schemas stream stream-logs silver silver-logs load-corpus airflow airflow-logs pull-raw up down ps logs tools stats

help:  ## List targets
	@uv run python -c "import re; [print(f\"{m[0]:<12} {m[1]}\") for m in re.findall(r\"^([a-z-]+):.*## (.*)$$\", open(\"Makefile\").read(), re.M)]"

setup:  ## Create .venv and install exact versions from uv.lock
	uv sync

test:  ## Run unit tests (no network or Docker needed)
	uv run pytest

it:  ## Integration tests against the running stack (make up; Spark image from make stream)
	uv run pytest -m integration -s tests/integration

SPEED ?= 10
replay:  ## Replay captured hours into Kafka: DAY=YYYY-MM-DD or FROM=.. TO=.. (UTC); SPEED=10, 0 = max
	uv run python -m ingest.replay $(if $(DAY),--day $(DAY),--from $(FROM) --to $(TO)) --speed $(SPEED)

tunnel:  ## Open the cloud UIs on localhost: Console 8085, Spark 4040/4041, Airflow 8086, Lakekeeper 8181/ui
	uv run python scripts/tunnel.py

# Q reaches the script as an environment variable, so no shell re-parses * or quotes in the SQL.
export Q SQL_ROWS
sql:  ## Query the cloud tables: make sql Q="SELECT ..." (SQL_ROWS=200 for more rows)
	uv run python scripts/cloud_sql.py

attribution-check:  ## Pattern check of gold vs official delay attribution (file in data/reference/)
	uv run --group analysis python scripts/attribution_check.py

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

stream:  ## Start the Spark bronze job (builds the image; Spark UI on http://localhost:4040)
	$(COMPOSE) --profile stream up -d --build spark-bronze

stream-logs:  ## Follow the Spark job's per-batch log
	$(COMPOSE) --profile stream logs -f spark-bronze

silver:  ## Start silver after make up, make stream and make load-corpus (UI on port 4041)
	$(COMPOSE) --profile stream up -d --build spark-silver

load-corpus:  ## Load data/reference/CORPUSExtract.json.gz with the DAG's writer (Airflow need not run)
	$(COMPOSE) --profile airflow build airflow
	$(COMPOSE) --profile airflow run --rm --no-deps -v "$(CURDIR)/data/reference:/reference:ro" airflow python -m orchestration.corpus

silver-logs:  ## Follow silver batch counts (movements / rejects / other TRUST types)
	$(COMPOSE) --profile stream logs -f spark-silver

# dbt runs in the Airflow image's own environment (/opt/dbt), inside the Docker network (ADR-017).
# ARGS reaches the container as an environment variable, so shells do not re-parse it.
export ARGS
dbt:  ## Run dbt on the stack: make dbt ARGS="build" (or "seed", "run -s delay_hotspots", ...)
	$(COMPOSE) --profile airflow run --rm --no-deps -e ARGS airflow bash -c 'mkdir -p /opt/airflow/dbt && /opt/dbt/bin/dbt $$ARGS'

# Rebuild gold for past dates, 7 days per dbt run so memory stays bounded (ADR-018). The vars
# are JSON without spaces: ARGS is word-split inside the container.
gold-backfill:  ## Rebuild gold history in 7-day chunks (on the VM): make gold-backfill FROM=2026-11-05
	@test -n "$(FROM)" || { echo "usage: make gold-backfill FROM=YYYY-MM-DD"; exit 1; }
	@d=$(FROM); end=$$(date -u -d tomorrow +%F); \
	while [ "$$d" \< "$$end" ]; do \
	  n=$$(date -u -d "$$d + 7 days" +%F); echo "== gold $$d .. $$n"; \
	  $(MAKE) --no-print-directory dbt ARGS='run --vars {"rebuild_from":"'$$d'","rebuild_to":"'$$n'"}' || exit 1; \
	  d=$$n; \
	done

verify-deploy:  ## On a VM: check the code in each running container matches this checkout
	bash scripts/verify_deploy.sh

dbt-docs:  ## Generate the dbt docs site on the cloud VM and save it to data/dbt-docs/index.html
	uv run python scripts/dbt_docs.py

airflow:  ## Start the local Airflow scheduler and UI on http://localhost:8086
	$(COMPOSE) up -d --wait postgres lakekeeper seaweedfs
	$(COMPOSE) --profile init run --rm lakehouse-init
	$(COMPOSE) exec -T postgres sh /docker-entrypoint-initdb.d/002-airflow.sh
	$(COMPOSE) --profile airflow up -d --build --wait airflow

airflow-logs:  ## Follow Airflow scheduler, API, parser and task logs
	$(COMPOSE) --profile airflow logs -f --tail=100 airflow

down:  ## Stop the stack (data volumes are kept)
	$(COMPOSE) --profile tools --profile init --profile stream --profile airflow down

ps:  ## Show stack status
	$(COMPOSE) --profile tools --profile stream --profile airflow ps -a

logs:  ## Follow stack logs
	$(COMPOSE) logs -f --tail=100

stats:  ## Memory and CPU per container (one snapshot)
	docker stats --no-stream --format "table {{.Name}}	{{.MemUsage}}	{{.CPUPerc}}"
