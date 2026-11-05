# Short aliases for the commands documented in docs/development.md.
# Recipes are plain `uv` / `docker compose` calls, so they work under sh (Linux, CI) and cmd (Windows).

.PHONY: help setup test lint format capture pull-raw

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
