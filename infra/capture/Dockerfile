# STOMP bridge image: captures the TRUST feed to /data/raw/trust.
# Built from the repo root: docker compose -f infra/capture/compose.yaml build
FROM python:3.12-slim

# uv from PyPI (pinned) rather than its ghcr.io image: one registry fewer to depend on.
RUN pip install --no-cache-dir uv==0.9.9

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

# Dependencies first (cached layer), exact versions from uv.lock, no dev tools.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev

COPY ingest ./ingest

# Run as an unprivileged user; /data is a bind mount owned by this uid on the host.
RUN useradd --uid 10001 --no-create-home bridge
USER bridge

CMD ["python", "-m", "ingest.stomp_bridge", "--out", "/data/raw/trust"]
