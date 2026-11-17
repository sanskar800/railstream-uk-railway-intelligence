"""STOMP bridge: durable subscription to the Network Rail TRUST feed with raw capture to disk.

Runs on the capture VM: `docker compose -f infra/capture/compose.yaml up -d --build`
(or locally: `uv run python -m ingest.stomp_bridge`).
"""
