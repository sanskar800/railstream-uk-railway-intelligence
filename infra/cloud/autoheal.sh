#!/usr/bin/env bash
# Restart RailStream containers that Docker reports as unhealthy (railstream-autoheal.timer).
# Docker's restart policy acts only when a container exits; an unhealthy one keeps running.
# On 2026-11-17 Airflow's scheduler died when Postgres was recreated, the container stayed up
# and unhealthy, and nothing was scheduled until a manual restart (findings F25).
set -euo pipefail
for name in $(docker ps --filter health=unhealthy --format '{{.Names}}' | grep '^railstream-' || true); do
  echo "unhealthy: $name; restarting"
  docker restart "$name" >/dev/null
done
