#!/usr/bin/env bash
# Does the code running in each container match this checkout? `make verify-deploy` (on a VM).
#
# The images COPY their code at build time; only the DAG files and the dbt project are mounted.
# A `git pull` without a rebuild therefore leaves old code running: on 2026-11-25 the Airflow
# image was two days stale and gold compaction (ADR-018) had never run (findings F34).
# Exit status 1 if any running container differs; containers that are not running are skipped.
set -u
cd "$(dirname "$0")/.."

status=0
check() {  # container, code root inside it, files...
  local container=$1 root=$2 differ=0
  shift 2
  if ! docker ps --format '{{.Names}}' | grep -qx "$container"; then
    echo "skip  $container (not running)"
    return
  fi
  for f in "$@"; do
    local repo image
    repo=$(md5sum "$f" | cut -c1-32)
    image=$(docker exec "$container" md5sum "$root/$f" 2>/dev/null | cut -c1-32)
    if [ "$repo" != "$image" ]; then
      echo "DIFF  $container: $f"
      differ=$((differ + 1))
    fi
  done
  if [ "$differ" -eq 0 ]; then
    echo "ok    $container ($# files)"
  else
    echo "STALE $container: $differ of $# files differ; rebuild its image"
    status=1
  fi
}

py() { git ls-files "$@" | grep -E '\.(py|json)$'; }

# Processing VM
check railstream-airflow-1 /opt/railstream $(py trust orchestration | grep -v '^orchestration/dags/')
for c in railstream-spark-bronze-1 railstream-spark-silver-1 railstream-spark-connect-1; do
  check "$c" /app $(py streaming trust ingest contracts)
done
# Capture VM (the bridge image holds only ingest/)
check railstream-capture-stomp-bridge-1 /app $(py ingest)

exit $status
