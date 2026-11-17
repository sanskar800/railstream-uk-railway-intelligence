#!/usr/bin/env bash
# Hourly (railstream-backup.timer): copy completed raw hours to Oracle Object Storage, then verify
# that every completed local hour exists there with the same size.
#
# Authentication: instance principal. The VM proves its identity to Oracle; no keys are stored.
# The IAM policy lets it list, read and create objects in this bucket only, never delete or
# overwrite, so a compromised VM cannot destroy the backups. Hence --ignore-existing: hour
# files are immutable once compressed, and an overwrite would be refused anyway.
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
env_get() { grep -s "^$1=" "$APP_DIR/.env" | tail -1 | cut -d= -f2-; }

NAMESPACE="$(env_get BACKUP_NAMESPACE)"
BUCKET="$(env_get BACKUP_BUCKET)"
BUCKET="${BUCKET:-railstream-raw-archive}"
[ -n "$NAMESPACE" ] || { echo "BACKUP_NAMESPACE is not set in $APP_DIR/.env" >&2; exit 2; }

# Region and compartment come from the instance metadata service, not from configuration.
META="$(curl -sf -H "Authorization: Bearer Oracle" http://169.254.169.254/opc/v2/instance/)"
meta() { python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])" <<<"$META"; }

export RCLONE_CONFIG_OCI_TYPE=oracleobjectstorage
export RCLONE_CONFIG_OCI_PROVIDER=instance_principal_auth
export RCLONE_CONFIG_OCI_NAMESPACE="$NAMESPACE"
export RCLONE_CONFIG_OCI_REGION="$(meta canonicalRegionName)"
export RCLONE_CONFIG_OCI_COMPARTMENT="$(meta compartmentId)"

SRC="$APP_DIR/data/raw/trust"
DST="oci:$BUCKET/trust"
ONLY_COMPLETED=(--include "date=*/hour=*.jsonl.gz")  # never the open plain-text hour

rclone copy "$SRC" "$DST" "${ONLY_COMPLETED[@]}" --ignore-existing --oos-no-check-bucket \
  --transfers 2 --stats-one-line -v
rclone check "$SRC" "$DST" "${ONLY_COMPLETED[@]}" --one-way --size-only --oos-no-check-bucket
echo "backup verified: $(find "$SRC" -name 'hour=*.jsonl.gz' | wc -l) completed hours in $DST"
