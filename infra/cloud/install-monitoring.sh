#!/usr/bin/env bash
# Install the per-minute feed_age_seconds publisher and the unhealthy-container watchdog on the
# processing VM. Idempotent.
# Prerequisites (Oracle console, ADR-014): Notifications topic railstream-alerts with a confirmed
# email subscription; dynamic group railstream-core; policy railstream-metrics.
# Usage on the VM: bash ~/railstream/infra/cloud/install-monitoring.sh
set -euo pipefail
APP_DIR="$HOME/railstream"
cd "$APP_DIR"
echo "== Python dependencies (ops group: Oracle SDK)"
"$HOME/.local/bin/uv" sync --frozen --group ops -q
echo "== one dry run (reads Kafka, publishes nothing)"
"$HOME/.local/bin/uv" run --frozen --group ops python -m ops.feed_age --dry-run
echo "== systemd timer"
chmod +x infra/cloud/autoheal.sh
sudo cp infra/cloud/railstream-feed-age.service infra/cloud/railstream-feed-age.timer \n        infra/cloud/railstream-autoheal.service infra/cloud/railstream-autoheal.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now railstream-feed-age.timer railstream-autoheal.timer
systemctl list-timers "railstream-*" --no-pager | head -4
echo "== done. Logs: journalctl -u railstream-feed-age / -u railstream-autoheal"
