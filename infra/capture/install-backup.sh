#!/usr/bin/env bash
# Install the hourly raw-archive backup on the capture VM. Idempotent: safe to re-run.
# Prerequisites (Oracle console, ADR-013): bucket railstream-raw-archive, dynamic group
# railstream-capture, policy railstream-archive-backup; BACKUP_NAMESPACE in ~/railstream/.env.
# Usage on the VM: bash ~/railstream/infra/capture/install-backup.sh
set -euo pipefail

RCLONE_VERSION="v1.75.2"   # pinned; checked against the release's SHA256SUMS
APP_DIR="$HOME/railstream"
ARCH="$(dpkg --print-architecture)"   # amd64 on the capture VM

if ! rclone version 2>/dev/null | head -1 | grep -q "rclone $RCLONE_VERSION"; then
  echo "== rclone $RCLONE_VERSION ($ARCH)"
  tmp="$(mktemp -d)"
  deb="rclone-$RCLONE_VERSION-linux-$ARCH.deb"
  curl -fsSL -o "$tmp/$deb" "https://downloads.rclone.org/$RCLONE_VERSION/$deb"
  (cd "$tmp" && curl -fsSL "https://downloads.rclone.org/$RCLONE_VERSION/SHA256SUMS" \
     | grep " $deb\$" | sha256sum -c -)
  sudo dpkg -i "$tmp/$deb"
  rm -rf "$tmp"
fi

echo "== systemd timer"
chmod +x "$APP_DIR/infra/capture/backup-archive.sh"
sudo cp "$APP_DIR/infra/capture/railstream-backup.service" \
        "$APP_DIR/infra/capture/railstream-backup.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now railstream-backup.timer
systemctl list-timers railstream-backup.timer --no-pager | head -2
echo "== done. First run now: sudo systemctl start railstream-backup.service; logs: journalctl -u railstream-backup"
