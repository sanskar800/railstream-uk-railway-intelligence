#!/usr/bin/env bash
# One-time setup of the capture VM (Ubuntu 24.04). Idempotent: safe to re-run.
# Usage on the VM: bash setup-vm.sh
set -euo pipefail

REPO_URL="https://github.com/sanskar800/railstream-uk-railway-intelligence.git"
APP_DIR="$HOME/railstream"
BRIDGE_UID=10001  # matches the user in infra/capture/Dockerfile

echo "== swap (the Micro shape has 1 GB RAM; swap gives builds headroom)"
if ! swapon --show | grep -q /swapfile; then
  sudo fallocate -l 2G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo "/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab
fi

echo "== packages: security updates, Docker, Compose, git"
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
  docker.io docker-compose-v2 git unattended-upgrades
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

echo "== code"
if [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$APP_DIR"
fi

echo "== data directory, writable by the container's unprivileged user"
mkdir -p "$APP_DIR/data/raw/trust"
sudo chown -R "$BRIDGE_UID:$BRIDGE_UID" "$APP_DIR/data/raw"

echo "== done. Next: copy .env to $APP_DIR/.env (chmod 600), then start the bridge."
