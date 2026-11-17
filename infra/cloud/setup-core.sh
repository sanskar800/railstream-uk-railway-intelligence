#!/usr/bin/env bash
# One-time setup of the processing VM (Oracle Always Free Ampere A1: ARM, 2 OCPU / 12 GB,
# Ubuntu 24.04). Runs the whole local stack (make up / stream / silver / airflow). Idempotent.
# Usage on the VM: bash setup-core.sh
set -euo pipefail

REPO_URL="https://github.com/sanskar800/railstream-uk-railway-intelligence.git"
APP_DIR="$HOME/railstream"
UV_VERSION="0.9.9"   # same as the laptop and the Docker images
BRIDGE_UID=10001     # the bridge container's unprivileged user (infra/capture/Dockerfile)

echo "== swap: 4 GB of headroom for JVM spikes; the stack is sized to stay in RAM"
if ! swapon --show | grep -q /swapfile; then
  sudo fallocate -l 4G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo "/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab
fi

echo "== packages: security updates, Docker, Compose, git, make"
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
  docker.io docker-compose-v2 git make unattended-upgrades
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

echo "== uv $UV_VERSION (runs the Makefile's Python helpers: topics, schemas, replay)"
if ! "$HOME/.local/bin/uv" --version 2>/dev/null | grep -q "$UV_VERSION"; then
  curl -LsSf "https://astral.sh/uv/$UV_VERSION/install.sh" | sh
fi

echo "== code"
if [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$APP_DIR"
fi

echo "== raw data directories (live bridge writes here; archive copies land here)"
mkdir -p "$APP_DIR/data/raw/trust" "$APP_DIR/data/raw/trust-live" "$APP_DIR/data/reference"
sudo chown -R "$BRIDGE_UID:$BRIDGE_UID" "$APP_DIR/data/raw"

echo "== done. Next: copy .env to $APP_DIR/.env (chmod 600), log in again (docker group), make up."
