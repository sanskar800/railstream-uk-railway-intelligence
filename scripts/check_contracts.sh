#!/usr/bin/env bash
# Check that the contracts in the working tree are compatible with those at a base commit.
#
#   scripts/check_contracts.sh <base-commit>
#
# Registers the base commit's contracts in an EMPTY Schema Registry (SCHEMA_REGISTRY_URL), then
# runs the compatibility check (registry + required-field guard, ADR-006) on the working tree's
# contracts. Exit code 1 = incompatible. Used by CI; runnable locally against a scratch registry.
set -euo pipefail

base="${1:-}"
zero="0000000000000000000000000000000000000000"

if [[ -n "$base" && "$base" != "$zero" ]] && git cat-file -e "${base}^{commit}" 2>/dev/null \
    && [[ -n "$(git ls-tree "$base" contracts/)" ]]; then
  echo "== registering contracts from base ${base:0:7}"
  # Keep the working-tree contracts aside (they may hold uncommitted edits) and always put them back.
  saved="$(mktemp -d)"
  cp -R contracts "$saved/"
  trap 'rm -rf contracts && cp -R "$saved/contracts" . && rm -rf "$saved"' EXIT
  rm -rf contracts && mkdir contracts
  git archive "$base" contracts/ | tar -x
  uv run python -m ingest.stream.schemas
  rm -rf contracts && cp -R "$saved/contracts" . && rm -rf "$saved"
  trap - EXIT
else
  echo "== base ${base:0:7} has no contracts: nothing to be compatible with"
fi

echo "== checking working-tree contracts"
uv run python -m ingest.stream.schemas --check
