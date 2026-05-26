#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="${CONDUCTOR_RS_IMAGE_TAG:-conductor-rs:local}"

run() {
  printf '\n+'
  printf ' %q' "$@"
  printf '\n'
  "$@"
}

cd "$ROOT"

run cargo fmt --check
run cargo test
run cargo clippy --all-targets --all-features -- -D warnings

if command -v docker >/dev/null 2>&1; then
  run docker build -t "$IMAGE_TAG" .
  run docker run --rm "$IMAGE_TAG" op-conductor --help
  run docker run --rm "$IMAGE_TAG" sh -c \
    'command -v op-conductor && command -v nc && command -v awk && command -v sh && command -v sleep && command -v ls'
else
  echo "Skipping Docker image check because docker is not on PATH."
fi

if [ "${CONDUCTOR_RS_RUN_OPTIMISM_CHECKS:-0}" = "1" ]; then
  if [ -z "${OPTIMISM_ROOT:-}" ]; then
    echo "CONDUCTOR_RS_RUN_OPTIMISM_CHECKS=1 requires OPTIMISM_ROOT=/path/to/optimism." >&2
    exit 1
  fi
  run scripts/audit-upstream-surface.sh
else
  echo "Skipping optional Optimism source checks. Set CONDUCTOR_RS_RUN_OPTIMISM_CHECKS=1 and OPTIMISM_ROOT=/path/to/optimism to run them."
fi

if [ "${CONDUCTOR_RS_RUN_LIVE:-0}" = "1" ]; then
  ran_live=0

  if [ -n "${CONDUCTOR_RS_LIVE_KONA_NODE_RPC:-}" ] && [ -n "${CONDUCTOR_RS_LIVE_KONA_EXECUTION_RPC:-}" ]; then
    run cargo test --test live_kona_conformance \
      live_kona_admin_rpc_supports_current_interop -- --ignored --nocapture
    ran_live=1
  fi

  if [ -n "${CONDUCTOR_RS_LIVE_CONDUCTOR_RPCS:-}" ]; then
    run cargo test --test live_kona_conformance \
      live_conductor_cluster_exposes_upstream_ha_contract -- --ignored --nocapture
    ran_live=1
  fi

  if [ "$ran_live" = "0" ]; then
    echo "CONDUCTOR_RS_RUN_LIVE=1 was set, but no live test env was complete." >&2
    echo "Set CONDUCTOR_RS_LIVE_KONA_NODE_RPC plus CONDUCTOR_RS_LIVE_KONA_EXECUTION_RPC, or set CONDUCTOR_RS_LIVE_CONDUCTOR_RPCS." >&2
    exit 1
  fi
else
  echo "Skipping live Kona/conductor checks. Set CONDUCTOR_RS_RUN_LIVE=1 with CONDUCTOR_RS_LIVE_* env to run them."
fi
