#!/bin/sh
set -eu
: "${RELAY_SNAPSHOT_PLAN:?Set RELAY_SNAPSHOT_PLAN to the non-secret JSON folder plan}"
: "${VAULT_RELAY_ROOT:?Set VAULT_RELAY_ROOT to the private snapshot directory}"
: "${RELAY_SERVER_API_KEY:?Supply the authorized Relay credential through environment injection}"
: "${RELAY_SNAPSHOT_BUDGET_BYTES:?Set measured snapshot allocation budget}"
: "${RELAY_SNAPSHOT_RESERVE_BYTES:?Set approved free-space reserve}"
: "${RELAY_SNAPSHOT_RESERVE_INODES:?Set approved inode reserve}"
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
cd "$script_dir/.."
exec uv run --frozen --no-dev relay-snapshot-export \
  --config "$RELAY_SNAPSHOT_PLAN" --root "$VAULT_RELAY_ROOT" \
  --interval "${RELAY_SNAPSHOT_INTERVAL:-10}" --retain "${RELAY_SNAPSHOT_RETAIN:-3}" \
  --budget-bytes "$RELAY_SNAPSHOT_BUDGET_BYTES" \
  --reserve-bytes "$RELAY_SNAPSHOT_RESERVE_BYTES" --reserve-inodes "$RELAY_SNAPSHOT_RESERVE_INODES"
