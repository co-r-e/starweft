#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <role> <data-dir> <listen> [display-name]" >&2
  exit 1
fi

ROLE="$1"
DATA_DIR="$2"
LISTEN="$3"
DISPLAY_NAME="${4:-}"

ARGS=(init --role "$ROLE" --data-dir "$DATA_DIR" --listen "$LISTEN")
if [ -n "$DISPLAY_NAME" ]; then
  ARGS+=(--display-name "$DISPLAY_NAME")
fi

starweft "${ARGS[@]}"

if [ "$ROLE" != "relay" ]; then
  starweft identity create --data-dir "$DATA_DIR"
fi

echo "bootstrapped role=$ROLE data_dir=$DATA_DIR"
