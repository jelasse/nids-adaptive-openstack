#!/usr/bin/env bash
# Copies the small evidence files of the pilot runs into runs/ (no Parquet predictions, no checkpoints).
# Usage: bash collect_runs.sh [source_M] [source_L]
set -euo pipefail
SRC_M=${1:-$HOME/w5/runs_M}
SRC_L=${2:-$HOME/w5/runs_L}
for pair in "$SRC_M:runs/pilot_M" "$SRC_L:runs/pilot_L"; do
  src=${pair%%:*}; dst=${pair##*:}
  [ -d "$src" ] || { echo "skip: $src not found"; continue; }
  mkdir -p "$dst"
  rsync -a --prune-empty-dirs \
    --exclude 'predictions/' --exclude 'checkpoint*' --exclude 'STOP' --exclude 'mode' \
    --include '*/' --include '*.json' --include '*.jsonl' --include '*.csv' --include '*.log' --exclude '*' \
    "$src"/ "$dst"/
  echo "copied $(find "$dst" -type f | wc -l) files from $src -> $dst ($(du -sh "$dst" | cut -f1))"
done
