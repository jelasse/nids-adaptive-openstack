#!/usr/bin/env bash
# Copies the small evidence files of all runs into runs/ (no Parquet predictions, no checkpoints).
# Usage (from the repository root):  bash collect_runs.sh
#   runs/pilot_M, runs/pilot_L   <- ~/w5/runs_M, ~/w5/runs_L            (Week 6 pilot)
#   runs/main/<run>              <- ~/w7/runs/<run>                      (VM side, copied from the VM)
#   runs/main/<run>/host         <- ~/w5/host_runs/<run>                 (laptop side: producers, laptop telemetry)
set -euo pipefail
copy_small() {   # copy_small <src> <dst>
  [ -d "$1" ] || { echo "skip: $1 not found"; return; }
  mkdir -p "$2"
  rsync -a --prune-empty-dirs \
    --exclude 'predictions/' --exclude 'checkpoint*' --exclude 'STOP' --exclude 'mode' \
    --include '*/' --include '*.json' --include '*.jsonl' --include '*.csv' --include '*.log' \
    --include '*.txt' --include 'records_sent' --exclude '*' \
    "$1"/ "$2"/
}
copy_small "$HOME/w5/runs_M" runs/pilot_M
copy_small "$HOME/w5/runs_L" runs/pilot_L
if [ -d "$HOME/w7/runs" ]; then
  for r in "$HOME"/w7/runs/*/; do
    n=$(basename "$r")
    copy_small "$r" "runs/main/$n"
    [ -d "$HOME/w5/host_runs/$n" ] && copy_small "$HOME/w5/host_runs/$n" "runs/main/$n/host"
  done
fi
cp -f "$HOME/w5/clock_offsets.csv" runs/ 2>/dev/null || true
echo "runs/ now holds $(find runs -type f | wc -l) files ($(du -sh runs | cut -f1))"
