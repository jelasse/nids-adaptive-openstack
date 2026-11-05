#!/usr/bin/env bash
# Offered load from the LAPTOP into Kafka in the VM, split over several producer processes.
# Usage:  bash ~/nids-host/stream/produce_parallel.sh <topic> <total_rate> <records> [processes] [replay_file] [bootstrap]
#   e.g.  bash ~/nids-host/stream/produce_parallel.sh L_full_45k 45000 2700000 2
# Process i sends rows i*N .. (i+1)*N-1 of the replay file with event IDs equal to the row numbers,
# so together they send event IDs 0 .. records-1, as analyze_run.py expects.
set -euo pipefail
TOPIC=$1; RATE=$2; TOTAL=$3; P=${4:-2}
REPLAY=${5:-$HOME/w5/replay/val.parquet}
BOOT=${6:-172.24.4.36:9094}
DIR=$(cd "$(dirname "$0")" && pwd)
N=$(( TOTAL / P )); R=$(( RATE / P ))
mkdir -p "$HOME/w5/producer_logs"
echo "Sending $TOTAL records to $TOPIC at $RATE/s with $P processes ($R/s and $N records each) via $BOOT"
pids=()
for i in $(seq 0 $((P - 1))); do
  python "$DIR/replay_producer.py" --replay "$REPLAY" --bootstrap "$BOOT" --topic "$TOPIC" \
     --rate "$R" --limit "$N" --start-row $((i * N)) --start-id $((i * N)) \
     > "$HOME/w5/producer_logs/${TOPIC}_p$i.log" 2>&1 &
  pids+=($!)
done
for p in "${pids[@]}"; do wait "$p" || true; done
grep -h -E "SENT|WARNING" "$HOME/w5/producer_logs/${TOPIC}"_p*.log
