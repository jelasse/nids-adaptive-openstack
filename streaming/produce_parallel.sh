#!/usr/bin/env bash
# Offered load from the LAPTOP into Kafka in the VM, split over several producer processes,
# with optional warm-up and burst profile, and telemetry of the laptop itself.
#
# Usage:  bash ~/nids-host/stream/produce_parallel.sh <topic> <total_rate> <records|all> [processes] [replay_file] [bootstrap]
#   e.g.  bash ~/nids-host/stream/produce_parallel.sh L_full_r1 30000 all 2
# Optional environment settings:
#   PROFILE="60:0.6,90:1.5,0:0.6"   burst profile (multipliers of total_rate; see replay_producer.py)
#   WARMUP=20 WARMUP_RATE=9000      warm-up: WARMUP seconds at WARMUP_RATE from the VALIDATION file,
#                                   event IDs from 1,000,000,000 (excluded from the analysis)
#   HOST_TAIL=120                   seconds the laptop telemetry keeps running after the last record
#
# Process i sends a contiguous slice of the replay file with event IDs equal to the row numbers,
# so together they send event IDs 0 .. records-1, as analyze_run.py expects.
# Everything about the run on the laptop goes to ~/w5/host_runs/<topic>/.
set -euo pipefail
TOPIC=$1; RATE=$2; TOTAL=$3; P=${4:-2}
REPLAY=${5:-$HOME/w5/replay/test.parquet}
BOOT=${6:-172.24.4.36:9094}
VAL=$HOME/w5/replay/val.parquet
DIR=$(cd "$(dirname "$0")" && pwd)
OUT=$HOME/w5/host_runs/$TOPIC
if [ -e "$OUT" ]; then echo "$OUT already exists: use a new run name."; exit 1; fi
mkdir -p "$OUT"
source "$HOME/spark-venv/bin/activate" 2>/dev/null || true

if [ "$TOTAL" = "all" ]; then
  TOTAL=$(python -c "import pyarrow.parquet as pq,sys; print(pq.ParquetFile(sys.argv[1]).metadata.num_rows)" "$REPLAY")
fi
N=$(( TOTAL / P )); R=$(( RATE / P ))

# laptop telemetry for the whole run (same tool as in the VM)
python "$DIR/telemetry.py" --out "$OUT/host_telemetry.csv" --interval 1 & TEL=$!

if [ -n "${WARMUP:-}" ]; then
  WR=${WARMUP_RATE:?set WARMUP_RATE (records/s) together with WARMUP}
  echo "Warm-up: ${WARMUP} s at ${WR}/s from the validation file (event IDs from 1,000,000,000)"
  python "$DIR/replay_producer.py" --replay "$VAL" --bootstrap "$BOOT" --topic "$TOPIC" \
     --rate "$WR" --limit $(( WR * WARMUP )) --start-row 9000000 --start-id 1000000000 \
     > "$OUT/warmup.log" 2>&1
  grep -h -E "SENT|WARNING" "$OUT/warmup.log"
fi

echo "Sending $TOTAL records from $(basename "$REPLAY") to $TOPIC at $RATE/s${PROFILE:+ with profile $PROFILE}" \
     "using $P processes via $BOOT"
START_AT=$(python -c "import time; print(time.time() + 2)")   # all processes start together
echo "$START_AT" > "$OUT/load_start"
pids=()
for i in $(seq 0 $((P - 1))); do
  n=$N; [ "$i" -eq $((P - 1)) ] && n=$(( TOTAL - i * N ))       # last process takes any remainder
  python "$DIR/replay_producer.py" --replay "$REPLAY" --bootstrap "$BOOT" --topic "$TOPIC" \
     --rate "$R" ${PROFILE:+--profile "$PROFILE"} --limit "$n" --start-row $((i * N)) --start-id $((i * N)) \
     --start-at "$START_AT" > "$OUT/producer_p$i.log" 2>&1 &
  pids+=($!)
done
for p in "${pids[@]}"; do wait "$p" || true; done
date +%s.%N > "$OUT/load_end"
grep -h -E "SENT|WARNING" "$OUT"/producer_p*.log
echo "$TOTAL" > "$OUT/records_sent"
echo "Load finished. Laptop telemetry continues for ${HOST_TAIL:-120} s while the VM drains ..."
sleep "${HOST_TAIL:-120}"
touch "$OUT/STOP"; wait $TEL || true
python "$DIR/host_summary.py" "$OUT"
