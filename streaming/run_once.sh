#!/usr/bin/env bash
# One streaming run, start to finish (inside the VM).
# Usage:  bash ~/nids/stream/run_once.sh <run_name> <rate> <records> [mode] [start_row] [replay_file]
#   e.g.  bash ~/nids/stream/run_once.sh smoke1 2000 60000 full
# Optional environment settings (defaults = the frozen protocol, Section 5 of the paper):
#   MAX_OFFSETS=75000   cap on records per micro-batch (0 = no cap)
#   TRIGGER=0           micro-batch interval in seconds (0 = back-to-back batches)
#   CLOCK_OFFSET_MS=-101  clock offset VM - laptop from clock_offset.sh (subtracted from latencies)
#   PROFILE, WARMUP, WARMUP_RATE   passed on to the laptop command (see produce_parallel.sh)
#   CONTROLLER="--high 100000 --low 10000 --k 2 --m 3 --dwell 15"   start the adaptive controller
#   EXTERNAL=1          the load comes from the laptop (produce_parallel.sh); EXT_PROCS=2 producer processes
# Change the mode during a run from another terminal:  echo light > ~/nids/runs/<run_name>/mode
set -euo pipefail
NAME=$1; RATE=$2; LIMIT=$3; MODE=${4:-full}; START=${5:-0}
REPLAY=${6:-$HOME/nids/replay/val.parquet}
S=$HOME/nids/stream; R=$HOME/nids/runs/$NAME; K=/opt/kafka/bin
PKG=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9

if [ -e "$R" ]; then echo "Run directory $R already exists: choose a new run name."; exit 1; fi
mkdir -p "$R"; echo "$MODE" > "$R/mode"
source "$HOME/nids-venv/bin/activate"

# 1. Kafka running, with the protocol's heap (512 MB)? A broker started by hand gets Kafka's default 1 GB:
#    restart it with the right heap, so every run has the same memory budget.
if ss -ltn | grep -q ':9092 ' && ! pgrep -af kafka.Kafka | grep -q -- '-Xmx512M'; then
  echo "Kafka is running with a different heap: restarting it with 512 MB ..."
  $K/kafka-server-stop.sh; for i in $(seq 1 30); do ss -ltn | grep -q ':9092 ' || break; sleep 2; done
fi
if ! ss -ltn | grep -q ':9092 '; then
  echo "Starting Kafka ..."
  KAFKA_HEAP_OPTS="-Xmx512M -Xms512M" $K/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
  for i in $(seq 1 30); do ss -ltn | grep -q ':9092 ' && break; sleep 2; done
fi
# 2. A fresh topic for this run (4 partitions)
$K/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$NAME" \
   --partitions 4 --replication-factor 1 >/dev/null
echo "Topic $NAME created. Run directory: $R"

# 3. Telemetry and the streaming job in the background
python "$S/telemetry.py" --out "$R/telemetry.csv" --interval 1 & TEL=$!
spark-submit --driver-memory 2g --packages $PKG "$S/stream_detect.py" \
   --topic "$NAME" --out "$R" --max-offsets "${MAX_OFFSETS:-75000}" --trigger "${TRIGGER:-0}" > "$R/stream.log" 2>&1 & SP=$!
echo "Waiting for the streaming job to start ..."
touch "$R/stream.log"
until grep -q "batch   rows" "$R/stream.log"; do
  if ! kill -0 $SP 2>/dev/null; then echo "Streaming job failed:"; tail -n 30 "$R/stream.log"; kill $TEL; exit 1; fi
  sleep 2
done
sleep 10
CTL=""
if [ -n "${CONTROLLER:-}" ]; then
  python "$S/controller.py" --run "$R" $CONTROLLER > "$R/controller.log" 2>&1 & CTL=$!
  echo "Controller started: $CONTROLLER"
fi

# 4. Offered load (from inside the VM, or from the laptop when EXTERNAL=1)
if [ -n "${EXTERNAL:-}" ]; then
  echo
  echo ">>> Detector ready. Now start the load ON THE LAPTOP:"
  ENVS="${PROFILE:+PROFILE=\"$PROFILE\" }${WARMUP:+WARMUP=$WARMUP WARMUP_RATE=${WARMUP_RATE:-} }"
  echo ">>>   ${ENVS}bash ~/nids-host/stream/produce_parallel.sh $NAME $RATE $LIMIT ${EXT_PROCS:-2} ${EXT_REPLAY:-}"
  echo ">>> (this window waits until the detector has processed everything)"
  echo
else
  python "$S/replay_producer.py" --replay "$REPLAY" --topic "$NAME" --rate "$RATE" \
     --limit "$LIMIT" --start-row "$START" | tee "$R/producer.log"
fi

# 5. The job stops by itself 30 s after the last data; then stop telemetry and analyze
wait $SP || true
touch "$R/STOP"; wait $TEL || true
[ -n "$CTL" ] && { wait $CTL || true; echo; echo "=== Controller decisions ==="; cat "$R/controller.log"; }
echo; echo "=== Streaming job (last lines) ==="
sed -n '/Models loaded/,$p' "$R/stream.log" | grep -v -E "WARN|INFO" | tail -n 40
echo; echo "=== Run summary ==="
KHEAP=$(pgrep -af kafka.Kafka | grep -o -- '-Xmx[0-9]*[MG]' | head -1)
echo "kafka_heap=${KHEAP:-unknown} max_offsets=${MAX_OFFSETS:-75000} trigger=${TRIGGER:-0} controller=${CONTROLLER:-none} profile=${PROFILE:-constant} warmup=${WARMUP:-0}@${WARMUP_RATE:-0} clock_offset_ms=${CLOCK_OFFSET_MS:-0}" > "$R/settings.txt"
spark-submit --driver-memory 2g "$S/analyze_run.py" "$R" --expected "$LIMIT" \
   --clock-offset-ms "${CLOCK_OFFSET_MS:-0}" 2>&1 | grep -v -E "WARN|INFO"
