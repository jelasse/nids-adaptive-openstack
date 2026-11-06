#!/usr/bin/env bash
# Run every line of a plan (from make_plan.py) unattended: start the detector in the VM, send the load
# from the laptop, wait, check validity, copy the small result files, delete the Kafka topic, next run.
#
# Usage (on the laptop, after prepare_session.sh):
#   nohup bash ~/nids-host/stream/run_queue.sh ~/w7/plans/E1_M.csv > ~/w7/queue_E1_M.log 2>&1 &
#   tail -f ~/w7/queue_E1_M.log          # watch (Ctrl-C stops watching, not the queue)
# Stop after the current run:  touch ~/w7/STOP_QUEUE
# Resumes where it left off: runs whose results are already in ~/w7/runs/<name> are skipped.
# Results: one line per run in ~/w7/queue_results.csv.
set -uo pipefail
PLAN=$1
KEY=${KEY:-$HOME/.ssh/testbed_key}
DIR=$(cd "$(dirname "$0")" && pwd)
RES=$HOME/w7/queue_results.csv
mkdir -p "$HOME/w7/runs"
[ -f "$RES" ] || echo "time,name,clock_offset_ms,vm_valid,laptop_valid,sustainable,p50_ms,p95_ms,slo_share,backlog_max,throughput,recall,fp,missing" > "$RES"
rm -f "$HOME/w7/STOP_QUEUE"
log() { echo "[$(date '+%H:%M:%S')] $*"; }

# the plan is read on file descriptor 3, so that ssh and the producers cannot consume its lines
while IFS=, read -r -u 3 name vm ip rate records mode controller profile procs warmup_rate; do
  warmup_rate=${warmup_rate%$'\r'}            # tolerate Windows line endings in the plan
  profile=${profile//;/,}
  [ -e "$HOME/w7/STOP_QUEUE" ] && { log "STOP_QUEUE found: stopping before $name"; break; }
  if [ -f "$HOME/w7/runs/$name/summary.json" ]; then log "skip $name (already done)"; continue; fi
  SSH=(ssh -n -i "$KEY" -o BatchMode=yes -o ConnectTimeout=10 "ubuntu@$ip")
  log "=== $name: $rate/s, $mode${controller:+, adaptive}${profile:+, burst $profile}, $procs producers"

  # 1. host still in the fixed state? (charger, profile; pinning is checked by prepare_session.sh)
  if [ -z "${SKIP_AC:-}" ] && [ "$(cat /sys/class/power_supply/AC*/online 2>/dev/null | head -1)" != "1" ]; then
    log "charger not connected: stopping the queue"; break; fi

  # 2. clock offset for this run
  off=$(bash "$DIR/clock_offset.sh" "ubuntu@$ip" -i "$KEY" | awk -F'= ' '/clock offset/{print $2}' | awk '{print $1}')
  [ -n "$off" ] || { log "clock measurement failed: stopping"; break; }
  log "clock offset $off ms"

  # 3. start the detector in the VM (in the background there), wait until it is ready
  ENVS="CLOCK_OFFSET_MS=$off EXTERNAL=1 EXT_PROCS=$procs WARMUP=20 WARMUP_RATE=$warmup_rate"
  [ -n "$controller" ] && ENVS="$ENVS CONTROLLER='$controller'"
  [ -n "$profile" ] && ENVS="$ENVS PROFILE='$profile'"
  "${SSH[@]}" "mkdir -p ~/nids/console; nohup env $ENVS bash ~/nids/stream/run_once.sh $name $rate $records $mode \
      > ~/nids/console/$name.txt 2>&1 < /dev/null &" || { log "could not start the run in the VM"; break; }
  ready=0
  for i in $(seq 1 60); do
    sleep 5
    if "${SSH[@]}" "grep -q 'Detector ready' ~/nids/console/$name.txt"; then ready=1; break; fi
    if "${SSH[@]}" "grep -q -E 'already exists|Error|Traceback' ~/nids/console/$name.txt"; then break; fi
  done
  [ "$ready" = 1 ] || { log "detector did not become ready; console:"; "${SSH[@]}" "tail -5 ~/nids/console/$name.txt"; break; }

  # 4. offer the load from the laptop
  env WARMUP=20 WARMUP_RATE="$warmup_rate" PROFILE="$profile" \
    bash "$DIR/produce_parallel.sh" "$name" "$rate" "$records" "$procs" "$HOME/w5/replay/test.parquet" "$ip:9094" < /dev/null \
    | grep -E "SENT|WARNING|LAPTOP SIDE|note:" | sed 's/^/    /'

  # 5. wait until the VM side has finished (detector stopped and analysis written)
  for i in $(seq 1 240); do
    "${SSH[@]}" "pgrep -f 'run_once[.]sh $name ' > /dev/null" || break   # [.] keeps pgrep from matching this command
    sleep 15
  done
  "${SSH[@]}" "test -f ~/nids/runs/$name/summary.json" || { log "no summary for $name; console:"; "${SSH[@]}" "tail -15 ~/nids/console/$name.txt"; break; }

  # 6. copy the small result files, record the verdict, free the VM disk
  rsync -a --exclude 'predictions/' --exclude 'checkpoint*' -e "ssh -i $KEY" \
        "ubuntu@$ip:~/nids/runs/$name" "ubuntu@$ip:~/nids/console/$name.txt" "$HOME/w7/runs/" 2>/dev/null
  mv -f "$HOME/w7/runs/$name.txt" "$HOME/w7/runs/$name/console.txt" 2>/dev/null
  line=$(python3 - "$HOME/w7/runs/$name/summary.json" "$HOME/w5/host_runs/$name/host_summary.json" <<'EOF'
import json, sys
v = json.load(open(sys.argv[1]))
try: h = json.load(open(sys.argv[2]))
except Exception: h = {"valid": False}
L, D = v.get("latency_ms", {}), v.get("detection", {})
print(",".join(str(x) for x in [v.get("clock_offset_ms"), v.get("valid_vm_side"), h.get("valid"), v.get("sustainable"),
      L.get("p50"), L.get("p95"), v.get("slo_violation_share"), v.get("backlog_max_records"), v.get("throughput_rps"),
      D.get("recall"), D.get("fp"), v.get("missing")]))
EOF
)
  echo "$(date -Is),$name,$line" >> "$RES"
  log "result: $line"
  "${SSH[@]}" "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $name > /dev/null 2>&1; \
               rm -rf ~/nids/runs/$name/checkpoint" || true
  sleep 30                                  # let the VM settle before the next run
done 3< <(tail -n +2 "$PLAN")
log "queue finished"
