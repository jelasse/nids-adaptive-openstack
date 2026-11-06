#!/usr/bin/env bash
# Measure the clock offset VM - laptop (in ms) over one shared SSH connection.
# Usage (on the laptop):  bash ~/nids-host/stream/clock_offset.sh [ssh_target] [extra ssh options]
#   e.g.  bash ~/nids-host/stream/clock_offset.sh ubuntu@172.24.4.36 -i ~/.ssh/testbed_key
# Takes 15 samples and keeps the one with the shortest round trip. Prints the value to pass
# to the analysis as CLOCK_OFFSET_MS, and appends it to ~/w5/clock_offsets.csv.
set -euo pipefail
TARGET=${1:-ubuntu@172.24.4.36}; shift || true
CP=/tmp/clockcheck-%r@%h:%p
SSH=(ssh -o ControlMaster=auto -o ControlPath=$CP -o ControlPersist=60 "$@" "$TARGET")
"${SSH[@]}" true                                   # open the shared connection
best=""
for i in $(seq 1 15); do
  t0=$(date +%s.%N); vm=$("${SSH[@]}" 'date +%s.%N'); t1=$(date +%s.%N)
  line=$(python3 -c "t0,vm,t1=$t0,$vm,$t1; print(f'{(t1-t0)*1000:.2f} {(vm-(t0+t1)/2)*1000:.1f}')")
  if [ -z "$best" ] || python3 -c "import sys; sys.exit(0 if float('${line% *}') < float('${best% *}') else 1)"; then
    best=$line
  fi
done
"${SSH[@]}" -O exit 2>/dev/null || true
rtt=${best% *}; off=${best#* }
echo "Best round trip ${rtt} ms  ->  clock offset VM - laptop = ${off} ms"
python3 -c "import sys; sys.exit(0 if $rtt < 10 else 1)" || echo "WARNING: round trip >= 10 ms; repeat the measurement"
mkdir -p ~/w5; echo "$(date -Is),$rtt,$off" >> ~/w5/clock_offsets.csv
echo "Use:  CLOCK_OFFSET_MS=$off"
