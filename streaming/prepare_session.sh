#!/usr/bin/env bash
# Put the laptop host into the fixed experimental state before a session, and pin the VM to fast cores.
# Usage (on the laptop, after the VM is running):   sudo bash ~/nids-host/stream/prepare_session.sh nids-exp-l
#
# i7-13620H layout (lscpu --extended): CPUs 0-11 = 6 P-cores x 2 threads (4.7-4.9 GHz),
# CPUs 12-15 = 4 E-cores (3.6 GHz). Plan:
#   VM vCPU i      -> its own P-core (both threads of that core): 2-3, 4-5, 6-7, 8-9
#   QEMU emulator and I/O threads -> P-cores 0 and 5 (CPUs 0,1,10,11)
#   replay producers -> E-cores 12-15 (set in produce_parallel.sh)
# Pinning is lost when the VM is stopped or rebooted, so run this at the start of every session.
set -euo pipefail
NAME=${1:?usage: sudo bash prepare_session.sh <openstack server name>}
ok=1
say() { printf '  %-28s %s\n' "$1" "$2"; }
echo "Preparing the host for $NAME"

# 1. Power: charger connected and performance profile
ac=$(cat /sys/class/power_supply/AC*/online 2>/dev/null | head -1 || echo "?")
if [ "$ac" = "1" ]; then say "charger" "connected"; else say "charger" "NOT CONNECTED -> plug in the laptop"; ok=0; fi
if command -v powerprofilesctl >/dev/null; then
  powerprofilesctl set performance 2>/dev/null || true
  say "power profile" "$(powerprofilesctl get 2>/dev/null)"
  [ "$(powerprofilesctl get 2>/dev/null)" = "performance" ] || ok=0
fi

# 2. Memory: low swappiness, swap emptied
sysctl -q vm.swappiness=10; say "vm.swappiness" "$(cat /proc/sys/vm/swappiness)"
sw=$(free -m | awk '/Swap/{print $3}')
if [ "$sw" -gt 0 ]; then
  av=$(free -m | awk '/Mem/{print $7}')
  if [ "$av" -gt "$((sw + 1000))" ]; then swapoff -a && swapon -a; fi
fi
say "swap used (MB)" "$(free -m | awk '/Swap/{print $3}')"
say "memory available (MB)" "$(free -m | awk '/Mem/{print $7}')"

# 3. Pin the VM
DOM=""
for d in $(virsh list --name); do
  if virsh dumpxml "$d" | grep -q "<nova:name>$NAME</nova:name>"; then DOM=$d; fi
done
if [ -z "$DOM" ]; then say "VM" "$NAME not running -> start it first"; exit 1; fi
NV=$(virsh vcpucount "$DOM" --live --active)
PAIRS=("2,3" "4,5" "6,7" "8,9")
[ "$NV" -le 4 ] || { say "VM vCPUs" "$NV (more than 4: no plan for that)"; exit 1; }
for i in $(seq 0 $((NV - 1))); do virsh vcpupin "$DOM" "$i" "${PAIRS[$i]}" --live >/dev/null; done
virsh emulatorpin "$DOM" 0,1,10,11 --live >/dev/null
say "VM $DOM" "$NV vCPUs pinned to P-cores: $(for i in $(seq 0 $((NV - 1))); do printf '%s ' "${PAIRS[$i]}"; done)"
sleep 2
QPID=$(pgrep -f "guest=$DOM" | head -1)
echo "  vCPU threads now on CPUs: $(ps -L -o psr=,comm= -p "$QPID" | awk '/KVM/{printf "%s ", $1}')"

# 4. Record the state for the run records
UHOME=$(getent passwd "${SUDO_USER:-$USER}" | cut -d: -f6)
mkdir -p "$UHOME/w5"; LOG=$UHOME/w5/sessions.csv
echo "$(date -Is),$NAME,$DOM,ac=$ac,profile=$(powerprofilesctl get 2>/dev/null || echo ?),swappiness=$(cat /proc/sys/vm/swappiness),pinned=yes" >> "$LOG"
chown "${SUDO_USER:-$USER}" "$LOG" 2>/dev/null || true

if [ "$ok" = 1 ]; then echo "HOST READY"; else echo "HOST NOT READY: fix the items above and run this again"; exit 1; fi
