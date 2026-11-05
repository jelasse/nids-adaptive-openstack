#!/usr/bin/env python3
"""Sample CPU and memory of the VM once per interval into a CSV file (no extra packages).

Columns: ts, cpu_pct (all vCPUs, 0-100), cpu_steal_pct, mem_used_mb, mem_avail_mb,
         swap_used_mb, load1
Usage (in the VM, in its own terminal or with &):
  python telemetry.py --out ~/nids/runs/run01/telemetry.csv --interval 1
Stops with Ctrl-C, or by itself when the file STOP exists next to the CSV.
"""
import argparse, os, time

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--interval", type=float, default=1.0)
a = ap.parse_args()
os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
stop_file = os.path.join(os.path.dirname(os.path.abspath(a.out)), "STOP")


def cpu():
    v = [int(x) for x in open("/proc/stat").readline().split()[1:]]
    idle = v[3] + v[4]; steal = v[7] if len(v) > 7 else 0
    return sum(v[:8]), idle, steal


def mem():
    d = {}
    for line in open("/proc/meminfo"):
        k, val = line.split(":"); d[k] = int(val.split()[0])
    return ((d["MemTotal"] - d["MemAvailable"]) / 1024, d["MemAvailable"] / 1024,
            (d["SwapTotal"] - d["SwapFree"]) / 1024)


new = not os.path.exists(a.out)
f = open(a.out, "a")
if new:
    f.write("ts,cpu_pct,cpu_steal_pct,mem_used_mb,mem_avail_mb,swap_used_mb,load1\n")
tot0, idle0, st0 = cpu()
try:
    while not os.path.exists(stop_file):
        time.sleep(a.interval)
        tot, idle, st = cpu()
        dt = max(tot - tot0, 1)
        used, avail, swap = mem()
        f.write(f"{time.time():.3f},{100 * (1 - (idle - idle0) / dt):.1f},{100 * (st - st0) / dt:.1f},"
                f"{used:.0f},{avail:.0f},{swap:.0f},{os.getloadavg()[0]:.2f}\n")
        f.flush()
        tot0, idle0, st0 = tot, idle, st
except KeyboardInterrupt:
    pass
