#!/usr/bin/env python3
"""Check the laptop side of one run: did every producer keep its rate, and did the laptop swap
while the measured load was running?

Usage (on the laptop):  python host_summary.py ~/w5/host_runs/<run_name>
Writes host_summary.json in that folder and prints VALID or INVALID with the reasons.

Swap rule: during the measured load (load_start .. load_end, warm-up excluded), at most 50 MB may be
moved in or out of swap. Swap that was already used before the run is reported but does not count,
because idle pages parked on disk earlier do not slow the run down; active swapping does.
"""
import csv, glob, json, os, re, statistics, sys

LIMIT_MB = 50.0
D = os.path.expanduser(sys.argv[1])
S, problems = {}, []

prods = []
for f in sorted(glob.glob(os.path.join(D, "producer_p*.log"))):
    txt = open(f).read()
    m = re.search(r"SENT ([\d,]+) records in ([\d.]+) s \(planned ([\d.]+) s\), achieved ([\d,]+)/s", txt)
    if not m:
        problems.append(f"{os.path.basename(f)}: no SENT line (producer failed?)"); continue
    sent, el, planned, ach = int(m[1].replace(",", "")), float(m[2]), float(m[3]), int(m[4].replace(",", ""))
    pct = round(100 * planned / el, 1) if el else 0.0
    prods.append(dict(log=os.path.basename(f), sent=sent, seconds=el, planned_s=planned,
                      achieved_rps=ach, pct_of_plan=pct))
    if pct < 98:
        problems.append(f"{os.path.basename(f)}: only {pct}% of the planned rate")
S["producers"] = prods
S["records_sent"] = sum(p["sent"] for p in prods)


def read_ts(name):
    p = os.path.join(D, name)
    return float(open(p).read().strip()) if os.path.exists(p) else None


t_start, t_end = read_ts("load_start"), read_ts("load_end")
T = os.path.join(D, "host_telemetry.csv")
if os.path.exists(T):
    rows = list(csv.DictReader(open(T)))
    load = [r for r in rows if (t_start is None or float(r["ts"]) >= t_start)
            and (t_end is None or float(r["ts"]) <= t_end + 1)] or rows
    if load:
        cpu = sorted(float(r["cpu_pct"]) for r in load)
        h = dict(samples=len(load), cpu_mean=round(statistics.mean(cpu), 1),
                 cpu_p95=cpu[int(0.95 * (len(cpu) - 1))],
                 mem_avail_min_mb=min(float(r["mem_avail_mb"]) for r in load),
                 swap_used_before_run_mb=float(rows[0]["swap_used_mb"]),
                 swap_used_max_mb=max(float(r["swap_used_mb"]) for r in rows))
        if "swap_in_mb" in load[0]:
            h["swap_in_during_load_mb"] = round(sum(float(r["swap_in_mb"]) for r in load), 1)
            h["swap_out_during_load_mb"] = round(sum(float(r["swap_out_mb"]) for r in load), 1)
            moved = h["swap_in_during_load_mb"] + h["swap_out_during_load_mb"]
        else:                                      # older telemetry without swap activity columns
            moved = max(float(r["swap_used_mb"]) for r in load) - float(load[0]["swap_used_mb"])
        h["swap_moved_during_load_mb"] = round(moved, 1)
        if moved > LIMIT_MB:
            problems.append(f"laptop moved {moved:.0f} MB in/out of swap during the measured load")
        S["host"] = h
else:
    problems.append("no host_telemetry.csv")

S["valid"] = not problems
S["problems"] = problems
json.dump(S, open(os.path.join(D, "host_summary.json"), "w"), indent=2)
for p in prods:
    print(f"  {p['log']}: {p['sent']:,} records, {p['achieved_rps']:,}/s, {p['pct_of_plan']}% of plan")
if "host" in S:
    h = S["host"]
    print(f"  laptop during load: CPU mean {h['cpu_mean']}% (p95 {h['cpu_p95']}%), min available memory "
          f"{h['mem_avail_min_mb']:.0f} MB, swap moved {h['swap_moved_during_load_mb']:.0f} MB "
          f"(swap already used before the run: {h['swap_used_before_run_mb']:.0f} MB)")
print("LAPTOP SIDE: " + ("VALID" if S["valid"] else "INVALID -> " + "; ".join(problems)))
