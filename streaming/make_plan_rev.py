#!/usr/bin/env python3
"""Run plans of the revision experiments on L (randomized order, seed 1), for run_queue.sh.

  R1  fallback speed     Static-Light with the security-aware fallback (light_rf), validation split,
                         54,000 / 63,000 / 67,500 flows/s (1.2 / 1.4 / 1.5 S), 4 M flows each        3 runs
  R2  policies           test split, interleaved producers (global order kept), 1.2 S and burst, 5 reps:
                           adapt   = original Adaptive (LR fallback)                 [replay-order check, MC7]
                           adaptrf = Adaptive with the security-aware fallback       [MC2]
                           adapts  = Adaptive + 10% of flows still classified by Full in Light mode   [MC2]
                           prop    = proportional mixed inference (only the excess to Light)          [MC6]   40 runs
  R3  evasion            induced-overload workload (compose_workload.py, Botnet hidden), seeds 1-5:
                           full, adapt, adaptrf, adapts, prop                                          [MC1]   25 runs
  R4  long runs          test split replayed 3 times: Static-Full at 0.9 S, Adaptive at 1.2 S        [MC10]   2 runs
  R5  sharper burst      30 s at 0.6 S, 60 s at 1.8 S, then 0.6 S: adapt, adaptrf, 3 reps            [MC12]   6 runs
  R6  static baselines   (second review, item 1) Static-Full and Static-Light under the SAME order-preserving
                         replay as R2: test split, 1.2 S and burst, 5 reps                             20 runs
Usage (laptop):  python make_plan_rev.py R2    -> ~/w8/plans/R2_L.csv     (R1 ... R6)
  --redo   only the runs still needed, judged from ~/w7/queue_results.csv: a run never done keeps its name; a run
           done but invalid on either side is repeated with the suffix b (then c, ...), as in the main experiments;
           a valid run is left out.          -> ~/w8/plans/<exp>_L_redo.csv
"""
import argparse, csv, json, math, os, random

S, VM, IP = 45000, "nids-exp-l", "172.24.4.36"
TEST, VAL = "~/w5/replay/test.parquet", "~/w5/replay/val.parquet"
RECORDS = 9_421_272
HYST = "--high 100000 --low 10000 --k 2 --m 3 --dwell 15"
PROP = "--policy proportional --low 10000 --cap-full 45000 --cap-light 90000 --horizon 10"
BURST, BURST2 = "60:0.6;90:1.5;0:0.6", "30:0.6;60:1.8;0:0.6"
POL = {  # name: (mode, controller, vm_env)
    "full": ("full", "", ""), "light": ("light", "", ""),
    "adapt": ("full", HYST, ""), "adaptrf": ("full", HYST, "LIGHT_MODEL=light_rf"),
    "adapts": ("full", HYST, "FULL_SAMPLE=0.1"), "prop": ("full", PROP, ""),
}
ap = argparse.ArgumentParser()
ap.add_argument("exp", choices=["R1", "R2", "R3", "R4", "R5", "R6"])
ap.add_argument("--workloads", default="~/w8/workloads")
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--out", default=os.path.expanduser("~/w8/plans"))
ap.add_argument("--redo", action="store_true")
ap.add_argument("--results", default=os.path.expanduser("~/w7/queue_results.csv"))
a = ap.parse_args()
rows = []


def add(name, rate, records, pol, profile="", replay=TEST, host_env="INTERLEAVE=1", peak=None, extra_vm=""):
    mode, ctl, vm_env = POL[pol]
    vm_env = ";".join(x for x in (vm_env, extra_vm) if x)
    procs = max(2, math.ceil((peak or rate) / 17000))
    rows.append(dict(name=name, vm=VM, ip=IP, rate=rate, records=records, mode=mode, controller=ctl, profile=profile,
                     procs=procs, warmup_rate=round(0.3 * S), replay=replay, vm_env=vm_env, host_env=host_env))


if a.exp == "R1":
    for r in (54000, 63000, 67500):
        rows.append(dict(name=f"R1_L_lightrf_{r // 1000}k_r1", vm=VM, ip=IP, rate=r, records=4_000_000, mode="light",
                         controller="", profile="", procs=max(2, math.ceil(r / 17000)), warmup_rate=round(0.3 * S),
                         replay=VAL, vm_env="LIGHT_MODEL=light_rf", host_env="INTERLEAVE=1"))
elif a.exp == "R2":
    for pol in ("adapt", "adaptrf", "adapts", "prop"):
        for rep in range(1, 6):
            add(f"R2_L_{pol}_1p2S_r{rep}", round(1.2 * S), RECORDS, pol)
            add(f"R2_L_{pol}_burst_r{rep}", S, RECORDS, pol, BURST, peak=1.5 * S)
elif a.exp == "R3":
    wl = os.path.expanduser(a.workloads)
    for seed in range(1, 6):
        man = json.load(open(os.path.join(wl, f"evasion_Botnet_s{seed}.json")))
        for pol in ("full", "adapt", "adaptrf", "adapts", "prop"):
            add(f"R3_L_{pol}_ev{seed}_r1", man["profile_rate"], man["rows"], pol, man["profile"].replace(",", ";"),
                replay=man["file"], peak=(man["timeline"]["bg"] + man["timeline"]["fl"]) * man["profile_rate"])
elif a.exp == "R4":
    add("R4_L_full_0p9S_r1", round(0.9 * S), 3 * RECORDS, "full", host_env="INTERLEAVE=1;LOOPS=3")
    add("R4_L_adapt_1p2S_r1", round(1.2 * S), 3 * RECORDS, "adapt", host_env="INTERLEAVE=1;LOOPS=3")
elif a.exp == "R5":
    for pol in ("adapt", "adaptrf"):
        for rep in range(1, 4):
            add(f"R5_L_{pol}_burst2_r{rep}", S, RECORDS, pol, BURST2, peak=1.8 * S)

elif a.exp == "R6":
    for pol in ("full", "light"):
        for rep in range(1, 6):
            add(f"R6_L_{pol}_1p2S_r{rep}", round(1.2 * S), RECORDS, pol)
            add(f"R6_L_{pol}_burst_r{rep}", S, RECORDS, pol, BURST, peak=1.5 * S)

random.Random(a.seed).shuffle(rows)
if a.redo:
    done = {}                                   # run name -> valid on both sides (last result line wins)
    if os.path.exists(a.results):
        for r in csv.DictReader(open(a.results)):
            done[r["name"]] = r.get("vm_valid") == "True" and r.get("laptop_valid") == "True"
    keep = []
    for r in rows:
        base, names = r["name"], [r["name"]] + [r["name"] + c for c in "bcdefg"]
        tried = [n for n in names if n in done]
        if any(done[n] for n in tried):
            continue                            # a valid result exists
        nxt = names[len(tried)] if len(tried) < len(names) else None
        if nxt is None:
            print(f"  {base}: too many invalid repeats, left out"); continue
        if tried and os.path.isdir(os.path.expanduser(f"~/w7/runs/{nxt}")):
            print(f"  {nxt}: folder exists without a result line, left out (check by hand)"); continue
        r["name"] = nxt; keep.append(r)
    rows = keep
    if not rows:
        raise SystemExit(f"{a.exp}: nothing left to run")
os.makedirs(a.out, exist_ok=True)
path = os.path.join(a.out, f"{a.exp}_L{'_redo' if a.redo else ''}.csv")
with open(path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n"); w.writeheader(); w.writerows(rows)
mins = sum(r["records"] / r["rate"] / 60 + 4 for r in rows)
print(f"{path}: {len(rows)} runs, about {mins / 60:.1f} h")
for r in rows:
    print(f"  {r['name']:<26} {r['rate']:>6,}/s {r['mode']:<5} {r['vm_env'] or '-':<22} "
          f"{'controller' if r['controller'] else '':<10} {os.path.basename(r['replay'])}")
