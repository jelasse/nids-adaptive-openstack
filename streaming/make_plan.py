#!/usr/bin/env python3
"""Write the run plan of one experiment on one VM, in randomized order (Section 5.6 of the paper).

Usage (on the laptop):
  python make_plan.py E1 M  [--seed 1]    -> ~/w7/plans/E1_M.csv
Experiments (S = saturation rate of Static-Full on the VM, measured on validation data):
  E1  baseline        Static-Full at 0.5 S                               3 repetitions   (M and L)
  E2  load sweep      Static-Full and Static-Light at 0.5/0.8/1.0/1.2/1.5 S  3 reps      (L)
  E3  allocation      Static-Full and Static-Light at the same absolute rates on M as E2 uses on L,
                      as far as M can be loaded (rates of E2 on L up to 1.5 S of M)   3 reps (M)
  E4  adaptive        Static-Full, Static-Light, Adaptive at 1.2 S constant and burst profile  5 reps (L)
Every run replays the whole test split (9,421,272 flows) after a 20 s warm-up at 0.3 S.
"""
import argparse, csv, math, os, random

S = {"M": 30000, "L": 45000}
VM = {"M": ("nids-exp", "172.24.4.100"), "L": ("nids-exp-l", "172.24.4.36")}
RECORDS = 9_421_272
CONTROLLER = "--high 100000 --low 10000 --k 2 --m 3 --dwell 15"
BURST = "60:0.6;90:1.5;0:0.6"          # ';' in the plan file (CSV); run_queue.sh turns it into ','

LEVELS = [0.5, 0.8, 1.0, 1.2, 1.5]

ap = argparse.ArgumentParser()
ap.add_argument("exp", choices=["E1", "E2", "E3", "E4"])
ap.add_argument("vm", choices=["M", "L"])
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--out", default=os.path.expanduser("~/w7/plans"))
a = ap.parse_args()
s = S[a.vm]
runs = []   # (label, rate, mode, controller, profile, reps)


def lvl(x):
    return f"{x:.1f}S".replace(".", "p")


if a.exp == "E1":
    runs = [(f"full_{lvl(0.5)}", round(0.5 * s), "full", "", "", 3)]
elif a.exp == "E2":
    assert a.vm == "L", "E2 runs on L"
    runs = [(f"{m}_{lvl(x)}", round(x * s), m, "", "", 3) for m in ("full", "light") for x in LEVELS]
elif a.exp == "E3":
    assert a.vm == "M", "E3 runs on M (L's runs come from E2)"
    rates = [round(x * S["L"]) for x in LEVELS if round(x * S["L"]) <= 1.5 * S["M"]]
    runs = [(f"{m}_{r // 1000}k", r, m, "", "", 3) for m in ("full", "light") for r in rates]
elif a.exp == "E4":
    assert a.vm == "L", "E4 runs on L"
    for pol, mode, ctl in (("full", "full", ""), ("light", "light", ""), ("adapt", "full", CONTROLLER)):
        runs.append((f"{pol}_{lvl(1.2)}", round(1.2 * s), mode, ctl, "", 5))
        runs.append((f"{pol}_burst", s, mode, ctl, BURST, 5))

rows = []
for label, rate, mode, ctl, prof, reps in runs:
    peak = rate * (1.5 if prof else 1.0)
    procs = max(2, math.ceil(peak / 17000))            # each producer <= ~17k/s on an E-core
    for r in range(1, reps + 1):
        rows.append(dict(name=f"{a.exp}_{a.vm}_{label}_r{r}", vm=VM[a.vm][0], ip=VM[a.vm][1],
                         rate=rate, records=RECORDS, mode=mode, controller=ctl, profile=prof,
                         procs=procs, warmup_rate=round(0.3 * s)))
random.Random(a.seed).shuffle(rows)
os.makedirs(a.out, exist_ok=True)
path = os.path.join(a.out, f"{a.exp}_{a.vm}.csv")
with open(path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")   # Unix line endings for bash
    w.writeheader(); w.writerows(rows)
mins = sum(r["records"] / r["rate"] / 60 + 3 for r in rows)
print(f"{path}: {len(rows)} runs in random order (seed {a.seed}), about {mins / 60:.1f} h")
for r in rows:
    print(f"  {r['name']:<28} {r['rate']:>7,}/s  {r['mode']:<5} procs={r['procs']}"
          f"{'  controller' if r['controller'] else ''}{'  burst' if r['profile'] else ''}")
