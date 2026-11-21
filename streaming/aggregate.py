#!/usr/bin/env python3
"""Turn the run folders into the paper's tables and figures (Section 5.8 / Section 6).

Usage (on the laptop):  python aggregate.py [~/w7/runs] [--out ~/w7/analysis]
Reads every <run>/summary.json (VM side) and the laptop-side host_summary.json, from --host/<run>/ or, as in the
published results/, from <run>/host/.
A run is VALID if both sides are valid (0 missing, 0 duplicates, producers >= 98% of plan, no laptop swap-in).
Invalid runs are listed in invalid_runs.csv and excluded from the statistics.

Outputs in --out:
  runs.csv          one row per run (all metrics, valid flag)
  conditions.csv    one row per condition: n, mean and 95% t-confidence interval of each metric
  tables.md         the main tables, ready to copy into the paper
  e4_effects.csv    Adaptive vs Static-Full and vs Static-Light: mean difference, 95% bootstrap CI, Hedges' g
  fig_e2_latency.png  latency (p50, p95) vs offered load for Full and Light on L (E2), log scale
Run names (C2/C4 = Kafka-heap sensitivity check of E2/E4): <EXP>_<VM>_<policy>_<load>_r<N>[b], e.g. E2_L_full_1p2S_r3, E4_L_adapt_burst_r2, E2_L_light_0p5S_r1b.
"""
import argparse, csv, glob, json, math, os, re, statistics
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("runs", nargs="?", default="~/w7/runs")
ap.add_argument("--host", default="~/w5/host_runs")
ap.add_argument("--out", default="~/w7/analysis")
a = ap.parse_args()
RUNS, HOST, OUT = (os.path.expanduser(x) for x in (a.runs, a.host, a.out))
os.makedirs(OUT, exist_ok=True)

T975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228}
NAME = re.compile(r"^([ECR]\d)_([ML])_([a-z]+)_([0-9p]+S|burst\d?|\d+k|ev\d+)_r(\d+)([bc]?)$")
FAMS = ["BruteForce", "DoS", "DDoS", "Web", "Infiltration", "Botnet"]


def load_json(p):
    try:
        return json.load(open(p))
    except Exception:
        return None


rows = []
for d in sorted(glob.glob(os.path.join(RUNS, "[ER]*"))):
    name = os.path.basename(d)
    m = NAME.match(name)
    if not m:
        continue
    exp, vm, pol, load, rep, redo = m.groups()
    v = load_json(os.path.join(d, "summary.json"))
    h = load_json(os.path.join(HOST, name, "host_summary.json")) or load_json(os.path.join(d, "host", "host_summary.json"))
    if v is None:
        continue
    L, D = v.get("latency_ms", {}), v.get("detection", {})
    bym = v.get("by_mode", {})
    light_rows = bym.get("light", {}).get("rows", 0)
    switches = 0
    cj = os.path.join(d, "controller.jsonl")
    if os.path.exists(cj):
        switches = sum(1 for l in open(cj) if '"switch_to"' in l)
    tel = v.get("telemetry", {})
    r = dict(run=name, exp=exp, vm=vm, policy=pol, load=load, rep=int(rep), redo=bool(redo),
             valid_vm=bool(v.get("valid_vm_side")), valid_laptop=bool(h and h.get("valid")),
             sustainable=v.get("sustainable"), rows=v.get("rows"), missing=v.get("missing"),
             clock_offset_ms=v.get("clock_offset_ms"), throughput=v.get("throughput_rps"),
             p50_s=L.get("p50", float("nan")) / 1000, p95_s=L.get("p95", float("nan")) / 1000,
             p99_s=L.get("p99", float("nan")) / 1000, max_s=L.get("max", float("nan")) / 1000,
             slo_share=v.get("slo_violation_share"), backlog_max=v.get("backlog_max_records"),
             drain_s=v.get("drain_s"), recall=D.get("recall"), precision=D.get("precision"),
             fpr=D.get("fpr"), macro_f1=D.get("macro_f1"), fp=D.get("fp"),
             light_share=round(light_rows / v["rows"], 4) if v.get("rows") else 0.0, switches=switches,
             cpu_mean=tel.get("cpu_mean"), cpu_p95=tel.get("cpu_p95"), mem_avail_min_mb=tel.get("mem_avail_min_mb"),
             steal_max=tel.get("steal_max"))
    for f in FAMS:
        r[f"rec_{f}"] = v.get("family_recall", {}).get(f)
    r["valid"] = r["valid_vm"] and r["valid_laptop"]
    rows.append(r)

if not rows:
    raise SystemExit(f"no runs found in {RUNS}")
cols = list(rows[0])
with open(os.path.join(OUT, "runs.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n"); w.writeheader(); w.writerows(rows)
with open(os.path.join(OUT, "invalid_runs.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n"); w.writeheader()
    w.writerows(r for r in rows if not r["valid"])

METRICS = ["throughput", "p50_s", "p95_s", "p99_s", "max_s", "slo_share", "backlog_max", "drain_s",
           "recall", "precision", "fpr", "macro_f1", "light_share", "switches", "cpu_mean", "mem_avail_min_mb"] + \
          [f"rec_{f}" for f in FAMS]


def ci(xs):
    xs = [x for x in xs if isinstance(x, (int, float)) and not math.isnan(x)]
    if not xs:
        return None, None
    mu = statistics.mean(xs)
    if len(xs) < 2:
        return mu, None
    return mu, T975.get(len(xs) - 1, 1.96) * statistics.stdev(xs) / math.sqrt(len(xs))


groups = defaultdict(list)
for r in rows:
    if r["valid"]:
        groups[(r["exp"], r["vm"], r["policy"], r["load"])].append(r)


def load_key(l):
    if l.endswith("S"):
        return float(l[:-1].replace("p", "."))
    if l.startswith("burst"):
        return 99 + (int(l[5:]) if l[5:] else 0)
    if l.startswith("ev"):
        return 200 + int(l[2:])
    return float(l[:-1]) / 1000


cond = []
for key in sorted(groups, key=lambda k: (k[0], k[1], k[2], load_key(k[3]))):
    g = groups[key]
    c = dict(exp=key[0], vm=key[1], policy=key[2], load=key[3], n=len(g),
             sustainable=f"{sum(1 for r in g if r['sustainable'])}/{len(g)}")
    for mtr in METRICS:
        mu, h = ci([r[mtr] for r in g])
        c[mtr] = None if mu is None else round(mu, 5)
        c[mtr + "_ci"] = None if h is None else round(h, 5)
    cond.append(c)
with open(os.path.join(OUT, "conditions.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(cond[0]), lineterminator="\n"); w.writeheader(); w.writerows(cond)


def fmt(c, m, dec=2, scale=1.0):
    if c.get(m) is None:
        return "—"
    s = f"{c[m] * scale:,.{dec}f}"
    return s + (f" ± {c[m + '_ci'] * scale:,.{dec}f}" if c.get(m + "_ci") is not None else "")


md = ["# Results tables (valid runs only; mean ± 95% CI half-width over repetitions)\n"]
for exp in sorted({c["exp"] for c in cond}):
    md.append(f"\n## {exp}\n")
    md.append("| VM | Policy | Load | n | Sustainable | Throughput (rec/s) | p50 (s) | p95 (s) | > 5 s (%) | "
              "Max backlog | Recall | FP rate (%) | Light share (%) |")
    md.append("|" + "---|" * 13)
    for c in (c for c in cond if c["exp"] == exp):
        md.append(f"| {c['vm']} | {c['policy']} | {c['load']} | {c['n']} | {c['sustainable']} | {fmt(c, 'throughput', 0)} | "
                  f"{fmt(c, 'p50_s')} | {fmt(c, 'p95_s')} | {fmt(c, 'slo_share', 1, 100)} | {fmt(c, 'backlog_max', 0)} | "
                  f"{fmt(c, 'recall', 4)} | {fmt(c, 'fpr', 3, 100)} | {fmt(c, 'light_share', 1, 100)} |")
md.append("\n## Recall per attack family (valid runs, mean)\n")
md.append("| Exp | VM | Policy | Load | " + " | ".join(FAMS) + " |")
md.append("|" + "---|" * (4 + len(FAMS)))
for c in cond:
    md.append(f"| {c['exp']} | {c['vm']} | {c['policy']} | {c['load']} | " +
              " | ".join("—" if c.get(f'rec_{f}') is None else f"{c[f'rec_{f}']:.4f}" for f in FAMS) + " |")
md.append(f"\nInvalid runs (excluded): {', '.join(r['run'] for r in rows if not r['valid']) or 'none'}\n")
open(os.path.join(OUT, "tables.md"), "w").write("\n".join(md) + "\n")

# ---- E4: Adaptive vs static policies (bootstrap CI over runs, Hedges' g) ----
import random
rng = random.Random(42)


def hedges_g(x, y):
    nx, ny = len(x), len(y)
    if nx < 2 or ny < 2:
        return None
    sp = math.sqrt(((nx - 1) * statistics.variance(x) + (ny - 1) * statistics.variance(y)) / (nx + ny - 2))
    if sp == 0:
        return None
    return (statistics.mean(x) - statistics.mean(y)) / sp * (1 - 3 / (4 * (nx + ny) - 9))


def boot_ci(x, y, B=10000):
    d = sorted(statistics.mean(rng.choices(x, k=len(x))) - statistics.mean(rng.choices(y, k=len(y))) for _ in range(B))
    return d[int(0.025 * B)], d[int(0.975 * B) - 1]


eff = []
for load in sorted({k[3] for k in groups if k[0] == "E4"}):
    ad = groups.get(("E4", "L", "adapt", load), [])
    for other in ("full", "light"):
        ot = groups.get(("E4", "L", other, load), [])
        if len(ad) < 2 or len(ot) < 2:
            continue
        for mtr in ("p95_s", "p99_s", "slo_share", "recall", "fpr", "macro_f1", "rec_Web", "rec_Botnet", "rec_Infiltration"):
            x = [r[mtr] for r in ad if r[mtr] is not None]; y = [r[mtr] for r in ot if r[mtr] is not None]
            if len(x) < 2 or len(y) < 2:
                continue
            lo, hi = boot_ci(x, y)
            g = hedges_g(x, y)
            eff.append(dict(load=load, comparison=f"adapt - {other}", metric=mtr, adapt_mean=round(statistics.mean(x), 5),
                            other_mean=round(statistics.mean(y), 5), diff=round(statistics.mean(x) - statistics.mean(y), 5),
                            ci_low=round(lo, 5), ci_high=round(hi, 5), hedges_g=None if g is None else round(g, 2)))
if eff:
    with open(os.path.join(OUT, "e4_effects.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(eff[0]), lineterminator="\n"); w.writeheader(); w.writerows(eff)

# ---- Figure: E2 latency vs load ----
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    e2 = [c for c in cond if c["exp"] == "E2"]
    if e2:
        fig, ax = plt.subplots(figsize=(6.4, 4.0), dpi=200)
        style = {"full": ("#1f4e79", "Static-Full"), "light": ("#c55a11", "Static-Light")}
        for pol, (col, lab) in style.items():
            cs = sorted((c for c in e2 if c["policy"] == pol), key=lambda c: load_key(c["load"]))
            if not cs:
                continue
            x = [load_key(c["load"]) for c in cs]
            for mtr, ls, suffix in (("p95_s", "-", "p95"), ("p50_s", "--", "p50")):
                y = [c[mtr] for c in cs]; e = [c[mtr + "_ci"] or 0 for c in cs]
                ax.errorbar(x, y, yerr=e, color=col, ls=ls, marker="o", ms=4, capsize=3, label=f"{lab} {suffix}")
        ax.axhline(5, color="grey", lw=0.8, ls=":"); ax.text(0.47, 5.6, "5 s objective", color="grey", fontsize=8)
        ax.axvline(1.0, color="grey", lw=0.6, ls="-.")
        ax.set_yscale("log"); ax.set_xlabel("Offered load (multiple of S, S = 45,000 records/s on L)")
        ax.set_ylabel("End-to-end latency (s, log scale)"); ax.set_xticks([0.5, 0.8, 1.0, 1.2, 1.5])
        ax.grid(alpha=0.3, which="both"); ax.legend(fontsize=7.5, ncol=2, loc="upper left")
        fig.tight_layout(); fig.savefig(os.path.join(OUT, "fig_e2_latency.png")); plt.close(fig)
except ImportError:
    print("matplotlib not installed: figure skipped (pip install matplotlib)")

nv = sum(r["valid"] for r in rows)
print(f"{len(rows)} runs read, {nv} valid, {len(rows) - nv} invalid; {len(cond)} conditions -> {OUT}")
print("\n".join(md[:40]))
