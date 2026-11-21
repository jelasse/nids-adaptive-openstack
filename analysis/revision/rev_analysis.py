"""Revision experiments R1-R5: one valid run per base name; medians and ranges per experiment, policy and load.
Usage:  python3 rev_analysis.py <runs dir> <queue_results.csv> <out dir>
        e.g. python3 analysis/revision/rev_analysis.py results/revision results/queue_results.csv /tmp/check
"""
import csv, re, json, os, sys, collections, statistics as st
RUNS, QUEUE, OUTD = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(OUTD, exist_ok=True)
rows = [r for r in csv.DictReader(open(QUEUE)) if re.match(r"R\d_", r["name"])]
last = {}
for r in rows: last[r["name"]] = r
chosen = {}
for n, r in last.items():
    b = re.sub(r"(_r\d+)[b-g]$", r"\1", n)
    if r["vm_valid"] == "True" and r["laptop_valid"] == "True":
        if b not in chosen or n > chosen[b]["name"]: chosen[b] = r
def dl(run):
    p = os.path.join(RUNS, run, "deadline.csv"); d = {}
    if os.path.exists(p):
        for r in csv.DictReader(open(p)):
            if r["value"] != "": d[(r["family"], r["metric"])] = float(r["value"])
    return d
def ev(run):
    p = os.path.join(RUNS, run, "evasion.csv"); d = {}
    if os.path.exists(p):
        for r in csv.DictReader(open(p)):
            if r["value"] != "": d[(r["part"], r["metric"])] = float(r["value"])
    return d
groups = collections.defaultdict(list)
for b, r in chosen.items():
    exp, vm, pol, cond = b.split("_")[:4]
    groups[(exp, pol, cond if not cond.startswith("ev") else "ev")].append((b, r))
def fmt(v, k=3): return "-" if v is None else (f"{v:,.0f}" if abs(v) >= 100 else f"{v:.{k}f}")
def med(xs): xs = [x for x in xs if x is not None]; return st.median(xs) if xs else None
def rng(xs): xs = [x for x in xs if x is not None]; return (min(xs), max(xs)) if xs else (None, None)
out = []
for key in sorted(groups):
    g = groups[key]; n = len(g)
    q = lambda f: [float(r[f]) for _, r in g]
    D = [dl(r["name"]) for _, r in g]
    get = lambda fam, m: [d.get((fam, m)) for d in D]
    rec = dict(exp=key[0], policy=key[1], cond=key[2], n=n,
               sustainable=sum(r["sustainable"] == "True" for _, r in g),
               p50_ms=med(q("p50_ms")), p95_ms=med(q("p95_ms")), backlog_max=med(q("backlog_max")),
               throughput=med(q("throughput")), recall=med(q("recall")), recall_min=rng(q("recall"))[0], recall_max=rng(q("recall"))[1],
               tpr1=med(get("ALL", "tpr_at_1s")), tpr5=med(get("ALL", "tpr_at_5s")), tpr10=med(get("ALL", "tpr_at_10s")),
               fp_min=med(get("ALL", "fp_per_min")), precision=med(get("ALL", "precision")), light_share=med(get("ALL", "light_share")))
    for fam in ("Botnet", "Infiltration", "Web", "BruteForce", "DoS", "DDoS"):
        rec[f"tpr5_{fam}"] = med(get(fam, "tpr_at_5s")); rec[f"rec_{fam}"] = med(get(fam, "tpr_eventual"))
    if key[0] == "R3":
        E = [ev(r["name"]) for _, r in g]
        for part, m in [("flood", "time_to_light_s"), ("flood", "light_share"), ("flood", "tpr_at_5s"), ("flood", "fp_per_min"),
                        ("control", "tpr_at_5s"), ("control", "tpr_eventual"), ("attack", "light_share"), ("attack", "tpr_at_1s"),
                        ("attack", "tpr_at_5s"), ("attack", "tpr_at_10s"), ("attack", "tpr_eventual"), ("attack", "first_alert_delay_s"),
                        ("attack", "start_s_after_flood_start")]:
            vals = [e.get((part, m)) for e in E]
            rec[f"{part}.{m}"] = med(vals); rec[f"{part}.{m}.min"], rec[f"{part}.{m}.max"] = rng(vals)
    out.append(rec)
keys = sorted({k for r in out for k in r}, key=lambda k: (list(out[0]).index(k) if k in out[0] else 999, k))
with open(os.path.join(OUTD, "revision_results_summary.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=keys, lineterminator="\n"); w.writeheader(); w.writerows(out)
for r in out:
    print(f"{r['exp']} {r['policy']:<8}{r['cond']:<6} n={r['n']} sust={r['sustainable']} p50={fmt(r['p50_ms'])} p95={fmt(r['p95_ms'])} "
          f"backlog={fmt(r['backlog_max'])} thr={fmt(r['throughput'])} recall={fmt(r['recall'])} [{fmt(r['recall_min'])}-{fmt(r['recall_max'])}] "
          f"tpr1={fmt(r['tpr1'])} tpr5={fmt(r['tpr5'])} tpr10={fmt(r['tpr10'])} fp/min={fmt(r['fp_min'])} prec={fmt(r['precision'])} light={fmt(r['light_share'],2)}")
    print("     fam tpr5: " + " ".join(f"{f}={fmt(r.get('tpr5_'+f))}/{fmt(r.get('rec_'+f))}" for f in ("Botnet","Infiltration","Web","BruteForce","DoS","DDoS")))
    if r["exp"] == "R3":
        print("     EVASION: " + "; ".join(f"{k}={fmt(r[k])} [{fmt(r[k+'.min'])}-{fmt(r[k+'.max'])}]" for k in r if "." in k and not k.endswith((".min", ".max"))))
