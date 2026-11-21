#!/usr/bin/env python3
"""Deadline-constrained detection and false-alarm metrics from the per-flow predictions (run inside the VM).

For every run folder in RUNS_DIR that has predictions/ and summary.json, computes
  - TPR@tau: share of attack flows correctly flagged within tau seconds (tau = 1, 2, 5, 10, 30, 60 s, and eventually),
    overall and per attack family
  - false positives: total, per minute of load, per million benign flows; precision and false discovery rate
  - per family: share of its flows classified in Light mode, delay of the first correct alert
Latencies are corrected by the run's measured clock offset (summary.json), as in analyze_run.py.
Warm-up records (event_id >= 1e9) are excluded.

Usage (in the VM):  ~/nids-venv/bin/python deadline_metrics.py ~/nids/runs ~/nids/deadline.csv
                    ~/nids-venv/bin/python deadline_metrics.py ~/nids/runs/<run> ~/nids/runs/<run>/deadline.csv  (one run)
Output: one long-format CSV row per (run, family, metric).
"""
import csv, glob, json, os, sys
import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

RUNS_DIR = os.path.expanduser(sys.argv[1] if len(sys.argv) > 1 else "~/nids/runs")
OUT = os.path.expanduser(sys.argv[2] if len(sys.argv) > 2 else "~/nids/deadline.csv")
TAUS = [1, 2, 5, 10, 30, 60]
COLS = ["event_id", "send_ts", "detect_ts", "latency_ms", "y", "family", "pred", "mode"]

rows = []
def add(run, fam, metric, value):
    rows.append((run, fam, metric, None if value is None else round(float(value), 6)))

cand = [RUNS_DIR] if os.path.isdir(os.path.join(RUNS_DIR, "predictions")) else glob.glob(os.path.join(RUNS_DIR, "[ER]*"))
runs = sorted(d for d in cand
              if os.path.isdir(os.path.join(d, "predictions")) and os.path.exists(os.path.join(d, "summary.json")))
print(f"{len(runs)} runs with predictions in {RUNS_DIR}", flush=True)
for i, d in enumerate(runs, 1):
    run = os.path.basename(d)
    S = json.load(open(os.path.join(d, "summary.json")))
    off_ms = float(S.get("clock_offset_ms") or 0.0)
    t = pq.read_table(os.path.join(d, "predictions"), columns=COLS)
    eid = t.column("event_id").to_numpy()
    keep = eid < 1_000_000_000
    send = t.column("send_ts").to_numpy()[keep]
    det = t.column("detect_ts").to_numpy()[keep]
    lat = (t.column("latency_ms").to_numpy()[keep] - off_ms) / 1000.0
    y = t.column("y").to_numpy()[keep].astype(np.int8)
    pred = t.column("pred").to_numpy()[keep].astype(np.int8)
    fd = t.column("family").combine_chunks().dictionary_encode()
    names = np.array(fd.dictionary.to_pylist(), dtype=object)
    fam = names[fd.indices.to_numpy(zero_copy_only=False)][keep]          # family name per flow
    light = pc.equal(t.column("mode"), "light").to_numpy(zero_copy_only=False)[keep]
    del t

    att, ben = y == 1, y == 0
    tp, fp = att & (pred == 1), ben & (pred == 1)
    load_min = (send.max() - send.min()) / 60.0
    add(run, "ALL", "attacks", att.sum()); add(run, "ALL", "benign", ben.sum())
    add(run, "ALL", "fp", fp.sum())
    add(run, "ALL", "fp_per_min", fp.sum() / load_min if load_min > 0 else None)
    add(run, "ALL", "fp_per_million_benign", 1e6 * fp.sum() / max(ben.sum(), 1))
    add(run, "ALL", "precision", tp.sum() / max(tp.sum() + fp.sum(), 1))
    add(run, "ALL", "fdr", fp.sum() / max(tp.sum() + fp.sum(), 1))
    add(run, "ALL", "load_min", load_min)
    for name, mask in [("ALL", att)] + [(f, att & (fam == f)) for f in sorted(set(fam[att]))]:
        n = mask.sum()
        if n == 0:
            continue
        add(run, name, "n_attack_flows", n)
        for tau in TAUS:
            add(run, name, f"tpr_at_{tau}s", (tp & mask & (lat <= tau)).sum() / n)
        add(run, name, "tpr_eventual", (tp & mask).sum() / n)
        add(run, name, "light_share", (light & mask).sum() / n)
        hit = tp & mask
        add(run, name, "first_alert_delay_s",
            (det[hit].min() - send[mask].min() - off_ms / 1000.0) if hit.any() else None)
    print(f"[{i}/{len(runs)}] {run}", flush=True)

with open(OUT, "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n"); w.writerow(["run", "family", "metric", "value"]); w.writerows(rows)
print("written", OUT, len(rows), "rows")
