#!/usr/bin/env python3
"""Metrics of the induced-overload evasion runs (review MC1), from the per-flow predictions (run in the VM).

A run's flows are recognised by family: Benign = background, FLOOD family (DoS) = attacker's flood,
HIDDEN family (Botnet/Infiltration) = the low-volume attack, which forms two episodes (control, before the
flood; attack, during the flood), split at the largest gap between their send times.
Times use the VM clock: send_ts + clock offset (summary.json).

Per run (one CSV row per metric):
  flood:   start, duration, time until the detector first used Light (controller.jsonl or batches.jsonl),
           share of the flood's flows classified by Light, flood TPR within 5 s and eventually
  episodes (control / attack): flows, share classified by Light, TPR within 1/5/10 s and eventually,
           delay from the episode's first flow to its first correct alert
  false positives per minute during the flood
Usage (in the VM):  ~/nids-venv/bin/python evasion_metrics.py "~/nids/runs/R3_*" ~/nids/evasion.csv
"""
import csv, glob, json, os, sys
import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

PATTERN = os.path.expanduser(sys.argv[1] if len(sys.argv) > 1 else "~/nids/runs/R3_*")
OUT = os.path.expanduser(sys.argv[2] if len(sys.argv) > 2 else "~/nids/evasion.csv")
FLOOD = os.environ.get("FLOOD_FAMILY", "DoS")
rows = []


def add(run, part, metric, value):
    rows.append((run, part, metric, None if value is None else round(float(value), 6)))


for d in sorted(glob.glob(PATTERN)):
    run = os.path.basename(d)
    if not os.path.isdir(os.path.join(d, "predictions")) or not os.path.exists(os.path.join(d, "summary.json")):
        continue
    off = float(json.load(open(os.path.join(d, "summary.json"))).get("clock_offset_ms") or 0.0) / 1000.0
    t = pq.read_table(os.path.join(d, "predictions"),
                      columns=["event_id", "send_ts", "detect_ts", "y", "family", "pred", "mode"])
    keep = t.column("event_id").to_numpy() < 1_000_000_000
    send = t.column("send_ts").to_numpy()[keep] + off
    det = t.column("detect_ts").to_numpy()[keep]
    lat = det - send
    y = t.column("y").to_numpy()[keep]; pred = t.column("pred").to_numpy()[keep]
    fd = t.column("family").combine_chunks().dictionary_encode()
    fam = np.array(fd.dictionary.to_pylist(), dtype=object)[fd.indices.to_numpy(zero_copy_only=False)][keep]
    light = pc.equal(t.column("mode"), "light").to_numpy(zero_copy_only=False)[keep]
    del t
    fl = fam == FLOOD
    hidden_fams = [f for f in set(fam[y == 1]) if f != FLOOD]
    if not fl.any() or len(hidden_fams) != 1:
        print(f"skip {run}: not an evasion workload"); continue
    hf = hidden_fams[0]; hm = fam == hf
    f0, f1 = send[fl].min(), send[fl].max()
    add(run, "flood", "start_s_after_load_start", f0 - send.min())
    add(run, "flood", "duration_s", f1 - f0)
    add(run, "flood", "light_share", light[fl].mean())
    add(run, "flood", "tpr_at_5s", ((pred == 1) & fl & (lat <= 5)).sum() / fl.sum())
    add(run, "flood", "tpr_eventual", ((pred == 1) & fl).sum() / fl.sum())
    # first use of Light after the flood started (controller log, else the first light-classified flow)
    t_light = None
    cj = os.path.join(d, "controller.jsonl")
    if os.path.exists(cj):
        for line in open(cj):
            r = json.loads(line)
            if "switch_to" in r and r["switch_to"] != "full" and r["ts"] >= f0 and r["switch_to"] != "mix:0.00":
                t_light = r["ts"]; break
    if t_light is None and (light & (send >= f0)).any():
        t_light = det[light & (send >= f0)].min()
    add(run, "flood", "time_to_light_s", None if t_light is None else t_light - f0)
    ben_flood = (y == 0) & (send >= f0) & (send <= f1)
    add(run, "flood", "fp_per_min", ((pred == 1) & ben_flood).sum() / max((f1 - f0) / 60.0, 1e-9))
    # split the hidden family into its two episodes at the largest gap in send time
    hs = np.sort(send[hm]); gap = np.argmax(np.diff(hs)); cut = (hs[gap] + hs[gap + 1]) / 2
    add(run, "hidden", "family_" + hf, 1)
    for name, m in (("control", hm & (send < cut)), ("attack", hm & (send >= cut))):
        n = m.sum()
        e0 = send[m].min()
        add(run, name, "flows", n)
        add(run, name, "start_s_after_flood_start", e0 - f0)
        add(run, name, "light_share", light[m].mean())
        for tau in (1, 5, 10):
            add(run, name, f"tpr_at_{tau}s", ((pred == 1) & m & (lat <= tau)).sum() / n)
        add(run, name, "tpr_eventual", ((pred == 1) & m).sum() / n)
        hit = (pred == 1) & m
        add(run, name, "first_alert_delay_s", (det[hit].min() - e0) if hit.any() else None)
    print(f"{run}: flood light share {light[fl].mean():.2f}; attack-episode recall "
          f"{((pred == 1) & hm & (send >= cut)).sum() / (hm & (send >= cut)).sum():.3f}", flush=True)

with open(OUT, "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n"); w.writerow(["run", "part", "metric", "value"]); w.writerows(rows)
print("written", OUT, len(rows), "rows")
