#!/usr/bin/env python3
"""Summarize one streaming run: completeness, throughput, latency, detection quality,
batch times, backlog and CPU/RAM. Writes summary.json in the run directory.

Usage (in the VM):
  spark-submit --driver-memory 2g analyze_run.py ~/nids/runs/run01 [--expected 600000 --start-id 0]
               [--clock-offset-ms -101] [--slo-ms 5000]
Warm-up records (event_id >= 1,000,000,000) are counted but excluded from every metric.
"""
import argparse, csv, json, os, statistics
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("run_dir")
ap.add_argument("--expected", type=int, default=0, help="number of records the producer sent")
ap.add_argument("--start-id", type=int, default=0)
ap.add_argument("--clock-offset-ms", type=float, default=0.0,
                help="VM clock minus producer clock (measured per session); subtracted from every latency")
ap.add_argument("--slo-ms", type=float, default=5000.0, help="latency objective for the violation share")
ap.add_argument("--warmup-id-from", type=int, default=1_000_000_000)
a = ap.parse_args()
R = a.run_dir

spark = SparkSession.builder.appName("analyze-run").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
P = spark.read.parquet(os.path.join(R, "predictions"))
W = P.filter(F.col("event_id") >= a.warmup_id_from)
n_warm = W.count()
warm_end_detect = W.agg(F.max("detect_ts")).first()[0] if n_warm else None
P = P.filter(F.col("event_id") < a.warmup_id_from)
if a.clock_offset_ms:
    P = P.withColumn("latency_ms", F.col("latency_ms") - F.lit(a.clock_offset_ms))
P = P.cache()
S = {"clock_offset_ms": a.clock_offset_ms, "warmup_records": n_warm, "slo_ms": a.slo_ms}

c = P.agg(F.count("*").alias("rows"), F.countDistinct("event_id").alias("uniq"),
          F.min("event_id").alias("lo"), F.max("event_id").alias("hi"),
          F.min("send_ts").alias("t0"), F.max("detect_ts").alias("t1")).first()
S["rows"], S["unique"] = c["rows"], c["uniq"]
S["duplicates"] = c["rows"] - c["uniq"]
lo, hi = (a.start_id, a.start_id + a.expected - 1) if a.expected else (c["lo"], c["hi"])
S["missing"] = (hi - lo + 1) - P.filter(F.col("event_id").between(lo, hi)).select("event_id").distinct().count()
S["span_s"] = round(c["t1"] - c["t0"], 1)
S["throughput_rps"] = round(c["rows"] / max(c["t1"] - c["t0"], 1e-9))


def lat(df):
    q = df.agg(F.expr("percentile_approx(latency_ms, array(0.5, 0.95, 0.99), 10000)").alias("q"),
               F.max("latency_ms").alias("mx"), F.avg("latency_ms").alias("mean")).first()
    return dict(p50=round(q["q"][0]), p95=round(q["q"][1]), p99=round(q["q"][2]),
                max=round(q["mx"]), mean=round(q["mean"]))


def det(df):
    r = df.agg(*[F.sum(F.when(cond, 1).otherwise(0)).alias(k) for k, cond in [
        ("tp", (F.col("pred") == 1) & (F.col("y") == 1)), ("fp", (F.col("pred") == 1) & (F.col("y") == 0)),
        ("fn", (F.col("pred") == 0) & (F.col("y") == 1)), ("tn", (F.col("pred") == 0) & (F.col("y") == 0))]]).first()
    tp, fp, fn, tn = (r[k] or 0 for k in ("tp", "fp", "fn", "tn"))
    prec = tp / (tp + fp) if tp + fp else 0.0; rec = tp / (tp + fn) if tp + fn else 0.0
    f1a = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    precb = tn / (tn + fn) if tn + fn else 0.0; recb = tn / (tn + fp) if tn + fp else 0.0
    f1b = 2 * precb * recb / (precb + recb) if precb + recb else 0.0
    return dict(tp=tp, fp=fp, fn=fn, tn=tn, recall=round(rec, 5), precision=round(prec, 5),
                fpr=round(fp / (fp + tn), 6) if fp + tn else 0.0, macro_f1=round((f1a + f1b) / 2, 5))


S["latency_ms"] = lat(P)
S["slo_violation_share"] = round(P.filter(F.col("latency_ms") > a.slo_ms).count() / max(c["rows"], 1), 5)
# load phase: from the first to the last send of a measured record (laptop clock -> VM clock)
off_s = a.clock_offset_ms / 1000.0
load_end_vm = P.agg(F.max("send_ts")).first()[0] + off_s
S["load_s"] = round(load_end_vm - off_s - c["t0"], 1)
S["drain_s"] = round(c["t1"] - load_end_vm, 1)
S["detection"] = det(P)
modes = [r["mode"] for r in P.select("mode").distinct().collect()]
S["by_mode"] = {m: dict(rows=P.filter(F.col("mode") == m).count(),
                        latency_ms=lat(P.filter(F.col("mode") == m)),
                        detection=det(P.filter(F.col("mode") == m))) for m in sorted(modes)}
S["family_recall"] = {r["family"]: round(r["rec"], 4) for r in
                      P.filter("y = 1").groupBy("family").agg(F.avg("pred").alias("rec")).collect()}

B = [json.loads(l) for l in open(os.path.join(R, "batches.jsonl"))] if os.path.exists(os.path.join(R, "batches.jsonl")) else []
if B:
    tot = [b["total_s"] for b in B]
    S["batches"] = dict(count=len(B), mean_total_s=round(statistics.mean(tot), 2), max_total_s=max(tot),
                        mean_rows=round(statistics.mean(b["rows"] for b in B)),
                        modes={m: sum(1 for b in B if b["mode"] == m) for m in {b["mode"] for b in B}})
PG = os.path.join(R, "progress.jsonl")
if os.path.exists(PG):
    beh = []                                   # (batch start time in VM clock, backlog)
    for l in open(PG):
        p = json.loads(l)
        try:
            src = p["sources"][0]
            lo, end = (x if isinstance(x, dict) else json.loads(x) for x in (src["latestOffset"], src["endOffset"]))
            ts = datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00")).timestamp()
            beh.append((ts, sum(int(o) - int(end[t][q]) for t, ps in lo.items() for q, o in ps.items())))
        except Exception:
            pass
    if beh:
        meas = [b for t, b in beh if warm_end_detect is None or t >= warm_end_detect] or [b for t, b in beh]
        S["backlog_max_records"] = max(meas)
        before_end = [b for t, b in beh if t <= load_end_vm]
        S["backlog_at_load_end"] = before_end[-1] if before_end else None
        S["sustainable"] = bool(S["missing"] == 0 and before_end and before_end[-1] < 75000)
TF = os.path.join(R, "telemetry.csv")
if os.path.exists(TF):
    rows = [r for r in csv.DictReader(open(TF)) if c["t0"] + off_s <= float(r["ts"]) <= c["t1"]]
    if rows:
        cpu = sorted(float(r["cpu_pct"]) for r in rows)
        S["telemetry"] = dict(samples=len(rows), cpu_mean=round(statistics.mean(cpu), 1),
                              cpu_p95=cpu[int(0.95 * (len(cpu) - 1))], cpu_max=cpu[-1],
                              mem_used_max_mb=max(float(r["mem_used_mb"]) for r in rows),
                              mem_avail_min_mb=min(float(r["mem_avail_mb"]) for r in rows),
                              swap_used_max_mb=max(float(r["swap_used_mb"]) for r in rows),
                              steal_max=max(float(r["cpu_steal_pct"]) for r in rows))

S["valid_vm_side"] = S["missing"] == 0 and S["duplicates"] == 0
json.dump(S, open(os.path.join(R, "summary.json"), "w"), indent=2)
print(json.dumps(S, indent=2))
spark.stop()
