#!/usr/bin/env python3
"""Select and freeze a security-aware fallback model (review MC2), using train and validation data only.

Candidates: small Random Forests (trees x depth) on the top-k features of the frozen Full model's ranking.
Selection rule, fixed before any test result of the candidates is seen:
  among the candidates whose offline scoring throughput on validation is at least BUDGET times the frozen
  Full model's, choose the highest WORST-FAMILY recall on validation; ties broken by validation Macro-F1.
The chosen candidate is refitted on the full training split, its threshold set on validation (best Macro-F1,
as for the frozen models), and saved as <models>/light_rf; frozen_models.json gets a 'light_rf' entry
(a backup of the original file is kept).

Usage (laptop):
  spark-submit --driver-memory 8g freeze_fallback.py ~/w4/data ~/w5/models ~/w8/rev [--budget 4]
"""
import argparse, csv, json, os, shutil, time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.functions import vector_to_array

ap = argparse.ArgumentParser()
ap.add_argument("data_dir"); ap.add_argument("models"); ap.add_argument("out_dir")
ap.add_argument("--budget", type=float, default=4.0, help="minimum throughput relative to the Full model")
ap.add_argument("--trees", default="10,20,30"); ap.add_argument("--depths", default="6,8,10"); ap.add_argument("--ks", default="10,20")
ap.add_argument("--sample", type=float, default=0.3); ap.add_argument("--seed", type=int, default=42)
a = ap.parse_args()
D, OUT = os.path.expanduser(a.data_dir), os.path.expanduser(a.out_dir)
M = os.path.expanduser(a.models)
if not os.path.exists(os.path.join(M, "frozen_models.json")) and os.path.exists(os.path.join(M, "models", "frozen_models.json")):
    M = os.path.join(M, "models")
os.makedirs(OUT, exist_ok=True)
spark = (SparkSession.builder.appName("freeze-fallback").config("spark.sql.shuffle.partitions", "64").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir(os.path.join(OUT, "_ckpt"))
FEATS = [l.strip() for l in open(os.path.join(D, "report", "features.txt")) if l.strip()]
FC = [F.col(f"`{c}`") for c in FEATS]
rk = next(p for p in (os.path.join(M, "feature_ranking.csv"), os.path.join(os.path.dirname(M), "feature_ranking.csv"))
          if os.path.exists(p))
RANK = [r["feature"] for r in csv.DictReader(open(rk))]
BINS = 1000
train = spark.read.parquet(os.path.join(D, "train")).select("y", "family", *FC)
val = spark.read.parquet(os.path.join(D, "val")).select("y", "family", *FC)


def binned(model, df):
    t0 = time.time()
    rows = (model.transform(df).select("y", "family", F.floor(vector_to_array("probability")[1] * BINS).cast("int").alias("b"))
                 .groupBy("b", "family", "y").count().collect())
    n = sum(r["count"] for r in rows)
    return rows, n / (time.time() - t0)


def metrics(rows):
    pos = [0] * (BINS + 1); neg = [0] * (BINS + 1)
    for r in rows:
        (pos if r["y"] == 1 else neg)[min(max(r["b"], 0), BINS)] += r["count"]
    P, N = sum(pos), sum(neg); best = None; tp = fp = 0
    for b in range(BINS, -1, -1):
        tp += pos[b]; fp += neg[b]; fn, tn = P - tp, N - fp
        pr = tp / (tp + fp) if tp + fp else 1.0; rc = tp / P if P else 0.0
        f1a = 2 * pr * rc / (pr + rc) if pr + rc else 0.0
        pb = tn / (tn + fn) if tn + fn else 1.0; rb = tn / N if N else 0.0
        f1b = 2 * pb * rb / (pb + rb) if pb + rb else 0.0
        if best is None or (f1a + f1b) / 2 > best["macro_f1"]:
            best = dict(thr_bin=b, macro_f1=(f1a + f1b) / 2, recall=rc, fpr=fp / N if N else 0.0)
    fam = {}
    for r in rows:
        if r["y"] == 1:
            d = fam.setdefault(r["family"], [0, 0]); d[1] += r["count"]
            if r["b"] >= best["thr_bin"]: d[0] += r["count"]
    best["family_recall"] = {f: round(x / n, 4) for f, (x, n) in sorted(fam.items())}
    best["worst_family"] = min(best["family_recall"].values())
    return best


full = PipelineModel.load(os.path.join(M, "full_rf"))
binned(full, val)                     # warm-up pass (JIT), not measured
_, full_rps = binned(full, val)
print(f"Frozen Full model: {full_rps:,.0f} validation rows/s; budget >= {a.budget} x = {a.budget * full_rps:,.0f} rows/s", flush=True)
fams = [r["family"] for r in train.select("family").distinct().collect()]
samp = train.sampleBy("family", {f: (1.0 if f in ("Web", "Infiltration", "BruteForce", "Botnet") else a.sample) for f in fams},
                      seed=a.seed).cache()

def pipe(t, d, k):
    return Pipeline(stages=[VectorAssembler(inputCols=RANK[:k], outputCol="features"),
                            RandomForestClassifier(labelCol="y", featuresCol="features", numTrees=t, maxDepth=d,
                                                   seed=a.seed, cacheNodeIds=True, checkpointInterval=10, maxMemoryInMB=512)])

recs = []
for k in [int(x) for x in a.ks.split(",")]:
    for t in [int(x) for x in a.trees.split(",")]:
        for d in [int(x) for x in a.depths.split(",")]:
            m = pipe(t, d, k).fit(samp)
            rows, rps = binned(m, val); r = metrics(rows)
            rec = dict(trees=t, depth=d, k=k, val_rows_per_s=round(rps), speed_vs_full=round(rps / full_rps, 2),
                       macro_f1=round(r["macro_f1"], 5), recall=round(r["recall"], 5), fpr=round(r["fpr"], 6),
                       worst_family_recall=r["worst_family"], family_recall=r["family_recall"])
            recs.append(rec)
            print(f"  RF {t}x{d} top-{k}: {rec['speed_vs_full']}x Full, MacroF1 {rec['macro_f1']:.4f}, "
                  f"worst family {rec['worst_family_recall']:.3f} {r['family_recall']}", flush=True)
with open(os.path.join(OUT, "fallback_candidates.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(recs[0]), lineterminator="\n"); w.writeheader()
    for r in recs: w.writerow({k: (json.dumps(v) if isinstance(v, dict) else v) for k, v in r.items()})
ok = [r for r in recs if r["speed_vs_full"] >= a.budget]
if not ok:
    raise SystemExit("no candidate meets the throughput budget; lower --budget or use smaller forests")
ch = max(ok, key=lambda r: (r["worst_family_recall"], r["macro_f1"]))
print(f"\nChosen: RF {ch['trees']}x{ch['depth']} on top-{ch['k']} features", flush=True)

samp.unpersist()
model = pipe(ch["trees"], ch["depth"], ch["k"]).fit(train)
rows, rps = binned(model, val); r = metrics(rows)
model.write().overwrite().save(os.path.join(M, "light_rf"))
meta_path = os.path.join(M, "frozen_models.json")
if not os.path.exists(meta_path + ".orig"):
    shutil.copy(meta_path, meta_path + ".orig")
meta = json.load(open(meta_path))
meta["light_rf"] = dict(threshold=r["thr_bin"] / BINS,
                        params=dict(trees=ch["trees"], depth=ch["depth"], k=ch["k"], features=RANK[:ch["k"]],
                                    budget=a.budget, rule="max worst-family validation recall within throughput budget"),
                        validation=dict(macro_f1=r["macro_f1"], recall=r["recall"], fpr=r["fpr"],
                                        family_recall=r["family_recall"], rows_per_s=round(rps)))
json.dump(meta, open(meta_path, "w"), indent=2)
print(f"Saved {os.path.join(M, 'light_rf')}; threshold {meta['light_rf']['threshold']:.3f}; validation "
      f"MacroF1 {r['macro_f1']:.4f}, worst family {r['worst_family']:.3f}", flush=True)
