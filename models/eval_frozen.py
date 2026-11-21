#!/usr/bin/env python3
"""Offline test-split evaluation of the frozen models with their frozen thresholds (review MC2, MC11).
For each of full_rf, light_lr, light_rf: recall per attack family, FPR, precision, Macro-F1, false positives,
and scoring throughput; plus the validation block of light_rf from frozen_models.json and its feature list.
Usage (laptop):  spark-submit --driver-memory 8g eval_frozen.py ~/w4/data ~/w5/models ~/w8/rev
Output: <out>/frozen_test_eval.csv, <out>/light_rf_frozen.json
"""
import csv, json, os, sys, time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

D, M, OUT = (os.path.expanduser(x) for x in sys.argv[1:4])
if not os.path.exists(os.path.join(M, "frozen_models.json")) and os.path.exists(os.path.join(M, "models", "frozen_models.json")):
    M = os.path.join(M, "models")
os.makedirs(OUT, exist_ok=True)
spark = (SparkSession.builder.appName("eval-frozen").config("spark.sql.shuffle.partitions", "64")
         .config("spark.sql.codegen.wholeStage", "false").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
FEATS = [l.strip() for l in open(os.path.join(D, "report", "features.txt")) if l.strip()]
te = spark.read.parquet(os.path.join(D, "test")).select("y", "family", *[F.col(f"`{c}`") for c in FEATS])
meta = json.load(open(os.path.join(M, "frozen_models.json")))
json.dump(meta.get("light_rf"), open(os.path.join(OUT, "light_rf_frozen.json"), "w"), indent=2)
recs = []
for sub in ("full_rf", "light_lr", "light_rf"):
    t = float(meta[sub]["threshold"]); model = PipelineModel.load(os.path.join(M, sub))
    t0 = time.time()
    rows = (model.transform(te).select("y", "family", (vector_to_array("probability")[1] >= t).cast("int").alias("p"))
                 .groupBy("family", "y", "p").count().collect())
    rps = sum(r["count"] for r in rows) / (time.time() - t0)
    tp = sum(r["count"] for r in rows if r["y"] == 1 and r["p"] == 1); fn = sum(r["count"] for r in rows if r["y"] == 1 and r["p"] == 0)
    fp = sum(r["count"] for r in rows if r["y"] == 0 and r["p"] == 1); tn = sum(r["count"] for r in rows if r["y"] == 0 and r["p"] == 0)
    pr = tp / (tp + fp) if tp + fp else 0; rc = tp / (tp + fn); f1a = 2 * pr * rc / (pr + rc) if pr + rc else 0
    pb = tn / (tn + fn) if tn + fn else 0; rb = tn / (tn + fp); f1b = 2 * pb * rb / (pb + rb) if pb + rb else 0
    fam = {}
    for r in rows:
        if r["y"] == 1:
            x = fam.setdefault(r["family"], [0, 0]); x[1] += r["count"]; x[0] += r["count"] * r["p"]
    rec = dict(model=sub, threshold=t, rows_per_s=round(rps), recall=round(rc, 5), precision=round(pr, 5),
               fpr=round(fp / (fp + tn), 7), fp=fp, macro_f1=round((f1a + f1b) / 2, 5),
               family_recall=json.dumps({k: round(a / n, 4) for k, (a, n) in sorted(fam.items())}),
               family_n=json.dumps({k: n for k, (a, n) in sorted(fam.items())}))
    recs.append(rec); print(rec, flush=True)
with open(os.path.join(OUT, "frozen_test_eval.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(recs[0]), lineterminator="\n"); w.writeheader(); w.writerows(recs)
print("written", os.path.join(OUT, "frozen_test_eval.csv"))
