#!/usr/bin/env python3
"""VM check of the frozen models on a RANDOM validation sample (never the test split).
The sample is spread over several partitions and kept in memory/disk, so that every
model is timed on the same in-memory data with all cores. One warm-up pass, then
3 timed passes per model, in both orders (Light first, then Full, then Light again)."""
import argparse, json, os, time, statistics
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

ap = argparse.ArgumentParser()
ap.add_argument("models_dir"); ap.add_argument("val_dir")
ap.add_argument("--rows", type=int, default=1000000)
a = ap.parse_args()

spark = SparkSession.builder.appName("vm-model-check").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
cores = spark.sparkContext.defaultParallelism
meta = json.load(open(os.path.join(a.models_dir, "frozen_models.json")))
feats = [l.strip() for l in open(os.path.join(os.path.dirname(a.val_dir.rstrip('/')), "report", "features.txt")) if l.strip()]
full = spark.read.parquet(a.val_dir).select("y", *[F.col(f"`{c}`") for c in feats])
frac = min(1.0, a.rows / full.count())
data = full.sample(False, frac, seed=42).repartition(cores * 2).persist(StorageLevel.MEMORY_AND_DISK)
n = data.count(); P = data.filter("y = 1").count()
print(f"Random validation sample: {n:,} rows ({P:,} attacks = {P / n:.2%}); cores: {cores}; "
      f"partitions: {data.rdd.getNumPartitions()}")

models = {name: PipelineModel.load(os.path.join(a.models_dir, name)) for name in ["full_rf", "light_lr"]}
for name in ["light_lr", "full_rf", "light_lr"]:
    thr = meta[name]["threshold"]
    q = (models[name].transform(data)
          .withColumn("pred", (vector_to_array("probability")[1] >= thr).cast("int"))
          .agg(F.sum(F.when((F.col("pred") == 1) & (F.col("y") == 1), 1).otherwise(0)).alias("tp"),
               F.sum(F.when((F.col("pred") == 1) & (F.col("y") == 0), 1).otherwise(0)).alias("fp")))
    q.first()                                   # warm-up
    times = []
    for _ in range(3):
        t0 = time.time(); r = q.first(); times.append(time.time() - t0)
    med = statistics.median(times)
    print(f"  {name:<9} threshold {thr:.3f}   recall {r['tp'] / max(P, 1):.4f}   "
          f"FPR {r['fp'] / max(n - P, 1):.5f}   median {n / med:,.0f} rows/s "
          f"(runs: {', '.join(f'{t:.1f}s' for t in times)})", flush=True)
spark.stop()
