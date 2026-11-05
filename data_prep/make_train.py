#!/usr/bin/env python3
"""Build the training set (W4 Thursday, step 2 of preprocessing).

preprocess.py already wrote _stage_clean, val, test, attempted and report/cutoffs.csv,
but ran out of memory while de-duplicating the 44 M training rows. This script
builds training in a lighter order:

  1. training rows = cleaned rows with timestamp <= the 70 % cut-off of their (day, label)
  2. keep ALL attack rows and a 10 % random sample of Benign rows (seed 42)
  3. remove exact duplicates (same 78 measurements and same label)

It then counts every split and checks that train(before sampling) + val + test
equals the number of cleaned rows, so no flow is lost or used twice.

Usage:
  spark-submit --driver-memory 8g make_train.py <out_dir> [--benign-frac 0.10]
"""
import argparse, os
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("out_dir")
ap.add_argument("--benign-frac", type=float, default=0.10)
ap.add_argument("--seed", type=int, default=42)
a = ap.parse_args()
OUT = a.out_dir

spark = (SparkSession.builder.appName("make-train")
         .config("spark.sql.shuffle.partitions", "200")
         .config("spark.sql.files.maxPartitionBytes", "64m")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

FEATS = [l.strip() for l in open(os.path.join(OUT, "report", "features.txt")) if l.strip()]
META = ["id", "day", "ts", "Label", "family", "y"]

clean = spark.read.parquet(os.path.join(OUT, "_stage_clean")).filter(F.col("drop_reason").isNull())
cut = (spark.read.option("header", True).csv(os.path.join(OUT, "report", "cutoffs.csv"))
       .select(F.col("day"), F.col("label").alias("Label"),
               F.to_timestamp("cut_70").cast("double").alias("c70")))

tr = (clean.join(F.broadcast(cut), ["day", "Label"])
           .filter(F.col("ts").cast("double") <= F.col("c70"))
           .select(*META, *[F.col(f"`{c}`") for c in FEATS]))

n_train_all = tr.count()
n_val = spark.read.parquet(os.path.join(OUT, "val")).count()
n_test = spark.read.parquet(os.path.join(OUT, "test")).count()
n_clean = clean.count()
print(f"\nCHECK  train(all) {n_train_all:,} + val {n_val:,} + test {n_test:,} = "
      f"{n_train_all + n_val + n_test:,}   cleaned rows = {n_clean:,}   "
      f"{'OK' if n_train_all + n_val + n_test == n_clean else 'MISMATCH'}")

samp = (tr.filter(F.col("y") == 1)
          .unionByName(tr.filter(F.col("y") == 0).sample(False, a.benign_frac, seed=a.seed)))
fp = F.xxhash64(*[F.col(f"`{c}`") for c in FEATS], F.col("Label"))
samp = samp.withColumn("_h", fp)
TMP = os.path.join(OUT, "_train_sampled")
samp.write.mode("overwrite").parquet(TMP)
s = spark.read.parquet(TMP)
n_samp = s.count()
s.dropDuplicates(["_h"]).drop("_h").write.mode("overwrite").parquet(os.path.join(OUT, "train"))

# ---------- counts per split ----------
parts = {"train_all": tr, "train": spark.read.parquet(os.path.join(OUT, "train")),
         "val": spark.read.parquet(os.path.join(OUT, "val")),
         "test": spark.read.parquet(os.path.join(OUT, "test")),
         "attempted": spark.read.parquet(os.path.join(OUT, "attempted"))}
rows = []
for p, x in parts.items():
    for r in x.groupBy("family", "Label").count().collect():
        rows.append((p, r["family"], r["Label"], r["count"]))
with open(os.path.join(OUT, "report", "split_counts.csv"), "w") as f:
    f.write("split,family,label,count\n")
    for p, fam, lab, n in sorted(rows):
        f.write(f'{p},{fam},"{lab}",{n}\n')

order = ["train_all", "train", "val", "test"]
fams = sorted({r[1] for r in rows if r[0] != "attempted"})
print("\n=== ROWS PER FAMILY AND SPLIT ===")
print("  (train_all = all training rows before sampling; train = what the models use)")
print(f"  {'family':<14}" + "".join(f"{p:>14}" for p in order))
for fam in fams:
    print(f"  {fam:<14}" + "".join(
        f"{sum(n for p2, f2, _, n in rows if p2 == p and f2 == fam):>14,}" for p in order))
tot = {p: sum(n for p2, _, _, n in rows if p2 == p) for p in parts}
print(f"  {'TOTAL':<14}" + "".join(f"{tot[p]:>14,}" for p in order))
print(f"\n  benign sample fraction {a.benign_frac} (seed {a.seed}): {n_samp:,} rows before "
      f"de-duplication, {tot['train']:,} after ({n_samp - tot['train']:,} duplicates removed)")
print(f"  attempted (kept aside): {tot['attempted']:,}")
spark.stop()
