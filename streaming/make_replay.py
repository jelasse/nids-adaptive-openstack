#!/usr/bin/env python3
"""Build a replay file from a split (val or test): the label, family and 78 features
of every flow, in timestamp order, in ONE Parquet file. The replay producer reads it
sequentially, so flows are sent in the order they occurred.

Usage (in the VM):
  spark-submit --driver-memory 2g make_replay.py ~/nids/data/val  ~/nids/replay/val.parquet
  spark-submit --driver-memory 2g make_replay.py ~/nids/data/test ~/nids/replay/test.parquet   (final runs only)
"""
import os, shutil, sys, glob
from pyspark.sql import SparkSession, functions as F

src, out = sys.argv[1], sys.argv[2]
feats_file = os.path.join(os.path.dirname(src.rstrip("/")), "report", "features.txt")
FEATS = [l.strip() for l in open(feats_file) if l.strip()]

spark = (SparkSession.builder.appName("make-replay")
         .config("spark.sql.shuffle.partitions", "16").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
df = spark.read.parquet(src)
tmp = out + ".tmp"
(df.orderBy("ts", "day", "id")
   .select(F.col("y").cast("int").alias("y"), "family", *[F.col(f"`{c}`") for c in FEATS])
   .coalesce(1)
   .write.mode("overwrite").parquet(tmp))
part = glob.glob(os.path.join(tmp, "part-*.parquet"))[0]
os.makedirs(os.path.dirname(out), exist_ok=True)
shutil.move(part, out)
shutil.rmtree(tmp)
n = spark.read.parquet(out).count()
print(f"Replay file written: {out}  rows: {n:,}  columns: {2 + len(FEATS)}")
spark.stop()
