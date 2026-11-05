#!/usr/bin/env python3
"""Profile one daily CSV file of the corrected CSE-CIC-IDS2018 dataset.

Reads every column as text (so nothing is silently converted), then reports per column:
empty values, non-numeric values, infinite and NaN markers, and the approximate number of
distinct values (1 = constant column). Also reports row count and label counts.

Usage: spark-submit --driver-memory 8g profile_day.py <file.csv> <output_dir>
"""
import csv, os, sys
from pyspark.sql import SparkSession, functions as F

path, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
day = os.path.basename(path).replace(".csv", "")

spark = SparkSession.builder.appName(f"profile-{day}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.option("header", True).option("inferSchema", False).csv(path)

label_col = next((c for c in df.columns if c.strip().lower() == "label"), None)
INF = ["inf", "+inf", "-inf", "infinity", "+infinity", "-infinity"]

aggs = [F.count(F.lit(1)).alias("__rows")]
for i, c in enumerate(df.columns):
    v = F.trim(F.col(f"`{c}`"))
    low = F.lower(v)
    is_empty = v.isNull() | (v == "")
    aggs += [
        F.sum(F.when(is_empty, 1).otherwise(0)).alias(f"e{i}"),
        F.sum(F.when(low.isin(INF), 1).otherwise(0)).alias(f"i{i}"),
        F.sum(F.when(low == "nan", 1).otherwise(0)).alias(f"n{i}"),
        F.sum(F.when(~is_empty & ~low.isin(INF + ["nan"]) & v.cast("double").isNull(), 1)
               .otherwise(0)).alias(f"t{i}"),
        F.approx_count_distinct(v).alias(f"d{i}"),
    ]
r = df.agg(*aggs).first()
rows = r["__rows"]

with open(os.path.join(outdir, f"{day}_columns.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["column", "empty", "infinite", "nan", "non_numeric_text", "approx_distinct", "note"])
    for i, c in enumerate(df.columns):
        e, inf, nan, txt, dist = r[f"e{i}"], r[f"i{i}"], r[f"n{i}"], r[f"t{i}"], r[f"d{i}"]
        note = []
        if dist <= 1: note.append("CONSTANT")
        if txt == rows - e: note.append("text column")
        elif txt: note.append("mixed text/number")
        if inf or nan: note.append("has inf/NaN")
        w.writerow([c, e, inf, nan, txt, dist, "; ".join(note)])

with open(os.path.join(outdir, f"{day}_summary.txt"), "w") as f:
    f.write(f"FILE {path}\nROWS {rows}\nCOLUMNS {len(df.columns)}\nLABEL_COLUMN {label_col}\n\n")
    if label_col:
        f.write("LABEL COUNTS\n")
        for row in df.groupBy(F.col(f"`{label_col}`")).count().orderBy(F.desc("count")).collect():
            f.write(f"  {row[0]!s:<45} {row['count']:>12,}  ({100*row['count']/rows:.3f}%)\n")
    f.write("\nCOLUMNS NEEDING ATTENTION\n")
    for i, c in enumerate(df.columns):
        e, inf, nan, txt, dist = r[f"e{i}"], r[f"i{i}"], r[f"n{i}"], r[f"t{i}"], r[f"d{i}"]
        if dist <= 1 or e or inf or nan or txt:
            f.write(f"  {c:<35} empty={e} inf={inf} nan={nan} text={txt} distinct~{dist}\n")

print(open(os.path.join(outdir, f"{day}_summary.txt")).read())
