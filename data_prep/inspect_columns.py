#!/usr/bin/env python3
"""Second profiling pass over all ten daily CSVs (corrected CSE-CIC-IDS2018).

1. Label x Attempted Category counts (what the category codes mean).
2. Value range of the suspicious low-variety columns.

Usage:
  spark-submit --driver-memory 8g inspect_columns.py <csv_dir> <output_dir>
"""
import os, sys
from pyspark.sql import SparkSession, functions as F

src, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
spark = SparkSession.builder.appName("inspect-columns").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.read.option("header", True).option("inferSchema", False)
      .csv(os.path.join(src, "*.csv"))
      .withColumn("day", F.regexp_extract(F.input_file_name(), r"([A-Za-z]+-\d\d-\d\d-\d{4})", 1)))

# 1. Label x Attempted Category
ct = (df.groupBy("day", "Label", "Attempted Category").count()
        .orderBy("day", "Label", "Attempted Category"))
rows = ct.collect()
with open(os.path.join(outdir, "label_attempted_category.csv"), "w") as f:
    f.write("day,label,attempted_category,count\n")
    for r in rows:
        f.write(f'{r["day"]},"{r["Label"]}",{r["Attempted Category"]},{r["count"]}\n')

print("\n=== LABEL x ATTEMPTED CATEGORY (all days) ===")
for r in (df.groupBy("Label", "Attempted Category").count()
            .orderBy("Label", "Attempted Category").collect()):
    print(f'  {r["Label"]:<48} cat={r["Attempted Category"]:>3}  {r["count"]:>12,}')

# 2. Range of low-variety columns, per day
cols = ["Fwd URG Flags", "Bwd URG Flags", "URG Flag Count",
        "Subflow Fwd Packets", "Subflow Bwd Packets", "Protocol"]
aggs = []
for c in cols:
    v = F.col(f"`{c}`").cast("double")
    aggs += [F.min(v).alias(f"{c}|min"), F.max(v).alias(f"{c}|max"),
             F.sum(F.when(v != 0, 1).otherwise(0)).alias(f"{c}|nonzero")]
res = df.groupBy("day").agg(*aggs).orderBy("day").collect()

print("\n=== LOW-VARIETY COLUMNS: min / max / non-zero rows per day ===")
with open(os.path.join(outdir, "low_variety_columns.csv"), "w") as f:
    f.write("day,column,min,max,nonzero\n")
    for r in res:
        print(f'  {r["day"]}')
        for c in cols:
            mn, mx, nz = r[f"{c}|min"], r[f"{c}|max"], r[f"{c}|nonzero"]
            print(f'    {c:<22} min={mn:<8g} max={mx:<10g} nonzero={nz:,}')
            f.write(f'{r["day"]},{c},{mn},{mx},{nz}\n')

spark.stop()
