#!/usr/bin/env python3
"""Duplicate and label-conflict check over all ten daily CSVs.

Two flows are 'identical' when every measurement column is the same
(identifiers id, Flow ID, Src IP, Src Port, Dst IP and Timestamp are ignored,
as in Liu et al. [13]). The script reports, per label:
  dup_rows      - rows that have at least one identical twin
  conflict_rows - rows whose identical twins carry a DIFFERENT class
                  (Benign vs Attack vs Attempted), i.e. impossible to separate
Each row is fingerprinted with a 64-bit hash of its measurement columns.

Usage:
  spark-submit --driver-memory 8g dup_check.py <csv_dir> <output_dir>
"""
import os, sys
from pyspark.sql import SparkSession, Window, functions as F

src, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
spark = (SparkSession.builder.appName("dup-check")
         .config("spark.sql.shuffle.partitions", "400").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.read.option("header", True).option("inferSchema", False)
      .csv(os.path.join(src, "*.csv"))
      .withColumn("day", F.regexp_extract(F.input_file_name(), r"([A-Za-z]+-\d\d-\d\d-\d{4})", 1)))

IGNORE = {"id", "Flow ID", "Src IP", "Src Port", "Dst IP", "Timestamp",
          "Label", "Attempted Category", "day"}
feats = [c for c in df.columns if c not in IGNORE]
print(f"Fingerprint uses {len(feats)} measurement columns")

cls = (F.when(F.col("Label") == "BENIGN", "Benign")
        .when(F.col("Label").endswith("- Attempted"), "Attempted")
        .otherwise("Attack"))
d = (df.select("day", "Label", cls.alias("cls"),
               F.xxhash64(*[F.col(f"`{c}`") for c in feats]).alias("h")))

w = Window.partitionBy("h")
d = (d.withColumn("n", F.count(F.lit(1)).over(w))
      .withColumn("ncls", F.size(F.collect_set("cls").over(w)))
      .withColumn("first", F.row_number().over(w.orderBy("day", "Label")))
      .cache())

tot = d.agg(F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("first") == 1, 1).otherwise(0)).alias("unique"),
            F.sum(F.when(F.col("ncls") > 1, 1).otherwise(0)).alias("conflict")).first()
print(f"\nTOTAL rows={tot['rows']:,}  unique flows={tot['unique']:,}  "
      f"surplus copies={tot['rows'] - tot['unique']:,} "
      f"({(tot['rows'] - tot['unique']) / tot['rows'] * 100:.2f}%)  "
      f"rows in label conflicts={tot['conflict']:,}")

per = (d.groupBy("Label")
        .agg(F.count(F.lit(1)).alias("rows"),
             F.sum(F.when(F.col("n") > 1, 1).otherwise(0)).alias("dup_rows"),
             F.sum(F.when(F.col("first") > 1, 1).otherwise(0)).alias("surplus"),
             F.sum(F.when(F.col("ncls") > 1, 1).otherwise(0)).alias("conflict_rows"))
        .orderBy(F.desc("rows")).collect())
print("\nPER LABEL")
print(f"  {'label':<48}{'rows':>12}{'dup_rows':>12}{'surplus':>12}{'conflict':>12}")
with open(os.path.join(outdir, "dup_by_label.csv"), "w") as f:
    f.write("label,rows,dup_rows,surplus_copies,conflict_rows\n")
    for r in per:
        print(f"  {r['Label']:<48}{r['rows']:>12,}{r['dup_rows']:>12,}{r['surplus']:>12,}{r['conflict_rows']:>12,}")
        f.write(f'"{r["Label"]}",{r["rows"]},{r["dup_rows"]},{r["surplus"]},{r["conflict_rows"]}\n')

# Which labels collide with which (one line per conflict group type)
pairs = (d.filter(F.col("ncls") > 1)
          .groupBy("h").agg(F.concat_ws(" | ", F.array_sort(F.collect_set("Label"))).alias("labels"),
                            F.count(F.lit(1)).alias("rows"))
          .groupBy("labels").agg(F.count(F.lit(1)).alias("groups"), F.sum("rows").alias("rows"))
          .orderBy(F.desc("rows")).limit(25).collect())
print("\nLABEL CONFLICTS (identical measurements, different class)")
for r in pairs:
    print(f"  {r['rows']:>10,} rows in {r['groups']:>8,} groups :  {r['labels']}")

# Copies that span different days (matters for a day-aware split)
xday = (d.filter(F.col("n") > 1).groupBy("h")
         .agg(F.countDistinct("day").alias("ndays"))
         .filter(F.col("ndays") > 1).count())
print(f"\nFingerprints that occur on more than one day: {xday:,}")

spark.stop()
