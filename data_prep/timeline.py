#!/usr/bin/env python3
"""When did each attack happen? (corrected CSE-CIC-IDS2018, all ten days)

Outputs (in <output_dir>):
  timeline_labels.csv  day, label, flows, first, last
  timeline_hours.csv   day, hour, benign, attack, attempted
and prints a short summary.

Usage:
  spark-submit --driver-memory 8g timeline.py <csv_dir> <output_dir>
"""
import os, sys
from pyspark.sql import SparkSession, functions as F

src, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
spark = (SparkSession.builder.appName("timeline")
         .config("spark.sql.session.timeZone", "UTC")   # no time-zone shifting
         .config("spark.sql.ansi.enabled", "false")      # bad formats -> null
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.read.option("header", True).option("inferSchema", False)
      .csv(os.path.join(src, "*.csv"))
      .withColumn("day", F.regexp_extract(F.input_file_name(),
                                          r"([A-Za-z]+-\d\d-\d\d-\d{4})", 1))
      .select("day", "Label", F.trim(F.col("Timestamp")).alias("ts_raw")))

print("\n=== RAW TIMESTAMP EXAMPLES ===")
for r in df.select("ts_raw").limit(3).collect():
    print("  ", r["ts_raw"])

# Try the common formats; the first that parses wins.
fmts = ["yyyy-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy hh:mm:ss a",
        "dd/MM/yyyy HH:mm", "dd/MM/yyyy hh:mm a"]
ts = F.coalesce(*[F.to_timestamp("ts_raw", f) for f in fmts])
df = df.withColumn("ts", ts)

kind = (F.when(F.col("Label") == "BENIGN", "benign")
         .when(F.col("Label").contains("Attempted"), "attempted")
         .otherwise("attack"))
df = df.withColumn("kind", kind).cache()

bad = df.filter(F.col("ts").isNull()).count()
print(f"\nUNPARSED TIMESTAMPS: {bad:,}")
if bad:
    for r in df.filter(F.col("ts").isNull()).select("ts_raw").limit(5).collect():
        print("   unparsed example:", r["ts_raw"])

# 1. first / last time of every label on every day
lab = (df.groupBy("day", "Label")
         .agg(F.count(F.lit(1)).alias("flows"),
              F.min("ts").alias("first"), F.max("ts").alias("last"))
         .orderBy("day", "first").collect())
with open(os.path.join(outdir, "timeline_labels.csv"), "w") as f:
    f.write("day,label,flows,first,last\n")
    for r in lab:
        f.write(f'{r["day"]},"{r["Label"]}",{r["flows"]},{r["first"]},{r["last"]}\n')

print("\n=== WHEN EACH LABEL OCCURS (first -> last, UTC as stored) ===")
cur = None
for r in lab:
    if r["day"] != cur:
        cur = r["day"]; print(f"\n  {cur}")
    fa = r["first"].strftime("%d/%m %H:%M") if r["first"] else "?"
    la = r["last"].strftime("%d/%m %H:%M") if r["last"] else "?"
    print(f'    {r["Label"]:<46} {r["flows"]:>10,}   {fa} -> {la}')

# 2. flows per hour, split into benign / attack / attempted
hrs = (df.withColumn("hour", F.date_trunc("hour", "ts"))
         .groupBy("day", "hour").pivot("kind", ["benign", "attack", "attempted"])
         .count().na.fill(0).orderBy("day", "hour").collect())
with open(os.path.join(outdir, "timeline_hours.csv"), "w") as f:
    f.write("day,hour,benign,attack,attempted\n")
    for r in hrs:
        f.write(f'{r["day"]},{r["hour"]},{r["benign"]},{r["attack"]},{r["attempted"]}\n')

print(f"\nWritten: {outdir}/timeline_labels.csv and timeline_hours.csv")
spark.stop()
