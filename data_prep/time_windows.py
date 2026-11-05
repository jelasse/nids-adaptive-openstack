#!/usr/bin/env python3
"""Wednesday checks (leakage review and split design), all ten daily CSVs.

1. Timestamp format and parse success (needed for a time-ordered split).
2. When each label occurs on each day: first and last flow, count.
3. Protocol = 0 rows per label.
4. Whether 'Fwd URG Flags' and 'URG Flag Count' are identical on every row.

Usage:
  spark-submit --driver-memory 8g time_windows.py <csv_dir> <output_dir>
"""
import os, sys
from pyspark.sql import SparkSession, functions as F

src, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
spark = (SparkSession.builder.appName("time-windows")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.read.option("header", True).option("inferSchema", False)
      .csv(os.path.join(src, "*.csv"))
      .withColumn("day", F.regexp_extract(F.input_file_name(), r"([A-Za-z]+-\d\d-\d\d-\d{4})", 1)))

print("\n=== SAMPLE RAW TIMESTAMPS ===")
for r in df.groupBy("day").agg(F.first("Timestamp").alias("ts")).orderBy("day").collect():
    print(f"  {r['day']:<24} {r['ts']}")

ts_raw = F.trim(F.col("Timestamp"))
FORMATS = ["yyyy-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss",
           "dd/MM/yyyy hh:mm:ss a", "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm"]
ts = F.coalesce(*[F.to_timestamp(ts_raw, f) for f in FORMATS])
d = df.select("day", "Label", ts.alias("ts"),
              F.col("Protocol").cast("int").alias("proto"),
              F.col("`Fwd URG Flags`").cast("double").alias("fu"),
              F.col("`URG Flag Count`").cast("double").alias("uc")).cache()

bad = d.filter(F.col("ts").isNull()).count()
print(f"\nTimestamps that could not be parsed: {bad:,}")

print("\n=== WHEN EACH LABEL OCCURS (first .. last flow) ===")
rows = (d.groupBy("day", "Label")
         .agg(F.count(F.lit(1)).alias("n"), F.min("ts").alias("first"), F.max("ts").alias("last"),
              F.sum(F.when(F.col("proto") == 0, 1).otherwise(0)).alias("proto0"))
         .orderBy("day", "first").collect())
with open(os.path.join(outdir, "label_time_windows.csv"), "w") as f:
    f.write("day,label,count,first,last,protocol0_rows\n")
    cur = None
    for r in rows:
        if r["day"] != cur:
            cur = r["day"]; print(f"  {cur}")
        print(f"    {r['Label']:<46}{r['n']:>11,}   {str(r['first'])[:19]} .. {str(r['last'])[:19]}   proto0={r['proto0']:,}")
        f.write(f'{r["day"]},"{r["Label"]}",{r["n"]},{r["first"]},{r["last"]},{r["proto0"]}\n')

print("\n=== PROTOCOL VALUES (all days) ===")
for r in d.groupBy("proto").count().orderBy("proto").collect():
    print(f"  protocol={r['proto']}  rows={r['count']:,}")

diff = d.filter(F.col("fu") != F.col("uc")).count()
print(f"\nRows where 'Fwd URG Flags' != 'URG Flag Count': {diff:,}")

spark.stop()
