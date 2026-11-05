#!/usr/bin/env python3
"""Preprocessing and train/validation/test split (W4 Thursday).

Applies the decisions of 18-21 Oct to all ten daily CSVs of the corrected
CSE-CIC-IDS2018 dataset and writes Parquet files:

  (training is built afterwards by make_train.py: all attack rows + a Benign
   sample, then exact duplicates removed; done separately to fit in 16 GB RAM)
  <out>/val          validation part (natural, not deduplicated, not sampled)
  <out>/test         test part      (natural, not deduplicated, not sampled)
  <out>/attempted    '- Attempted' flows, kept aside for a separate analysis
  <out>/report/      dropped_rows.csv, split_counts.csv, cutoffs.csv, features.txt

Decisions applied
  - removed columns: identifiers (id, Flow ID, Src IP, Src Port, Dst IP, Dst Port,
    Timestamp), targets (Label, Attempted Category), constant/duplicate/unreliable
    (Bwd URG Flags, URG Flag Count, Subflow Fwd Packets, Subflow Bwd Packets)
  - removed rows: Attempted labels (kept aside), Protocol = 0, any non-finite
    feature value, timestamp earlier than the file's day
  - split: within each (day, label), ordered by time: first 70 % train,
    next 15 % validation, last 15 % test
  - training only (make_train.py): Benign sampled, then exact duplicates removed
Feature scaling is NOT done here; it is fitted on training data inside the
model pipeline (Week 5).

Usage:
  spark-submit --driver-memory 8g preprocess.py <csv_dir> <out_dir> [--benign-frac 0.10]
"""
import argparse, os, sys
from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from label_map import LABEL_MAP  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("csv_dir"); ap.add_argument("out_dir")
ap.add_argument("--benign-frac", type=float, default=0.10)
ap.add_argument("--seed", type=int, default=42)
a = ap.parse_args()
OUT = a.out_dir
os.makedirs(os.path.join(OUT, "report"), exist_ok=True)

spark = (SparkSession.builder.appName("preprocess")
         .config("spark.sql.shuffle.partitions", "400")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

DROP_COLS = {"id", "Flow ID", "Src IP", "Src Port", "Dst IP", "Dst Port", "Timestamp",
             "Label", "Attempted Category",
             "Bwd URG Flags", "URG Flag Count", "Subflow Fwd Packets", "Subflow Bwd Packets"}

raw = (spark.read.option("header", True).option("inferSchema", False)
       .csv(os.path.join(a.csv_dir, "*.csv"))
       .withColumn("day", F.regexp_extract(F.input_file_name(), r"([A-Za-z]+-\d\d-\d\d-\d{4})", 1)))
FEATS = [c for c in raw.columns if c not in DROP_COLS and c != "day"]
with open(os.path.join(OUT, "report", "features.txt"), "w") as f:
    f.write("\n".join(FEATS) + "\n")
print(f"Features kept: {len(FEATS)}")

# ---------- Stage 1: clean once, write to Parquet ----------
ts_raw = F.trim(F.col("Timestamp"))
ts = F.coalesce(F.to_timestamp(ts_raw, "yyyy-MM-dd HH:mm:ss.SSSSSS"),
                F.to_timestamp(ts_raw, "yyyy-MM-dd HH:mm:ss"))
file_date = F.to_date(F.regexp_extract("day", r"(\d\d-\d\d-\d{4})", 1), "dd-MM-yyyy")

fam_map = F.create_map(*[x for k, (fam, _) in LABEL_MAP.items() for x in (F.lit(k), F.lit(fam))])
y_map = F.create_map(*[x for k, (_, y) in LABEL_MAP.items() for x in (F.lit(k), F.lit(y))])

cols = [F.col(f"`{c}`").cast("double").alias(c) for c in FEATS]
df = raw.select(F.col("id").cast("long").alias("id"), "day", F.trim("Label").alias("Label"),
                ts.alias("ts"), file_date.alias("file_date"), *cols)

nonfinite = reduce(lambda x, y: x | y,
                   [F.col(f"`{c}`").isNull() | F.isnan(f"`{c}`") |
                    (F.abs(F.col(f"`{c}`")) == float("inf")) for c in FEATS])
reason = (F.when(F.col("Label").endswith("- Attempted"), "attempted")
           .when(F.col("Protocol") == 0, "protocol_0")
           .when(F.col("ts").isNull(), "bad_timestamp")
           .when(F.to_date("ts") < F.col("file_date"), "timestamp_before_day")
           .when(nonfinite, "non_finite_value")
           .when(fam_map[F.col("Label")].isNull(), "unknown_label"))
df = (df.withColumn("drop_reason", reason)
        .withColumn("family", F.when(F.col("Label").endswith("- Attempted"), "Attempted")
                               .otherwise(fam_map[F.col("Label")]))
        .withColumn("y", y_map[F.col("Label")].cast("int"))
        .drop("file_date"))

STAGE = os.path.join(OUT, "_stage_clean")
print("Stage 1: reading CSVs and writing cleaned Parquet (longest step) ...")
df.write.mode("overwrite").parquet(STAGE)
d = spark.read.parquet(STAGE)

# ---------- Dropped rows report ----------
print("\n=== DROPPED ROWS ===")
drops = d.groupBy("drop_reason", "Label").count().orderBy("drop_reason", F.desc("count")).collect()
with open(os.path.join(OUT, "report", "dropped_rows.csv"), "w") as f:
    f.write("reason,label,count\n")
    for r in drops:
        rs = r["drop_reason"] or "kept"
        f.write(f'{rs},"{r["Label"]}",{r["count"]}\n')
for r in d.groupBy("drop_reason").count().orderBy("drop_reason").collect():
    print(f"  {(r['drop_reason'] or 'KEPT'):<24}{r['count']:>14,}")
unknown = [r["Label"] for r in drops if r["drop_reason"] == "unknown_label"]
if unknown:
    print("  WARNING unknown labels:", sorted(set(unknown)))

d.filter(F.col("drop_reason") == "attempted").drop("drop_reason", "y") \
 .write.mode("overwrite").parquet(os.path.join(OUT, "attempted"))

keep = d.filter(F.col("drop_reason").isNull()).drop("drop_reason")

# ---------- Stage 2: time cut-offs per (day, label) ----------
keep = keep.withColumn("tsd", F.col("ts").cast("double"))
cut = (keep.groupBy("day", "Label")
           .agg(F.percentile_approx("tsd", F.array(F.lit(0.70), F.lit(0.85)), 1000000).alias("q"),
                F.count(F.lit(1)).alias("n"))
           .select("day", "Label", "n", F.col("q")[0].alias("c70"), F.col("q")[1].alias("c85")))
cut_rows = cut.orderBy("day", "Label").collect()
with open(os.path.join(OUT, "report", "cutoffs.csv"), "w") as f:
    f.write("day,label,rows,cut_70,cut_85\n")
    for r in cut_rows:
        t70, t85 = (datetime.fromtimestamp(v) for v in (r["c70"], r["c85"]))
        f.write(f'{r["day"]},"{r["Label"]}",{r["n"]},{t70},{t85}\n')

s = (keep.join(F.broadcast(cut.drop("n")), ["day", "Label"])
         .withColumn("split", F.when(F.col("tsd") <= F.col("c70"), "train")
                              .when(F.col("tsd") <= F.col("c85"), "val")
                              .otherwise("test"))
         .drop("c70", "c85", "tsd"))

META = ["id", "day", "ts", "Label", "family", "y"]
s = s.select(*META, "split", *[F.col(f"`{c}`") for c in FEATS])

print("\nStage 2: writing validation and test ...")
for part in ["val", "test"]:
    s.filter(F.col("split") == part).drop("split") \
     .write.mode("overwrite").parquet(os.path.join(OUT, part))

print("\nStage 2 finished. Next: spark-submit --driver-memory 8g make_train.py " + OUT)
spark.stop()
