#!/usr/bin/env python3
"""Check a run's Parquet output: completeness, duplicates, partition spread and latency percentiles.
Usage: spark-submit check_output.py /home/ubuntu/nids-out/run1
"""
import sys
from pyspark.sql import SparkSession, functions as F

OUT = sys.argv[1] if len(sys.argv) > 1 else "/home/ubuntu/nids-out/run1"
spark = SparkSession.builder.appName("check-output").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.parquet(OUT + "/data")

s = df.agg(F.count("*").alias("rows"), F.countDistinct("event_id").alias("unique_ids"),
           F.min("event_id").alias("min_id"), F.max("event_id").alias("max_id")).first()
print(f"\nRUN {OUT}")
print(f"rows={s.rows}  unique_ids={s.unique_ids}  duplicates={s.rows - s.unique_ids}  "
      f"missing_in_id_range={s.max_id - s.min_id + 1 - s.unique_ids}  ids={s.min_id}..{s.max_id}")

print("\nRecords per Kafka partition:")
df.groupBy("partition").count().orderBy("partition").show()

print("Latency (ms):")
df.select(F.expr("percentile_approx(latency_ms, 0.50)").alias("p50"),
          F.expr("percentile_approx(latency_ms, 0.95)").alias("p95"),
          F.expr("percentile_approx(latency_ms, 0.99)").alias("p99"),
          F.round(F.mean("latency_ms"), 1).alias("mean"),
          F.round(F.max("latency_ms"), 1).alias("max")).show()

print("Records and mean latency per micro-batch:")
(df.groupBy("batch_id").agg(F.count("*").alias("rows"), F.round(F.mean("latency_ms"), 0).alias("mean_latency_ms"))
   .orderBy("batch_id").show(50))
