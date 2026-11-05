#!/usr/bin/env python3
"""Read replayed flow records from Kafka with Spark Structured Streaming and write them to Parquet.

For each record it stores latency_ms = (time the micro-batch was read and parsed) - (send time).
Usage: spark-submit ... stream_to_parquet.py /home/ubuntu/nids-out/run1
"""
import json, sys, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

OUT = sys.argv[1] if len(sys.argv) > 1 else "/home/ubuntu/nids-out/run1"
MAX_PER_BATCH = 2000          # maxOffsetsPerTrigger: at most 2000 records per micro-batch
TRIGGER = "5 seconds"

spark = SparkSession.builder.appName("replay-to-parquet").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([StructField("event_id", LongType()),
                     StructField("send_ts", DoubleType()),
                     StructField("Label", StringType())])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "flows")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", MAX_PER_BATCH)
       .load())

parsed = (raw.select("partition", "offset",
                     from_json(col("value").cast("string"), schema).alias("r"))
             .select("partition", "offset", "r.*"))

def write_batch(df, batch_id):
    df.persist()
    n = df.count()                       # forces the Kafka read and JSON parsing
    out_ts = time.time()                 # time at which this micro-batch was read and parsed
    if n:
        (df.withColumn("batch_id", lit(batch_id))
           .withColumn("out_ts", lit(out_ts))
           .withColumn("latency_ms", (lit(out_ts) - col("send_ts")) * 1000.0)
           .write.mode("append").parquet(OUT + "/data"))
    df.unpersist()

q = (parsed.writeStream.foreachBatch(write_batch)
     .option("checkpointLocation", OUT + "/checkpoint")
     .trigger(processingTime=TRIGGER).start())

last = -1
with open(OUT + "/progress.jsonl", "a") as log:
    while q.isActive:
        time.sleep(1)
        p = q.lastProgress
        if p and p["batchId"] != last:          # print each micro-batch once
            last = p["batchId"]
            rec = {"batch": p["batchId"], "rows": p["numInputRows"],
                   "inputRowsPerSecond": p.get("inputRowsPerSecond"),
                   "processedRowsPerSecond": p.get("processedRowsPerSecond"),
                   "batchMs": p["durationMs"].get("triggerExecution"),
                   "behindLatest": p["sources"][0].get("metrics", {}).get("avgOffsetsBehindLatest")}
            print("PROGRESS", json.dumps(rec), flush=True)
            log.write(json.dumps(rec) + "\n"); log.flush()
