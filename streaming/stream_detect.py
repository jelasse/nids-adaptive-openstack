#!/usr/bin/env python3
"""Streaming detection job: Kafka -> Spark Structured Streaming -> Full or Light model.

Every micro-batch:
  1. reads the active mode ('full' or 'light') from the mode file (default if missing),
  2. parses the CSV messages and classifies them with that frozen model and its threshold,
  3. records for every flow: event_id, send_ts, label, family, probability, prediction,
     mode, batch_id, detect_ts and latency_ms = (detect_ts - send_ts) * 1000,
     where detect_ts is the time at which the batch's predictions are available,
  4. appends one summary line per batch to batches.jsonl.
The driver also writes Spark's progress report of every batch to progress.jsonl
(input and processing rates, durations, offsets behind latest = backlog).

The mode can be changed at any time (by hand or by the controller):  echo light > MODE_FILE
It takes effect at the next micro-batch, without restarting the job.

Usage (in the VM):
  spark-submit --driver-memory 2g \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
     stream_detect.py --topic run01 --out ~/nids/runs/run01 --mode-file ~/nids/runs/run01/mode
Local test without Kafka:  --test-dir <directory of .csv files with the same lines>
"""
import argparse, json, os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

ap = argparse.ArgumentParser()
ap.add_argument("--bootstrap", default="localhost:9092")
ap.add_argument("--topic", default="flows")
ap.add_argument("--models", default=os.path.expanduser("~/nids/models"))
ap.add_argument("--features", default=os.path.expanduser("~/nids/data/report/features.txt"))
ap.add_argument("--out", required=True)
ap.add_argument("--mode-file", default="")
ap.add_argument("--default-mode", default="full", choices=["full", "light"])
ap.add_argument("--trigger", type=float, default=5.0,
                help="micro-batch interval in seconds; 0 = start the next batch as soon as the previous one ends")
ap.add_argument("--max-offsets", type=int, default=0, help="maxOffsetsPerTrigger (0 = no cap)")
ap.add_argument("--starting", default="earliest")
ap.add_argument("--stop-idle", type=int, default=6,
                help="stop when no new data has arrived for this many trigger intervals, once data has been seen (0 = never)")
ap.add_argument("--duration", type=float, default=0, help="stop after this many seconds (0 = no limit)")
ap.add_argument("--test-dir", default="", help="read CSV lines from files in this directory instead of Kafka")
a = ap.parse_args()

os.makedirs(a.out, exist_ok=True)
MODE_FILE = a.mode_file or os.path.join(a.out, "mode")
if not os.path.exists(MODE_FILE):
    open(MODE_FILE, "w").write(a.default_mode + "\n")
FEATS = [l.strip() for l in open(a.features) if l.strip()]
META = json.load(open(os.path.join(a.models, "frozen_models.json")))

spark = (SparkSession.builder.appName("stream-detect")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.streaming.metricsEnabled", "true").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

MODELS = {"full": PipelineModel.load(os.path.join(a.models, "full_rf")),
          "light": PipelineModel.load(os.path.join(a.models, "light_lr"))}
THR = {"full": META["full_rf"]["threshold"], "light": META["light_lr"]["threshold"]}
print(f"Models loaded. Thresholds: full {THR['full']:.3f}, light {THR['light']:.3f}. "
      f"Mode file: {MODE_FILE}", flush=True)

if a.test_dir:
    raw = spark.readStream.option("maxFilesPerTrigger", 1).text(a.test_dir).select(F.col("value"))
else:
    r = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", a.bootstrap)
         .option("subscribe", a.topic)
         .option("startingOffsets", a.starting)
         .option("failOnDataLoss", "false"))
    if a.max_offsets > 0:
        r = r.option("maxOffsetsPerTrigger", a.max_offsets)
    raw = r.load().select(F.col("value").cast("string").alias("value"))

arr = F.split(F.col("value"), ",")
parsed = raw.select(
    arr[0].cast("long").alias("event_id"), arr[1].cast("double").alias("send_ts"),
    arr[2].cast("int").alias("y"), arr[3].alias("family"),
    *[arr[i + 4].cast("double").alias(c) for i, c in enumerate(FEATS)])

BATCH_LOG = open(os.path.join(a.out, "batches.jsonl"), "a")
PRED_DIR = os.path.join(a.out, "predictions")


def total_behind(p):
    """Total records waiting in Kafka after a batch = sum over partitions of (latest - end offset)."""
    try:
        src = p["sources"][0]
        lat, end = (x if isinstance(x, dict) else json.loads(x) for x in (src["latestOffset"], src["endOffset"]))
        return sum(int(o) - int(end[t][part]) for t, parts in lat.items() for part, o in parts.items())
    except Exception:
        return None


def read_mode():
    try:
        m = open(MODE_FILE).read().strip().lower()
        return m if m in MODELS else a.default_mode
    except OSError:
        return a.default_mode


def detect(df, batch_id):
    t_start = time.time()
    mode = read_mode()
    pred = (MODELS[mode].transform(df)
            .select("event_id", "send_ts", "y", "family",
                    vector_to_array("probability")[1].alias("prob"))
            .withColumn("pred", (F.col("prob") >= THR[mode]).cast("int"))
            .persist())
    n = pred.count()
    t_det = time.time()
    if n == 0:
        pred.unpersist()
        return
    out = (pred.withColumn("mode", F.lit(mode)).withColumn("batch_id", F.lit(batch_id))
               .withColumn("detect_ts", F.lit(t_det))
               .withColumn("latency_ms", (F.lit(t_det) - F.col("send_ts")) * 1000.0))
    s = out.agg(
        F.sum(F.when((F.col("pred") == 1) & (F.col("y") == 1), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col("pred") == 1) & (F.col("y") == 0), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col("pred") == 0) & (F.col("y") == 1), 1).otherwise(0)).alias("fn"),
        F.expr("percentile_approx(latency_ms, array(0.5, 0.95, 0.99), 1000)").alias("q"),
        F.max("latency_ms").alias("lat_max"), F.min("send_ts").alias("first_send")).first()
    out.write.mode("append").parquet(PRED_DIR)
    t_end = time.time()
    pred.unpersist()
    rec = dict(batch_id=batch_id, mode=mode, rows=n, t_start=round(t_start, 3),
               t_detect=round(t_det, 3), t_end=round(t_end, 3),
               detect_s=round(t_det - t_start, 3), total_s=round(t_end - t_start, 3),
               lat_p50_ms=round(s["q"][0]), lat_p95_ms=round(s["q"][1]), lat_p99_ms=round(s["q"][2]),
               lat_max_ms=round(s["lat_max"]), tp=s["tp"], fp=s["fp"], fn=s["fn"])
    BATCH_LOG.write(json.dumps(rec) + "\n"); BATCH_LOG.flush()


q = (parsed.writeStream.foreachBatch(detect)
     .option("checkpointLocation", os.path.join(a.out, "checkpoint"))
     .trigger(processingTime=f"{a.trigger} seconds").start())

PROG = open(os.path.join(a.out, "progress.jsonl"), "a")
seen, started, t0 = -1, False, time.time()
last_data = t0
print("batch   rows    in/s    proc/s   dur_ms   backlog  mode", flush=True)
try:
    while q.isActive:
        time.sleep(1)
        p = q.lastProgress
        if p and p["batchId"] != seen:
            seen = p["batchId"]
            PROG.write(json.dumps(p) + "\n"); PROG.flush()
            src = p["sources"][0] if p.get("sources") else {}
            tb = total_behind(p)
            behind = "-" if tb is None else f"{tb:,}"
            rows = p.get("numInputRows", 0)
            print(f"{seen:>5} {rows:>7,} {p.get('inputRowsPerSecond', 0):>8,.0f} "
                  f"{p.get('processedRowsPerSecond', 0):>8,.0f} {p['durationMs'].get('triggerExecution', 0):>8,} "
                  f"{behind:>9} {read_mode():>5}", flush=True)
            if rows > 0:
                started, last_data = True, time.time()
        st = q.status
        if st.get("isDataAvailable") or st.get("isTriggerActive"):
            last_data = max(last_data, time.time())   # a batch is running or data is waiting: not idle
        if a.stop_idle and started and time.time() - last_data > a.stop_idle * max(a.trigger, 5.0):
            print("No new data; stopping.", flush=True); break
        if a.duration and time.time() - t0 > a.duration:
            print("Duration reached; stopping.", flush=True); break
except KeyboardInterrupt:
    print("Interrupted; stopping.", flush=True)
q.stop()
spark.stop()
