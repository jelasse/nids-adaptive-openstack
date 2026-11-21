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
It takes effect at the next micro-batch, without restarting the job. Mode file values:
  full | light | mix:<f>   (mix: each micro-batch goes to Light with probability f, else to Full, drawn
                            per batch from a generator seeded with the batch id; used by the proportional
                            controller. Mixing is per batch, not per flow: splitting one batch between the
                            two models costs both models' fixed per-batch overhead plus a second parse and
                            was slower than Full alone (about 28,000 flows/s at f = 0.9 on L))
Options added for the revision experiments:
  --light-model light_rf   use another frozen fallback model (default light_lr, the original Light model)
  --full-sample 0.1        in Light mode, a random 10% of the micro-batches are still classified by Full
                           (security mitigation; 'light' then behaves as mix:0.9)

Usage (in the VM):
  spark-submit --driver-memory 2g \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
     stream_detect.py --topic run01 --out ~/nids/runs/run01 --mode-file ~/nids/runs/run01/mode
Local test without Kafka:  --test-dir <directory of .csv files with the same lines>
"""
import argparse, json, os, random, time
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
ap.add_argument("--light-model", default="light_lr", help="frozen fallback model used as 'light' (light_lr or light_rf)")
ap.add_argument("--full-sample", type=float, default=0.0, help="share of flows still sent to Full in Light mode")
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
          "light": PipelineModel.load(os.path.join(a.models, a.light_model))}
THR = {"full": META["full_rf"]["threshold"], "light": META[a.light_model]["threshold"]}
print(f"Models loaded. Thresholds: full {THR['full']:.3f}, light ({a.light_model}) {THR['light']:.3f}; "
      f"full sample in light mode {a.full_sample:.2f}. Mode file: {MODE_FILE}", flush=True)

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
        if m in MODELS or m.startswith("mix:"):
            return m
    except OSError:
        pass
    return a.default_mode


def light_share(mode):
    """Share of the batch's flows classified by Light for a mode string."""
    if mode == "full":
        return 0.0
    if mode == "light":
        return 1.0 - a.full_sample
    try:
        return min(max(float(mode.split(":", 1)[1]), 0.0), 1.0)
    except ValueError:
        return 0.0


def score(df, which):
    return (MODELS[which].transform(df)
            .select("event_id", "send_ts", "y", "family", vector_to_array("probability")[1].alias("prob"))
            .withColumn("pred", (F.col("prob") >= THR[which]).cast("int"))
            .withColumn("mode", F.lit(which)))


def detect(df, batch_id):
    t_start = time.time()
    mode = read_mode()
    f = light_share(mode)
    if f <= 0.0:
        which = "full"
    elif f >= 1.0:
        which = "light"
    else:                                   # mixed mode: the whole batch goes to one model, drawn with prob. f
        which = "light" if random.Random(f"{a.topic}:{batch_id}").random() < f else "full"
    pred = score(df, which)
    pred = pred.persist()
    n = pred.count()
    t_det = time.time()
    if n == 0:
        pred.unpersist()
        return
    out = (pred.withColumn("batch_id", F.lit(batch_id))
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
    rec = dict(batch_id=batch_id, mode=mode, model=which, light_share=1.0 if which == "light" else 0.0,
               target_light_share=round(f, 3), rows=n, t_start=round(t_start, 3),
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
last_data, last_tb = t0, None
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
            last_tb = tb
            behind = "-" if tb is None else f"{tb:,}"
            rows = p.get("numInputRows", 0)
            print(f"{seen:>5} {rows:>7,} {p.get('inputRowsPerSecond', 0):>8,.0f} "
                  f"{p.get('processedRowsPerSecond', 0):>8,.0f} {p['durationMs'].get('triggerExecution', 0):>8,} "
                  f"{behind:>9} {read_mode():>5}", flush=True)
            if rows > 0:
                started, last_data = True, time.time()
        # Idle = data has been seen, the last batch left no backlog in Kafka, and no batch with rows
        # has completed for stop_idle x max(trigger, 5) s. (Spark's isTriggerActive flag is not used:
        # with back-to-back batches it flickers while Spark polls Kafka, so it never looks idle.)
        # A long-running batch cannot cause a false stop: the batch before it reported a backlog.
        if a.stop_idle and started and (last_tb == 0 or (a.test_dir and last_tb is None)) and time.time() - last_data > a.stop_idle * max(a.trigger, 5.0):
            print("No new data; stopping.", flush=True); break
        if a.duration and time.time() - t0 > a.duration:
            print("Duration reached; stopping.", flush=True); break
except KeyboardInterrupt:
    print("Interrupted; stopping.", flush=True)
q.stop()
spark.stop()
