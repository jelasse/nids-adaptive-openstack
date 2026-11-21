#!/usr/bin/env python3
"""Supplementary analyses requested in the pre-submission review (MC3, MC11).

Stages (run on the laptop, each writes CSV files to <out_dir>):
  split      MC3. Temporal relationship between the train/val/test splits per day, and exact and
             near-duplicate overlap of feature vectors ACROSS splits (test vs train).
  ablation   MC11. Model ablation evaluated on the test split: LR on all 78 features, a small RF on
             the top-10 features, and RF/LR without the initial TCP window features. Decision thresholds
             are chosen on validation, as for the frozen models.
  lodo       MC3. Leave-one-day-out: for every day with attacks, train RF and LR without that day and
             evaluate on that day's validation+test rows; families absent from the remaining days are
             marked 'unseen'.
  attempted  MC11. Share of the excluded 'Attempted' flows that the frozen Full and Light models flag.
  dupeval    MC3. Recall/FPR of the frozen models on test flows WITH and WITHOUT an exact duplicate in the
             training split, and the label(s) of those training duplicates (attack, benign or both).

All training uses the same stratified sample as model tuning (--sample of each large family, all rows of
the small families) to keep run times practical on the laptop; this is reported with the results.

Usage (laptop, in the Spark venv):
  source ~/spark-venv/bin/activate
  spark-submit --driver-memory 10g revision_checks.py split     ~/w4/data ~/w8/rev
  spark-submit --driver-memory 10g revision_checks.py ablation  ~/w4/data ~/w8/rev --ranking ~/w5/models/feature_ranking.csv
  spark-submit --driver-memory 10g revision_checks.py lodo      ~/w4/data ~/w8/rev
  spark-submit --driver-memory 10g revision_checks.py attempted ~/w4/data ~/w8/rev --models ~/w5/models
"""
import argparse, csv, json, os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.functions import vector_to_array

ap = argparse.ArgumentParser()
ap.add_argument("stage", choices=["split", "ablation", "lodo", "attempted", "dupeval"])
ap.add_argument("data_dir"); ap.add_argument("out_dir")
ap.add_argument("--models", default=os.path.expanduser("~/w5/models"))
ap.add_argument("--ranking", default=None, help="feature_ranking.csv of the frozen RF (default: <models>/feature_ranking.csv)")
ap.add_argument("--sample", type=float, default=0.3)
ap.add_argument("--seed", type=int, default=42)
ap.add_argument("--rf", default="50,20", help="trees,depth of the Full model")
ap.add_argument("--lr", default="10,0.01", help="k,reg of the Light model")
ap.add_argument("--window", default="Bwd Init Win Bytes,FWD Init Win Bytes", help="initial TCP window features")
a = ap.parse_args()
os.makedirs(a.out_dir, exist_ok=True)
D = os.path.expanduser(a.data_dir)

spark = (SparkSession.builder.appName(f"revision-{a.stage}")
         .config("spark.sql.shuffle.partitions", "64")
         .config("spark.sql.codegen.wholeStage", "false")      # 78-column hashes exceed Java's 64 KB method limit
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir(os.path.join(a.out_dir, "_ckpt"))
FEATS = [l.strip() for l in open(os.path.join(D, "report", "features.txt")) if l.strip()]
FC = [F.col(f"`{c}`") for c in FEATS]
BINS = 1000
RF_T, RF_D = (int(x) for x in a.rf.split(","))
LR_K, LR_REG = int(a.lr.split(",")[0]), float(a.lr.split(",")[1])
WINDOW = [x.strip() for x in a.window.split(",") if x.strip()]


def read(part, extra=()):
    return spark.read.parquet(os.path.join(D, part)).select("day", "ts", "y", "family", *extra, *FC)


def write_csv(name, rows):
    path = os.path.join(a.out_dir, name)
    with open(path, "w", newline="") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n"); w.writeheader()
            for r in rows:
                w.writerow({k: (json.dumps(v) if isinstance(v, dict) else v) for k, v in r.items()})
    print(f"  -> {path} ({len(rows)} rows)", flush=True)


def sample(df):
    fams = [r["family"] for r in df.select("family").distinct().collect()]
    fr = {f: (1.0 if f in ("Web", "Infiltration", "BruteForce", "Botnet") else a.sample) for f in fams}
    return df.sampleBy("family", fr, seed=a.seed)


def rf_pipe(trees, depth, feats):
    return Pipeline(stages=[VectorAssembler(inputCols=feats, outputCol="features"),
                            RandomForestClassifier(labelCol="y", featuresCol="features", numTrees=trees,
                                                   maxDepth=depth, seed=a.seed, cacheNodeIds=True,
                                                   checkpointInterval=10, maxMemoryInMB=512)])


def lr_pipe(reg, feats):
    return Pipeline(stages=[VectorAssembler(inputCols=feats, outputCol="raw"),
                            StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True),
                            LogisticRegression(labelCol="y", featuresCol="features", regParam=reg,
                                               elasticNetParam=0.0, maxIter=100)])


def binned(model, df):
    p = model.transform(df).select("y", "family",
                                   F.floor(vector_to_array("probability")[1] * BINS).cast("int").alias("b"))
    return p.groupBy("b", "family", "y").count().collect()


def metrics(rows, thr_bin=None):
    """Binary metrics from binned counts; threshold = best validation Macro-F1 unless given."""
    pos = [0] * (BINS + 1); neg = [0] * (BINS + 1)
    for r in rows:
        (pos if r["y"] == 1 else neg)[min(max(r["b"], 0), BINS)] += r["count"]
    P, N = sum(pos), sum(neg)
    best = None; tp = fp = 0
    for b in range(BINS, -1, -1):
        tp += pos[b]; fp += neg[b]
        fn, tn = P - tp, N - fp
        prec = tp / (tp + fp) if tp + fp else 1.0
        rec = tp / P if P else 0.0
        f1a = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        precb = tn / (tn + fn) if tn + fn else 1.0
        recb = tn / N if N else 0.0
        f1b = 2 * precb * recb / (precb + recb) if precb + recb else 0.0
        m = dict(thr_bin=b, macro_f1=(f1a + f1b) / 2, precision=prec, recall=rec, fpr=fp / N if N else 0.0)
        if thr_bin is not None and b == thr_bin:
            best = m
        if thr_bin is None and (best is None or m["macro_f1"] > best["macro_f1"]):
            best = m
    fam = {}
    for r in rows:
        if r["y"] == 1:
            d = fam.setdefault(r["family"], [0, 0]); d[1] += r["count"]
            if r["b"] >= best["thr_bin"]:
                d[0] += r["count"]
    best["family_recall"] = {f: round(x / n, 4) for f, (x, n) in sorted(fam.items())}
    best["family_n"] = {f: n for f, (x, n) in sorted(fam.items())}
    return best


def fit_eval(name, pipe, tr, va, te, extra=None):
    t0 = time.time(); model = pipe.fit(tr); fit_s = time.time() - t0
    mv = metrics(binned(model, va))                         # threshold chosen on validation
    t1 = time.time(); rows = binned(model, te); sc_s = time.time() - t1
    mt = metrics(rows, mv["thr_bin"])
    n = sum(r["count"] for r in rows)
    rec = dict(name=name, **(extra or {}), threshold=mv["thr_bin"] / BINS, fit_s=round(fit_s, 1),
               test_rows=n, test_rows_per_s=round(n / sc_s), macro_f1=round(mt["macro_f1"], 5),
               recall=round(mt["recall"], 5), precision=round(mt["precision"], 5), fpr=round(mt["fpr"], 6),
               family_recall=mt["family_recall"], family_n=mt["family_n"])
    print(f"  {name:<34} MacroF1 {rec['macro_f1']:.4f} rec {rec['recall']:.4f} FPR {rec['fpr']:.6f} "
          f"fam {rec['family_recall']}", flush=True)
    return rec


def ranking_path():
    if a.ranking:
        return os.path.expanduser(a.ranking)
    M = os.path.expanduser(a.models)
    for p in (os.path.join(M, "feature_ranking.csv"), os.path.join(os.path.dirname(M), "feature_ranking.csv")):
        if os.path.exists(p):
            return p
    raise SystemExit("feature_ranking.csv not found: give it with --ranking")


def ranking():
    return [r["feature"] for r in csv.DictReader(open(ranking_path()))]


# ------------------------------------------------------------------ split checks
if a.stage == "split":
    parts = {p: read(p).withColumn("split", F.lit(p)) for p in ("train", "val", "test")}
    allp = parts["train"].unionByName(parts["val"]).unionByName(parts["test"]).withColumn("tsd", F.col("ts").cast("timestamp").cast("double"))
    # 1. time ranges per day and split, and overlap with the training period of the same day
    rng = (allp.groupBy("day", "split").agg(F.min("tsd").alias("t_min"), F.max("tsd").alias("t_max"),
                                             F.count(F.lit(1)).alias("rows"),
                                             F.sum("y").alias("attacks")).collect())
    tr_max = {r["day"]: r["t_max"] for r in rng if r["split"] == "train"}
    ov = (allp.filter("split = 'test'").join(spark.createDataFrame(list(tr_max.items()), ["day", "train_max"]), "day")
               .groupBy("day", "y").agg(F.count(F.lit(1)).alias("rows"),
                                        F.sum((F.col("tsd") < F.col("train_max")).cast("int")).alias("before_train_end"))
               .collect())
    out = [dict(day=r["day"], split=r["split"], rows=r["rows"], attacks=r["attacks"],
                start=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(r["t_min"])),
                end=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(r["t_max"]))) for r in sorted(rng, key=lambda r: (r["day"], r["split"]))]
    write_csv("split_time_ranges.csv", out)
    write_csv("split_test_before_train_end.csv",
              [dict(day=r["day"], y=r["y"], test_rows=r["rows"], test_rows_before_last_train_row=r["before_train_end"],
                    share=round(r["before_train_end"] / r["rows"], 5)) for r in sorted(ov, key=lambda r: (r["day"], r["y"]))])
    # 2. exact and near duplicates of test feature vectors in the training split
    def sig3(c):   # round to 3 significant digits
        x = F.col(f"`{c}`")
        e = F.floor(F.log10(F.abs(x))) - 2
        return F.when(x == 0, F.lit(0.0)).otherwise(F.round(x / F.pow(F.lit(10.0), e)) * F.pow(F.lit(10.0), e))
    def hashes(df):
        return (df.withColumn("h_exact", F.sha2(F.concat_ws("|", *[F.col(f"`{c}`").cast("string") for c in FEATS]), 256))
                  .withColumn("h_near", F.sha2(F.concat_ws("|", *[sig3(c).cast("string") for c in FEATS]), 256)))
    tr = hashes(parts["train"]).select("h_exact", "h_near")
    te = hashes(parts["test"]).select("y", "family", "h_exact", "h_near")
    ex = tr.select("h_exact").distinct().withColumn("in_train_exact", F.lit(1))
    nr = tr.select("h_near").distinct().withColumn("in_train_near", F.lit(1))
    j = te.join(ex, "h_exact", "left").join(nr, "h_near", "left").fillna(0, ["in_train_exact", "in_train_near"])
    res = (j.groupBy("family").agg(F.count(F.lit(1)).alias("test_rows"),
                                   F.sum("in_train_exact").alias("exact_in_train"),
                                   F.sum("in_train_near").alias("near_in_train")).collect())
    write_csv("split_duplicates_test_vs_train.csv",
              [dict(family=r["family"], test_rows=r["test_rows"], exact_in_train=r["exact_in_train"],
                    exact_share=round(r["exact_in_train"] / r["test_rows"], 5), near_in_train=r["near_in_train"],
                    near_share=round(r["near_in_train"] / r["test_rows"], 5)) for r in sorted(res, key=lambda r: r["family"])])

# ------------------------------------------------------------------ ablation on the test split
elif a.stage == "ablation":
    rank = ranking()
    tr = sample(read("train")).cache(); va = read("val"); te = read("test")
    print(f"Ablation: training sample {tr.count():,} rows", flush=True)
    no_win = [c for c in FEATS if c not in WINDOW]
    rank_nw = [c for c in rank if c not in WINDOW]
    variants = [
        ("RF 50x20, 78 features (as Full)", rf_pipe(RF_T, RF_D, FEATS)),
        (f"LR top-{LR_K} (as Light)", lr_pipe(LR_REG, rank[:LR_K])),
        ("LR, all 78 features", lr_pipe(LR_REG, FEATS)),
        (f"RF 20x10, top-{LR_K} features", rf_pipe(20, 10, rank[:LR_K])),
        ("RF 50x20, without TCP window", rf_pipe(RF_T, RF_D, no_win)),
        (f"LR top-{LR_K}, without TCP window", lr_pipe(LR_REG, rank_nw[:LR_K])),
    ]
    recs = [fit_eval(n, p, tr, va, te) for n, p in variants]
    write_csv("ablation_test.csv", recs)
    write_csv("light_features.csv", [dict(rank=i + 1, feature=c) for i, c in enumerate(rank[:LR_K])])

# ------------------------------------------------------------------ leave-one-day-out
elif a.stage == "lodo":
    rank = ranking()
    train, val, test = read("train"), read("val"), read("test")
    days = [r["day"] for r in train.filter("y = 1").select("day").distinct().orderBy("day").collect()]
    fam_day = {}
    for r in train.filter("y = 1").groupBy("day", "family").count().collect():
        fam_day.setdefault(r["family"], set()).add(r["day"])
    recs = []
    for d in days:
        print(f"Held-out day {d}", flush=True)
        tr = sample(train.filter(F.col("day") != d)).cache()
        va = val.filter(F.col("day") != d)
        te = val.filter(F.col("day") == d).unionByName(test.filter(F.col("day") == d))
        for n, p in [("RF 50x20, 78 features", rf_pipe(RF_T, RF_D, FEATS)),
                     (f"LR top-{LR_K}", lr_pipe(LR_REG, rank[:LR_K]))]:
            r = fit_eval(n, p, tr, va, te, dict(held_out_day=d))
            r["unseen_families"] = sorted(f for f in r["family_recall"] if fam_day.get(f, set()) <= {d})
            recs.append(r)
        tr.unpersist()
    write_csv("lodo.csv", recs)

# ------------------------------------------------------------------ Attempted flows
elif a.stage == "attempted":
    M = os.path.expanduser(a.models)
    if not os.path.exists(os.path.join(M, "frozen_models.json")) and os.path.exists(os.path.join(M, "models", "frozen_models.json")):
        M = os.path.join(M, "models")
    meta = json.load(open(os.path.join(M, "frozen_models.json")))
    att = spark.read.parquet(os.path.join(D, "attempted")).select("Label", *FC)
    recs = []
    for sub in ("full_rf", "light_lr"):
        t = float(meta[sub]["threshold"])
        model = PipelineModel.load(os.path.join(M, sub))
        p = model.transform(att).select("Label", (vector_to_array("probability")[1] >= t).cast("int").alias("flag"))
        for r in p.groupBy("Label").agg(F.count(F.lit(1)).alias("rows"), F.sum("flag").alias("flagged")).collect():
            recs.append(dict(model=sub, threshold=t, label=r["Label"], rows=r["rows"], flagged=r["flagged"],
                             share_flagged=round(r["flagged"] / r["rows"], 4)))
    write_csv("attempted_flagged.csv", sorted(recs, key=lambda r: (r["model"], r["label"])))
# ------------------------------------------------------------------ frozen models on duplicate vs new test flows
elif a.stage == "dupeval":
    M = os.path.expanduser(a.models)
    if not os.path.exists(os.path.join(M, "frozen_models.json")) and os.path.exists(os.path.join(M, "models", "frozen_models.json")):
        M = os.path.join(M, "models")
    meta = json.load(open(os.path.join(M, "frozen_models.json")))
    h = F.sha2(F.concat_ws("|", *[F.col(f"`{c}`").cast("string") for c in FEATS]), 256)
    tr = (read("train").withColumn("h", h).groupBy("h")
          .agg(F.max("y").alias("tr_attack"), F.max(1 - F.col("y")).alias("tr_benign")))
    te = read("test").withColumn("h", h).join(tr, "h", "left")
    te = te.withColumn("dup", F.when(F.col("tr_attack").isNull(), "new")
                               .when((F.col("tr_attack") == 1) & (F.col("tr_benign") == 1), "dup_both_labels")
                               .when(F.col("tr_attack") == 1, "dup_attack_label")
                               .otherwise("dup_benign_label"))
    te = te.drop("h", "tr_attack", "tr_benign")          # not cached: the full test split does not fit in memory
    recs = []
    for sub in ("full_rf", "light_lr"):
        t = float(meta[sub]["threshold"])
        model = PipelineModel.load(os.path.join(M, sub))
        p = model.transform(te).select("family", "y", "dup", (vector_to_array("probability")[1] >= t).cast("int").alias("pred"))
        for r in p.groupBy("family", "y", "dup").agg(F.count(F.lit(1)).alias("rows"), F.sum("pred").alias("flagged")).collect():
            recs.append(dict(model=sub, family=r["family"], y=r["y"], dup=r["dup"], rows=r["rows"], flagged=r["flagged"],
                             share_flagged=round(r["flagged"] / r["rows"], 5)))
    write_csv("dup_eval.csv", sorted(recs, key=lambda r: (r["model"], r["family"], r["dup"])))
print("done", a.stage)
