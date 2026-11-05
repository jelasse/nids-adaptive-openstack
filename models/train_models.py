#!/usr/bin/env python3
"""Train, tune and freeze the Full (Random Forest) and Light (Logistic Regression)
models (Week 5). Uses ONLY the train and val splits; the test split is never read.

Stages (run in this order):
  tune_rf  : Random Forest grid on a stratified sample of the training split,
             each candidate scored on the full validation split.
  tune_lr  : Logistic Regression grid on the same sample, using the top-k features
             ranked by the best Random Forest.
  final    : refit the chosen RF and LR on the FULL training split, choose each
             decision threshold on validation, save both PipelineModels.

For every candidate the validation split is scored once; predictions are grouped into
1,000 probability bins per attack family, and all metrics are computed from those
counts: Macro-F1 at the best threshold, attack precision/recall, FPR, PR-AUC
(average precision), recall per family, training time and scoring throughput.

Usage (laptop host):
  spark-submit --driver-memory 10g train_models.py tune_rf <data_dir> <out_dir> [--sample 0.3]
  spark-submit --driver-memory 10g train_models.py tune_lr <data_dir> <out_dir> [--sample 0.3]
  spark-submit --driver-memory 10g train_models.py ablate  <data_dir> <out_dir>   (optional check)
  spark-submit --driver-memory 10g train_models.py final   <data_dir> <out_dir>
"""
import argparse, csv, json, os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.functions import vector_to_array

ap = argparse.ArgumentParser()
ap.add_argument("stage", choices=["tune_rf", "tune_lr", "ablate", "final"])
ap.add_argument("data_dir"); ap.add_argument("out_dir")
ap.add_argument("--sample", type=float, default=0.3, help="fraction of training rows used for tuning")
ap.add_argument("--seed", type=int, default=42)
ap.add_argument("--rf-trees", default="50,100"); ap.add_argument("--rf-depths", default="10,15,20")
ap.add_argument("--lr-regs", default="0,0.001,0.01"); ap.add_argument("--lr-ks", default="10,20,30")
ap.add_argument("--drop", default="Bwd Init Win Bytes,FWD Init Win Bytes", help="ablate: features to remove")
ap.add_argument("--tolerance", type=float, default=0.02, help="Light: smallest k within this Macro-F1 of the best LR")
a = ap.parse_args()
os.makedirs(a.out_dir, exist_ok=True)

spark = (SparkSession.builder.appName(f"train-{a.stage}")
         .config("spark.sql.shuffle.partitions", "64").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir(os.path.join(a.out_dir, "_ckpt"))

FEATS = [l.strip() for l in open(os.path.join(a.data_dir, "report", "features.txt")) if l.strip()]
train = spark.read.parquet(os.path.join(a.data_dir, "train"))
val = spark.read.parquet(os.path.join(a.data_dir, "val")).select("y", "family", *[F.col(f"`{c}`") for c in FEATS])
N_VAL = None
BINS = 1000


def tuning_sample():
    """Stratified by family so that small families keep all their rows."""
    fams = [r["family"] for r in train.select("family").distinct().collect()]
    fr = {f: (1.0 if f in ("Web", "Infiltration", "BruteForce", "Botnet") else a.sample) for f in fams}
    return train.sampleBy("family", fr, seed=a.seed)


def rf_pipeline(trees, depth, feats=FEATS):
    return Pipeline(stages=[
        VectorAssembler(inputCols=feats, outputCol="features"),
        RandomForestClassifier(labelCol="y", featuresCol="features", numTrees=trees, maxDepth=depth,
                               seed=a.seed, cacheNodeIds=True, checkpointInterval=10, maxMemoryInMB=512)])


def lr_pipeline(reg, feats):
    return Pipeline(stages=[
        VectorAssembler(inputCols=feats, outputCol="raw"),
        StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True),
        LogisticRegression(labelCol="y", featuresCol="features", regParam=reg, elasticNetParam=0.0,
                           maxIter=100)])


def score(model):
    """One pass over validation -> counts per (probability bin, family, y)."""
    t0 = time.time()
    p = model.transform(val).select(
        "y", "family", (F.floor(vector_to_array("probability")[1] * BINS)).cast("int").alias("b"))
    rows = p.groupBy("b", "family", "y").count().collect()
    secs = time.time() - t0
    n = sum(r["count"] for r in rows)
    return rows, n / secs, n


def metrics(rows, thr_bin=None):
    """Metrics from binned counts. A flow is 'attack' if its bin >= threshold bin."""
    pos = [0] * (BINS + 1); neg = [0] * (BINS + 1)
    for r in rows:
        b = min(max(r["b"], 0), BINS)
        (pos if r["y"] == 1 else neg)[b] += r["count"]
    P, N = sum(pos), sum(neg)
    best = None; ap_sum = 0.0; prev_rec = 0.0
    tp = fp = 0
    for b in range(BINS, -1, -1):           # lower the threshold step by step
        tp += pos[b]; fp += neg[b]
        fn, tn = P - tp, N - fp
        prec = tp / (tp + fp) if tp + fp else 1.0
        rec = tp / P if P else 0.0
        ap_sum += prec * (rec - prev_rec); prev_rec = rec
        f1a = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        precb = tn / (tn + fn) if tn + fn else 1.0
        recb = tn / N if N else 0.0
        f1b = 2 * precb * recb / (precb + recb) if precb + recb else 0.0
        m = dict(thr=b / BINS, macro_f1=(f1a + f1b) / 2, precision=prec, recall=rec,
                 fpr=fp / N if N else 0.0, fp=fp, fn=fn)
        if thr_bin is not None and b == thr_bin:
            best = m
        if thr_bin is None and (best is None or m["macro_f1"] > best["macro_f1"]):
            best = m
    best["pr_auc"] = ap_sum
    tb = round(best["thr"] * BINS)
    fam = {}
    for r in rows:
        if r["y"] == 1:
            d = fam.setdefault(r["family"], [0, 0]); d[1] += r["count"]
            if r["b"] >= tb: d[0] += r["count"]
    best["family_recall"] = {f: round(x / n, 4) for f, (x, n) in sorted(fam.items())}
    return best


def evaluate(name, pipe, data, extra):
    t0 = time.time(); model = pipe.fit(data); fit_s = time.time() - t0
    rows, rps, n = score(model)
    m = metrics(rows)
    rec = dict(name=name, **extra, fit_s=round(fit_s, 1), val_rows=n, val_rows_per_s=round(rps),
               **{k: (round(v, 5) if isinstance(v, float) else v) for k, v in m.items()})
    print(f"  {name:<28} fit {fit_s:7.1f}s  MacroF1 {m['macro_f1']:.4f}  rec {m['recall']:.4f}  "
          f"prec {m['precision']:.4f}  FPR {m['fpr']:.5f}  PR-AUC {m['pr_auc']:.4f}  thr {m['thr']:.3f}  "
          f"{rps:,.0f} rows/s", flush=True)
    return model, rec


def save_results(fname, recs):
    path = os.path.join(a.out_dir, fname)
    with open(path, "w", newline="") as f:
        keys = list(recs[0].keys())
        w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
        for r in recs:
            w.writerow({k: (json.dumps(v) if isinstance(v, dict) else v) for k, v in r.items()})
    print(f"  -> {path}")


if a.stage == "tune_rf":
    s = tuning_sample().cache()
    print(f"Tuning sample: {s.count():,} rows "
          f"({s.filter('y=1').count():,} attacks); validation rows scored per candidate")
    recs = []; best = None
    for t in [int(x) for x in a.rf_trees.split(",")]:
        for dpt in [int(x) for x in a.rf_depths.split(",")]:
            model, r = evaluate(f"RF trees={t} depth={dpt}", rf_pipeline(t, dpt), s, dict(trees=t, depth=dpt))
            recs.append(r)
            if best is None or r["macro_f1"] > best[1]["macro_f1"]:
                best = (model, r)
    save_results("tune_rf.csv", recs)
    imp = best[0].stages[-1].featureImportances.toArray()
    rank = sorted(zip(FEATS, imp), key=lambda x: -x[1])
    with open(os.path.join(a.out_dir, "feature_ranking.csv"), "w") as f:
        f.write("rank,feature,importance\n")
        for i, (c, v) in enumerate(rank, 1):
            f.write(f'{i},"{c}",{v:.6f}\n')
    json.dump({"trees": best[1]["trees"], "depth": best[1]["depth"]},
              open(os.path.join(a.out_dir, "best_rf.json"), "w"))
    print(f"\nBest RF: {best[1]['name']}  (Macro-F1 {best[1]['macro_f1']:.4f})")
    print("Top 10 features:", ", ".join(c for c, _ in rank[:10]))

elif a.stage == "tune_lr":
    rank = [r["feature"] for r in csv.DictReader(open(os.path.join(a.out_dir, "feature_ranking.csv")))]
    s = tuning_sample().cache()
    print(f"Tuning sample: {s.count():,} rows")
    recs = []
    for k in [int(x) for x in a.lr_ks.split(",")]:
        for reg in [float(x) for x in a.lr_regs.split(",")]:
            _, r = evaluate(f"LR k={k} reg={reg}", lr_pipeline(reg, rank[:k]), s, dict(k=k, reg=reg))
            recs.append(r)
    save_results("tune_lr.csv", recs)
    top = max(r["macro_f1"] for r in recs)
    ok = [r for r in recs if r["macro_f1"] >= top - a.tolerance]
    light = min(ok, key=lambda r: (r["k"], -r["macro_f1"]))
    json.dump({"k": light["k"], "reg": light["reg"]}, open(os.path.join(a.out_dir, "best_lr.json"), "w"))
    print(f"\nBest LR Macro-F1 {top:.4f}; Light choice (smallest k within {a.tolerance}): {light['name']} "
          f"Macro-F1 {light['macro_f1']:.4f}")

elif a.stage == "ablate":
    # Check whether the chosen models depend on specific features (shortcut check).
    drop = [x.strip() for x in a.drop.split(",") if x.strip()]
    rank = [r["feature"] for r in csv.DictReader(open(os.path.join(a.out_dir, "feature_ranking.csv")))]
    brf = json.load(open(os.path.join(a.out_dir, "best_rf.json")))
    blr = json.load(open(os.path.join(a.out_dir, "best_lr.json")))
    keep = [c for c in FEATS if c not in drop]
    keep_rank = [c for c in rank if c not in drop]
    s = tuning_sample().cache()
    print(f"Ablation: removing {drop}; tuning sample {s.count():,} rows")
    recs = []
    for name, pipe in [
            ("RF all features", rf_pipeline(brf["trees"], brf["depth"])),
            ("RF without dropped", rf_pipeline(brf["trees"], brf["depth"], keep)),
            (f"LR top-{blr['k']}", lr_pipeline(blr["reg"], rank[:blr["k"]])),
            (f"LR top-{blr['k']} without dropped", lr_pipeline(blr["reg"], keep_rank[:blr["k"]]))]:
        _, r = evaluate(name, pipe, s, {})
        recs.append(r)
    save_results("ablation.csv", recs)

else:  # final
    rank = [r["feature"] for r in csv.DictReader(open(os.path.join(a.out_dir, "feature_ranking.csv")))]
    brf = json.load(open(os.path.join(a.out_dir, "best_rf.json")))
    blr = json.load(open(os.path.join(a.out_dir, "best_lr.json")))
    tr = train.cache()
    print(f"Final training on the full training split: {tr.count():,} rows")
    out = {}
    for name, pipe, extra in [
            ("full_rf", rf_pipeline(brf["trees"], brf["depth"]), brf),
            ("light_lr", lr_pipeline(blr["reg"], rank[:blr["k"]]), dict(blr, features=rank[:blr["k"]]))]:
        model, r = evaluate(name, pipe, tr, {})
        model.write().overwrite().save(os.path.join(a.out_dir, "models", name))
        out[name] = dict(threshold=r["thr"], params=extra, validation=r)
    json.dump(out, open(os.path.join(a.out_dir, "models", "frozen_models.json"), "w"), indent=2)
    print(f"\nSaved models and thresholds in {os.path.join(a.out_dir, 'models')}")

spark.stop()
