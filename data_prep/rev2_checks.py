#!/usr/bin/env python3
"""Checks requested in the SECOND review (items 2 and 3). Same data, seed and helpers as revision_checks.py.

Stages (run on the laptop, each writes CSV files to <out_dir>):
  lodo3     Item 2. Leave-one-day-out for the THREE frozen configurations: Full (RF 50x20, 78 features),
            Light (LR top-10) and the security-aware fallback (RF 10x10 top-20). The fallback is fitted twice:
            on the top-20 of the global ranking (exactly the deployed feature set) and on the top-20 of the
            ranking of the Full model trained in the same fold (no information from the held-out day).
            -> lodo3.csv
  valdup    Item 3a. Exact and near duplicates of validation flows in the training split, per family
            (-> val_duplicates_vs_train.csv). Then the fallback selection of freeze_fallback.py is repeated
            with the same 18 candidates, scored once on the full validation split (as in the paper) and once
            on validation flows WITHOUT an exact duplicate in training; the throughput of each candidate is
            taken from the original fallback_candidates.csv, so only the validation set differs.
            -> selection_dedup.csv (one row per candidate and validation set, chosen = 1 for the winner)
  dupeval3  Item 3b. Frozen full_rf, light_lr and light_rf on test flows with and without an exact duplicate
            in training (benign rows give the FPR, attack rows the recall).  -> dup_eval3.csv
  dedup     Item 3c. The fallback configuration refitted on a training split from which every flow whose
            features also occur in validation or test was removed; threshold on validation; evaluated on
            the whole test split next to the frozen light_rf.  -> dedup_retrain.csv
            (--with-full also refits the Full configuration; slower.)

Usage (laptop, in the Spark venv):
  source ~/spark-venv/bin/activate
  spark-submit --driver-memory 10g rev2_checks.py lodo3    ~/w4/data ~/w8/rev2
  spark-submit --driver-memory 10g rev2_checks.py valdup   ~/w4/data ~/w8/rev2 --candidates ~/w8/rev/fallback_candidates.csv
  spark-submit --driver-memory 10g rev2_checks.py dupeval3 ~/w4/data ~/w8/rev2
  spark-submit --driver-memory 10g rev2_checks.py dedup    ~/w4/data ~/w8/rev2
"""
import argparse, csv, json, os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.functions import vector_to_array

ap = argparse.ArgumentParser()
ap.add_argument("stage", choices=["lodo3", "valdup", "dupeval3", "dedup"])
ap.add_argument("data_dir"); ap.add_argument("out_dir")
ap.add_argument("--models", default=os.path.expanduser("~/w5/models"))
ap.add_argument("--ranking", default=None, help="feature_ranking.csv of the frozen RF (default: <models>/feature_ranking.csv)")
ap.add_argument("--sample", type=float, default=0.3)
ap.add_argument("--seed", type=int, default=42)
ap.add_argument("--rf", default="50,20", help="trees,depth of the Full model")
ap.add_argument("--lr", default="10,0.01", help="k,reg of the Light model")
ap.add_argument("--fb", default="10,10,20", help="trees,depth,k of the fallback")
ap.add_argument("--candidates", default=os.path.expanduser("~/w8/rev/fallback_candidates.csv"))
ap.add_argument("--with-full", action="store_true")
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
FB_T, FB_D, FB_K = (int(x) for x in a.fb.split(","))
WINDOW = [x.strip() for x in a.window.split(",") if x.strip()]


def read(part, extra=()):
    return spark.read.parquet(os.path.join(D, part)).select("day", "ts", "y", "family", *extra, *FC)


def write_csv(name, rows):
    path = os.path.join(a.out_dir, name)
    with open(path, "w", newline="") as f:
        if rows:
            keys = []
            for r in rows:
                keys += [k for k in r if k not in keys]
            w = csv.DictWriter(f, fieldnames=keys, lineterminator="\n", restval=""); w.writeheader()
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


def models_dir():
    M = os.path.expanduser(a.models)
    if not os.path.exists(os.path.join(M, "frozen_models.json")) and os.path.exists(os.path.join(M, "models", "frozen_models.json")):
        M = os.path.join(M, "models")
    return M


def sig3(c):   # round to 3 significant digits (near duplicates, as in revision_checks.py split)
    x = F.col(f"`{c}`")
    e = F.floor(F.log10(F.abs(x))) - 2
    return F.when(x == 0, F.lit(0.0)).otherwise(F.round(x / F.pow(F.lit(10.0), e)) * F.pow(F.lit(10.0), e))


H_EXACT = F.sha2(F.concat_ws("|", *[F.col(f"`{c}`").cast("string") for c in FEATS]), 256)
H_NEAR = F.sha2(F.concat_ws("|", *[sig3(c).cast("string") for c in FEATS]), 256)


def fit_eval_m(name, pipe, tr, va, te, extra=None):
    """fit_eval that also returns the fitted model."""
    t0 = time.time(); model = pipe.fit(tr); fit_s = time.time() - t0
    mv = metrics(binned(model, va))
    rows = binned(model, te); mt = metrics(rows, mv["thr_bin"])
    rec = dict(name=name, **(extra or {}), threshold=mv["thr_bin"] / BINS, fit_s=round(fit_s, 1),
               test_rows=sum(r["count"] for r in rows), macro_f1=round(mt["macro_f1"], 5),
               recall=round(mt["recall"], 5), precision=round(mt["precision"], 5), fpr=round(mt["fpr"], 6),
               false_pos=None, family_recall=mt["family_recall"], family_n=mt["family_n"])
    neg = sum(r["count"] for r in rows if r["y"] == 0)
    rec["false_pos"] = round(mt["fpr"] * neg)
    print(f"  {name:<44} MacroF1 {rec['macro_f1']:.4f} rec {rec['recall']:.4f} FP {rec['false_pos']:,} "
          f"fam {rec['family_recall']}", flush=True)
    return rec, model


def eval_fixed(name, model, thr, te, extra=None):
    rows = binned(model, te); mt = metrics(rows, int(round(thr * BINS)))
    neg = sum(r["count"] for r in rows if r["y"] == 0)
    rec = dict(name=name, **(extra or {}), threshold=thr, fit_s="", test_rows=sum(r["count"] for r in rows),
               macro_f1=round(mt["macro_f1"], 5), recall=round(mt["recall"], 5), precision=round(mt["precision"], 5),
               fpr=round(mt["fpr"], 6), false_pos=round(mt["fpr"] * neg),
               family_recall=mt["family_recall"], family_n=mt["family_n"])
    print(f"  {name:<44} MacroF1 {rec['macro_f1']:.4f} rec {rec['recall']:.4f} FP {rec['false_pos']:,} "
          f"fam {rec['family_recall']}", flush=True)
    return rec


# ------------------------------------------------------------------ item 2: leave-one-day-out, three models
if a.stage == "lodo3":
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
        ex = dict(held_out_day=d)
        out = []
        r, full = fit_eval_m(f"Full: RF {RF_T}x{RF_D}, 78 features", rf_pipe(RF_T, RF_D, FEATS), tr, va, te, ex); out.append(r)
        r, _ = fit_eval_m(f"Light: LR top-{LR_K}", lr_pipe(LR_REG, rank[:LR_K]), tr, va, te, ex); out.append(r)
        r, _ = fit_eval_m(f"Fallback: RF {FB_T}x{FB_D} top-{FB_K} (global ranking)",
                          rf_pipe(FB_T, FB_D, rank[:FB_K]), tr, va, te, ex); out.append(r)
        imp = full.stages[-1].featureImportances.toArray()
        fold_rank = [c for _, c in sorted(zip(imp, FEATS), key=lambda t: -t[0])]
        r, _ = fit_eval_m(f"Fallback: RF {FB_T}x{FB_D} top-{FB_K} (fold ranking)",
                          rf_pipe(FB_T, FB_D, fold_rank[:FB_K]), tr, va, te,
                          dict(ex, overlap_with_global_top=len(set(fold_rank[:FB_K]) & set(rank[:FB_K])))); out.append(r)
        for r in out:
            r["unseen_families"] = sorted(f for f in r["family_recall"] if fam_day.get(f, set()) <= {d})
        recs += out
        tr.unpersist()
        write_csv("lodo3.csv", recs)          # rewritten after every day, so a crash keeps the finished days

# ------------------------------------------------------------------ item 3a: train-validation duplicates and selection
elif a.stage == "valdup":
    tr = read("train").select(H_EXACT.alias("h_exact"), H_NEAR.alias("h_near"))
    ex = tr.select("h_exact").distinct().withColumn("in_train_exact", F.lit(1))
    nr = tr.select("h_near").distinct().withColumn("in_train_near", F.lit(1))
    va = (read("val").withColumn("h_exact", H_EXACT).withColumn("h_near", H_NEAR)
          .join(ex, "h_exact", "left").join(nr, "h_near", "left").fillna(0, ["in_train_exact", "in_train_near"])
          .withColumnRenamed("in_train_exact", "dup").drop("h_exact", "h_near"))
    vpath = os.path.join(a.out_dir, "_val_flagged")
    va.write.mode("overwrite").parquet(vpath)
    va = spark.read.parquet(vpath)
    res = (va.groupBy("family").agg(F.count(F.lit(1)).alias("val_rows"), F.sum("dup").alias("exact_in_train"),
                                    F.sum("in_train_near").alias("near_in_train")).collect())
    write_csv("val_duplicates_vs_train.csv",
              [dict(family=r["family"], val_rows=r["val_rows"], exact_in_train=r["exact_in_train"],
                    exact_share=round(r["exact_in_train"] / r["val_rows"], 5), near_in_train=r["near_in_train"],
                    near_share=round(r["near_in_train"] / r["val_rows"], 5)) for r in sorted(res, key=lambda r: r["family"])])
    rank = ranking()
    # throughput of each candidate, from the original selection (so only the validation set changes)
    speed, budget = {}, 4.0
    if os.path.exists(a.candidates):
        for r in csv.DictReader(open(a.candidates)):
            speed[(int(r["trees"]), int(r["depth"]), int(r["k"]))] = float(r["speed_vs_full"])
        print(f"Throughput of the candidates from {a.candidates}", flush=True)
    else:
        print(f"{a.candidates} not found: throughput is measured again", flush=True)
        full = PipelineModel.load(os.path.join(models_dir(), "full_rf"))
        vv = va.select("y", "family", *FC)
        binned(full, vv); t0 = time.time(); binned(full, vv); full_rps = vv.count() / (time.time() - t0)
    tr_s = read("train").select("y", "family", *FC)
    fams = [r["family"] for r in tr_s.select("family").distinct().collect()]
    samp = tr_s.sampleBy("family", {f: (1.0 if f in ("Web", "Infiltration", "BruteForce", "Botnet") else a.sample) for f in fams},
                         seed=a.seed).cache()
    recs = []
    for k in (10, 20):
        for t in (10, 20, 30):
            for d in (6, 8, 10):
                m = rf_pipe(t, d, rank[:k]).fit(samp)
                t0 = time.time()
                rows = (m.transform(va).select("y", "family", "dup",
                                               F.floor(vector_to_array("probability")[1] * BINS).cast("int").alias("b"))
                        .groupBy("b", "family", "y", "dup").count().collect())
                el = time.time() - t0
                sp = speed.get((t, d, k))
                if sp is None:
                    sp = round(sum(r["count"] for r in rows) / el / full_rps, 2)
                for vset, sel in (("all", lambda r: True), ("without_train_duplicates", lambda r: r["dup"] == 0)):
                    agg = {}
                    for r in rows:
                        if sel(r):
                            key = (r["b"], r["family"], r["y"]); agg[key] = agg.get(key, 0) + r["count"]
                    mm = metrics([dict(b=b, family=f, y=y, count=c) for (b, f, y), c in agg.items()])
                    recs.append(dict(validation=vset, trees=t, depth=d, k=k, speed_vs_full=sp,
                                     macro_f1=round(mm["macro_f1"], 5), recall=round(mm["recall"], 5), fpr=round(mm["fpr"], 6),
                                     threshold=mm["thr_bin"] / BINS, worst_family_recall=min(mm["family_recall"].values()),
                                     family_recall=mm["family_recall"], family_n=mm["family_n"], chosen=0))
                print(f"  RF {t}x{d} top-{k}: {sp}x Full | all: worst {recs[-2]['worst_family_recall']:.3f} "
                      f"F1 {recs[-2]['macro_f1']:.4f} | without dups: worst {recs[-1]['worst_family_recall']:.3f} "
                      f"F1 {recs[-1]['macro_f1']:.4f} {recs[-1]['family_recall']}", flush=True)
    samp.unpersist()
    winners = {}
    for vset in ("all", "without_train_duplicates"):
        ok = [r for r in recs if r["validation"] == vset and r["speed_vs_full"] >= budget]
        w = max(ok, key=lambda r: (r["worst_family_recall"], r["macro_f1"]))
        w["chosen"] = 1; winners[vset] = (w["trees"], w["depth"], w["k"])
        print(f"Chosen on validation '{vset}': RF {w['trees']}x{w['depth']} top-{w['k']}", flush=True)
    write_csv("selection_dedup.csv", recs)
    if winners["all"] != winners["without_train_duplicates"]:
        # the choice depends on the duplicates: compare the two winners on the test split
        print("The choice changes: both winners are refitted on the training split and evaluated on test", flush=True)
        te = read("test"); vnew = va.filter("dup = 0")
        out = []
        for vset, (t, d, k) in winners.items():
            r, _ = fit_eval_m(f"RF {t}x{d} top-{k} (chosen on validation '{vset}')", rf_pipe(t, d, rank[:k]),
                              read("train"), vnew, te)
            out.append(r)
        write_csv("selection_dedup_test.csv", out)
    else:
        print("The same candidate is chosen with and without the duplicated validation flows", flush=True)

# ------------------------------------------------------------------ item 3b: frozen models on new vs duplicated test flows
elif a.stage == "dupeval3":
    M = models_dir()
    meta = json.load(open(os.path.join(M, "frozen_models.json")))
    tr = (read("train").withColumn("h", H_EXACT).groupBy("h")
          .agg(F.max("y").alias("tr_attack"), F.max(1 - F.col("y")).alias("tr_benign")))
    te = read("test").withColumn("h", H_EXACT).join(tr, "h", "left")
    te = te.withColumn("dup", F.when(F.col("tr_attack").isNull(), "new")
                               .when((F.col("tr_attack") == 1) & (F.col("tr_benign") == 1), "dup_both_labels")
                               .when(F.col("tr_attack") == 1, "dup_attack_label")
                               .otherwise("dup_benign_label")).drop("h", "tr_attack", "tr_benign")
    tpath = os.path.join(a.out_dir, "_test_flagged")
    te.write.mode("overwrite").parquet(tpath)
    te = spark.read.parquet(tpath)
    recs = []
    for sub in ("full_rf", "light_lr", "light_rf"):
        t = float(meta[sub]["threshold"])
        model = PipelineModel.load(os.path.join(M, sub))
        p = model.transform(te).select("family", "y", "dup", (vector_to_array("probability")[1] >= t).cast("int").alias("pred"))
        for r in p.groupBy("family", "y", "dup").agg(F.count(F.lit(1)).alias("rows"), F.sum("pred").alias("flagged")).collect():
            recs.append(dict(model=sub, family=r["family"], y=r["y"], dup=r["dup"], rows=r["rows"], flagged=r["flagged"],
                             share_flagged=round(r["flagged"] / r["rows"], 5)))
        print(f"  {sub} done", flush=True)
    write_csv("dup_eval3.csv", sorted(recs, key=lambda r: (r["model"], r["family"], r["dup"])))

# ------------------------------------------------------------------ item 3c: refit without any flow shared with val/test
elif a.stage == "dedup":
    M = models_dir()
    meta = json.load(open(os.path.join(M, "frozen_models.json")))
    rank = ranking()
    held = (read("val").select(H_EXACT.alias("h")).unionByName(read("test").select(H_EXACT.alias("h"))).distinct())
    tr = read("train").withColumn("h", H_EXACT)
    kept = tr.join(held, "h", "left_anti").drop("h")
    kpath = os.path.join(a.out_dir, "_train_dedup")
    kept.write.mode("overwrite").parquet(kpath)
    kept = spark.read.parquet(kpath)
    before = {r["family"]: r["count"] for r in read("train").groupBy("family").count().collect()}
    after = {r["family"]: r["count"] for r in kept.groupBy("family").count().collect()}
    write_csv("dedup_train_rows.csv", [dict(family=f, train_rows=before[f], kept=after.get(f, 0),
                                            removed=before[f] - after.get(f, 0),
                                            removed_share=round(1 - after.get(f, 0) / before[f], 5)) for f in sorted(before)])
    va, te = read("val"), read("test")
    recs = [eval_fixed("Frozen fallback (light_rf), frozen threshold", PipelineModel.load(os.path.join(M, "light_rf")),
                       float(meta["light_rf"]["threshold"]), te)]
    r, _ = fit_eval_m(f"Fallback RF {FB_T}x{FB_D} top-{FB_K}, refitted without shared flows",
                      rf_pipe(FB_T, FB_D, rank[:FB_K]), kept, va, te); recs.append(r)
    write_csv("dedup_retrain.csv", recs)
    if a.with_full:
        recs.append(eval_fixed("Frozen Full (full_rf), frozen threshold", PipelineModel.load(os.path.join(M, "full_rf")),
                               float(meta["full_rf"]["threshold"]), te))
        r, _ = fit_eval_m(f"Full RF {RF_T}x{RF_D}, refitted without shared flows", rf_pipe(RF_T, RF_D, FEATS), kept, va, te)
        recs.append(r)
        write_csv("dedup_retrain.csv", recs)
print("done", a.stage)
