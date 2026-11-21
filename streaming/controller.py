#!/usr/bin/env python3
"""Resource-aware Full/Light switching controller (threshold + hysteresis).

Watches Spark's progress reports (progress.jsonl, one line per micro-batch) and
writes the mode file read by stream_detect.py before every batch.

Signal: backlog B = records waiting in Kafka after the batch (sum over partitions).
Policy:
  Full -> Light  when B > HIGH for K consecutive batches   (system is falling behind)
  Light -> Full  when B < LOW  for M consecutive batches   (backlog has drained)
  and a mode is kept for at least DWELL seconds after any switch (prevents flapping).
Every batch and every decision is logged to controller.jsonl.

--policy proportional (revision baseline): instead of switching all traffic, send only the excess to Light.
  After every batch it estimates the arrival rate lam (growth of Kafka's latest offsets over time) and the
  per-flow cost of Full and Light (EWMA of batch duration / rows, per model actually used for the batch as recorded
  in batches.jsonl, starting from --cap-full and --cap-light; learned only from saturated batches, i.e. batches of at
  least --learn-rows flows, since a small batch's duration is mostly fixed per-batch overhead), and sets the Light share f so that the processing rate covers lam plus draining the backlog over
  --horizon seconds:  f = (1/target - c_full) / (c_light - c_full),  target = lam + max(B - LOW, 0) / horizon,
  clipped to [0, 1] and rounded to steps of 0.05. The mode file then holds "mix:<f>": the detector sends each
  micro-batch to Light with probability f (whole batches; see stream_detect.py).

Usage (in the VM, normally started by run_once.sh when CONTROLLER is set):
  python controller.py --run ~/nids/runs/<name> --high 100000 --low 10000 --k 2 --m 3 --dwell 15
Stops by itself when the file STOP appears in the run directory.
"""
import argparse, json, os, time

ap = argparse.ArgumentParser()
ap.add_argument("--run", required=True)
ap.add_argument("--high", type=int, default=100000)
ap.add_argument("--low", type=int, default=10000)
ap.add_argument("--k", type=int, default=2)
ap.add_argument("--m", type=int, default=3)
ap.add_argument("--dwell", type=float, default=15.0)
ap.add_argument("--start-mode", default="full", choices=["full", "light"])
ap.add_argument("--policy", default="hysteresis", choices=["hysteresis", "proportional"])
ap.add_argument("--cap-full", type=float, default=45000.0, help="proportional: initial Full capacity, flows/s")
ap.add_argument("--cap-light", type=float, default=90000.0, help="proportional: initial Light capacity, flows/s")
ap.add_argument("--learn-rows", type=int, default=60000, help="proportional: learn costs only from batches this large")
ap.add_argument("--horizon", type=float, default=10.0, help="proportional: seconds to drain the backlog")
a = ap.parse_args()

PROG = os.path.join(a.run, "progress.jsonl")
BATCHES = os.path.join(a.run, "batches.jsonl")
MODE = os.path.join(a.run, "mode")
LOG = open(os.path.join(a.run, "controller.jsonl"), "a")
STOP = os.path.join(a.run, "STOP")


def backlog(p):
    try:
        src = p["sources"][0]
        lat, end = (x if isinstance(x, dict) else json.loads(x) for x in (src["latestOffset"], src["endOffset"]))
        return sum(int(o) - int(end[t][q]) for t, ps in lat.items() for q, o in ps.items())
    except Exception:
        return None


def latest_total(p):
    try:
        lat = p["sources"][0]["latestOffset"]
        lat = lat if isinstance(lat, dict) else json.loads(lat)
        return sum(int(o) for ps in lat.values() for o in ps.values())
    except Exception:
        return None


def iso_ts(p):
    from datetime import datetime
    try:
        return datetime.strptime(p["timestamp"][:23], "%Y-%m-%dT%H:%M:%S.%f").timestamp()
    except Exception:
        return None


def set_mode(m):
    tmp = MODE + ".tmp"
    with open(tmp, "w") as f:
        f.write(m + "\n")
    os.replace(tmp, MODE)                      # atomic: the job never reads a half-written file


mode = a.start_mode; set_mode(mode)
c_full, c_light = 1.0 / a.cap_full, 1.0 / a.cap_light       # seconds per flow (proportional policy)
f_cur, prev_lat, prev_t = 0.0, None, None
last_switch = time.time(); above = below = 0; pos = 0
bpos, model_of = 0, {}                                      # batch id -> model used (from batches.jsonl)


def read_models():
    global bpos
    if not os.path.exists(BATCHES):
        return
    with open(BATCHES) as f:
        f.seek(bpos); lines = f.readlines(); bpos = f.tell()
    for line in lines:
        if not line.endswith("\n"):
            bpos -= len(line.encode()); break
        try:
            r = json.loads(line); model_of[r["batch_id"]] = r.get("model")
        except (ValueError, KeyError):
            pass
LOG.write(json.dumps(dict(ts=round(time.time(), 3), event="start", mode=mode, high=a.high, low=a.low,
                          k=a.k, m=a.m, dwell=a.dwell)) + "\n"); LOG.flush()
while not os.path.exists(STOP):
    time.sleep(0.5)
    if not os.path.exists(PROG):
        continue
    with open(PROG) as f:
        f.seek(pos); lines = f.readlines(); pos = f.tell()
    for line in lines:
        if not line.endswith("\n"):
            pos -= len(line.encode()); break        # incomplete line: read it again next time
        p = json.loads(line)
        if p.get("numInputRows", 0) == 0:
            continue
        B = backlog(p)
        if B is None:
            continue
        if a.policy == "proportional":
            L, T = latest_total(p), iso_ts(p)
            rows, dur = p["numInputRows"], p["durationMs"].get("triggerExecution", 0) / 1000.0
            read_models()
            used = model_of.pop(p["batchId"], None)
            if used is None:                             # not logged yet: infer from a pure mode
                used = "full" if f_cur <= 0.0 else ("light" if f_cur >= 1.0 else None)
            if rows >= a.learn_rows and dur > 0:         # learn per-flow costs from saturated batches only
                if used == "full":
                    c_full = 0.7 * c_full + 0.3 * dur / rows
                elif used == "light":
                    c_light = 0.7 * c_light + 0.3 * dur / rows
            lam = None
            if L is not None and T is not None and prev_lat is not None and T > prev_t:
                lam = (L - prev_lat) / (T - prev_t)
            prev_lat, prev_t = L, T
            rec = dict(ts=round(time.time(), 3), batch=p["batchId"], backlog=B, rows=rows,
                       dur_ms=p["durationMs"].get("triggerExecution"), mode=f"mix:{f_cur:.2f}", model=used,
                       lam=None if lam is None else round(lam), c_full_us=round(c_full * 1e6, 2),
                       c_light_us=round(c_light * 1e6, 2))
            if lam is not None:
                target = max(lam, 1.0) + max(B - a.low, 0) / a.horizon
                if c_light >= c_full:
                    f = 1.0 if target * c_full > 1 else 0.0
                else:
                    f = (1.0 / target - c_full) / (c_light - c_full)
                f = round(min(max(f, 0.0), 1.0) * 20) / 20
                if f != f_cur:
                    set_mode(f"mix:{f:.2f}"); rec["switch_to"] = f"mix:{f:.2f}"; f_cur = f
            LOG.write(json.dumps(rec) + "\n"); LOG.flush()
            continue
        above = above + 1 if B > a.high else 0
        below = below + 1 if B < a.low else 0
        now = time.time(); new = mode
        if now - last_switch >= a.dwell:
            if mode == "full" and above >= a.k:
                new = "light"
            elif mode == "light" and below >= a.m:
                new = "full"
        rec = dict(ts=round(now, 3), batch=p["batchId"], backlog=B, rows=p["numInputRows"],
                   dur_ms=p["durationMs"].get("triggerExecution"), mode=mode)
        if new != mode:
            set_mode(new); rec["switch_to"] = new
            print(f"batch {p['batchId']}: backlog {B:,} -> switch {mode} -> {new}", flush=True)
            mode, last_switch, above, below = new, now, 0, 0
        LOG.write(json.dumps(rec) + "\n"); LOG.flush()
LOG.write(json.dumps(dict(ts=round(time.time(), 3), event="stop", mode=mode)) + "\n")
