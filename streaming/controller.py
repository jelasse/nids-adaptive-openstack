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
a = ap.parse_args()

PROG = os.path.join(a.run, "progress.jsonl")
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


def set_mode(m):
    tmp = MODE + ".tmp"
    with open(tmp, "w") as f:
        f.write(m + "\n")
    os.replace(tmp, MODE)                      # atomic: the job never reads a half-written file


mode = a.start_mode; set_mode(mode)
last_switch = time.time(); above = below = 0; pos = 0
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
