#!/usr/bin/env python3
"""Replay flows from a replay Parquet file into Kafka at a controlled offered rate.

Each message is one CSV line:  event_id,send_ts,y,family,<78 feature values>
(the features in the order of features.txt; numbers written exactly).
The key is the event ID, so messages spread evenly over the topic's partitions.

Usage:
  python replay_producer.py --replay ~/w5/replay/test.parquet --topic run01 --rate 20000 --limit 600000
  python replay_producer.py ... --profile "60:0.6,90:1.5,0:0.6"      (burst load, see below)
  python replay_producer.py ... --dry-run /tmp/sample.csv            (no Kafka; writes the lines to a file)
Options: --start-row N skips the first N rows; --rate 0 sends as fast as possible.

--profile is a list of  seconds:multiplier  segments applied to --rate, in order; 0 seconds means
"until the end". Example with --rate 10000:  "60:0.6,90:1.5,0:0.6"  sends 6,000/s for 60 s,
15,000/s for 90 s, then 6,000/s until --limit records have been sent. Without --profile the
rate is constant.
"""
import argparse, time
import pyarrow.parquet as pq

ap = argparse.ArgumentParser()
ap.add_argument("--replay", required=True)
ap.add_argument("--topic", default="flows")
ap.add_argument("--bootstrap", default="localhost:9092")
ap.add_argument("--rate", type=float, default=1000.0, help="offered load, records/s (0 = unthrottled)")
ap.add_argument("--profile", default="", help='rate profile, e.g. "60:0.6,90:1.5,0:0.6" (multipliers of --rate)')
ap.add_argument("--limit", type=int, default=0, help="number of records to send (0 = all)")
ap.add_argument("--start-row", type=int, default=0)
ap.add_argument("--start-id", type=int, default=0, help="first event_id")
ap.add_argument("--start-at", type=float, default=0.0, help="wall-clock time (epoch s) to start sending")
ap.add_argument("--dry-run", default="", help="write lines to this file instead of Kafka")
a = ap.parse_args()


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else repr(v)
    return str(v)


def make_schedule(rate, profile):
    """Return f(i) = seconds after start at which message i is due."""
    if rate <= 0:
        return None
    if not profile:
        return lambda i: i / rate
    segs = []
    for part in profile.split(","):
        d, m = part.split(":")
        segs.append((float(d), float(m) * rate))
    # cumulative (start_time, start_count, rate) per segment
    table, t, n = [], 0.0, 0.0
    for d, r in segs:
        table.append((t, n, r))
        if d <= 0:
            break
        t += d; n += d * r
    if segs[-1][0] > 0:                      # last segment had a duration: keep its rate afterwards
        table.append((t, n, segs[-1][1]))

    def due(i):
        for k in range(len(table) - 1, -1, -1):
            if i >= table[k][1]:
                t0, n0, r = table[k]
                return t0 + (i - n0) / r
        return 0.0
    return due


pf = pq.ParquetFile(a.replay)
total_rows = pf.metadata.num_rows
want = (total_rows - a.start_row) if a.limit <= 0 else min(a.limit, total_rows - a.start_row)
print(f"Replay file rows: {total_rows:,}; sending {want:,} from row {a.start_row:,} "
      f"at {'max' if a.rate <= 0 else f'{a.rate:,.0f}/s'}"
      f"{' with profile ' + a.profile if a.profile else ''} to topic '{a.topic}'", flush=True)

if a.dry_run:
    out = open(a.dry_run, "w")
    produce = lambda key, val: out.write(val + "\n")
    flush = out.close
    poll = lambda t: None
else:
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": a.bootstrap, "linger.ms": 5,
                  "queue.buffering.max.messages": 1000000, "batch.num.messages": 10000})

    def produce(key, val):
        while True:
            try:
                p.produce(a.topic, key=key, value=val); return
            except BufferError:
                p.poll(0.05)
    flush = lambda: p.flush(60)
    poll = p.poll

due = make_schedule(a.rate, a.profile)
if a.start_at:
    d = a.start_at - time.time()
    if d > 0:
        time.sleep(d)
sent, skipped, eid = 0, 0, a.start_id
t0 = time.time(); last_report = t0; max_lag = 0.0
for batch in pf.iter_batches(batch_size=20000):
    if sent >= want:
        break
    n = batch.num_rows
    if skipped + n <= a.start_row:
        skipped += n; continue
    off = max(0, a.start_row - skipped); skipped += off
    cols = [c.to_pylist()[off:] for c in batch.columns]
    for row in zip(*cols):
        if sent >= want:
            break
        if due and sent % 50 == 0:
            delay = t0 + due(sent) - time.time()
            if delay > 0:
                time.sleep(delay)
            else:
                max_lag = max(max_lag, -delay)
        body = ",".join(fmt(v) for v in row)
        produce(str(eid), f"{eid},{time.time():.6f},{body}")
        eid += 1; sent += 1
        if sent % 2000 == 0:
            poll(0)
        now = time.time()
        if now - last_report >= 10:
            print(f"  sent {sent:,}  achieved {sent / (now - t0):,.0f}/s", flush=True)
            last_report = now
flush()
el = time.time() - t0
planned = due(sent) if due else el
print(f"SENT {sent:,} records in {el:.1f} s (planned {planned:.1f} s), achieved {sent / el:,.0f}/s "
      f"(event_id {a.start_id:,}..{eid - 1:,}; max lag behind schedule {max_lag:.2f} s)", flush=True)
if due and planned / el < 0.98:
    print(f"WARNING: producer could not keep the offered rate ({100 * planned / el:.1f}% of plan)")
