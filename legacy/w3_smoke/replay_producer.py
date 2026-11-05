#!/usr/bin/env python3
"""Replay flow records from a CSV file into Kafka at a fixed offered rate.

Each record is sent as JSON with an event_id and its send time (epoch seconds),
and is keyed by event_id so that records spread evenly over the topic's partitions.
Feature values are sent as strings; type conversion happens in Spark.
"""
import argparse, csv, json, time

p = argparse.ArgumentParser()
p.add_argument("--csv", required=True)
p.add_argument("--rate", type=float, required=True, help="offered load, records per second")
p.add_argument("--limit", type=int, default=0, help="max records to send (0 = all)")
p.add_argument("--start-id", type=int, default=0, help="first event_id (keep IDs unique across runs)")
p.add_argument("--topic", default="flows")
p.add_argument("--bootstrap", default="localhost:9092")
p.add_argument("--dry-run", action="store_true", help="print the first messages instead of sending")
a = p.parse_args()

if not a.dry_run:
    from confluent_kafka import Producer
    prod = Producer({"bootstrap.servers": a.bootstrap, "linger.ms": 5})

interval = 1.0 / a.rate
sent = 0
t0 = time.perf_counter()
with open(a.csv, newline="") as f:
    for i, row in enumerate(csv.DictReader(f)):
        if a.limit and i >= a.limit:
            break
        delay = t0 + i * interval - time.perf_counter()   # pace to the target rate
        if delay > 0:
            time.sleep(delay)
        eid = a.start_id + i
        msg = json.dumps({"event_id": eid, "send_ts": time.time(), **row})
        if a.dry_run:
            if i < 3:
                print(msg)
            sent += 1
            continue
        while True:
            try:
                prod.produce(a.topic, key=str(eid), value=msg)
                break
            except BufferError:            # local queue full: let it drain, then retry
                prod.poll(0.1)
        prod.poll(0)
        sent += 1

if not a.dry_run:
    prod.flush()
elapsed = time.perf_counter() - t0
print(f"SENT {sent} records in {elapsed:.1f} s; target {a.rate:.0f}/s, achieved {sent/elapsed:.1f}/s; "
      f"event_id {a.start_id}..{a.start_id + sent - 1}")
