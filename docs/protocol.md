# Frozen experimental settings (as of 8 November 2026)

## VM allocations (OpenStack flavours)
| Paper name | OpenStack flavour | vCPU | RAM | Used in experiments |
|---|---|---|---|---|
| S | F1 | 1 | 2 GB | No (pipeline does not fit) |
| M | F2 | 2 | 4 GB | Yes |
| L | F3 | 4 | 6 GB | Yes |
Nova: `[libvirt] cpu_mode = host-passthrough` in `/etc/nova/nova-cpu.conf`.

## Models (frozen 2 November 2026, MD5 in `models/frozen_models.json`)
- Full: Random Forest, 50 trees, max depth 20, 78 features, threshold 0.540
- Light: Logistic Regression, regParam 0.01, top-10 features, StandardScaler, threshold 0.946

## Pipeline
- Kafka 4.3.1 KRaft, heap 512 MB, new 4-partition topic per run, external listener on port 9094
- Spark driver 2 GB, back-to-back micro-batches (trigger 0), maxOffsetsPerTrigger = 75,000
- Message: CSV `event_id,send_ts,y,family,<78 features>`, key = event_id

## Controller (Algorithm 1)
HIGH = 100,000 records · LOW = 10,000 records · k = 2 · m = 3 · dwell T = 15 s · signal = total Kafka backlog

## Workload and runs
- Whole test split (9,421,272 flows) replayed in time order by parallel producers on the host
- Valid run: every producer ≥ 98% of target rate; 0 missing, 0 duplicate event IDs; no host swap
- Loads: 0.5, 0.8, 1.0, 1.2, 1.5 × S (S = saturation rate of Static-Full); burst 0.6S/60 s → 1.5S/90 s → 0.6S
- 20 s warm-up at 0.3 S; latency objective 5 s
- Repetitions: 3 (E1–E3), 5 (E4); randomized order; clock offset VM−host measured per session and subtracted
