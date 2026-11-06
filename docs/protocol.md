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

## Saturation rate S (measured in Week 7)
- Measured on the **validation** split (first 4,000,000 rows of `val.parquet`), never on the test split,
  because S sets the load levels (a decision)
- Static-Full, constant rate, grid of 5,000 records/s; sustainable = 0 missing and backlog at the end of the
  load phase < 75,000 records (`sustainable` in summary.json)

## Tools (Week 7)
- `replay_producer.py --profile "60:0.6,90:1.5,0:0.6"`: burst profile (multipliers of the base rate)
- `produce_parallel.sh`: WARMUP/WARMUP_RATE (validation rows from 9,000,000, event IDs from 1,000,000,000,
  excluded from analysis); laptop telemetry; everything in `~/w5/host_runs/<run>/`
- `host_summary.py`: producer ≥ 98% of plan and ≤ 10 MB swapped IN on the laptop during the measured load
  (warm-up excluded). Swap-out (idle memory parked on disk) is reported, warning above 50 MB, but does not
  invalidate a run, because it does not make running processes wait -> LAPTOP SIDE VALID
- Laptop setting: `vm.swappiness = 10` (standard for KVM hosts; set permanently in /etc/sysctl.d/99-nids.conf)
- Detector stops when the last batch left backlog 0 and no rows arrived for 30 s
- `rechunk_replay.py`: replay files stored in row groups of 100,000 rows (≈ 65 MB per producer in memory)
- Session preparation: close Chrome and other desktop apps; `sudo swapoff -a && sudo swapon -a` when swap is in use
- `clock_offset.sh`: offset VM − laptop over one shared SSH connection, best of 15, round trip < 10 ms
- `analyze_run.py`: warm-up excluded; `slo_violation_share` (> 5 s); `backlog_at_load_end`; `drain_s`;
  `sustainable`; `valid_vm_side`

## Host state (fixed from 7 November 2026; `prepare_session.sh` at the start of every session)
- Laptop on mains power, power profile `performance`, vm.swappiness = 10, swap emptied
- i7-13620H is hybrid: CPUs 0-11 = 6 P-cores (2 threads each), 12-15 = 4 E-cores
- VM vCPU i pinned to its own P-core (CPUs 2-3, 4-5, 6-7, 8-9); QEMU emulator/I-O threads on 0,1,10,11
- Replay producers pinned to the E-cores 12-15 (`PRODUCER_CPUS`)
- Saturation runs of 6 November (battery, balanced profile, unpinned) are discarded as calibration only

## Clock (from 6 November 2026)
- Each VM's clock follows the laptop's through the KVM PTP device: `ptp_kvm` module (linux-modules-extra),
  chrony with `refclock PHC /dev/ptp0` as the only source. Guest error ~1 µs; measured offset 1.6–1.9 ms,
  stable (the residual is the asymmetry of the SSH measurement). Before this, the offset drifted by up to 135 ms.

## Saturation rates (Static-Full, validation split, pinned host, 6 November 2026)
| VM | 15k | 20k | 25k | 30k | 35k | 40k | 45k | 50k | S |
|---|---|---|---|---|---|---|---|---|---|
| M | ✓ | ✓ | ✓ | ✓ | ✗ |  |  |  | **30,000** |
| L |  |  |  |  | ✓ | ✓ | ✓ | ✗ | **45,000** |
Load levels: M 15k / 24k / 30k / 36k / 45k; L 22.5k / 36k / 45k / 54k / 67.5k.

## Main runs (test split)
- `make_plan.py <E1..E4> <M|L>` writes the runs of one experiment on one VM in random order (seed 1)
- `run_queue.sh <plan>` runs them unattended (desktop off): clock offset per run, detector start, load,
  validity, results to ~/w7/queue_results.csv, small files to ~/w7/runs/, Kafka topic deleted after each run
