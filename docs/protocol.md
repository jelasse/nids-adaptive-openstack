# Frozen experimental protocol

All settings below were fixed on validation data before the test split was used, and were not changed during
the main experiments (E1–E4).

## Testbed
- Host: laptop with Intel Core i7-13620H (CPUs 0–11 = 6 performance cores × 2 threads, CPUs 12–15 = 4 efficiency
  cores), 16 GB RAM, NVMe SSD, Ubuntu 24.04, KVM.
- OpenStack 2026.1, DevStack all-in-one; only Keystone, Nova, Neutron (OVN), Glance, Placement; one worker per API
  service. Nova: `[libvirt] cpu_mode = host-passthrough` in `/etc/nova/nova-cpu.conf`.

## VM allocations (OpenStack flavours)
| Paper name | OpenStack flavour | vCPU | RAM | Used in experiments |
|---|---|---|---|---|
| small (no letter) | F1 | 1 | 2 GB | No: Kafka heap + Spark driver exceed its RAM |
| M | F2 | 2 | 4 GB | Yes |
| L | F3 | 4 | 6 GB | Yes |

The small flavour has no letter because S denotes the saturation rate.

## Host state (before every session, `testbed/prepare_session.sh`)
Mains power · performance power profile · graphical desktop off (`multi-user.target`) · swappiness 10 · swap emptied ·
each vCPU pinned to its own performance core (CPUs 2–9) · QEMU emulator threads on CPUs 0, 1, 10, 11 · replay producers
on the efficiency cores (12–15).

## Clock
The VM clock follows the host through the KVM paravirtual PTP clock (`ptp_kvm`, loaded at boot via
`/etc/modules-load.d/ptp_kvm.conf`) as chrony's only source (`refclock PHC /dev/ptp0`). The offset VM − host is
measured before every run (about 2 ms) and subtracted; a run does not start if it exceeds 10 ms.

## Models (frozen 2 November 2026)
- Full: Random Forest, 50 trees, max depth 20, 78 features, threshold 0.540.
- Light: logistic regression, regParam 0.01, top-10 features by Random Forest importance, StandardScaler, threshold 0.946.

## Pipeline
- Kafka 4.3.1, KRaft, single node, **heap 512 MB** (checked by `run_once.sh` before every run), new 4-partition
  topic per run, external listener on port 9094.
- Spark driver 2 GB, back-to-back micro-batches (trigger 0), `maxOffsetsPerTrigger` = 75,000.
- Message: CSV `event_id,send_ts,y,family,<78 features>`, key = event_id.

## Controller (Algorithm 1)
HIGH = 100,000 records · LOW = 10,000 · k = 2 · m = 3 · dwell T = 15 s · signal = total Kafka backlog.

## Workload
- Saturation rate S: highest rate on a 5,000 records/s grid at which Static-Full ends the load with a backlog below
  75,000 records (validation split). S(M) = 30,000 and S(L) = 45,000 records/s.
- Constant loads 0.5, 0.8, 1.0, 1.2, 1.5 S; burst profile 0.6 S for 60 s, 1.5 S for 90 s, then 0.6 S.
- Warm-up: 20 s at 0.3 S from the validation split (event IDs from 10⁹), excluded from the analysis.
- Whole test split (9,421,272 flows) in time order, up to four parallel producers on the host.
- Order of runs randomized once (seed 1); repetitions: 3 (E1–E3), 5 (E4).

## Validity of a run
- VM side: every event ID detected exactly once (0 missing, 0 duplicates).
- Host side: every producer ≥ 98% of its target rate; ≤ 10 MB read back from swap during the load.
- Sustainable: backlog at the end of the load < 75,000 records. Latency objective: 5 s.
- Invalid runs are repeated with suffix `b` (e.g. `E2_L_light_1p2S_r3b`).

## Deviation
A first complete set of the L runs (E1-L, E2, E4) used a 1 GB Kafka heap because the broker had been started outside
the run script. It was set aside and repeated; it is kept in `results/set_aside_kafka1g/` and used only as a robustness check
(Section 6.5 of the paper).
