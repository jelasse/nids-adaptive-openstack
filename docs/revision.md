# Revision experiments (R1–R5)

These experiments answer the pre-submission review of the paper. They run on the L VM (4 vCPU, 6 GB;
S = 45,000 flows/s) with the same testbed, frozen models and validity rules as E1–E4, plus the changes below.

| Experiment | Question | Policies | Load | Runs |
|---|---|---|---|---|
| R1 | Capacity of the security-aware fallback | Static fallback (`light_rf`) | 54,000 / 63,000 / 67,500 flows/s, 4 M validation flows | 1 each |
| R2 | Mitigations and baselines | Adaptive (LR), Adaptive (RF), Adaptive + 10% Full, Proportional (LR) | 1.2 S; burst | 5 |
| R3 | Induced-overload evasion | Static-Full + the four policies of R2 | evasion workload, seeds 1–5 | 1 per seed |
| R4 | Long runs | Static-Full at 0.9 S; Adaptive (LR) at 1.2 S | test split replayed 3 times | 1 |
| R5 | Sharper burst | Adaptive (LR), Adaptive (RF) | 30 s at 0.6 S, 60 s at 1.8 S, then 0.6 S | 3 |

## What changed in the tools

- **Order-preserving replay** (`replay_producer.py --stride`, `produce_parallel.sh` with `INTERLEAVE=1`): producer *i*
  sends rows *i*, *i+P*, *i+2P*, … of the time-ordered file. E1–E4 used one contiguous slice per producer.
- **Producer memory**: the Parquet reader runs without pre-buffering (`pre_buffer=False`), which keeps each producer
  at about 0.45 GB. With pre-buffering the producers grew towards the file size and the host swapped.
- **Security-aware fallback** (`models/freeze_fallback.py`): among 18 small Random Forests with at least 4× the offline
  throughput of Full, the highest worst-family recall on validation. Chosen: 10 trees, depth 10, top 20 features,
  threshold 0.306 (`light_rf`). Test-split evaluation of all frozen models: `models/eval_frozen.py`.
- **Controllers** (`streaming/controller.py`): `--policy proportional` sends each micro-batch to the fallback with
  probability *f*, with per-flow costs learned from saturated batches (≥ 60,000 flows). The detector
  (`streaming/stream_detect.py`) reads `mix:<f>` and `FULL_SAMPLE`; the choice is made per micro-batch, seeded by
  topic and batch id.
- **Evasion workloads** (`streaming/compose_workload.py`): 40 s background at 0.6 S, 90 s DoS flood adding 0.8 S,
  50 s background; botnet control episode at 12–27 s and attack episode at a random offset 15–65 s into the flood.
  Manifests are in `experiments/workloads/`; the Parquet files are regenerated with `--seed 1` … `--seed 5`.
- **Per-run metrics in the VM**: `deadline_metrics.py` (TPR@τ, false positives per minute, precision, per family) and
  `evasion_metrics.py` (R3). Their CSV files are in every run folder.
- **Testbed**: the guest kernel is pinned to 6.8.0-147 and unattended upgrades are disabled, because an automatic
  update to 6.8.0-149 removed the `ptp_kvm` module used for clock synchronization.

## Run accounting

111 attempts, 76 valid. All 35 invalid attempts failed the host-side swap-in rule (traced to producer memory and fixed);
none was invalid on the VM side and no run lost or duplicated a record. Invalid runs were repeated with a suffix
(`b`, `c`). 46 further runs were superseded by two corrected implementation errors and are kept in
`results/revision_superseded/`:

- `mix/` (30 runs): an earlier detector split every micro-batch between the two models; this was slower than Full alone.
- `prop/` (16 runs): an earlier proportional controller learned per-flow costs from small batches, overestimated
  Full's cost and sent almost everything to the fallback.

All attempts, including the original experiments, are listed in `results/queue_results.csv`.

## Recomputing

```bash
python3 analysis/revision/rev_analysis.py results/revision results/queue_results.csv /tmp/check
cmp /tmp/check/revision_results_summary.csv analysis/revision/revision_results_summary.csv && echo identical
python3 analysis/revision/rev_figs.py results/revision results/queue_results.csv analysis/revision/w7_deadline_L.csv /tmp/check
```

`analysis/revision/w7_deadline_*.csv` hold the deadline metrics of the E2–E4 runs, computed with the same
`deadline_metrics.py` from their archived per-flow predictions. `analysis/revision/data_checks/` holds the dataset
checks of `data_prep/revision_checks.py` (split overlap, duplicates, leave-one-day-out, ablation, Attempted flows).
