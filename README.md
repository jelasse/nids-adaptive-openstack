# Security–Latency Trade-offs in Resource-Constrained Streaming Intrusion Detection

Code, experiment plans and per-run results for the paper

> J. Mazite, N. T. Thắng, "Security–Latency Trade-offs in Resource-Constrained Streaming Intrusion Detection: An
> Experimental Study of Adaptive Inference and Overload-Induced Evasion" (submitted, 2026).

Release `v1.0-submitted` holds the first version of the study (experiments E1–E4); release `v2.0-revision` adds the
revision experiments R1–R5 described in [`docs/revision.md`](docs/revision.md).

A flow-record streaming intrusion detector (Apache Kafka + Spark Structured Streaming) runs in one VM of a
single-host OpenStack cloud. Flow records of the corrected CSE-CIC-IDS2018 dataset are replayed at controlled
rates, below and beyond the detector's measured capacity. The policies compared are:

| Policy | Model(s) | Features |
|---|---|---|
| Static-Full | Random Forest, 50 trees, depth 20 | 78 |
| Static-Light | Logistic regression, regularization 0.01 | 10 |
| Adaptive (LR) | switches between Full and Light from the Kafka backlog (Algorithm 1 in the paper) | – |
| Adaptive (RF) | same rule, security-aware fallback: Random Forest, 10 trees, depth 10 | 20 |
| Adaptive + 10% Full | Adaptive (LR), with 10% of fallback-mode batches still classified by Full | – |
| Proportional (LR) | each micro-batch goes to Light with a probability set from the arrival rate and backlog | – |

## Main results

| Finding | Value |
|---|---|
| Saturation rate of Static-Full, M (2 vCPU, 4 GB) / L (4 vCPU, 6 GB) | 30,000 / 45,000 flows/s |
| Static-Full beyond saturation on L (1.2 S / 1.5 S): p95 latency | 41 s / 69 s (no record lost in any valid run) |
| Attack flows detected within 5 s at 1.2 S: Static-Full / Adaptive (LR) / Adaptive (RF) | 2.7% / 97.8% / 99.9% |
| Static-Light: recall on botnet, web, infiltration | 0.000, 0.000, 0.022 |
| Induced-overload evasion, botnet episode detected (median of 5 placements): Adaptive (LR) / Proportional / Adaptive + 10% Full / Adaptive (RF) | 70% / 32% / 48% / 100% |
| False positives per minute under overload: Adaptive (LR) / Adaptive (RF) | 1,605–1,965 / 107–128 |

## Repository layout

| Folder | Contents |
|---|---|
| `testbed/` | DevStack `local.conf` example (no passwords), version recording |
| `data_prep/` | Profiling, label mapping, duplicate check, cleaning, time-ordered train/validation/test split; revision checks of the split (`revision_checks.py`) |
| `models/` | Training, tuning and freezing of the Full and Light models; selection of the security-aware fallback (`freeze_fallback.py`); test-split evaluation of the frozen models (`eval_frozen.py`); in-VM scoring speed check |
| `streaming/` | Replay producers, streaming detector, controller, telemetry, host session and Kafka start scripts, run script, run queue, per-run analysis, aggregation |
| `experiments/plans/` | Randomized run plans of E1–E4 and R1–R5 (one CSV line per run) |
| `experiments/workloads/` | Manifests of the five evasion workloads of R3 |
| `results/` | Evidence files of every run: final E1–E4 runs, saturation measurements, set-aside robustness set, pilots, revision runs R1–R5 and superseded revision runs; `queue_results.csv` lists every attempt (see `results/README.md`) |
| `analysis/` | Aggregated tables and the latency figure computed from `results/final/`; `analysis/revision/` with the revision tables, figures, deadline metrics of E2–E4 and dataset checks |
| `docs/` | Frozen experimental protocol, step-by-step reproduction guide, revision experiments (`revision.md`) |

## Experiments

| Experiment | Question | VM | Policies | Load | Runs |
|---|---|---|---|---|---|
| E1 | Feasibility | M, L | Static-Full | 0.5 S | 3 |
| E2 | Full vs Light under load | L | Static-Full, Static-Light | 0.5, 0.8, 1.0, 1.2, 1.5 S | 3 |
| E3 | Effect of the allocation | M (L values from E2) | Static-Full, Static-Light | 22,500, 36,000, 45,000 flows/s | 3 |
| E4 | Adaptive policy | L | Static-Full, Static-Light, Adaptive | 1.2 S; burst | 5 |

The revision experiments R1–R5 are listed in [`docs/revision.md`](docs/revision.md). S is the saturation rate of Static-Full on the VM. Every run replays the whole test split (9,421,272 flows) after a
20 s warm-up. Run names follow `<experiment>_<VM>_<policy>_<load>_r<repetition>`, e.g. `E4_L_adapt_burst_r3`.

## Reproducing

See [`docs/reproduce.md`](docs/reproduce.md) for the full procedure (testbed, clock synchronization, data
preparation, models, run queue, aggregation) and [`docs/protocol.md`](docs/protocol.md) for every frozen setting.
Short version, after the testbed and VM are prepared (the scripts expect `streaming/` copied to
`~/nids-host/stream/` on the laptop and to `~/nids/stream/` in the VM):

```bash
# laptop: fixed host state, randomized plan, unattended queue, then tables and figures
sudo bash ~/nids-host/stream/prepare_session.sh nids-exp-l
python3 ~/nids-host/stream/make_plan.py E2 L
nohup bash ~/nids-host/stream/run_queue.sh ~/w7/plans/E2_L.csv > ~/w7/queue_E2_L.log 2>&1 &
python3 ~/nids-host/stream/aggregate.py ~/w7/runs --host ~/w5/host_runs --out ~/w7/analysis
```

## Recomputing the tables

```bash
python3 streaming/aggregate.py results/final --out analysis
python3 analysis/revision/rev_analysis.py results/revision results/queue_results.csv /tmp/check
```

## Data

The dataset is not included. Use the corrected CSE-CIC-IDS2018 release of Liu et al. (IEEE CNS 2022; code and
download links at <https://github.com/GintsEngelen/CNS2022_Code>) and run the scripts in `data_prep/` as described in
`docs/reproduce.md`. Per-flow predictions (30–60 MB per run) are not stored on GitHub.

## Software

Ubuntu 24.04 host (Intel i7-13620H, 16 GB) · OpenStack 2026.1 (DevStack, all-in-one) · KVM, host CPU passthrough ·
Kafka 4.3.1 (KRaft) · PySpark 3.5.9 · OpenJDK 17 · Python 3.12. Python packages: `requirements-*.txt`.

## License

Code: MIT (see `LICENSE`). Run results, tables and documentation: CC BY 4.0.

## Citation

See `CITATION.cff`. Volume, pages and DOI will be added after publication.
