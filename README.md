# Adaptive Intrusion Detection under Resource Constraints on an OpenStack Private Cloud

Code, experiment plans and per-run results for the paper

> J. Mazite, "Adaptive Intrusion Detection under Resource Constraints: An Empirical Study on an OpenStack
> Private Cloud", *Journal of Telecommunications and Information Technology* (submitted, 2026).

A streaming network intrusion detector (Apache Kafka + Spark Structured Streaming) runs in one VM of a
single-host OpenStack cloud. Flow records of the corrected CSE-CIC-IDS2018 dataset are replayed at controlled
rates, below and beyond the detector's measured capacity, and three inference policies are compared:

| Policy | Model | Features |
|---|---|---|
| Static-Full | Random Forest, 50 trees, depth 20 | 78 |
| Static-Light | Logistic regression, regularization 0.01 | 10 |
| Adaptive | switches between Full and Light from the Kafka backlog (Algorithm 1 in the paper) | – |

## Main results

| Finding | Value |
|---|---|
| Saturation rate of Static-Full, M (2 vCPU, 4 GB) / L (4 vCPU, 6 GB) | 30,000 / 45,000 flows/s |
| Static-Full at 0.5 S on M: p95 latency, attack recall | 2.29 s, 0.9986 |
| Static-Full beyond saturation on L (1.2 S / 1.5 S): p95 latency | 41 s / 69 s (no flow lost) |
| Static-Light on L up to 1.5 S: p95 latency, attack recall | < 0.9 s, 0.8418 |
| Adaptive vs Static-Full at 1.2 S / burst: p95 latency reduction | 33.2 s / 41.1 s |
| Adaptive at 1.2 S / burst: Macro-F1 | 0.990 / 0.982 |

## Repository layout

| Folder | Contents |
|---|---|
| `testbed/` | DevStack `local.conf` example (no passwords), version recording |
| `data_prep/` | Profiling, label mapping, duplicate check, cleaning, time-ordered train/validation/test split |
| `models/` | Training, tuning and freezing of the Full and Light models; in-VM scoring speed check |
| `streaming/` | Replay producers, streaming detector, controller, telemetry, host session and Kafka start scripts, run script, run queue, per-run analysis, aggregation |
| `experiments/plans/` | Randomized run plans of E1–E4 (one CSV line per run) |
| `results/` | Evidence files of every run: final E1–E4 runs, saturation measurement, set-aside robustness set, pilots (see `results/README.md`) |
| `analysis/` | Aggregated tables, bootstrap effect sizes and the latency figure computed from `results/final/` |
| `docs/` | Frozen experimental protocol and step-by-step reproduction guide |

## Experiments

| Experiment | Question | VM | Policies | Load | Runs |
|---|---|---|---|---|---|
| E1 | Feasibility | M, L | Static-Full | 0.5 S | 3 |
| E2 | Full vs Light under load | L | Static-Full, Static-Light | 0.5, 0.8, 1.0, 1.2, 1.5 S | 3 |
| E3 | Effect of the allocation | M (L values from E2) | Static-Full, Static-Light | 22,500, 36,000, 45,000 flows/s | 3 |
| E4 | Adaptive policy | L | Static-Full, Static-Light, Adaptive | 1.2 S; burst | 5 |

S is the saturation rate of Static-Full on the VM. Every run replays the whole test split (9,421,272 flows) after a
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
