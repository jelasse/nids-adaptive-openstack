# Adaptive Intrusion Detection under Resource Constraints on an OpenStack Private Cloud

Code, configuration and experiment logs for the MSc project and JTIT paper
"Adaptive Intrusion Detection under Resource Constraints: An Empirical Study on an OpenStack Private Cloud"
(J. Mazite; supervisor Dr. Nguyễn Tất Thắng).

> **Status:** private working repository. It will be made public (and archived on Zenodo) when the paper is submitted or accepted.

## What the system does
Flow records from the corrected CSE-CIC-IDS2018 dataset are replayed at a controlled rate into Kafka inside an
OpenStack VM. Spark Structured Streaming classifies every record with either a **Full** model
(Random Forest, 78 features) or a **Light** model (Logistic Regression, 10 features). A controller watches the
Kafka backlog and switches between the two at runtime (Algorithm 1 in the paper).

## Repository layout
| Folder | Contents | Paper section |
|---|---|---|
| `testbed/` | DevStack configuration (passwords removed), version-recording script, VM and Nova settings | §4.1 |
| `data_prep/` | Profiling, label mapping, duplicate check, time windows, cleaning, train/validation/test split | §5.2–5.3 |
| `models/` | Training, tuning, ablation and freezing of the Full and Light models; in-VM speed check | §5.4 |
| `streaming/` | Replay file builder, producers, streaming detector, controller, telemetry, run script, analysis | §4.2–4.6, §5.5–5.8 |
| `runs/` | Small evidence files of every experiment run (no per-flow predictions) | §6 |
| `docs/` | Frozen settings of the experimental protocol | §5 |
| `legacy/w3_smoke/` | First end-to-end smoke test (Week 3), kept for the record | — |

## Software versions
Ubuntu 24.04 host (HP Omen 16, i7-13620H, 16 GB) · OpenStack 2026.1 via DevStack · KVM with host CPU passthrough ·
Kafka 4.3.1 (KRaft) · PySpark 3.5.9 · Java 17 · Python 3.12 · confluent-kafka · pyarrow.

## Data
The dataset is **not** included. Use the corrected CSE-CIC-IDS2018 release of Liu et al. (IEEE CNS 2022),
then run the scripts in `data_prep/` in this order:
`profile_day.py` → `inspect_columns.py` → `label_map.py` → `dup_check.py` → `time_windows.py` →
`preprocess.py` → `make_train.py`. The resulting split sizes and MD5 manifest are in `docs/`.

## Reproducing one streaming run (inside the experiment VM)
```bash
# example: Static-Full at 30,000 records/s on 1,000,000 records
TRIGGER=0 MAX_OFFSETS=75000 bash streaming/run_once.sh L_full_30k 30000 1000000 full
```
Adaptive run: add `CONTROLLER="--high 100000 --low 10000 --k 2 --m 3 --dwell 15"`.
Producers on the host: `EXTERNAL=1` (the script prints the host command to run).
See `docs/protocol.md` for all frozen settings.

## Secrets
No passwords, keys or OpenStack credentials are stored here. `testbed/local.conf.example` has a placeholder password.
