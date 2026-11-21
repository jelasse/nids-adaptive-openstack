# Reproducing the experiments

The scripts expect the directory layout used in the study. Each script's header gives its exact usage.

| Machine | Location | Contents |
|---|---|---|
| Laptop (OpenStack host) | `~/nids-host/stream/` | copy of `streaming/` |
| Laptop | `~/w5/replay/` | `val.parquet`, `test.parquet` (replay files) |
| Laptop | `~/w5/host_runs/`, `~/w7/plans/`, `~/w7/runs/`, `~/w7/analysis/` | host-side run data, plans, collected runs, tables |
| Experiment VM | `~/nids/stream/`, `~/nids/models/`, `~/nids/data/report/features.txt`, `~/nids-venv/` | copy of `streaming/`, frozen models, feature list, Python venv |
| Experiment VM | `/opt/kafka/` | Kafka 4.3.1 binary release |

## 1. Testbed
1. Install Ubuntu 24.04 on the host and DevStack (OpenStack 2026.1) with `testbed/local.conf.example`
   (set a password). Set `[libvirt] cpu_mode = host-passthrough` in `/etc/nova/nova-cpu.conf` and restart
   `devstack@n-cpu`.
2. Create the flavours F2 (2 vCPU, 4 GB) and F3 (4 vCPU, 6 GB) and an Ubuntu 24.04 VM with a floating IP for each
   size used. Record versions with `testbed/record_testbed.sh`.

## 2. Experiment VM
1. Install OpenJDK 17, unpack Kafka 4.3.1 to `/opt/kafka`, format KRaft storage once
   (`kafka-storage.sh format -t $(kafka-storage.sh random-uuid) -c config/server.properties --standalone`) and add
   an external listener on port 9094 for the floating IP. Start Kafka only with `streaming/kafka_start.sh`
   (512 MB heap); `run_once.sh` refuses other heap sizes and restarts the broker if needed.
2. Create `~/nids-venv` with `pip install -r requirements-vm.txt`; copy `streaming/` to `~/nids/stream/`.
3. Clock: `sudo apt install chrony linux-modules-extra-$(uname -r)`;
   `echo ptp_kvm | sudo tee /etc/modules-load.d/ptp_kvm.conf`; in `/etc/chrony/chrony.conf` comment out the
   `pool` lines and add `refclock PHC /dev/ptp0 poll 2`; reboot and check `chronyc sources` (PHC0 selected).
   `streaming/clock_offset.sh` measures the offset from the laptop (expected: a few ms).

## 3. Data and models
1. Download the corrected CSE-CIC-IDS2018 dataset (Liu et al., IEEE CNS 2022; see
   <https://github.com/GintsEngelen/CNS2022_Code>).
2. Run `data_prep/` in this order: `profile_day.py` → `inspect_columns.py` → `label_map.py` → `dup_check.py` →
   `time_windows.py` → `preprocess.py` → `make_train.py`. Result: time-ordered train/validation/test splits
   (70/15/15% per day and label), Attempted flows excluded, 78 features.
3. `models/train_models.py` stages `tune_rf`, `tune_lr`, `final` (train and validation splits only), then
   `models/vm_model_check.py` in the VM for the scoring speed. Copy the two frozen PipelineModels to `~/nids/models/`.
4. `streaming/make_replay.py` builds `val.parquet` and `test.parquet`; `streaming/rechunk_replay.py` rewrites them
   with 100,000-row groups (lower producer memory). Copy both to `~/w5/replay/` on the laptop.

## 4. Saturation rate
On validation data, run Static-Full with `run_once.sh` (producers on the host, `EXTERNAL=1`) at increasing rates on
a 5,000 records/s grid. S is the highest rate whose backlog at the end of the load stays below 75,000 records.

## 5. Experiments
```bash
# laptop, once per session (charger connected, other applications closed)
sudo bash ~/nids-host/stream/prepare_session.sh nids-exp-l   # VM name; pins vCPUs, sets host state
sudo systemctl isolate multi-user.target                     # desktop off during runs
python3 ~/nids-host/stream/make_plan.py E2 L                  # -> ~/w7/plans/E2_L.csv
nohup bash ~/nids-host/stream/run_queue.sh ~/w7/plans/E2_L.csv > ~/w7/queue_E2_L.log 2>&1 &
tail -f ~/w7/queue_E2_L.log                                   # Ctrl-C stops watching only
```
The queue checks the charger and the clock offset before every run, starts the detector in the VM, sends the load,
checks validity, copies the small result files to `~/w7/runs/<run>/` and deletes the Kafka topic. Results per run:
`~/w7/queue_results.csv`. Stop after the current run with `touch ~/w7/STOP_QUEUE`; a restarted queue skips finished
runs. Invalid runs are repeated with a `b` suffix.

## 6. Analysis
```bash
python3 ~/nids-host/stream/aggregate.py ~/w7/runs --host ~/w5/host_runs --out ~/w7/analysis
```
Outputs: `runs.csv`, `conditions.csv`, `tables.md`, `e4_effects.csv` (bootstrap CIs, Hedges' g),
`fig_e2_latency.png`. The aggregated outputs of the study are in `analysis/`; they can be recomputed from the
published run files with `python3 streaming/aggregate.py results/final --out analysis`.

## 7. Revision experiments (R1–R5)
The frozen models of the paper, including the security-aware fallback, are in `models/frozen/` (Spark
PipelineModels with `frozen_models.json`, `features.txt` and `feature_ranking.csv`). Copy `full_rf`, `light_lr`,
`light_rf` and `frozen_models.json` to `~/nids/models/` in the VM to use exactly the models of the paper, or
recreate them with steps 3 and 7.1.

1. **Dataset checks and models** (laptop, Spark venv; outputs in `~/w8/rev`):
   ```bash
   spark-submit --driver-memory 10g data_prep/revision_checks.py split     ~/w4/data ~/w8/rev
   spark-submit --driver-memory 10g data_prep/revision_checks.py ablation  ~/w4/data ~/w8/rev --ranking ~/w5/models/feature_ranking.csv
   spark-submit --driver-memory 10g data_prep/revision_checks.py lodo      ~/w4/data ~/w8/rev
   spark-submit --driver-memory 10g data_prep/revision_checks.py attempted ~/w4/data ~/w8/rev --models ~/w5/models
   spark-submit --driver-memory 8g  models/freeze_fallback.py ~/w4/data ~/w5/models ~/w8/rev   # -> light_rf
   spark-submit --driver-memory 8g  models/eval_frozen.py     ~/w4/data ~/w5/models ~/w8/rev   # test-split table
   ```
2. **Tools**: copy `streaming/` to `~/nids-host/stream/` on the laptop and to `~/nids/stream/` in the VM, and the
   fallback model to `~/nids/models/light_rf` with its entry in `frozen_models.json`. Pin the guest kernel to one that
   includes the `ptp_kvm` module and disable unattended upgrades; otherwise the clock check stops the queue.
3. **Evasion workloads** (R3), about 8.1 M rows each:
   ```bash
   for s in 1 2 3 4 5; do python ~/nids-host/stream/compose_workload.py ~/w5/replay/test.parquet ~/w8/workloads --seed $s; done
   ```
4. **Plans**: `python3 ~/nids-host/stream/make_plan_rev.py R1` (and `R2` … `R5`) writes `~/w8/plans/R<n>_L.csv`.
   On another host, first edit `S`, `VM` and `IP` at the top of `make_plan_rev.py` to the measured saturation rate,
   the VM name and its address.
5. **Runs**: as in Section 5, one queue per plan, e.g.
   `nohup bash ~/nids-host/stream/run_queue.sh ~/w8/plans/R2_L.csv > ~/w8/queue_R2.log 2>&1 &`. The plan columns set
   the replay file, the order-preserving replay (`INTERLEAVE=1`), the fallback model and the controller; after each
   run the queue computes `deadline.csv` (and `evasion.csv` for R3) in the VM and copies them to `~/w7/runs/<run>/`.
   The 28 M-flow runs of R4 need about 14 GB of free disk in the VM for the Kafka topic.
6. **Repeats**: `python3 ~/nids-host/stream/make_plan_rev.py R2 --redo` writes `~/w8/plans/R2_L_redo.csv` with only the
   runs still missing, invalid ones renamed with a `b` (then `c`) suffix.
7. **Analysis**:
   ```bash
   python3 analysis/revision/rev_analysis.py ~/w7/runs ~/w7/queue_results.csv ~/w8/analysis
   python3 analysis/revision/rev_figs.py ~/w7/runs ~/w7/queue_results.csv analysis/revision/w7_deadline_L.csv ~/w8/analysis
   ```
