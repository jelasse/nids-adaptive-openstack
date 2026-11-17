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
