# Results

Evidence files of every run used in the paper. Per-flow predictions (30–60 MB per run) and Spark's verbose logs are
not included.

| Folder | Contents | Runs |
|---|---|---|
| `final/` | Experiments E1–E4 with the frozen protocol (Kafka heap 512 MB). These are the runs reported in the paper. | 85 (84 valid + 1 invalid) |
| `saturation_L/` | Saturation measurement of Static-Full on L (validation split, final host state): S(L) = 45,000 records/s. | 8 |
| `set_aside_kafka1g/` | First set of the L runs (E1-L, E2, E4) with a 1 GB Kafka heap. Set aside and repeated; used only for the robustness check. | 65 (incl. 2 repetitions of invalid runs) |
| `pilots/M`, `pilots/L` | Pilot runs on the validation split used to fix the batch cap, the trigger and the controller thresholds before the main experiments. | 18 |
| `clock_offsets.csv` | VM − host clock offset measured before each run. | – |

## Run names
`<experiment>_<VM>_<policy>_<load>_r<repetition>[b]`, e.g. `E4_L_adapt_burst_r3`
- experiment `E1`–`E4`; VM `M` (2 vCPU, 4 GB) or `L` (4 vCPU, 6 GB); policy `full`, `light`, `adapt`
- load `0p5S` … `1p5S` = 0.5 … 1.5 × the saturation rate S of the VM, `burst` = burst profile, or an absolute rate
  (`22k` = 22,500, `36k` = 36,000, `45k` = 45,000 records/s, E3)
- suffix `b`: repetition of an invalid run. `E2_L_light_1p2S_r3` is invalid (the laptop read memory back from swap
  during the load) and was repeated as `E2_L_light_1p2S_r3b`; the invalid run is kept for transparency and excluded
  from the analysis (`analysis/invalid_runs.csv`).

## Files in a run folder
| File | Side | Contents |
|---|---|---|
| `summary.json` | VM | Run result: throughput, latency percentiles, latency-objective violations, backlog, detection metrics overall and per family, validity checks |
| `batches.jsonl` | VM | One line per micro-batch: rows, mode (Full/Light), timing |
| `progress.jsonl` | VM | Spark streaming progress (offsets, rates, durations) |
| `controller.jsonl` | VM | Adaptive only: backlog, counters and every switch |
| `telemetry.csv` | VM | CPU, memory and swap of the VM, once per second |
| `settings.txt` | VM | Settings of the run (rate, mode, cap, trigger, Kafka heap, clock offset) |
| `console.txt` | VM | Console output of the run script |
| `host/host_summary.json` | Laptop | Host-side validity: producer rates, swap activity |
| `host/host_telemetry.csv` | Laptop | CPU, memory and swap of the host, once per second |
| `host/producer_p*.log`, `host/warmup.log` | Laptop | Replay producers: records sent, achieved rate, lag |
| `host/load_start`, `host/load_end`, `host/records_sent`, `host/host_settings.txt` | Laptop | Load timing and host settings |

To recompute the tables from these files: `python3 streaming/aggregate.py results/final --out analysis`.
