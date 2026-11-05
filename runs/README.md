# Run evidence

One folder per run, named `<size>_<policy>_<rate>` (e.g. `L_adapt_45k`). Only small files are kept:
`summary.json`, `batches.jsonl`, `progress.jsonl`, `controller.jsonl`, `telemetry.csv`, `stream.log`, producer logs.
Per-flow predictions (Parquet, 30–60 MB per run) stay on the laptop and will go to Zenodo, not GitHub.

- `pilot_M/` — Week 6 pilot on size M (validation split, 5 s trigger, producer in VM)
- `pilot_L/` — Week 6 pilot on size L (validation split, back-to-back batches; 45k runs with host producers,
  latencies to be corrected by +101 ms)

Collect them with `bash collect_runs.sh` from the repository root.
