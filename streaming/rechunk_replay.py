#!/usr/bin/env python3
"""Rewrite a replay Parquet file with small row groups, so each producer keeps only a small block in memory.
Row order and content are unchanged (checked by row count and first/last event values).

Usage (on the laptop):  python rechunk_replay.py ~/w5/replay/val.parquet [rows_per_group=100000]
The original is replaced only after the new file has been checked.
"""
import os, sys
import pyarrow.parquet as pq

src = os.path.expanduser(sys.argv[1]); rg = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
tmp = src + ".rechunk.tmp"
pf = pq.ParquetFile(src)
n = pf.metadata.num_rows
print(f"{src}: {n:,} rows in {pf.metadata.num_row_groups} row groups -> groups of {rg:,} rows")
w = None
for i in range(pf.metadata.num_row_groups):          # one original group at a time keeps memory bounded
    t = pf.read_row_group(i)
    if w is None:
        w = pq.ParquetWriter(tmp, t.schema, compression="snappy")
    w.write_table(t, row_group_size=rg)
w.close()
new = pq.ParquetFile(tmp)
assert new.metadata.num_rows == n, "row count changed"
first = lambda f: f.read_row_group(0).slice(0, 1).to_pylist()
lastrow = lambda f: (lambda t: t.slice(t.num_rows - 1, 1).to_pylist())(f.read_row_group(f.metadata.num_row_groups - 1))
assert first(pf) == first(new), "first row differs"
assert lastrow(pf) == lastrow(new), "last row differs"
os.replace(tmp, src)
print(f"done: {n:,} rows in {new.metadata.num_row_groups} row groups of up to {rg:,} rows")
