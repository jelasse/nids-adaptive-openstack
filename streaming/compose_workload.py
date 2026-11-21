#!/usr/bin/env python3
"""Compose the induced-overload evasion workload (review MC1) from the test replay file.

Timeline (seconds from the start of the load; rates as multiples of S, the saturation rate of Full on the VM):
  0 .. PRE                benign background at BG x S                         (normal operation)
  PRE .. PRE+FLOOD        background + attacker's flood of DoS flows at FL x S (total (BG+FL) x S: overload)
  PRE+FLOOD .. end        benign background at BG x S for POST seconds         (recovery)
Two short episodes of a low-volume attack family (default Botnet) of EP seconds at EPR flows/s replace part of
the background:
  control episode   starts at CTRL s, during normal operation  -> how the detector handles it without overload
  attack episode    starts at a random offset (seeded, uniform in [OFF_MIN, OFF_MAX]) after the flood begins
The two episodes use different flows of the same family. Within every second the rows are shuffled (seeded).
Event IDs are the row numbers, as in the other replay files; roles are recognised by family and send time.

The producer offers the file with  --rate S  --profile "PRE:BG;FLOOD:BG+FL;0:BG"  (written to the manifest).

Usage (laptop):
  python compose_workload.py ~/w5/replay/test.parquet ~/w8/workloads --seed 1 [--family Botnet] [--s 45000]
Output: <out>/evasion_<family>_s<seed>.parquet and .json (manifest: timeline, episode times, row counts, profile)
"""
import argparse, json, os
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ap = argparse.ArgumentParser()
ap.add_argument("replay"); ap.add_argument("out_dir")
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--family", default="Botnet", help="hidden low-volume family (Botnet or Infiltration)")
ap.add_argument("--flood-family", default="DoS")
ap.add_argument("--s", type=int, default=45000, help="saturation rate S of Full on the VM (records/s)")
ap.add_argument("--bg", type=float, default=0.6); ap.add_argument("--fl", type=float, default=0.8)
ap.add_argument("--pre", type=int, default=40); ap.add_argument("--flood", type=int, default=90); ap.add_argument("--post", type=int, default=50)
ap.add_argument("--ep", type=int, default=15, help="episode length, s"); ap.add_argument("--epr", type=int, default=100, help="episode rate, flows/s")
ap.add_argument("--ctrl", type=int, default=12, help="start of the control episode, s")
ap.add_argument("--off-min", type=int, default=15); ap.add_argument("--off-max", type=int, default=65)
a = ap.parse_args()
rng = np.random.default_rng(a.seed)
pf = pq.ParquetFile(os.path.expanduser(a.replay))
schema = pf.schema_arrow


def stream(pred, cycle=False):
    """Yield the rows matching pred, in file order, as tables; restart at the end if cycle."""
    while True:
        for b in pf.iter_batches(batch_size=100000):
            t = pa.Table.from_batches([b])
            m = pred(t)
            if pc.any(m).as_py():
                yield t.filter(m)
        if not cycle:
            return


class Take:
    def __init__(self, gen):
        self.gen, self.buf = gen, []
        self.n = 0

    def take(self, k):
        out, need = [], k
        while need > 0:
            if not self.buf:
                self.buf = [next(self.gen)]
            t = self.buf[0]
            if t.num_rows <= need:
                out.append(t); need -= t.num_rows; self.buf.pop(0)
            else:
                out.append(t.slice(0, need)); self.buf[0] = t.slice(need); need = 0
        self.n += k
        return pa.concat_tables(out) if out else schema.empty_table()


fam = lambda name: (lambda t: pc.equal(t.column("family"), name))
bg = Take(stream(fam("Benign")))
fl = Take(stream(fam(a.flood_family), cycle=True))
hid = Take(stream(fam(a.family)))
S, PRE, FL, POST = a.s, a.pre, a.flood, a.post
T = PRE + FL + POST
off = int(rng.integers(a.off_min, a.off_max + 1))
episodes = {"control": (a.ctrl, a.ctrl + a.ep), "attack": (PRE + off, PRE + off + a.ep)}
assert episodes["control"][1] <= PRE, "control episode must end before the flood"
assert episodes["attack"][1] <= PRE + FL, "attack episode must end before the flood ends"

os.makedirs(os.path.expanduser(a.out_dir), exist_ok=True)
base = os.path.join(os.path.expanduser(a.out_dir), f"evasion_{a.family}_s{a.seed}")
w = pq.ParquetWriter(base + ".parquet", schema)
pending, rows = [], 0
for sec in range(T):
    n_bg = round(a.bg * S)
    n_fl = round(a.fl * S) if PRE <= sec < PRE + FL else 0
    n_h = a.epr if any(s0 <= sec < s1 for s0, s1 in episodes.values()) else 0
    parts = [bg.take(n_bg - n_h), fl.take(n_fl) if n_fl else None, hid.take(n_h) if n_h else None]
    t = pa.concat_tables([p for p in parts if p is not None and p.num_rows])
    t = t.take(pa.array(rng.permutation(t.num_rows)))
    pending.append(t); rows += t.num_rows
    if sum(p.num_rows for p in pending) >= 100000 or sec == T - 1:
        w.write_table(pa.concat_tables(pending), row_group_size=100000); pending = []
w.close()
profile = f"{PRE}:{a.bg};{FL}:{round(a.bg + a.fl, 3)};0:{a.bg}"
man = dict(file=base + ".parquet", rows=rows, seed=a.seed, family=a.family, flood_family=a.flood_family, s=S,
           timeline=dict(pre=PRE, flood=FL, post=POST, bg=a.bg, fl=a.fl), profile_rate=S, profile=profile,
           episodes={k: dict(start_s=v[0], end_s=v[1], flows=a.ep * a.epr) for k, v in episodes.items()},
           attack_offset_s=off, benign_rows=bg.n, flood_rows=fl.n, hidden_rows=hid.n)
json.dump(man, open(base + ".json", "w"), indent=2)
print(f"{base}.parquet: {rows:,} rows, {T} s; attack episode at +{off} s into the flood; "
      f"producer: --rate {S} --profile '{profile}'")
