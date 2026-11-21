"""Figures 3 and 4 of the revised paper.
Usage: python3 rev_figs.py results/revision results/queue_results.csv analysis/revision/w7_deadline_L.csv <out dir>
"""
import csv, re, collections, statistics as st, os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.rcParams.update({"font.family": "DejaVu Serif", "font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.edgecolor": "#52514e", "axes.labelcolor": "#0b0b0b", "xtick.color": "#52514e", "ytick.color": "#52514e"})
GRID = "#e4e3df"
COL = {"full": "#2a78d6", "light": "#6250d6", "adapt": "#eb6834", "adaptrf": "#1baf7a", "prop": "#eda100", "adapts": "#e87ba4"}
LAB = {"full": "Static-Full", "light": "Static-Light", "adapt": "Adaptive (LR fallback)", "adaptrf": "Adaptive (RF fallback)",
       "prop": "Proportional (LR fallback)", "adapts": "Adaptive + 10% Full"}
MK = {"full": "o", "light": "v", "adapt": "s", "adaptrf": "D", "prop": "^", "adapts": "P"}
LS = {"full": "-", "light": ":", "adapt": "--", "adaptrf": "-", "prop": "-.", "adapts": (0, (1, 1))}
import sys
RUNS, QUEUE, W7DL, OUTD = sys.argv[1:5]   # results/revision  results/queue_results.csv  analysis/revision/w7_deadline_L.csv  out dir
os.makedirs(OUTD, exist_ok=True)
def load_deadline(path, pat):
    d = collections.defaultdict(dict)
    for r in csv.DictReader(open(path)):
        if r["value"] != "" and re.match(pat, r["run"]): d[r["run"]][(r["family"], r["metric"])] = float(r["value"])
    return d
TAUS = [1, 2, 5, 10, 30, 60]
def curve(runs, fam="ALL"):
    return [st.median([v[(fam, f"tpr_at_{t}s")] for v in runs.values() if (fam, f"tpr_at_{t}s") in v]) for t in TAUS]
W7 = load_deadline(W7DL, r"E4_L_")
# valid revision runs
rows = [r for r in csv.DictReader(open(QUEUE)) if re.match(r"R\d_", r["name"])]
chosen = {}
for r in rows:
    if r["vm_valid"] == "True" and r["laptop_valid"] == "True": chosen[re.sub(r"(_r\d+)[b-g]$", r"\1", r["name"])] = r["name"]
def rev_runs(prefix):
    out = {}
    for b, n in chosen.items():
        if b.startswith(prefix):
            d = {}
            for r in csv.DictReader(open(os.path.join(RUNS, n, "deadline.csv"))):
                if r["value"] != "": d[(r["family"], r["metric"])] = float(r["value"])
            out[n] = d
    return out
fig, axs = plt.subplots(1, 2, figsize=(7.0, 2.9), sharey=True)
for ax, cond, w7c, title in ((axs[0], "1p2S", "1p2S", "Constant overload, 1.2 S"), (axs[1], "burst", "burst", "Burst, 90 s at 1.5 S")):
    series = [("full", {k: v for k, v in W7.items() if f"_full_{w7c}_" in k}),
              ("light", {k: v for k, v in W7.items() if f"_light_{w7c}_" in k}),
              ("adapt", rev_runs(f"R2_L_adapt_{cond}_")), ("prop", rev_runs(f"R2_L_prop_{cond}_")),
              ("adaptrf", rev_runs(f"R2_L_adaptrf_{cond}_"))]
    for k, runs in series:
        ax.plot(TAUS, curve(runs), LS[k], marker=MK[k], color=COL[k], lw=2 if k != "light" else 1.6, ms=5, label=LAB[k])
    ax.set_xscale("log"); ax.set_xticks(TAUS); ax.set_xticklabels([str(t) for t in TAUS])
    ax.set_ylim(0, 1.02); ax.grid(True, color=GRID, lw=0.6); ax.set_axisbelow(True)
    ax.axvline(5, color="#8a8984", lw=0.8, ls=":"); ax.set_title(title, fontsize=9.5, color="#0b0b0b")
    ax.set_xlabel("Deadline τ (s)")
axs[0].set_ylabel("Share of attack flows detected\nwithin τ (TPR@τ)")
h, l = axs[0].get_legend_handles_labels()
fig.legend(h, l, loc="lower center", ncol=3, frameon=False, fontsize=8.2, bbox_to_anchor=(0.5, -0.02))
fig.tight_layout(rect=(0, 0.13, 1, 1))
fig.savefig(os.path.join(OUTD, "fig3_tpr_deadline.png"), dpi=300); plt.close(fig)

# evasion: hidden-episode detection per policy, five placements
ev = collections.defaultdict(list)
for b, n in chosen.items():
    if b.startswith("R3_"):
        pol = b.split("_")[2]; d = {}
        for r in csv.DictReader(open(os.path.join(RUNS, n, "evasion.csv"))):
            if r["value"] != "": d[(r["part"], r["metric"])] = float(r["value"])
        ev[pol].append(d)
order = ["full", "adapt", "adapts", "prop", "adaptrf"]
fig, axs = plt.subplots(1, 2, figsize=(7.0, 2.8), sharey=True)
for ax, (part, title) in zip(axs, (("control", "Control episode (before the flood)"), ("attack", "Attack episode (during the flood)"))):
    for i, k in enumerate(order):
        for j, (m, off, filled) in enumerate((("tpr_at_10s", -0.13, True), ("tpr_eventual", 0.13, False))):
            v = [d[(part, m)] for d in ev[k]]
            xs = [i + off] * len(v)
            ax.scatter(xs, v, s=22, marker=MK[k], facecolor=COL[k] if filled else "white", edgecolor=COL[k], lw=1.2, zorder=3, alpha=0.9)
            ax.plot([i + off - 0.09, i + off + 0.09], [st.median(v)] * 2, color="#0b0b0b", lw=1.4, zorder=4)
    ax.set_xticks(range(len(order))); ax.set_xticklabels(["Static-\nFull", "Adaptive\nLR", "Adaptive\n+10% Full", "Propor-\ntional LR", "Adaptive\nRF"], fontsize=7.6)
    ax.set_ylim(-0.04, 1.06); ax.grid(True, axis="y", color=GRID, lw=0.6); ax.set_axisbelow(True)
    ax.set_title(title, fontsize=9.5, color="#0b0b0b")
axs[0].set_ylabel("Share of the episode's\nbotnet flows detected")
from matplotlib.lines import Line2D
hl = [Line2D([], [], marker="o", ls="", mfc="#52514e", mec="#52514e", label="within 10 s"),
      Line2D([], [], marker="o", ls="", mfc="white", mec="#52514e", label="eventually"),
      Line2D([], [], color="#0b0b0b", lw=1.4, label="median of 5 placements")]
fig.legend(handles=hl, loc="lower center", ncol=3, frameon=False, fontsize=8.2, bbox_to_anchor=(0.5, -0.02))
fig.tight_layout(rect=(0, 0.1, 1, 1))
fig.savefig(os.path.join(OUTD, "fig4_evasion.png"), dpi=300); plt.close(fig)
print("ok")
