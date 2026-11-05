#!/usr/bin/env python3
"""Create a small synthetic flow-record CSV for pipeline testing only (not for experiments)."""
import csv, random, sys

n = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
random.seed(42)
cols = ["Dst Port", "Protocol", "Flow Duration", "Tot Fwd Pkts", "Tot Bwd Pkts",
        "TotLen Fwd Pkts", "TotLen Bwd Pkts", "Flow Pkts/s", "Label"]
w = csv.writer(sys.stdout)
w.writerow(cols)
for _ in range(n):
    attack = random.random() < 0.2
    w.writerow([random.choice([80, 443, 22, 53, 8080]), random.choice([6, 17]),
                random.randint(1, 10_000_000), random.randint(1, 200), random.randint(0, 200),
                random.randint(0, 50_000), random.randint(0, 50_000), round(random.uniform(0, 5000), 3),
                "Attack" if attack else "Benign"])
