"""Label mapping for the corrected CSE-CIC-IDS2018 dataset (draft, W4 Tuesday).

Every original label maps to:
  family  - attack family, used to report recall per family
  binary  - 0 = Benign, 1 = Attack, None = excluded from training/main evaluation
The "- Attempted" labels (attack traffic that did not reach or affect the victim,
Liu et al. [13]) are excluded from the main experiments and analysed separately.
"""

LABEL_MAP = {
    # label                                           (family,        binary)
    "BENIGN":                                         ("Benign",       0),
    "DoS Hulk":                                       ("DoS",          1),
    "DoS GoldenEye":                                  ("DoS",          1),
    "DoS Slowloris":                                  ("DoS",          1),
    "DDoS-HOIC":                                      ("DDoS",         1),
    "DDoS-LOIC-HTTP":                                 ("DDoS",         1),
    "DDoS-LOIC-UDP":                                  ("DDoS",         1),
    "SSH-BruteForce":                                 ("BruteForce",   1),
    "Web Attack - Brute Force":                       ("Web",          1),
    "Web Attack - XSS":                               ("Web",          1),
    "Web Attack - SQL":                               ("Web",          1),
    "Infiltration - NMAP Portscan":                   ("Infiltration", 1),
    "Infiltration - Communication Victim Attacker":   ("Infiltration", 1),
    "Infiltration - Dropbox Download":                ("Infiltration", 1),
    "Botnet Ares":                                    ("Botnet",       1),
}


def map_label(label):
    """Return (family, binary). Attempted labels -> ('Attempted', None)."""
    if label.endswith("- Attempted"):
        return ("Attempted", None)
    if label not in LABEL_MAP:
        raise KeyError(f"Unknown label: {label!r}")
    return LABEL_MAP[label]


# Columns that are never used as model input
ID_COLS = ["id", "Flow ID", "Src IP", "Src Port", "Dst IP", "Timestamp"]
TARGET_COLS = ["Label", "Attempted Category"]
CONSTANT_COLS = ["Bwd URG Flags"]
UNDECIDED_COLS = ["Dst Port", "Subflow Fwd Packets", "Subflow Bwd Packets"]  # decide Wed 21 Oct
