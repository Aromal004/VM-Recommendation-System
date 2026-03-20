"""
experiments/arch_comparison.py
--------------------------------
Architecture comparison: Intel (c6i) vs AMD (c6a) vs ARM Graviton.

Network constraint is dropped for this experiment since Graviton instances
in the dataset don't meet the 25 Gbps threshold — this is noted in results.
fit_score is recomputed without the network penalty for fair comparison.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter

DATA_PATH = "/home/aromal/VM-Recommendation-System/AWS/two-stage recommender/aws_with_coremark.csv"

REQUIREMENTS_NO_NET = {
    "required_compute": 16 * 27000,
    "memory_gib":       64,
    "network_mbps":     0,       # dropped for arch comparison
    "max_price":        10.0,
}

def add_fit_score_no_net(df, req):
    """fit_score using only compute and memory penalty (no network term)."""
    df = df.copy()
    compute_penalty = ((df["compute_score"] - req["required_compute"]) / req["required_compute"]).clip(lower=0)
    mem_penalty     = ((df["memory_gib"]    - req["memory_gib"])       / req["memory_gib"]).clip(lower=0)
    df["fit_score"] = 1 / (1 + compute_penalty + mem_penalty)
    return df

df_raw = add_features(pd.read_csv(DATA_PATH))
pool   = hard_filter(df_raw, REQUIREMENTS_NO_NET)
pool   = add_fit_score_no_net(pool, REQUIREMENTS_NO_NET)

# Find best available ARM family (prefer compute: c6g > m6g > others)
arm_candidates = [f for f in pool["family"].unique()
                  if any(f.startswith(p) for p in ["c6g","c7g","m6g","m7g","r6g","r7g"])]
arm_family = next((f for f in ["c6g","c7g","m6g","m7g"] if f in arm_candidates), 
                  arm_candidates[0] if arm_candidates else None)

print(f"Available ARM families: {arm_candidates}")
print(f"Selected ARM family: {arm_family}\n")

ARCH_MAP = {
    "c6i": "Intel (c6i)",
    "c6a": "AMD (c6a)",
}
if arm_family:
    ARCH_MAP[arm_family] = f"ARM Graviton ({arm_family})"

print(f"{'Architecture':<26} {'Instances':>10} {'Avg perf/dollar':>16} "
      f"{'Avg price/hr':>13} {'Avg fit_score':>14} {'Avg compute_score':>18}")
print("-" * 92)

records = []
for family, arch_name in ARCH_MAP.items():
    subset = pool[pool["family"] == family]
    if subset.empty:
        print(f"{arch_name:<26} {'not found':>10}")
        continue
    rec = {
        "Architecture":      arch_name,
        "Instance count":    len(subset),
        "Avg perf/dollar":   round(subset["perf_per_dollar"].mean(), 2),
        "Avg price/hr":      round(subset["price_per_hr"].mean(), 4),
        "Avg fit_score":     round(subset["fit_score"].mean(), 4),
        "Avg compute_score": round(subset["compute_score"].mean(), 0),
    }
    records.append(rec)
    print(f"{arch_name:<26} {rec['Instance count']:>10} "
          f"{rec['Avg perf/dollar']:>16.2f} {rec['Avg price/hr']:>13.4f} "
          f"{rec['Avg fit_score']:>14.4f} {rec['Avg compute_score']:>18.0f}")

print("\nNote: network_mbps constraint removed for this comparison.")
print("      Graviton instances do not meet the 25 Gbps network requirement")
print("      of the main workload, so they are excluded from primary recommendations.")

df_out = pd.DataFrame(records)
df_out.to_csv(os.path.join(os.path.dirname(__file__), "arch_comparison_results.csv"), index=False)
print("\nSaved to experiments/arch_comparison_results.csv")