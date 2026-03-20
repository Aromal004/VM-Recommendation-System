"""
experiments/composite_summary.py
---------------------------------
Produces the final comparison table for the paper, combining all metrics
into a composite score that captures the three-way trade-off:
ranking quality (NDCG), cost efficiency, and workload fit (right-sizing).

Composite score = 0.4 * NDCG@5  +  0.4 * cost_efficiency  +  0.2 * fit_quality
where:
  cost_efficiency = cost_savings_pct / 100  (normalised to [0,1])
  fit_quality     = 1 / (1 + right_sizing_error)  (inverse, bounded to [0,1])

Weights reflect that NDCG and cost are the primary objectives,
fit quality is the tiebreaker.
"""

import pandas as pd
import numpy as np

# Load the multi-run statistical summary
raw = pd.read_csv("experiments/multi_run_results.csv")

methods = raw["method"].unique()
records = []

for method in methods:
    grp = raw[raw["method"] == method]
    ndcg_mean  = grp["ndcg_at_k"].mean()
    ndcg_std   = grp["ndcg_at_k"].std(ddof=1)
    prec_mean  = grp["precision_at_k"].mean()
    cost_mean  = grp["cost_savings_pct"].mean()
    cost_std   = grp["cost_savings_pct"].std(ddof=1)
    rse_mean   = grp["right_sizing_error"].mean()

    # Composite score components
    cost_eff   = max(cost_mean / 100, 0)          # normalise to [0,1]
    fit_qual   = 1 / (1 + rse_mean)               # inverse RSE, bounded (0,1]

    composite  = 0.4 * ndcg_mean + 0.4 * cost_eff + 0.2 * fit_qual

    records.append({
        "Method":              method,
        "NDCG@5":              f"{ndcg_mean:.4f} ± {ndcg_std:.4f}",
        "Precision@5":         f"{prec_mean:.4f}",
        "Cost savings (%)":    f"{cost_mean:.2f} ± {cost_std:.2f}",
        "Right-sizing error":  f"{rse_mean:.4f}",
        "Composite score":     round(composite, 4),
    })

df = pd.DataFrame(records).sort_values("Composite score", ascending=False)
df = df.reset_index(drop=True)

print("\n" + "=" * 90)
print("FINAL COMPARISON TABLE  (sorted by composite score)")
print("=" * 90)
print(df.to_string(index=False))
print("\nComposite = 0.4 × NDCG@5  +  0.4 × (cost_savings/100)  +  0.2 × (1/(1+RSE))")

df.to_csv("experiments/final_comparison_table.csv", index=False)
print("\nSaved to experiments/final_comparison_table.csv")


# ---------------------------------------------------------------------------
# Diversity audit — add unique family count to the table
# ---------------------------------------------------------------------------

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify
from baselines.baseline_methods import run_all_baselines
import pandas as pd

REQUIREMENTS = {"required_compute": 16*27000, "memory_gib": 64,
                "network_mbps": 25000, "max_price": 10.0}

df_raw  = add_features(pd.read_csv(
    next(p for p in [
        "aws_with_coremark.csv",
        "/home/aromal/VM-Recommendation-System/AWS/two-stage recommender/aws_with_coremark.csv"
    ] if os.path.exists(p))
))
pool    = hard_filter(df_raw, REQUIREMENTS)
pool    = add_fit_score(pool, REQUIREMENTS)
eq_w    = {"fit": 1/3, "cost": 1/3, "generation": 1/3}
pool_r  = rank_instances(pool, eq_w)

weights  = optimize_weights(pool, top_k=10, n_calls=30, random_state=42)
ranked   = rank_instances(pool, weights)
proposed = diversify(ranked, per_family=2, top_n=10)
bases    = run_all_baselines(pool_r, top_n=10, seed=42)

all_picks = {"Proposed": proposed, **bases}

print("\n" + "=" * 70)
print("DIVERSITY AUDIT — unique instance types and families in top-5")
print("=" * 70)
print(f"{'Method':<12} {'Unique types':>13} {'Unique families':>16}")
print("-" * 45)
for name, picks in all_picks.items():
    top5        = picks.head(5)
    u_types     = top5["instanceType"].nunique()
    u_families  = top5["family"].nunique()
    print(f"{name:<12} {u_types:>13} {u_families:>16}")