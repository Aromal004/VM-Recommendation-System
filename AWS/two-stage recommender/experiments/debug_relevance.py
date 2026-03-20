"""
Quick diagnostic: print top-5 picks and their relevance grades for each method.
Run from project root after multi_run.py has completed at least once.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify
from baselines.baseline_methods import run_all_baselines
from evaluation.metrics import compute_relevance

REQUIREMENTS = {
    "required_compute": 16 * 27000,
    "memory_gib": 64,
    "network_mbps": 25000,
    "max_price": 10.0,
}

df_raw = add_features(pd.read_csv(
    "/home/aromal/VM-Recommendation-System/AWS/two-stage recommender/aws_with_coremark.csv"
))
pool_df = hard_filter(df_raw, REQUIREMENTS)
pool_df = add_fit_score(pool_df, REQUIREMENTS)
equal_w  = {"fit": 1/3, "cost": 1/3, "generation": 1/3}
pool_ranked = rank_instances(pool_df, equal_w)

# Pool-level thresholds (what relevance grading sees)
price_thresh = np.percentile(pool_ranked["price_per_hr"].values, 80)
fit_thresh   = np.percentile(pool_ranked["fit_score"].values, 20)
print(f"Pool price_per_hr 80th pct : {price_thresh:.4f}")
print(f"Pool fit_score    20th pct : {fit_thresh:.6f}")
print(f"Pool size: {len(pool_ranked)}\n")

# Proposed
weights = optimize_weights(pool_df, top_k=10, n_calls=30, random_state=42)
ranked  = rank_instances(pool_df, weights)
final   = diversify(ranked, per_family=2, top_n=10)

# Baselines
baselines = run_all_baselines(pool_ranked, top_n=10, seed=42)

all_methods = {"Proposed": final, **baselines}

for name, top_k in all_methods.items():
    top5 = top_k.head(5)[["instanceType", "price_per_hr", "fit_score"]].copy()
    rel  = compute_relevance(top5, REQUIREMENTS)
    top5["relevance"] = rel
    print(f"=== {name} top-5 ===")
    print(top5.to_string(index=False))
    print()