"""
experiments/scalability.py
---------------------------
Scalability analysis: measures pipeline runtime as instance pool size grows.
Tests: 100, 500, 1000, 5000, full pool
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify

REQUIREMENTS = {"required_compute": 16*27000, "memory_gib": 64,
                "network_mbps": 25000, "max_price": 10.0}
SIZES = [100, 500, 1000, 5000, None]  # None = full pool

df_raw = add_features(pd.read_csv(
    "/home/aromal/VM-Recommendation-System/AWS/two-stage recommender/aws_with_coremark.csv"
))
pool = hard_filter(df_raw, REQUIREMENTS)
pool = add_fit_score(pool, REQUIREMENTS)
print(f"Full pool after hard filter: {len(pool)} instances\n")

records = []
print(f"{'Instances':>10}  {'Runtime (s)':>12}  {'BO time (s)':>12}")
print("-" * 40)
for size in SIZES:
    subset = pool.sample(n=size, random_state=42) if size else pool.copy()

    t0 = time.perf_counter()
    t_bo = time.perf_counter()
    weights = optimize_weights(subset, top_k=10, n_calls=30, random_state=42)
    bo_time = time.perf_counter() - t_bo

    ranked = rank_instances(subset, weights)
    diversify(ranked, per_family=2, top_n=10)
    total = time.perf_counter() - t0

    label = size if size else len(pool)
    records.append({"Instances": label, "Runtime (s)": round(total, 2),
                    "BO time (s)": round(bo_time, 2)})
    print(f"{label:>10}  {total:>12.2f}  {bo_time:>12.2f}")

df_out = pd.DataFrame(records)
df_out.to_csv(os.path.join(os.path.dirname(__file__), "scalability_results.csv"), index=False)
print("\nSaved to experiments/scalability_results.csv")