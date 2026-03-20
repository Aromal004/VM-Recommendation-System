# main.py  (updated — adds independent evaluation metrics)
import pandas as pd

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify
from evaluation.metrics import evaluate_all          # NEW

# Load dataset
df = pd.read_csv("/home/aromal/VM-Recommendation-System/AWS/two-stage recommender/aws_with_coremark.csv")

# Feature engineering
df = add_features(df)

# Baseline compute per core (median approx)
baseline_coremark_per_core = 27000

# Workload requirements
requirements = {
    "required_compute": 16 * baseline_coremark_per_core,
    "memory_gib": 64,
    "network_mbps": 25000,
    "max_price": 10.0
}

# Stage 1 – Hard filter
df = hard_filter(df, requirements)

if df.empty:
    raise ValueError("No instances satisfy hard constraints")

# Stage 2 – Fit scoring
df = add_fit_score(df, requirements)

# Keep a reference to the full scored pool for NDCG ideal baseline  # NEW
scored_pool = df.copy()                                               # NEW

# Stage 3 – Bayesian weight optimization
weights = optimize_weights(df)
print("\nOptimized Weights:", weights)

# Stage 4 – Final ranking
ranked = rank_instances(df, weights)

# Stage 5 – Diversification
final = diversify(ranked, per_family=2, top_n=10)

print("\n🏆 FINAL EC2 RECOMMENDATIONS\n")
print(
    final[
        [
            "instanceType",
            "physicalProcessor",
            "vcpu",
            "compute_score",
            "memory_gib",
            "network_mbps",
            "price_per_hr",
            "perf_per_dollar",
            "final_score"
        ]
    ]
)

# Stage 6 – Independent evaluation metrics                           # NEW
metrics = evaluate_all(final, scored_pool, requirements, k=5)        # NEW
print("\n📊 INDEPENDENT EVALUATION METRICS")                         # NEW
print(f"  NDCG@5              : {metrics['ndcg_at_k']}")             # NEW
print(f"  Precision@5         : {metrics['precision_at_k']}")        # NEW
print(f"  Cost savings (%)    : {metrics['cost_savings_pct']}%")     # NEW
print(f"  Right-sizing error  : {metrics['right_sizing_error']}")    # NEW