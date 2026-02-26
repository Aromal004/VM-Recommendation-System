import pandas as pd

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify

# Load dataset
df = pd.read_csv("C:/Users/USER/Desktop/Final Project/AWS/aws_with_coremark.csv")

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