import pandas as pd

from preprocessing.feature_engineering import add_features
from preprocessing.normalization import normalize
from scoring.scorer import score_vms
from optimization.bayesian_optimizer import optimize_weights
from config.workload_constraints import WORKLOAD_CONSTRAINTS
from preprocessing.workload_filter import filter_vms_by_workload

# Load AWS data
df = pd.read_csv("C:/Users/USER/Desktop/Final Project/AWS/aws_ec2_full_dataset.csv")

# Drop rows with missing prices
df = df[df["price_per_hr"] > 0]

# USER INPUT
workload = "network_intensive"

# Feature engineering first (to get numeric memory/network)
df = add_features(df)

# FILTER VMS BASED ON WORKLOAD
df = filter_vms_by_workload(
    df,
    WORKLOAD_CONSTRAINTS[workload]
)

if df.empty:
    raise ValueError("No VM satisfies the workload requirements")

# Normalize AFTER filtering
df = normalize(df, [
    "compute_score",
    "memory_score",
    "network_score",
    "cost_efficiency"
])

# Bayesian optimization (filtered data only)
best_weights = optimize_weights(df, score_vms)

print("\n🔍 Optimized Weights (learned via Bayesian Optimization)")
for k, v in best_weights.items():
    print(f"{k}: {v:.3f}")

# Final ranking
ranked = score_vms(df, best_weights)

print("\n🏆 Top EC2 Recommendations")
print(
    ranked[
        ["instanceType", "vcpu", "memory", "networkPerformance", "price_per_hr", "final_score"]
    ].head(250)
)