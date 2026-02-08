import pandas as pd

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify

# Load data
df = pd.read_csv("C:/Users/USER/Desktop/Final Project/AWS/aws_ec2_full_dataset.csv")
df = df[df["price_per_hr"] > 0]

df = add_features(df)

# 🔧 WORKLOAD REQUIREMENTS
requirements = {
    "vcpu": 16,
    "memory_gib": 64,
    "network_mbps": 25000,
    "max_price": 10.0
}

# Stage 1
df = hard_filter(df, requirements)
if df.empty:
    raise ValueError("No instances satisfy hard constraints")

# Stage 2
df = add_fit_score(df, requirements)

# Stage 3
weights = optimize_weights(df)
print("\nOptimized Weights:", weights)

ranked = rank_instances(df, weights)

# Stage 4
final = diversify(ranked, per_family=2, top_n=10)

print("\n🏆 FINAL EC2 RECOMMENDATIONS\n")
print(
    final[
        ["instanceType", "vcpu", "memory_gib", "network_mbps", "price_per_hr", "final_score"]
    ]
)
