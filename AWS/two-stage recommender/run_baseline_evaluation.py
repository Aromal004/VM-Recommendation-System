"""
run_baseline_evaluation.py
--------------------------
Standalone script to run baseline evaluation and generate comprehensive results.
Compares the proposed NEW workload-aware BO pipeline against all baseline methods.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from optimization.workload_aware_bo import (
    optimize_resource_weights,
    add_workload_aware_fit_score,
    FIXED_OUTER_WEIGHTS,
)
from postprocessing.diversify import diversify
from baselines.baseline_methods import run_all_baselines
from evaluation.metrics import evaluate_all

print("=" * 80)
print("BASELINE EVALUATION - EC2 RECOMMENDATION PIPELINE")
print("=" * 80)
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# ============================================================================
# 1. Load and prepare data
# ============================================================================
print("1. Loading dataset...")
try:
    df = pd.read_csv("combined_vms.csv")
    print(f"   ✓ Loaded {len(df):,} instances from combined_vms.csv")
except Exception as e:
    print(f"   ✗ Failed to load data: {e}")
    sys.exit(1)

# ============================================================================
# 2. Define workload requirements
# ============================================================================
REQUIREMENTS = {
    "required_compute": 16 * 27000,  # 16 vCPUs × 27000 compute units
    "memory_gib": 64,
    "network_mbps": 25000,
    "max_price": 10.0,
}

print("\n2. Workload requirements:")
print(f"   - Compute: {REQUIREMENTS['required_compute']:,} units")
print(f"   - Memory: {REQUIREMENTS['memory_gib']} GiB")
print(f"   - Network: {REQUIREMENTS['network_mbps']:,} Mbps")
print(f"   - Max price: ${REQUIREMENTS['max_price']}/hr")

# ============================================================================
# 3. Preprocess data
# ============================================================================
print("\n3. Preprocessing...")
df = add_features(df)
print(f"   ✓ Feature engineering complete")

# Sanitize pricing data
df = df[df["price_per_hr"] >= 0.01].copy()
print(f"   ✓ Removed price outliers: {len(df):,} instances remain")

# Apply hard filter
filtered = hard_filter(df, REQUIREMENTS)
print(f"   ✓ Hard filter: {len(df):,} → {len(filtered):,} instances")

if len(filtered) == 0:
    print("   ✗ No instances satisfy constraints!")
    sys.exit(1)

# ============================================================================
# 4. Run proposed method (NEW workload-aware BO)
# ============================================================================
print("\n4. Running proposed method (NEW workload-aware BO)...")
try:
    # Learn penalty weights with workload-aware BO
    penalty_weights = optimize_resource_weights(
        filtered, REQUIREMENTS,
        top_k=10,
        n_calls=30,
        outer_weights=FIXED_OUTER_WEIGHTS,
        verbose=True
    )
    print(f"   ✓ BO optimization complete")
    print(f"     - α (compute penalty): {penalty_weights.alpha:.4f}")
    print(f"     - β (memory penalty): {penalty_weights.beta:.4f}")
    print(f"     - γ (network penalty): {penalty_weights.gamma:.4f}")
    
    # Apply learned penalty weights to compute fit_score
    scored = add_workload_aware_fit_score(filtered, REQUIREMENTS, penalty_weights)
    
    # Helper function for minmax normalization
    def _minmax(series):
        lo, hi = series.min(), series.max()
        if hi == lo:
            return pd.Series(1.0, index=series.index)
        return (series - lo) / (hi - lo)
    
    # Rank with fixed outer weights
    scored["final_score"] = (
        FIXED_OUTER_WEIGHTS["fit"]        * _minmax(scored["fit_score"])
        + FIXED_OUTER_WEIGHTS["cost"]     * _minmax(scored["perf_per_dollar"])
        + FIXED_OUTER_WEIGHTS["generation"] * _minmax(scored["generation_score"])
    )
    ranked = scored.sort_values("final_score", ascending=False)
    final = diversify(ranked, per_family=2, top_n=10)
    print(f"   ✓ Ranking and diversification complete: {len(final)} recommendations")
    
    proposed_metrics = evaluate_all(final, ranked, REQUIREMENTS, k=5)
    print(f"   ✓ Evaluation complete")
    
except Exception as e:
    print(f"   ✗ Proposed method failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# 5. Run baseline methods
# ============================================================================
print("\n5. Running baseline methods...")
try:
    # Use the same ranked pool for baselines
    baselines = run_all_baselines(ranked, top_n=10, seed=42)
    baseline_metrics = {}
    
    for name, result in baselines.items():
        metrics = evaluate_all(result, ranked, REQUIREMENTS, k=5)
        baseline_metrics[name] = metrics
        print(f"   ✓ {name:<12} evaluated")
    
except Exception as e:
    print(f"   ✗ Baselines failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# 6. Compile results
# ============================================================================
print("\n6. Compiling results...")

results = []

# Add proposed method
results.append({
    "Method": "Proposed",
    "NDCG@5": proposed_metrics["ndcg_at_k"],
    "Precision@5": proposed_metrics["precision_at_k"],
    "Cost Savings (%)": proposed_metrics["cost_savings_pct"],
    "Right-sizing Error": proposed_metrics["right_sizing_error"],
})

# Add baselines
for name, metrics in baseline_metrics.items():
    results.append({
        "Method": name,
        "NDCG@5": metrics["ndcg_at_k"],
        "Precision@5": metrics["precision_at_k"],
        "Cost Savings (%)": metrics["cost_savings_pct"],
        "Right-sizing Error": metrics["right_sizing_error"],
    })

results_df = pd.DataFrame(results)

# ============================================================================
# 7. Display results
# ============================================================================
print("\n" + "=" * 80)
print("BASELINE EVALUATION RESULTS")
print("=" * 80)
print()
print(results_df.to_string(index=False))
print()

# ============================================================================
# 8. Compute relative improvements
# ============================================================================
print("=" * 80)
print("RELATIVE IMPROVEMENTS (Proposed vs Baselines)")
print("=" * 80)
print()

proposed_rse = proposed_metrics["right_sizing_error"]
proposed_cost = proposed_metrics["cost_savings_pct"]
proposed_ndcg = proposed_metrics["ndcg_at_k"]

for name, metrics in baseline_metrics.items():
    baseline_rse = metrics["right_sizing_error"]
    baseline_cost = metrics["cost_savings_pct"]
    baseline_ndcg = metrics["ndcg_at_k"]
    
    # Right-sizing improvement (lower is better)
    if baseline_rse > 0:
        rse_improvement = ((baseline_rse - proposed_rse) / baseline_rse) * 100
        rse_factor = baseline_rse / proposed_rse if proposed_rse > 0 else float('inf')
    else:
        rse_improvement = 0
        rse_factor = 1
    
    # Cost savings difference
    cost_diff = proposed_cost - baseline_cost
    
    # NDCG difference
    ndcg_diff = proposed_ndcg - baseline_ndcg
    
    print(f"Proposed vs {name}:")
    print(f"  Right-sizing error: {proposed_rse:.4f} vs {baseline_rse:.4f}")
    print(f"    → {rse_improvement:+.1f}% improvement ({rse_factor:.1f}x better)")
    print(f"  Cost savings: {proposed_cost:.2f}% vs {baseline_cost:.2f}%")
    print(f"    → {cost_diff:+.2f} percentage points")
    print(f"  NDCG@5: {proposed_ndcg:.4f} vs {baseline_ndcg:.4f}")
    print(f"    → {ndcg_diff:+.4f} difference")
    print()

# ============================================================================
# 9. Save results
# ============================================================================
output_file = "baseline_evaluation_results.csv"
results_df.to_csv(output_file, index=False)
print("=" * 80)
print(f"Results saved to: {output_file}")
print("=" * 80)

# ============================================================================
# 10. Display top recommendations
# ============================================================================
print("\n" + "=" * 80)
print("TOP 5 RECOMMENDATIONS (Proposed Method)")
print("=" * 80)
print()

top_5 = final.head(5)[[
    "provider", "instanceType", "vcpu", "memory_gib", 
    "network_mbps", "price_per_hr", "final_score"
]]
print(top_5.to_string(index=False))
print()

print("=" * 80)
print("EVALUATION COMPLETE ✓")
print("=" * 80)

# Made with Bob
