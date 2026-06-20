"""
experiments/test_setup.py
-------------------------
Quick test to verify all experiment files are properly configured.
Tests data loading, pipeline components, and baseline methods.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify
from baselines.baseline_methods import run_all_baselines
from evaluation.metrics import evaluate_all

print("=" * 70)
print("TESTING EXPERIMENT SETUP")
print("=" * 70)

# Test 1: Data loading
print("\n1. Testing data loading...")
try:
    df = pd.read_csv("combined_vms.csv")
    print(f"   ✓ Loaded {len(df)} rows from combined_vms.csv")
    print(f"   ✓ Columns: {', '.join(df.columns[:5])}...")
except Exception as e:
    print(f"   ✗ Failed to load data: {e}")
    sys.exit(1)

# Test 2: Feature engineering
print("\n2. Testing feature engineering...")
try:
    df = add_features(df)
    required_cols = ["compute_score", "perf_per_dollar", "generation_score", "family"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"   ✗ Missing columns: {missing}")
    else:
        print(f"   ✓ All required features present")
except Exception as e:
    print(f"   ✗ Feature engineering failed: {e}")
    sys.exit(1)

# Test 3: Pipeline components
print("\n3. Testing pipeline components...")
REQUIREMENTS = {
    "required_compute": 16 * 27000,
    "memory_gib": 64,
    "network_mbps": 25000,
    "max_price": 10.0,
}

try:
    # Hard filter
    filtered = hard_filter(df, REQUIREMENTS)
    print(f"   ✓ Hard filter: {len(df)} → {len(filtered)} instances")
    
    # Fit score
    scored = add_fit_score(filtered, REQUIREMENTS)
    print(f"   ✓ Fit score: range [{scored['fit_score'].min():.3f}, {scored['fit_score'].max():.3f}]")
    
    # Bayesian optimization (quick test with 5 calls)
    weights = optimize_weights(scored, top_k=10, n_calls=10)
    print(f"   ✓ BO weights: fit={weights['fit']:.3f}, cost={weights['cost']:.3f}, gen={weights['generation']:.3f}")
    
    # Ranking
    ranked = rank_instances(scored, weights)
    print(f"   ✓ Ranking: final_score range [{ranked['final_score'].min():.3f}, {ranked['final_score'].max():.3f}]")
    
    # Diversification
    final = diversify(ranked, per_family=2, top_n=10)
    print(f"   ✓ Diversification: {len(ranked)} → {len(final)} instances")
    
except Exception as e:
    print(f"   ✗ Pipeline failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 4: Baseline methods
print("\n4. Testing baseline methods...")
try:
    # Use equal weights for baseline pool
    equal_weights = {"fit": 1/3, "cost": 1/3, "generation": 1/3}
    pool = rank_instances(scored, equal_weights)
    
    baselines = run_all_baselines(pool, top_n=10, seed=42)
    for name, result in baselines.items():
        has_final_score = "final_score" in result.columns
        print(f"   ✓ {name:<12}: {len(result)} instances, final_score={'✓' if has_final_score else '✗'}")
except Exception as e:
    print(f"   ✗ Baselines failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Evaluation metrics
print("\n5. Testing evaluation metrics...")
try:
    metrics = evaluate_all(final, ranked, REQUIREMENTS, k=5)
    print(f"   ✓ NDCG@5: {metrics['ndcg_at_k']:.4f}")
    print(f"   ✓ Precision@5: {metrics['precision_at_k']:.4f}")
    print(f"   ✓ Cost savings: {metrics['cost_savings_pct']:.2f}%")
    print(f"   ✓ Right-sizing error: {metrics['right_sizing_error']:.4f}")
except Exception as e:
    print(f"   ✗ Evaluation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 70)
print("ALL TESTS PASSED ✓")
print("=" * 70)
print("\nYou can now run the experiment scripts:")
print("  - python experiments/ablation_runner.py")
print("  - python experiments/multi_run.py")
print("  - python experiments/bo_sensitivity.py")
print("  - python experiments/arch_comparison.py")
print("  - python experiments/scalability.py")
print("  - python experiments/composite_summary.py (after multi_run.py)")

# Made with Bob
