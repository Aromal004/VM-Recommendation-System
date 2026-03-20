"""
experiments/bo_sensitivity.py
------------------------------
Bayesian Optimisation sensitivity analysis.

Experiment 1 — Iteration study
    How does recommendation quality (NDCG@5) change as BO iterations increase?
    Tests: n_calls ∈ {5, 10, 20, 30, 45, 60}

Experiment 2 — Weight stability
    Are the optimised weights stable across different random seeds?
    Tests: seeds 0–9 at fixed n_calls = 30

Usage
-----
  python experiments/bo_sensitivity.py

Output
------
  bo_iteration_study.csv  — score vs iterations
  bo_weight_stability.csv — weight values per seed
"""

import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify
from evaluation.metrics import evaluate_all


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_PATH = "/home/aromal/VM-Recommendation-System/AWS/two-stage recommender/aws_with_coremark.csv"
BASELINE_COREMARK_PER_CORE = 27_000

REQUIREMENTS = {
    "required_compute": 16 * BASELINE_COREMARK_PER_CORE,
    "memory_gib":       64,
    "network_mbps":     25_000,
    "max_price":        10.0,
}

TOP_K = 10
EVAL_K = 5

ITERATION_GRID = [5, 10, 20, 30, 45, 60]
SEED_GRID      = list(range(10))
FIXED_CALLS    = 30


# ---------------------------------------------------------------------------
# Shared setup: load + filter + score once
# ---------------------------------------------------------------------------

def prepare_pool(data_path: str) -> pd.DataFrame:
    df = add_features(pd.read_csv(data_path))
    df = hard_filter(df, REQUIREMENTS)
    df = add_fit_score(df, REQUIREMENTS)
    if df.empty:
        raise ValueError("No instances survive the hard filter.")
    return df


# ---------------------------------------------------------------------------
# Experiment 1: iteration study
# ---------------------------------------------------------------------------

def bo_iteration_study(pool: pd.DataFrame,
                       iteration_grid: list = ITERATION_GRID,
                       seed: int = 42) -> pd.DataFrame:
    """
    Run BO with each n_calls value and record NDCG@K.
    Uses a single fixed seed so the only variable is the number of iterations.
    """
    records = []
    for n in iteration_grid:
        print(f"  n_calls = {n:3d} ...", end="  ")
        weights = optimize_weights(pool, top_k=TOP_K,
                                   n_calls=n, random_state=seed)
        ranked  = rank_instances(pool, weights)
        final   = diversify(ranked, per_family=2, top_n=TOP_K)
        metrics = evaluate_all(final, ranked, REQUIREMENTS, k=EVAL_K)
        records.append({
            "n_calls":   n,
            "NDCG@5":    metrics["ndcg_at_k"],
            "Precision@5": metrics["precision_at_k"],
            "w_fit":     round(weights["fit"], 4),
            "w_cost":    round(weights["cost"], 4),
            "w_generation": round(weights["generation"], 4),
        })
        print(f"NDCG={metrics['ndcg_at_k']:.4f}")
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Experiment 2: weight stability across seeds
# ---------------------------------------------------------------------------

def bo_weight_stability(pool: pd.DataFrame,
                        seed_grid: list = SEED_GRID,
                        n_calls: int = FIXED_CALLS) -> pd.DataFrame:
    """
    Run BO with fixed n_calls but varying random seeds.
    Shows how stable the optimised weights are across initialisations.
    """
    records = []
    for seed in seed_grid:
        print(f"  seed = {seed:2d} ...", end="  ")
        weights = optimize_weights(pool, top_k=TOP_K,
                                   n_calls=n_calls, random_state=seed)
        ranked  = rank_instances(pool, weights)
        final   = diversify(ranked, per_family=2, top_n=TOP_K)
        metrics = evaluate_all(final, ranked, REQUIREMENTS, k=EVAL_K)
        records.append({
            "seed":          seed,
            "w_fit":         round(weights["fit"], 4),
            "w_cost":        round(weights["cost"], 4),
            "w_generation":  round(weights["generation"], 4),
            "NDCG@5":        metrics["ndcg_at_k"],
        })
        print(f"w_fit={weights['fit']:.3f}  w_cost={weights['cost']:.3f}  "
              f"w_gen={weights['generation']:.3f}  NDCG={metrics['ndcg_at_k']:.4f}")
    return pd.DataFrame(records)


def weight_stability_summary(stability_df: pd.DataFrame) -> dict:
    """Mean and std of each weight dimension."""
    dims = ["w_fit", "w_cost", "w_generation"]
    return {
        d: f"{stability_df[d].mean():.4f} ± {stability_df[d].std(ddof=1):.4f}"
        for d in dims
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    out_dir = os.path.dirname(__file__)

    print(f"\nLoading and preparing pool from:\n  {DATA_PATH}")
    pool = prepare_pool(DATA_PATH)
    print(f"Pool size after hard filter: {len(pool)} instances\n")

    # Experiment 1
    print("=" * 60)
    print("Experiment 1: BO iteration study")
    print("=" * 60)
    iter_df = bo_iteration_study(pool)
    iter_df.to_csv(os.path.join(out_dir, "bo_iteration_study.csv"), index=False)
    print("\n", iter_df[["n_calls", "NDCG@5", "Precision@5"]].to_string(index=False))

    # Experiment 2
    print("\n" + "=" * 60)
    print("Experiment 2: Weight stability across seeds")
    print("=" * 60)
    stab_df = bo_weight_stability(pool)
    stab_df.to_csv(os.path.join(out_dir, "bo_weight_stability.csv"), index=False)

    summary = weight_stability_summary(stab_df)
    print("\nWeight stability summary (mean ± std across 10 seeds):")
    for dim, val in summary.items():
        print(f"  {dim:<16}: {val}")

    ndcg_std = stab_df["NDCG@5"].std(ddof=1)
    print(f"\n  NDCG@5 variance across seeds: std = {ndcg_std:.4f}")
    if ndcg_std < 0.02:
        print("  → Weights are STABLE (std < 0.02): BO converges consistently.")
    else:
        print("  → Consider increasing n_calls for tighter convergence.")