"""
experiments/ablation_runner_v2.py
----------------------------------
Ablation study comparing OLD vs NEW Bayesian optimization approaches.

Variants
--------
  full_v2           : NEW workload-aware BO (learns α,β,γ penalty weights)
  full_v1           : OLD BO (learns outer fit/cost/gen weights)
  no_filter         : skip hard_filter — all instances enter scoring
  no_fit_score      : replace fit_score with uniform 1.0
  no_bo             : equal penalty weights (α=β=γ=1.0) + fixed outer weights
  no_diversify      : skip family-diversity cap on final list

Key Comparison: full_v2 vs full_v1
-----------------------------------
The critical test is whether the new workload-aware BO (which learns
per-dimension penalty multipliers INSIDE fit_score) outperforms the old
approach (which learned outer weights AFTER fit_score collapsed dimensions).

Usage
-----
  python experiments/ablation_runner_v2.py

Output
------
  Console table + ablation_results_v2.csv saved to experiments/
"""

import sys
import os
import time
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from optimization.workload_aware_bo import (
    optimize_resource_weights,
    add_workload_aware_fit_score,
    WorkloadResourceWeights,
    FIXED_OUTER_WEIGHTS,
)
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify
from evaluation.metrics import evaluate_all


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_PATH = "combined_vms.csv"
BASELINE_COREMARK_PER_CORE = 27_000

REQUIREMENTS = {
    "required_compute": 16 * BASELINE_COREMARK_PER_CORE,
    "memory_gib":       64,
    "network_mbps":     25_000,
    "max_price":        10.0,
}

TOP_K   = 10
EVAL_K  = 5
BO_CALLS = 30


# ---------------------------------------------------------------------------
# Helper: minmax normalization
# ---------------------------------------------------------------------------

def _minmax(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series(1.0, index=series.index)
    return (series - lo) / (hi - lo)


# ---------------------------------------------------------------------------
# Pipeline variants
# ---------------------------------------------------------------------------

def load_and_engineer(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return add_features(df)


def run_full_v2(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """NEW: Workload-aware BO learning α,β,γ penalty weights."""
    t0 = time.perf_counter()
    
    df = hard_filter(df_raw, REQUIREMENTS)
    
    # Learn penalty weights with workload-aware BO
    penalty_weights = optimize_resource_weights(
        df, REQUIREMENTS, 
        top_k=TOP_K, 
        n_calls=BO_CALLS,
        outer_weights=FIXED_OUTER_WEIGHTS,
        verbose=False
    )
    
    # Apply learned penalty weights to compute fit_score
    df = add_workload_aware_fit_score(df, REQUIREMENTS, penalty_weights)
    
    # Rank with fixed outer weights
    df["final_score"] = (
        FIXED_OUTER_WEIGHTS["fit"]        * _minmax(df["fit_score"])
        + FIXED_OUTER_WEIGHTS["cost"]     * _minmax(df["perf_per_dollar"])
        + FIXED_OUTER_WEIGHTS["generation"] * _minmax(df["generation_score"])
    )
    ranked = df.sort_values("final_score", ascending=False)
    final  = diversify(ranked, per_family=2, top_n=TOP_K)
    
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


def run_full_v1(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """OLD: Original BO learning outer fit/cost/gen weights."""
    t0 = time.perf_counter()
    
    df = hard_filter(df_raw, REQUIREMENTS)
    df = add_fit_score(df, REQUIREMENTS)  # Old fit_score with fixed α=β=γ=1
    
    # Learn outer weights with old BO
    weights = optimize_weights(df, top_k=TOP_K, n_calls=BO_CALLS)
    ranked  = rank_instances(df, weights)
    final   = diversify(ranked, per_family=2, top_n=TOP_K)
    
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


def run_no_filter(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Skip hard_filter — all instances pass to scoring."""
    t0 = time.perf_counter()
    
    df = df_raw.copy()
    
    penalty_weights = optimize_resource_weights(
        df, REQUIREMENTS, 
        top_k=TOP_K, 
        n_calls=BO_CALLS,
        outer_weights=FIXED_OUTER_WEIGHTS,
        verbose=False
    )
    
    df = add_workload_aware_fit_score(df, REQUIREMENTS, penalty_weights)
    df["final_score"] = (
        FIXED_OUTER_WEIGHTS["fit"]        * _minmax(df["fit_score"])
        + FIXED_OUTER_WEIGHTS["cost"]     * _minmax(df["perf_per_dollar"])
        + FIXED_OUTER_WEIGHTS["generation"] * _minmax(df["generation_score"])
    )
    ranked = df.sort_values("final_score", ascending=False)
    final  = diversify(ranked, per_family=2, top_n=TOP_K)
    
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


def run_no_fit_score(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Replace fit_score with constant 1.0 — removes right-sizing signal."""
    t0 = time.perf_counter()
    
    df = hard_filter(df_raw, REQUIREMENTS)
    df = df.copy()
    df["fit_score"] = 1.0  # Neutralize fit component
    
    df["final_score"] = (
        FIXED_OUTER_WEIGHTS["fit"]        * _minmax(df["fit_score"])
        + FIXED_OUTER_WEIGHTS["cost"]     * _minmax(df["perf_per_dollar"])
        + FIXED_OUTER_WEIGHTS["generation"] * _minmax(df["generation_score"])
    )
    ranked = df.sort_values("final_score", ascending=False)
    final  = diversify(ranked, per_family=2, top_n=TOP_K)
    
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


def run_no_bo(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Equal penalty weights (α=β=γ=1.0) + fixed outer weights."""
    t0 = time.perf_counter()
    
    df = hard_filter(df_raw, REQUIREMENTS)
    
    # Equal penalty weights (no BO)
    equal_penalty = WorkloadResourceWeights(1.0, 1.0, 1.0)
    df = add_workload_aware_fit_score(df, REQUIREMENTS, equal_penalty)
    
    df["final_score"] = (
        FIXED_OUTER_WEIGHTS["fit"]        * _minmax(df["fit_score"])
        + FIXED_OUTER_WEIGHTS["cost"]     * _minmax(df["perf_per_dollar"])
        + FIXED_OUTER_WEIGHTS["generation"] * _minmax(df["generation_score"])
    )
    ranked = df.sort_values("final_score", ascending=False)
    final  = diversify(ranked, per_family=2, top_n=TOP_K)
    
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


def run_no_diversify(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Skip family-diversity cap."""
    t0 = time.perf_counter()
    
    df = hard_filter(df_raw, REQUIREMENTS)
    
    penalty_weights = optimize_resource_weights(
        df, REQUIREMENTS, 
        top_k=TOP_K, 
        n_calls=BO_CALLS,
        outer_weights=FIXED_OUTER_WEIGHTS,
        verbose=False
    )
    
    df = add_workload_aware_fit_score(df, REQUIREMENTS, penalty_weights)
    df["final_score"] = (
        FIXED_OUTER_WEIGHTS["fit"]        * _minmax(df["fit_score"])
        + FIXED_OUTER_WEIGHTS["cost"]     * _minmax(df["perf_per_dollar"])
        + FIXED_OUTER_WEIGHTS["generation"] * _minmax(df["generation_score"])
    )
    ranked = df.sort_values("final_score", ascending=False)
    final  = ranked.head(TOP_K)  # Raw top-k, no family cap
    
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


# ---------------------------------------------------------------------------
# Run all variants and collect metrics
# ---------------------------------------------------------------------------

VARIANTS = {
    "Full v2 (NEW workload-aware BO)": run_full_v2,
    "Full v1 (OLD outer weight BO)":   run_full_v1,
    "No filtering":                    run_no_filter,
    "No fit score":                    run_no_fit_score,
    "No BO (equal weights)":           run_no_bo,
    "No diversification":              run_no_diversify,
}


def run_ablation(data_path: str = DATA_PATH) -> pd.DataFrame:
    print("Loading and engineering features...")
    df_raw = load_and_engineer(data_path)

    records = []
    for name, fn in VARIANTS.items():
        print(f"  Running: {name}...")
        try:
            top_k_df, pool_df, latency = fn(df_raw)
            metrics = evaluate_all(top_k_df, pool_df, REQUIREMENTS, k=EVAL_K)
            records.append({
                "Configuration":     name,
                "NDCG@5":            metrics["ndcg_at_k"],
                "Precision@5":       metrics["precision_at_k"],
                "Cost savings (%)":  metrics["cost_savings_pct"],
                "Right-sizing error": metrics["right_sizing_error"],
                "Latency (s)":       round(latency, 2),
            })
        except Exception as e:
            print(f"    ERROR in {name}: {e}")
            import traceback
            traceback.print_exc()

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    results = run_ablation()

    # Pretty-print
    pd.set_option("display.float_format", "{:.4f}".format)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 120)

    print("\n" + "=" * 90)
    print("ABLATION STUDY RESULTS (v2 — NEW vs OLD BO)")
    print("=" * 90)
    print(results.to_string(index=False))

    # Save
    out_path = os.path.join(os.path.dirname(__file__), "ablation_results_v2.csv")
    results.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")

    # Delta analysis
    if "Full v2 (NEW workload-aware BO)" in results["Configuration"].values:
        v2_row = results[results["Configuration"] == "Full v2 (NEW workload-aware BO)"].iloc[0]
        
        print("\n" + "=" * 90)
        print("DELTA ANALYSIS (positive = v2 better)")
        print("=" * 90)
        
        for _, row in results.iterrows():
            if row["Configuration"] == "Full v2 (NEW workload-aware BO)":
                continue
            
            delta_ndcg = v2_row["NDCG@5"] - row["NDCG@5"]
            delta_prec = v2_row["Precision@5"] - row["Precision@5"]
            delta_cost = v2_row["Cost savings (%)"] - row["Cost savings (%)"]
            delta_rse  = row["Right-sizing error"] - v2_row["Right-sizing error"]  # Lower is better
            
            print(f"\n{row['Configuration']}")
            print(f"  ΔNDCG@5:      {delta_ndcg:+.4f}")
            print(f"  ΔPrecision@5: {delta_prec:+.4f}")
            print(f"  ΔCost:        {delta_cost:+.2f}%")
            print(f"  ΔRSE:         {delta_rse:+.4f}  (positive = v2 has lower error)")

# Made with Bob
