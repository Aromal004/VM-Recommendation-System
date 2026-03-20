"""
experiments/ablation_runner.py
------------------------------
Ablation study: isolates the contribution of each pipeline component.

Variants
--------
  full          : complete pipeline (hard filter + fit score + BO weights + diversify)
  no_filter     : skip hard_filter  — all instances enter scoring
  no_fit_score  : replace fit_score with uniform 1.0  (removes right-sizing signal)
  no_bo         : replace Bayesian-optimized weights with equal 1/3 weights
  no_diversify  : skip family-diversity cap on final list

Usage
-----
  python experiments/ablation_runner.py

Output
------
  Console table + ablation_results.csv saved to experiments/
"""

import sys
import os
import time
import pandas as pd
import numpy as np

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify
from evaluation.metrics import evaluate_all


# ---------------------------------------------------------------------------
# Config — mirror main.py exactly
# ---------------------------------------------------------------------------

DATA_PATH = "/home/aromal/VM-Recommendation-System/AWS/two-stage recommender/aws_with_coremark.csv"
BASELINE_COREMARK_PER_CORE = 27_000

REQUIREMENTS = {
    "required_compute": 16 * BASELINE_COREMARK_PER_CORE,
    "memory_gib":       64,
    "network_mbps":     25_000,
    "max_price":        10.0,
}

TOP_K   = 10
EVAL_K  = 5   # NDCG / Precision cut-off
BO_CALLS = 30


# ---------------------------------------------------------------------------
# Pipeline variants
# ---------------------------------------------------------------------------

def load_and_engineer(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return add_features(df)


def run_full(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    t0 = time.perf_counter()
    df = hard_filter(df_raw, REQUIREMENTS)
    df = add_fit_score(df, REQUIREMENTS)
    weights = optimize_weights(df, top_k=TOP_K, n_calls=BO_CALLS)
    ranked  = rank_instances(df, weights)
    final   = diversify(ranked, per_family=2, top_n=TOP_K)
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed     # pool = ranked (has final_score)


def run_no_filter(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Skip hard_filter — all instances pass to scoring."""
    t0 = time.perf_counter()
    df = df_raw.copy()
    df = add_fit_score(df, REQUIREMENTS)
    weights = optimize_weights(df, top_k=TOP_K, n_calls=BO_CALLS)
    ranked  = rank_instances(df, weights)
    final   = diversify(ranked, per_family=2, top_n=TOP_K)
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


def run_no_fit_score(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Replace fit_score with constant 1.0 — removes right-sizing signal."""
    t0 = time.perf_counter()
    df = hard_filter(df_raw, REQUIREMENTS)
    df = df.copy()
    df["fit_score"] = 1.0             # neutralise fit component
    weights = optimize_weights(df, top_k=TOP_K, n_calls=BO_CALLS)
    ranked  = rank_instances(df, weights)
    final   = diversify(ranked, per_family=2, top_n=TOP_K)
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


def run_no_bo(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Replace Bayesian-optimized weights with fixed equal weights."""
    t0 = time.perf_counter()
    df = hard_filter(df_raw, REQUIREMENTS)
    df = add_fit_score(df, REQUIREMENTS)
    equal_weights = {"fit": 1/3, "cost": 1/3, "generation": 1/3}
    ranked  = rank_instances(df, equal_weights)
    final   = diversify(ranked, per_family=2, top_n=TOP_K)
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


def run_no_diversify(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Skip family-diversity cap."""
    t0 = time.perf_counter()
    df = hard_filter(df_raw, REQUIREMENTS)
    df = add_fit_score(df, REQUIREMENTS)
    weights = optimize_weights(df, top_k=TOP_K, n_calls=BO_CALLS)
    ranked  = rank_instances(df, weights)
    final   = ranked.head(TOP_K)      # raw top-k, no family cap
    elapsed = time.perf_counter() - t0
    return final, ranked, elapsed


# ---------------------------------------------------------------------------
# Run all variants and collect metrics
# ---------------------------------------------------------------------------

VARIANTS = {
    "Full model":        run_full,
    "No filtering":      run_no_filter,
    "No fit score":      run_no_fit_score,
    "No BO":             run_no_bo,
    "No diversification": run_no_diversify,
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

    print("\n" + "=" * 80)
    print("ABLATION STUDY RESULTS")
    print("=" * 80)
    print(results.to_string(index=False))

    # Save
    out_path = os.path.join(os.path.dirname(__file__), "ablation_results.csv")
    results.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")

    # Delta vs full model
    full_row = results[results["Configuration"] == "Full model"].iloc[0]
    print("\nDelta vs Full model (positive = full model better):")
    for _, row in results.iterrows():
        if row["Configuration"] == "Full model":
            continue
        delta_ndcg = full_row["NDCG@5"] - row["NDCG@5"]
        delta_prec = full_row["Precision@5"] - row["Precision@5"]
        print(f"  {row['Configuration']:<22}  ΔNDCG={delta_ndcg:+.4f}  ΔPrecision={delta_prec:+.4f}")