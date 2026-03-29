"""
experiments/multi_run.py
------------------------
Statistical validation of the two-stage pipeline.

Runs the full pipeline and all baselines across N random seeds,
collects per-run metrics, reports mean ± std, and performs
significance tests (paired t-test + Wilcoxon signed-rank).

Usage
-----
  python experiments/multi_run.py

Output
------
  Console summary table
  multi_run_results.csv      — per-run raw data
  statistical_summary.csv    — mean ± std per method
  significance_tests.csv     — p-values vs proposed method (NDCG, Cost, RSE)
"""

import sys
import os
import pandas as pd
import numpy as np
from scipy import stats

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter import hard_filter
from scoring.fit_score import add_fit_score
from optimization.bayesian_ranker import optimize_weights
from scoring.final_scorer import rank_instances
from postprocessing.diversify import diversify
from baselines.baseline_methods import run_all_baselines
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

TOP_K    = 10
EVAL_K   = 5
BO_CALLS = 30
N_RUNS   = 10   # number of independent seeds; use 5 for a quicker run


# ---------------------------------------------------------------------------
# Single-run helpers
# ---------------------------------------------------------------------------

def run_proposed(df_raw: pd.DataFrame, seed: int) -> pd.DataFrame:
    """One run of the full two-stage pipeline with a given random seed."""
    df = hard_filter(df_raw, REQUIREMENTS)
    df = add_fit_score(df, REQUIREMENTS)
    weights = optimize_weights(df, top_k=TOP_K, n_calls=BO_CALLS,
                               random_state=seed)
    ranked = rank_instances(df, weights)
    return diversify(ranked, per_family=2, top_n=TOP_K), ranked


def run_baseline(df_raw: pd.DataFrame, name: str, seed: int) -> pd.DataFrame:
    """One run of a named baseline method."""
    df = hard_filter(df_raw, REQUIREMENTS)
    df = add_fit_score(df, REQUIREMENTS)
    # Give pool a final_score column so evaluate_all works uniformly
    equal_weights = {"fit": 1/3, "cost": 1/3, "generation": 1/3}
    pool = rank_instances(df, equal_weights)
    all_baselines = run_all_baselines(pool, top_n=TOP_K, seed=seed)
    return all_baselines[name], pool


# ---------------------------------------------------------------------------
# Main experiment loop
# ---------------------------------------------------------------------------

def run_multi(data_path: str = DATA_PATH,
              n_runs: int = N_RUNS) -> pd.DataFrame:

    print(f"Loading data from {data_path} ...")
    df_raw = add_features(pd.read_csv(data_path))

    methods = ["Proposed", "Random", "Heuristic", "CherryPick", "Micky"]
    seeds   = list(range(n_runs))
    records = []

    for seed in seeds:
        print(f"  Seed {seed:02d} / {n_runs - 1} ...", end="  ")

        # Proposed pipeline
        top_k, pool = run_proposed(df_raw, seed)
        m = evaluate_all(top_k, pool, REQUIREMENTS, k=EVAL_K)
        records.append({"seed": seed, "method": "Proposed", **m})

        # Baselines
        for bname in ["Random", "Heuristic", "CherryPick", "Micky"]:
            top_k_b, pool_b = run_baseline(df_raw, bname, seed)
            m_b = evaluate_all(top_k_b, pool_b, REQUIREMENTS, k=EVAL_K)
            records.append({"seed": seed, "method": bname, **m_b})

        print("done")

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def summarise(raw: pd.DataFrame) -> pd.DataFrame:
    """Mean ± std for each method across all seeds."""
    metric_cols = ["ndcg_at_k", "precision_at_k",
                   "cost_savings_pct", "right_sizing_error"]
    rows = []
    for method, grp in raw.groupby("method"):
        row = {"Method": method}
        for col in metric_cols:
            mu  = grp[col].mean()
            std = grp[col].std(ddof=1)
            row[col] = f"{mu:.4f} ± {std:.4f}"
        rows.append(row)
    return pd.DataFrame(rows).set_index("Method")


# ---------------------------------------------------------------------------
# Significance tests  — NDCG@5, Cost savings, and RSE
# ---------------------------------------------------------------------------

def significance_tests(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Paired t-test and Wilcoxon signed-rank test:
    Proposed vs each other method on three metrics:
      - ndcg_at_k        (higher is better → alternative='greater')
      - cost_savings_pct (higher is better → alternative='greater')
      - right_sizing_error (lower is better → alternative='less')

    Seeds provide the paired observations.
    """
    proposed = raw[raw["method"] == "Proposed"].sort_values("seed")

    # metric label → (column name, wilcoxon alternative, friendly label)
    metrics = [
        ("ndcg_at_k",          "greater", "NDCG@5"),
        ("cost_savings_pct",   "greater", "Cost savings (%)"),
        ("right_sizing_error", "less",    "Right-sizing error"),
    ]

    rows = []
    for method in raw["method"].unique():
        if method == "Proposed":
            continue
        other = raw[raw["method"] == method].sort_values("seed")

        for col, alternative, label in metrics:
            prop_vals  = proposed[col].values
            other_vals = other[col].values

            _, p_ttest = stats.ttest_rel(prop_vals, other_vals)

            # Wilcoxon requires non-zero differences; catch the edge case
            try:
                _, p_wilcox = stats.wilcoxon(
                    prop_vals, other_vals,
                    alternative=alternative,
                    zero_method="wilcox",
                )
            except ValueError:
                # All differences are zero — methods are identical on this metric
                p_wilcox = 1.0

            rows.append({
                "Comparison":    f"Proposed vs {method}",
                "Metric":        label,
                "t-test p":      round(p_ttest,  4),
                "Wilcoxon p":    round(p_wilcox, 4),
                "Significant?":  "Yes" if p_ttest < 0.05 else "No",
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    raw = run_multi()

    # Save raw per-run data
    out_dir = os.path.dirname(__file__)
    raw.to_csv(os.path.join(out_dir, "multi_run_results.csv"), index=False)

    # Summary table
    summary = summarise(raw)
    summary.to_csv(os.path.join(out_dir, "statistical_summary.csv"))

    print("\n" + "=" * 80)
    print("STATISTICAL SUMMARY  (mean ± std over", N_RUNS, "runs)")
    print("=" * 80)
    print(summary.to_string())

    # Significance tests across all three metrics
    sig = significance_tests(raw)
    sig.to_csv(os.path.join(out_dir, "significance_tests.csv"), index=False)

    print("\n" + "=" * 80)
    print("SIGNIFICANCE TESTS  (paired t-test + Wilcoxon, Proposed vs baselines)")
    print("=" * 80)

    for metric_label in ["NDCG@5", "Cost savings (%)", "Right-sizing error"]:
        subset = sig[sig["Metric"] == metric_label]
        print(f"\n  Metric: {metric_label}")
        print(f"  {'Comparison':<28}  {'t-test p':>10}  {'Wilcoxon p':>12}  {'Significant?':>13}")
        print("  " + "-" * 70)
        for _, row in subset.iterrows():
            print(f"  {row['Comparison']:<28}  {row['t-test p']:>10.4f}"
                  f"  {row['Wilcoxon p']:>12.4f}  {row['Significant?']:>13}")