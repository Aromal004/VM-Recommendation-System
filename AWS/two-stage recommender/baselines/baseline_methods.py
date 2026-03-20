"""
baselines/baseline_methods.py
------------------------------
Simplified external baselines for comparison against the two-stage pipeline.

Baseline         Description
---------        -----------------------------------------------------------
Random           Shuffle the filtered pool and pick top-k at random
Heuristic        Sort by vCPU + memory ratio (typical developer strategy)
CherryPick-like  Bayesian optimization on a single config metric (no fit score)
Micky-like       Greedy multi-armed bandit: pick the instance with best
                 historic perf_per_dollar, with epsilon-greedy exploration

Each function accepts the same inputs and returns a ranked DataFrame,
so they can be dropped in wherever rank_instances() is called.
"""

import numpy as np
import pandas as pd
from skopt import gp_minimize
from skopt.space import Real


# ---------------------------------------------------------------------------
# Random baseline
# ---------------------------------------------------------------------------

def random_baseline(df: pd.DataFrame, top_n: int = 10,
                    seed: int = 42) -> pd.DataFrame:
    """
    Randomly shuffle the candidate pool and return top_n instances.
    Acts as a sanity / lower-bound baseline.
    """
    return df.sample(frac=1, random_state=seed).head(top_n).copy()


# ---------------------------------------------------------------------------
# Heuristic baseline  (vCPU + memory normalised sum — typical dev strategy)
# ---------------------------------------------------------------------------

def heuristic_baseline(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Rank by a normalised sum of vCPU count and memory, divided by price.
    Mimics the typical developer rule-of-thumb: biggest machine for the money.
    """
    df = df.copy()
    vcpu_norm   = df["vcpu"]       / df["vcpu"].max()
    mem_norm    = df["memory_gib"] / df["memory_gib"].max()

    df["heuristic_score"] = (vcpu_norm + mem_norm) / (2 * df["price_per_hr"])
    return df.sort_values("heuristic_score", ascending=False).head(top_n)


# ---------------------------------------------------------------------------
# CherryPick-like baseline
# ---------------------------------------------------------------------------

def cherrypick_baseline(df: pd.DataFrame, top_n: int = 10,
                        n_calls: int = 30) -> pd.DataFrame:
    """
    Simplified CherryPick (Alipourfard et al., 2017).

    The real CherryPick profiles a small number of configurations on the
    actual cloud to build a Bayesian surrogate model.  Here we simulate that
    using Bayesian optimisation over the same weight space as our pipeline,
    but with NO fit_score component — only raw performance per dollar and
    generation score.  This tests whether our fit_score adds value over
    a pure perf-per-dollar BO.

    Returns the top_n instances ranked by the BO-optimised score.
    """
    df = df.copy()

    space = [
        Real(0.3, 0.7, name="cost"),
        Real(0.1, 0.5, name="generation"),
    ]

    def objective(params):
        w_cost, w_gen = params
        total = w_cost + w_gen
        score = (
            (w_cost / total) * df["perf_per_dollar"]
            + (w_gen / total) * df["generation_score"]
        )
        return -score.nlargest(top_n).mean()

    res = gp_minimize(objective, space, n_calls=n_calls, random_state=42)
    w_cost, w_gen = res.x
    total = w_cost + w_gen

    df["cp_score"] = (
        (w_cost / total) * df["perf_per_dollar"]
        + (w_gen  / total) * df["generation_score"]
    )
    return df.sort_values("cp_score", ascending=False).head(top_n)


# ---------------------------------------------------------------------------
# Micky-like baseline  (multi-armed bandit)
# ---------------------------------------------------------------------------

def micky_baseline(df: pd.DataFrame, top_n: int = 10,
                   epsilon: float = 0.15, seed: int = 42) -> pd.DataFrame:
    """
    Simplified Micky (Hsu et al., 2018) — epsilon-greedy bandit.

    The real Micky uses online profiling to update arm estimates.  Here we
    simulate the arm reward as perf_per_dollar (the metric Micky would
    converge to after sufficient exploration).  Epsilon controls exploration:
    with probability epsilon we pick a random instance; otherwise we pick
    greedily.

    This is a reasonable offline approximation since our CSV already contains
    the 'true' performance values Micky would converge to.
    """
    rng = np.random.default_rng(seed)
    df = df.copy().reset_index(drop=True)

    # Simulate arm estimates starting from uniform noise + true value
    # (as if Micky has run a few initial profiling rounds)
    noise = rng.normal(0, df["perf_per_dollar"].std() * 0.05, len(df))
    df["arm_estimate"] = df["perf_per_dollar"] + noise

    selected_indices = []
    remaining = df.index.tolist()

    while len(selected_indices) < top_n and remaining:
        if rng.random() < epsilon:
            # Explore: random pick
            idx = rng.choice(remaining)
        else:
            # Exploit: best estimated arm
            idx = df.loc[remaining, "arm_estimate"].idxmax()

        selected_indices.append(idx)
        remaining.remove(idx)

        # Update arm estimate (simulated reward = observed perf_per_dollar)
        reward = df.loc[idx, "perf_per_dollar"]
        df.loc[idx, "arm_estimate"] = reward   # convergence

    return df.loc[selected_indices].copy()


# ---------------------------------------------------------------------------
# Convenience: run all baselines and return comparison table
# ---------------------------------------------------------------------------

def run_all_baselines(df: pd.DataFrame, top_n: int = 10,
                      seed: int = 42) -> dict[str, pd.DataFrame]:
    """
    Returns a dict of {baseline_name: ranked_dataframe}.
    Pass each to evaluate_all() from evaluation.metrics for fair comparison.
    """
    return {
        "Random":      random_baseline(df, top_n, seed),
        "Heuristic":   heuristic_baseline(df, top_n),
        "CherryPick":  cherrypick_baseline(df, top_n),
        "Micky":       micky_baseline(df, top_n, seed=seed),
    }