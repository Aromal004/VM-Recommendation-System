"""
evaluation/metrics.py
---------------------
Independent evaluation metrics for the EC2 recommendation pipeline.
These are deliberately separate from the optimization objective (top-k score)
to avoid circular evaluation.

Metrics
-------
- ndcg_at_k          : Ranking quality (normalized discounted cumulative gain)
- precision_at_k     : How many top-k picks are "good" choices
- cost_savings_pct   : Economic impact vs a naive baseline
- right_sizing_error : How closely recommended instances match workload needs
- recommendation_latency : Wall-clock time for one pipeline run (utility)

Relevance grading
-----------------
Relevance is computed from workload fit criteria ONLY — NOT from any method's
final_score. This avoids circular evaluation where the grading favours whichever
method's scoring aligns with our own objective.

A "relevant" instance is one that:
  - satisfies the hard constraints (guaranteed by hard_filter upstream)
  - is cost-efficient: price <= 80th percentile of the candidate pool
  - is well-fitted: fit_score >= 80th percentile of the candidate pool

Grade 3: cost-efficient AND well-fitted        (ideal)
Grade 2: cost-efficient OR well-fitted         (good)
Grade 1: neither, but within 20% of thresholds (acceptable)
Grade 0: poor fit / expensive                  (irrelevant)
"""

import numpy as np
import time
from typing import Optional


# ---------------------------------------------------------------------------
# Relevance helper  (workload-fit based, NOT score-based)
# ---------------------------------------------------------------------------

def compute_relevance(df, req: dict, score_col: str = "final_score") -> np.ndarray:
    """
    Grade each instance 0-3 based on cost efficiency and workload fit.

    Uses fit_score and price_per_hr from the DataFrame — both are
    workload-relative and independent of any method's ranking objective.

    Parameters
    ----------
    df         : DataFrame — candidate instances (must have fit_score, price_per_hr)
    req        : dict — workload requirements (used for context; thresholds are
                 percentile-based so they adapt to the pool)
    score_col  : unused (kept for API compatibility); relevance no longer
                 depends on final_score
    """
    # Cost efficiency: lower price is better
    # Threshold = 80th percentile of pool prices (cheaper 80% are "cost-efficient")
    price_threshold = np.percentile(df["price_per_hr"].values, 80)
    cost_efficient  = df["price_per_hr"].values <= price_threshold

    # Workload fit: higher fit_score is better
    # Threshold = 20th percentile (top 80% by fit are "well-fitted")
    fit_threshold = np.percentile(df["fit_score"].values, 40)
    well_fitted   = df["fit_score"].values >= fit_threshold

    # Near-threshold band (within 20% of each threshold)
    near_cost = df["price_per_hr"].values <= price_threshold * 1.20
    near_fit  = df["fit_score"].values    >= fit_threshold  * 0.80

    relevance = np.zeros(len(df), dtype=float)
    for i in range(len(df)):
        if cost_efficient[i] and well_fitted[i]:
            relevance[i] = 3   # ideal: cheap + fits well
        elif cost_efficient[i] or well_fitted[i]:
            relevance[i] = 2   # good: one of the two criteria met
        elif near_cost[i] or near_fit[i]:
            relevance[i] = 1   # acceptable: just outside thresholds
        else:
            relevance[i] = 0   # poor fit or expensive

    return relevance


# ---------------------------------------------------------------------------
# NDCG@K
# ---------------------------------------------------------------------------

def dcg(relevances: np.ndarray) -> float:
    """Discounted Cumulative Gain for a ranked list of relevance scores."""
    gains = np.array(relevances, dtype=float)
    discounts = np.log2(np.arange(2, len(gains) + 2))
    return float(np.sum(gains / discounts))


def ndcg_at_k(ranked_df, all_df, req: dict, k: int = 5,
              score_col: str = "final_score") -> float:
    """
    Normalized Discounted Cumulative Gain at K.

    Parameters
    ----------
    ranked_df : DataFrame — your pipeline's recommendations (already sorted)
    all_df    : DataFrame — full candidate pool (used to compute ideal DCG)
    req       : dict — workload requirements
    k         : int — cutoff
    score_col : str — ignored; kept for API compatibility

    Returns
    -------
    float in [0, 1], higher is better
    """
    top_k = ranked_df.head(k)
    rel_top = compute_relevance(top_k, req, score_col)

    rel_all  = compute_relevance(all_df, req, score_col)
    ideal_rel = np.sort(rel_all)[::-1][:k]

    actual_dcg = dcg(rel_top)
    ideal_dcg  = dcg(ideal_rel)

    if ideal_dcg == 0:
        return 0.0
    return round(actual_dcg / ideal_dcg, 4)


# ---------------------------------------------------------------------------
# Precision@K
# ---------------------------------------------------------------------------

def precision_at_k(ranked_df, all_df, req: dict, k: int = 5,
                   relevance_threshold: float = 2.0,
                   score_col: str = "final_score") -> float:
    """
    Fraction of top-k recommendations that are 'relevant' (grade >= threshold).
    """
    top_k = ranked_df.head(k)
    rel   = compute_relevance(top_k, req, score_col)
    relevant_count = int(np.sum(rel >= relevance_threshold))
    return round(relevant_count / k, 4)


# ---------------------------------------------------------------------------
# Cost savings %
# ---------------------------------------------------------------------------

def cost_savings_pct(ranked_df, all_df, k: int = 5) -> float:
    """
    Cost saving of the top-k vs the mean price of the candidate pool.
    Positive = pipeline recommends cheaper instances than average.
    """
    baseline_cost    = all_df["price_per_hr"].mean()
    recommended_cost = ranked_df.head(k)["price_per_hr"].mean()
    if baseline_cost == 0:
        return 0.0
    return round((baseline_cost - recommended_cost) / baseline_cost * 100, 2)


# ---------------------------------------------------------------------------
# Right-sizing error
# ---------------------------------------------------------------------------

def right_sizing_error(ranked_df, req: dict, k: int = 5) -> float:
    """
    Mean normalised over-provisioning across compute, memory, and network.
    0 = perfect fit; larger = more over-provisioning.
    """
    top_k = ranked_df.head(k).copy()

    compute_err = ((top_k["compute_score"] - req["required_compute"]).clip(lower=0)
                   / req["required_compute"])
    mem_err     = ((top_k["memory_gib"]    - req["memory_gib"]).clip(lower=0)
                   / req["memory_gib"])
    net_err     = ((top_k["network_mbps"]  - req["network_mbps"]).clip(lower=0)
                   / req["network_mbps"])

    return round(((compute_err + mem_err + net_err) / 3).mean(), 4)


# ---------------------------------------------------------------------------
# Recommendation latency
# ---------------------------------------------------------------------------

def measure_latency(pipeline_fn, *args, n_runs: int = 5, **kwargs) -> dict:
    """Time a full pipeline call and return mean ± std latency in seconds."""
    times = []
    for _ in range(n_runs):
        t0 = time.perf_counter()
        pipeline_fn(*args, **kwargs)
        times.append(time.perf_counter() - t0)
    return {
        "mean_s": round(np.mean(times), 3),
        "std_s":  round(np.std(times),  3),
        "min_s":  round(np.min(times),  3),
        "max_s":  round(np.max(times),  3),
    }


# ---------------------------------------------------------------------------
# Convenience: evaluate all metrics at once
# ---------------------------------------------------------------------------

def evaluate_all(ranked_df, all_df, req: dict, k: int = 5,
                 score_col: str = "final_score") -> dict:
    """
    Run every metric and return a results dict.

    Parameters
    ----------
    ranked_df : output of rank_instances() — sorted, full pool
    all_df    : same pool (used for ideal NDCG baseline)
    req       : workload requirements dict
    """
    return {
        "ndcg_at_k":          ndcg_at_k(ranked_df, all_df, req, k, score_col),
        "precision_at_k":     precision_at_k(ranked_df, all_df, req, k,
                                             score_col=score_col),
        "cost_savings_pct":   cost_savings_pct(ranked_df, all_df, k),
        "right_sizing_error": right_sizing_error(ranked_df, req, k),
    }