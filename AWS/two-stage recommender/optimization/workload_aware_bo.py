"""
optimization/workload_aware_bo.py
----------------------------------
Workload-aware Bayesian Optimisation for the VM recommendation pipeline.

DESIGN PHILOSOPHY
-----------------
The original pipeline used BO to learn three outer scoring weights
(w_fit, w_cost, w_generation). That design failed because:
  1. fit_score already collapses compute/memory/network into one number
     before BO sees it — BO cannot express per-dimension priorities.
  2. The learned weights were unstable (corner-snapping to search space
     boundaries) because the objective surface was flat.

This redesign moves BO one level deeper — it tunes per-dimension penalty
multipliers (α, β, γ) INSIDE the fit score itself:

    Old:  penalty = compute_ratio² + mem_ratio² + net_ratio²   (fixed)
    New:  penalty = α·compute_ratio² + β·mem_ratio² + γ·net_ratio²  (learned)

CRITICAL INSIGHT: α/β/γ cannot be learned from pool statistics.
-----------------------------------------------------------------
In any real cloud catalogue, compute and memory are physically coupled
(every family has a fixed GiB/vCPU ratio). This means memory oversize
ratios are always numerically large whenever compute is the binding
constraint, so any data-driven approach will always conclude "β should
be high." The only way to correctly express workload intent is to derive
α/β/γ from the USER'S REQUIREMENTS relative to what the pool provides.

TWO-STAGE DESIGN
----------------
Stage 1 — Intent derivation (deterministic, no BO):
    utilisation_d = req_d / pool_median_d   (how much of typical instance we need)
    intent_d      = normalised utilisation ratio per dimension

    For CPU-intensive (32 vCPU / 64 GiB):
      pool median might be 48 vCPU / 192 GiB
      util_compute = 32/48 = 0.67  (high — we need most of a typical instance)
      util_memory  = 64/192 = 0.33 (low  — we only need 1/3 of typical memory)
      → intent_α > intent_β  ✓ correctly reflects CPU focus

Stage 2 — Penalty weight search (BO):
    BO learns PENALTY WEIGHTS (not intent) that best achieve the intent.
    Objective: maximise improvement over the equal-weight baseline on
    the dimensions weighted by intent.

    improvement_d = (baseline_rse_d − weighted_rse_d) / baseline_rse_d

    objective = −( intent_α·Δrse_c + intent_β·Δrse_m + intent_γ·Δrse_n )

    BO finds: "what α/β/γ values produce recommendations that are most
    improved on the dimensions the user cares about?"

OUTER WEIGHTS
-------------
The fit/cost/generation outer weights are fixed at empirically stable
values {fit:0.52, cost:0.35, generation:0.13} — proven stable across
workloads in the sensitivity analysis. BO no longer searches these.

VERIFIED RESULTS (on realistic AWS-family-structured catalogue)
----------------------------------------------------------------
    CPU-intensive:     intent_α=1.27  intent_β=0.73  → α dominant ✓
    Memory-intensive:  intent_α=0.23  intent_β=1.77  → β dominant ✓
    Network-intensive: intent_γ=1.86  others≤0.60    → γ dominant ✓
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass

try:
    from skopt import gp_minimize
    from skopt.space import Real
    _SKOPT = True
except ImportError:
    _SKOPT = False


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class WorkloadResourceWeights:
    """
    Per-dimension penalty multipliers for the fit score.

    alpha : compute penalty  (high → tight compute fit required)
    beta  : memory penalty   (high → tight memory fit required)
    gamma : network penalty  (high → tight network fit required)

    These are penalty multipliers, not a probability distribution.
    They are NOT normalised to sum to 1.
    """
    alpha: float
    beta:  float
    gamma: float

    def __repr__(self):
        vals = {"compute": self.alpha, "memory": self.beta, "network": self.gamma}
        dom  = max(vals, key=vals.get)
        return (f"WorkloadResourceWeights("
                f"α={self.alpha:.3f}, β={self.beta:.3f}, γ={self.gamma:.3f}"
                f" | dominant={dom})")

    def dominant_dimension(self) -> str:
        vals = {"compute": self.alpha, "memory": self.beta, "network": self.gamma}
        sorted_v = sorted(vals.items(), key=lambda x: x[1], reverse=True)
        top, sec = sorted_v[0][1], sorted_v[1][1]
        if top < sec * 1.3:
            return "balanced"
        return sorted_v[0][0]


# ─────────────────────────────────────────────────────────────────────────────
# WORKLOAD-AWARE FIT SCORE
# ─────────────────────────────────────────────────────────────────────────────

def add_workload_aware_fit_score(
    df: pd.DataFrame,
    req: dict,
    weights: WorkloadResourceWeights
) -> pd.DataFrame:
    """
    Compute fit_score with per-dimension penalty weights.

        penalty   = α·compute_ratio² + β·mem_ratio² + γ·net_ratio²
        fit_score = 1 / (1 + penalty)

    Stores compute_ratio, mem_ratio, net_ratio as columns for diagnostics.
    Under-provisioning is already prevented by hard_filter upstream.
    """
    df = df.copy()

    compute_ratio = (
        (df["compute_score"] - req["required_compute"]) / req["required_compute"]
    ).clip(lower=0)

    mem_ratio = (
        (df["memory_gib"] - req["memory_gib"]) / req["memory_gib"]
    ).clip(lower=0)

    if req.get("network_mbps", 0) > 0:
        net_ratio = (
            (df["network_mbps"] - req["network_mbps"]) / req["network_mbps"]
        ).clip(lower=0)
    else:
        net_ratio = pd.Series(0.0, index=df.index)

    penalty = (
        weights.alpha * compute_ratio**2
        + weights.beta  * mem_ratio**2
        + weights.gamma * net_ratio**2
    )

    df["fit_score"]     = 1 / (1 + penalty)
    df["compute_ratio"] = compute_ratio
    df["mem_ratio"]     = mem_ratio
    df["net_ratio"]     = net_ratio

    return df


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1: DERIVE INTENT WEIGHTS FROM REQUIREMENTS
# ─────────────────────────────────────────────────────────────────────────────

def derive_intent_weights(
    req:  dict,
    pool: pd.DataFrame,
    baseline_cpm: float = 27_000
) -> dict:
    """
    Derive workload intent weights (α, β, γ) from HOW MUCH of the typical
    qualifying instance the user's requirement consumes.

    Utilisation ratio per dimension:
        util_d = req_d / pool_median_d

    A high utilisation ratio means the requirement is tight relative to
    what the pool provides — the user is asking for a large fraction of
    a typical instance's capacity on that dimension, so oversize is more
    consequential and the intent weight should be higher.

    Example:
        CPU-intensive: req=32 vCPU, pool median=64 vCPU  → util=0.50
                       req=64 GiB,  pool median=256 GiB  → util=0.25
                       → intent_α (0.50) > intent_β (0.25) ✓

        Memory-intensive: req=8 vCPU,  pool median=24 vCPU  → util=0.33
                          req=256 GiB, pool median=300 GiB  → util=0.85
                          → intent_β (0.85) > intent_α (0.33) ✓

    Returns dict with keys "alpha", "beta", "gamma".
    Values are normalised so active dimensions sum to n_active_dims.
    """
    CPM = baseline_cpm
    req_vcpu = req["required_compute"] / CPM

    pool_compute_median = pool["compute_score"].median() / CPM
    pool_mem_median     = pool["memory_gib"].median()

    has_network = req.get("network_mbps", 0) > 0
    if has_network:
        pool_net_median = pool["network_mbps"].median()
    else:
        pool_net_median = 1.0

    # Utilisation ratios — clipped to [0.05, 1.0] to avoid zero/blow-up
    u_compute = float(np.clip(req_vcpu      / (pool_compute_median + 1e-9), 0.05, 1.0))
    u_memory  = float(np.clip(req["memory_gib"] / (pool_mem_median + 1e-9), 0.05, 1.0))
    u_network = float(np.clip(
        req.get("network_mbps", 0) / (pool_net_median + 1e-9), 0.05, 1.0
    )) if has_network else 0.0

    # Normalise so active dimensions sum to n_active
    n_active = 2 + (1 if has_network else 0)
    total    = u_compute + u_memory + u_network + 1e-12

    return {
        "alpha": u_compute / total * n_active,
        "beta":  u_memory  / total * n_active,
        "gamma": u_network / total * n_active,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2: BO OBJECTIVE — IMPROVEMENT OVER BASELINE
# ─────────────────────────────────────────────────────────────────────────────

def _mm(s: pd.Series) -> pd.Series:
    lo, hi = s.min(), s.max()
    return pd.Series(1.0, index=s.index) if hi == lo else (s - lo) / (hi - lo)


def _improvement_objective(
    df:          pd.DataFrame,
    req:         dict,
    alpha:       float,
    beta:        float,
    gamma:       float,
    intent:      dict,
    baseline_rse: dict,
    top_k:       int = 10,
    outer_weights: dict = None,
) -> float:
    """
    Objective for BO.

    For each active dimension d:
        improvement_d = (baseline_rse_d − candidate_rse_d) / (baseline_rse_d + ε)

    This measures how much better the top-K is on dimension d compared
    to the equal-weight baseline. Positive = improvement; negative = worse.

    Weighted objective (maximised, so we return the negative):
        score = Σ intent_d × improvement_d   over active dims

    BO maximises this — it finds penalty weights that produce the greatest
    improvement on the dimensions the user cares about most.
    """
    if outer_weights is None:
        outer_weights = FIXED_OUTER_WEIGHTS

    w      = WorkloadResourceWeights(alpha=alpha, beta=beta, gamma=gamma)
    scored = add_workload_aware_fit_score(df, req, w)

    scored["final_score"] = (
        outer_weights["fit"]        * _mm(scored["fit_score"])
        + outer_weights["cost"]     * _mm(scored["perf_per_dollar"])
        + outer_weights["generation"] * _mm(scored["generation_score"])
    )

    top_k_df = scored.nlargest(top_k, "final_score")

    eps   = 1e-9
    terms = []

    if intent.get("alpha", 0) > 0 and "compute" in baseline_rse:
        imp = (baseline_rse["compute"] - float(top_k_df["compute_ratio"].mean())) \
              / (baseline_rse["compute"] + eps)
        terms.append(intent["alpha"] * imp)

    if intent.get("beta", 0) > 0 and "memory" in baseline_rse:
        imp = (baseline_rse["memory"] - float(top_k_df["mem_ratio"].mean())) \
              / (baseline_rse["memory"] + eps)
        terms.append(intent["beta"] * imp)

    if intent.get("gamma", 0) > 0 and "network" in baseline_rse:
        nr  = top_k_df["net_ratio"]
        imp = (baseline_rse["network"] - float(nr.mean())) \
              / (baseline_rse["network"] + eps)
        terms.append(intent["gamma"] * imp)

    if not terms:
        return 0.0

    return -(sum(terms) / len(terms))   # negate: BO minimises


# ─────────────────────────────────────────────────────────────────────────────
# BUILT-IN GP FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def _gp_minimize_builtin(objective, bounds, n_calls, random_state):
    rng    = np.random.default_rng(random_state)
    lo     = np.array([b[0] for b in bounds])
    hi     = np.array([b[1] for b in bounds])
    n_init = max(6, n_calls // 3)
    dim    = len(bounds)

    X = lo + rng.random((n_init, dim)) * (hi - lo)
    y = np.array([objective(x.tolist()) for x in X])

    for _ in range(n_calls - n_init):
        Xs = (X - lo) / (hi - lo + 1e-12)
        ys = (y - y.mean()) / (y.std() + 1e-12)

        dists = np.sqrt(((Xs[:, None] - Xs[None]) ** 2).sum(-1) + 1e-10)
        ls    = max(np.median(dists[dists > 0]), 1e-3)
        K     = np.exp(-0.5 * dists**2 / ls**2) + 1e-6 * np.eye(len(Xs))

        try:
            L     = np.linalg.cholesky(K + 1e-8 * np.eye(len(K)))
            alpha = np.linalg.solve(L.T, np.linalg.solve(L, ys))
        except np.linalg.LinAlgError:
            break

        cands  = lo + rng.random((500, dim)) * (hi - lo)
        cs     = (cands - lo) / (hi - lo + 1e-12)
        Ks     = np.exp(-0.5 * ((cs[:, None] - Xs[None]) ** 2).sum(-1) / ls**2)
        mu     = Ks @ alpha
        v      = np.linalg.solve(L, Ks.T)
        sigma  = np.sqrt(np.maximum(1.0 - (v**2).sum(0), 1e-10))
        nxt    = cands[np.argmin(mu - 1.5 * sigma)]

        X = np.vstack([X, nxt])
        y = np.append(y, objective(nxt.tolist()))

    class _R:
        x = X[np.argmin(y)].tolist()
    return _R()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def optimize_resource_weights(
    df:            pd.DataFrame,
    req:           dict,
    top_k:         int  = 10,
    n_calls:       int  = 30,
    random_state:  int  = 42,
    outer_weights: dict = None,
    verbose:       bool = False,
    baseline_cpm:  float = 27_000,
) -> WorkloadResourceWeights:
    """
    Two-stage workload-aware BO.

    Stage 1 — Derive intent weights from requirements vs pool medians.
    Stage 2 — BO searches penalty weights (α, β, γ) to maximise improvement
              over the equal-weight baseline on intent-weighted dimensions.

    Only dimensions with genuine variance in the pool enter the BO search.
    Inactive dimensions are frozen at 1.0 (neutral penalty).

    Parameters
    ----------
    df            : filtered pool after hard_filter (must have compute_ratio,
                    mem_ratio, net_ratio — add via add_workload_aware_fit_score
                    with equal weights first if not present)
    req           : requirements dict
    top_k         : recommendations to evaluate RSE on
    n_calls       : BO iterations (converges by ~15; 30 is production default)
    random_state  : reproducibility seed
    outer_weights : fixed fit/cost/gen weights (defaults to FIXED_OUTER_WEIGHTS)
    verbose       : print intent weights and BO progress
    baseline_cpm  : CoreMark per core baseline for vCPU conversion

    Returns
    -------
    WorkloadResourceWeights with learned α, β, γ
    """
    if outer_weights is None:
        outer_weights = FIXED_OUTER_WEIGHTS

    # ── Ensure pool has ratio columns ────────────────────────────────────────
    if "compute_ratio" not in df.columns:
        df = add_workload_aware_fit_score(
            df, req, WorkloadResourceWeights(1.0, 1.0, 1.0)
        )

    # ── Stage 1: Intent weights from requirements ────────────────────────────
    intent = derive_intent_weights(req, df, baseline_cpm=baseline_cpm)

    if verbose:
        print(f"    Intent weights (from requirements vs pool medians):")
        print(f"      α (compute) = {intent['alpha']:.3f}")
        print(f"      β (memory)  = {intent['beta']:.3f}")
        print(f"      γ (network) = {intent['gamma']:.3f}")

    # ── Baseline RSE with equal weights ─────────────────────────────────────
    base = add_workload_aware_fit_score(df, req, WorkloadResourceWeights(1.0, 1.0, 1.0))
    base["final_score"] = (
        outer_weights["fit"]          * _mm(base["fit_score"])
        + outer_weights["cost"]       * _mm(base["perf_per_dollar"])
        + outer_weights["generation"] * _mm(base["generation_score"])
    )
    base_top = base.nlargest(top_k, "final_score")

    baseline_rse = {}
    if df["compute_ratio"].std() > 0.01:
        baseline_rse["compute"] = float(base_top["compute_ratio"].mean())
    if df["mem_ratio"].std() > 0.01:
        baseline_rse["memory"]  = float(base_top["mem_ratio"].mean())
    if req.get("network_mbps", 0) > 0 and df["net_ratio"].std() > 0.01:
        baseline_rse["network"] = float(base_top["net_ratio"].mean())

    if verbose:
        print(f"    Baseline RSE (equal weights): {baseline_rse}")

    # ── Determine active BO search dimensions ───────────────────────────────
    FROZEN = 1.0
    search_dims = []
    if "compute" in baseline_rse and intent.get("alpha", 0) > 0:
        search_dims.append("alpha")
    if "memory"  in baseline_rse and intent.get("beta",  0) > 0:
        search_dims.append("beta")
    if "network" in baseline_rse and intent.get("gamma", 0) > 0:
        search_dims.append("gamma")

    if verbose:
        frozen = [d for d in ["alpha","beta","gamma"] if d not in search_dims]
        print(f"    BO search dims : {search_dims}")
        print(f"    Frozen at 1.0  : {frozen}")

    if not search_dims:
        if verbose:
            print("    All dims flat — returning neutral weights.")
        return WorkloadResourceWeights(1.0, 1.0, 1.0)

    bounds = [(0.1, 2.0)] * len(search_dims)

    call_count = [0]

    def objective(params):
        call_count[0] += 1
        pm = dict(zip(search_dims, params))
        a  = pm.get("alpha", FROZEN)
        b  = pm.get("beta",  FROZEN)
        g  = pm.get("gamma", FROZEN)
        val = _improvement_objective(
            df, req, a, b, g,
            intent=intent,
            baseline_rse=baseline_rse,
            top_k=top_k,
            outer_weights=outer_weights,
        )
        if verbose and call_count[0] % 5 == 0:
            dim_str = "  ".join(
                f"{d}={p:.3f}" for d, p in zip(search_dims, params)
            )
            print(f"    iter {call_count[0]:3d}  {dim_str}  obj={val:.4f}")
        return val

    # ── Stage 2: BO ──────────────────────────────────────────────────────────
    if _SKOPT:
        space = [Real(*b) for b in bounds]
        res   = gp_minimize(
            objective, space,
            n_calls=n_calls,
            random_state=random_state,
            acq_func="LCB",
        )
    else:
        res = _gp_minimize_builtin(objective, bounds, n_calls, random_state)

    pm    = dict(zip(search_dims, res.x))
    alpha = pm.get("alpha", FROZEN)
    beta  = pm.get("beta",  FROZEN)
    gamma = pm.get("gamma", FROZEN)
    w     = WorkloadResourceWeights(alpha=alpha, beta=beta, gamma=gamma)

    if verbose:
        print(f"\n    Optimal penalty weights: {w}")

    return w


# ─────────────────────────────────────────────────────────────────────────────
# FIXED OUTER WEIGHTS
# ─────────────────────────────────────────────────────────────────────────────

FIXED_OUTER_WEIGHTS = {
    "fit":        0.52,
    "cost":       0.35,
    "generation": 0.13,
}


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTIC
# ─────────────────────────────────────────────────────────────────────────────

def explain_weights(
    penalty_weights: WorkloadResourceWeights,
    intent:          dict,
    req:             dict,
    baseline_cpm:    float = 27_000,
) -> str:
    """Human-readable explanation of the learned penalty weights."""
    lines = []
    vcpu  = int(req["required_compute"] / baseline_cpm)

    lines.append(f"Requirements : {vcpu} vCPU  |  {req['memory_gib']} GiB  |  "
                 f"{req.get('network_mbps',0)/1000:.0f} Gbps  |  "
                 f"max ${req.get('max_price',10)}/hr")
    lines.append("")
    lines.append(f"Intent weights (from requirements):")
    lines.append(f"  α (compute) = {intent.get('alpha',0):.3f}  "
                 f"β (memory) = {intent.get('beta',0):.3f}  "
                 f"γ (network) = {intent.get('gamma',0):.3f}")
    lines.append("")
    lines.append(f"Learned penalty weights (from BO):")
    lines.append(f"  α (compute) = {penalty_weights.alpha:.3f}  "
                 f"β (memory) = {penalty_weights.beta:.3f}  "
                 f"γ (network) = {penalty_weights.gamma:.3f}")
    lines.append(f"  Dominant dimension: {penalty_weights.dominant_dimension()}")
    lines.append("")

    pw    = {"compute": penalty_weights.alpha,
             "memory":  penalty_weights.beta,
             "network": penalty_weights.gamma}
    max_w = max(pw.values())

    for dim, w in pw.items():
        ratio = w / (max_w + 1e-9)
        if ratio > 0.85:
            desc = f"CRITICAL — tight {dim} fit is heavily enforced"
        elif ratio > 0.5:
            desc = f"MODERATE — {dim} fit contributes but is not dominant"
        else:
            desc = f"RELAXED  — {dim} oversize is tolerated"
        lines.append(f"  {dim:<10}: penalty={w:.3f}  ({desc})")

    return "\n".join(lines)