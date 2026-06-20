"""
experiments/bo_workload_sensitivity.py
---------------------------------------
Tests whether Bayesian Optimisation genuinely adapts its weights to
different workload profiles, or whether it converges to the same
~0.52 / 0.35 / 0.13 split regardless of what the workload actually needs.

Self-contained: uses only numpy + pandas.
Uses a built-in lightweight GP when skopt is unavailable.

Usage
-----
  python bo_workload_sensitivity.py --synthetic          # no CSV needed
  python bo_workload_sensitivity.py --data combined_vms.csv

Output
------
  bo_workload_sensitivity.csv          — full results (profile x seed)
  bo_workload_sensitivity_summary.csv  — mean +/- std per profile
  Verdict + correlation table printed to stdout
"""

import sys, os, argparse, warnings, re
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── BO backend ────────────────────────────────────────────────────────────────
try:
    from skopt import gp_minimize as _skopt_min
    from skopt.space import Real as _Real
    _SKOPT = True
except ImportError:
    _SKOPT = False


# ─────────────────────────────────────────────────────────────────────────────
# A. BUILT-IN GP  (fallback when skopt unavailable)
# ─────────────────────────────────────────────────────────────────────────────

class _GP:
    """Minimal RBF Gaussian Process for acquisition."""
    def __init__(self):
        self.X = self.y = self.alpha = self.L = None
        self.ls = 1.0

    def fit(self, X, y):
        self.X, self.y = X.copy(), y.copy()
        if len(X) > 1:
            d = X[:, None] - X[None, :]
            dists = np.sqrt((d**2).sum(-1))
            med = np.median(dists[dists > 0])
            self.ls = max(med, 1e-3)
        K = np.exp(-0.5 * ((X[:, None] - X[None]) ** 2).sum(-1) / self.ls**2)
        K += 1e-6 * np.eye(len(X))
        self.L = np.linalg.cholesky(K + 1e-8 * np.eye(len(K)))
        self.alpha = np.linalg.solve(self.L.T, np.linalg.solve(self.L, y))

    def predict(self, Xs):
        Ks = np.exp(-0.5 * ((Xs[:, None] - self.X[None]) ** 2).sum(-1) / self.ls**2)
        mu = Ks @ self.alpha
        v  = np.linalg.solve(self.L, Ks.T)
        std = np.sqrt(np.maximum(1.0 - (v**2).sum(0), 1e-10))
        return mu, std


def _gp_min_builtin(objective, bounds, n_calls, random_state):
    rng    = np.random.default_rng(random_state)
    lo     = np.array([b[0] for b in bounds])
    hi     = np.array([b[1] for b in bounds])
    n_init = max(5, n_calls // 3)
    dim    = len(bounds)

    X = lo + rng.random((n_init, dim)) * (hi - lo)
    y = np.array([objective(x.tolist()) for x in X])

    gp = _GP()
    for _ in range(n_calls - n_init):
        Xs = (X - lo) / (hi - lo + 1e-12)
        ys = (y - y.mean()) / (y.std() + 1e-12)
        gp.fit(Xs, ys)

        cands  = lo + rng.random((400, dim)) * (hi - lo)
        cs     = (cands - lo) / (hi - lo + 1e-12)
        mu, sg = gp.predict(cs)
        nxt    = cands[np.argmin(mu - sg)]   # LCB acquisition

        yn  = objective(nxt.tolist())
        X   = np.vstack([X, nxt])
        y   = np.append(y, yn)

    best = X[np.argmin(y)]

    class R:
        x = best.tolist()
    return R()


def _run_bo(objective, bounds, n_calls, random_state):
    if _SKOPT:
        space = [_Real(*b) for b in bounds]
        return _skopt_min(objective, space, n_calls=n_calls,
                          random_state=random_state)
    return _gp_min_builtin(objective, bounds, n_calls, random_state)


# ─────────────────────────────────────────────────────────────────────────────
# B. PIPELINE FUNCTIONS  (self-contained copies)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_net(val):
    if pd.isna(val): return 0.0
    s = str(val).lower()
    for pat, mult in [(r"([\d.]+)\s*gigabit", 1000),
                      (r"([\d.]+)\s*megabit", 1),
                      (r"([\d.]+)\s*gbps",    1000),
                      (r"([\d.]+)\s*mbps",    1)]:
        m = re.search(pat, s)
        if m: return float(m.group(1)) * mult
    m = re.search(r"([\d.]+)", s)
    return float(m.group(1)) if m else 0.0


def add_features(df):
    df = df.copy()
    df = df.dropna(subset=["instanceType"])
    df["instanceType"] = df["instanceType"].astype(str)
    df = df[df["price_per_hr"] > 0].replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["coremark_total", "coremark_per_dollar"])
    df["vcpu"]             = df["vcpu"].astype(float)
    df["memory_gib"]       = df["memory"].str.replace(" GiB","",regex=False).astype(float)
    df["network_mbps"]     = df["networkPerformance"].apply(_parse_net)
    df["compute_score"]    = df["coremark_total"]
    df["perf_per_dollar"]  = df["coremark_per_dollar"]
    df["generation_score"] = df["coremark_per_core"] / df["coremark_per_core"].max()
    df["family"]           = df["instanceType"].str.split(".").str[0]
    if "provider" not in df.columns:
        df["provider"] = "aws"
    return df


def hard_filter(df, req):
    df = df[df["compute_score"] >= req["required_compute"]]
    df = df[df["memory_gib"]    >= req["memory_gib"]]
    if req.get("network_mbps", 0) > 0:
        df = df[df["network_mbps"] >= req["network_mbps"]]
    if req.get("max_price", 0) > 0:
        df = df[df["price_per_hr"] <= req["max_price"]]
    return df.copy()


def add_fit_score(df, req):
    df = df.copy()
    cr = ((df["compute_score"] - req["required_compute"]) / req["required_compute"]).clip(lower=0)
    mr = ((df["memory_gib"]    - req["memory_gib"])       / req["memory_gib"]).clip(lower=0)
    nr = 0
    if req.get("network_mbps", 0) > 0:
        nr = ((df["network_mbps"] - req["network_mbps"]) / req["network_mbps"]).clip(lower=0)
    df["fit_score"] = 1 / (1 + cr**2 + mr**2 + (nr**2 if not isinstance(nr, int) else 0))
    return df


def _mm(s):
    lo, hi = s.min(), s.max()
    return pd.Series(1.0, index=s.index) if hi == lo else (s - lo) / (hi - lo)


def optimize_weights(df, top_k=10, n_calls=30, random_state=42):
    fn = _mm(df["fit_score"]).values
    cn = _mm(df["perf_per_dollar"]).values
    gn = _mm(df["generation_score"]).values
    bounds = [(0.3, 0.7), (0.1, 0.4), (0.05, 0.2)]

    def obj(p):
        s = sum(p)
        w = [x/s for x in p]
        scores = w[0]*fn + w[1]*cn + w[2]*gn
        return -np.mean(sorted(scores, reverse=True)[:top_k])

    res = _run_bo(obj, bounds, n_calls, random_state)
    raw = dict(zip(["fit","cost","generation"], res.x))
    s   = sum(raw.values())
    return {k: v/s for k, v in raw.items()}


# ─────────────────────────────────────────────────────────────────────────────
# C. WORKLOAD PROFILES
# ─────────────────────────────────────────────────────────────────────────────

CPM = 27_000   # CoreMark per core baseline

PROFILES = {
    # vCPU varies: tests whether compute-dominated requirements shift w_fit
    "compute_tiny":     {"vcpu":  2, "memory_gib":   8, "network_mbps":     0, "max_price": 10.0},
    "compute_small":    {"vcpu":  4, "memory_gib":  16, "network_mbps":  1000, "max_price": 10.0},
    "compute_medium":   {"vcpu": 16, "memory_gib":  64, "network_mbps": 25000, "max_price": 10.0},
    "compute_large":    {"vcpu": 64, "memory_gib": 128, "network_mbps": 25000, "max_price": 10.0},
    "compute_xlarge":   {"vcpu": 96, "memory_gib": 192, "network_mbps": 25000, "max_price": 20.0},

    # Memory-dominated: should push fit weight up when memory is hard constraint
    "memory_moderate":  {"vcpu":  8, "memory_gib": 256, "network_mbps":  5000, "max_price": 10.0},
    "memory_heavy":     {"vcpu": 16, "memory_gib": 512, "network_mbps":  5000, "max_price": 20.0},

    # Network-dominated
    "network_moderate": {"vcpu":  8, "memory_gib":  32, "network_mbps": 50000, "max_price": 10.0},
    "network_heavy":    {"vcpu": 16, "memory_gib":  64, "network_mbps":100000, "max_price": 10.0},

    # Budget-constrained: tight price should push cost weight up
    "tight_budget":     {"vcpu":  4, "memory_gib":  16, "network_mbps":  1000, "max_price":  0.5},
    "relaxed_budget":   {"vcpu":  4, "memory_gib":  16, "network_mbps":  1000, "max_price": 10.0},

    # Balanced
    "balanced":         {"vcpu": 32, "memory_gib": 128, "network_mbps": 50000, "max_price": 15.0},
}

REQS = {n: {**{k: v for k, v in p.items() if k != "vcpu"},
            "required_compute": p["vcpu"] * CPM}
        for n, p in PROFILES.items()}


# ─────────────────────────────────────────────────────────────────────────────
# D. SYNTHETIC CATALOGUE
# ─────────────────────────────────────────────────────────────────────────────

def make_synthetic(n=800, seed=0):
    rng  = np.random.default_rng(seed)
    vcpu = rng.choice([2,4,8,16,32,48,64,96,128,192], size=n)
    mem  = vcpu * rng.choice([2,4,8,16], size=n)

    # Network correlated with size
    net_map = {2:1000,4:2000,8:5000,16:10000,32:25000,
               48:25000,64:50000,96:100000,128:100000,192:150000}
    net  = np.array([net_map[v] * rng.uniform(0.7, 1.3) for v in vcpu])

    cpm  = rng.uniform(22000, 38000, n)
    ct   = vcpu * cpm
    pr   = vcpu * rng.uniform(0.04, 0.12, n)

    fams = ["c6i","c6a","c6g","m6i","r6i","c5","m5","Standard_D","n2","n2d"]
    szs  = ["small","medium","large","xlarge","2xlarge",
            "4xlarge","8xlarge","16xlarge","32xlarge"]

    def ns(x):
        if x >= 100000: return f"Up to {int(x)} Megabit"
        if x >= 10000:  return f"{int(x/1000)} Gigabit"
        return f"Up to {int(x)} Megabit"

    return pd.DataFrame({
        "instanceType":       [f"{rng.choice(fams)}.{rng.choice(szs)}" for _ in range(n)],
        "provider":           rng.choice(["aws","azure","gcp"], size=n, p=[.5,.3,.2]),
        "vcpu":               vcpu.astype(float),
        "memory":             [f"{m:.1f} GiB" for m in mem],
        "networkPerformance": [ns(x) for x in net],
        "price_per_hr":       pr,
        "coremark_total":     ct,
        "coremark_per_dollar":ct / pr,
        "coremark_per_core":  cpm,
        "physicalProcessor":  rng.choice(["Intel Xeon","AMD EPYC","AWS Graviton3"], size=n),
    })


# ─────────────────────────────────────────────────────────────────────────────
# E. EXPERIMENT
# ─────────────────────────────────────────────────────────────────────────────

def run(df_raw, n_seeds=5, n_calls=30):
    df = add_features(df_raw)
    records = []

    for name, req in REQS.items():
        pool = hard_filter(df, req)
        if len(pool) < 10:
            print(f"  [SKIP] {name}: {len(pool)} candidates after filter")
            continue
        pool = add_fit_score(pool, req)

        ps = {
            "pool_size":       len(pool),
            "fit_mean":        round(pool["fit_score"].mean(), 4),
            "ppd_mean":        round(pool["perf_per_dollar"].mean(), 2),
            "req_compute":     req["required_compute"],
            "req_memory":      req["memory_gib"],
            "req_network":     req.get("network_mbps", 0),
            "req_price":       req.get("max_price", 10.0),
        }
        print(f"\n  {name:<22}  pool={ps['pool_size']:4d}  "
              f"fit_mean={ps['fit_mean']:.3f}")

        for seed in range(n_seeds):
            w = optimize_weights(pool, top_k=10,
                                 n_calls=n_calls, random_state=seed)
            records.append({
                "profile": name, "seed": seed,
                "w_fit":        round(w["fit"],        4),
                "w_cost":       round(w["cost"],       4),
                "w_generation": round(w["generation"], 4),
                **ps
            })
            print(f"    seed={seed}  "
                  f"w_fit={w['fit']:.4f}  "
                  f"w_cost={w['cost']:.4f}  "
                  f"w_gen={w['generation']:.4f}")

    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────────────────
# F. SUMMARY + VERDICT + CORRELATION
# ─────────────────────────────────────────────────────────────────────────────

def summarise(df):
    rows = []
    for name, g in df.groupby("profile"):
        rows.append({
            "profile":     name,
            "pool_size":   int(g["pool_size"].iloc[0]),
            "w_fit_mean":  round(g["w_fit"].mean(), 4),
            "w_fit_std":   round(g["w_fit"].std(ddof=1), 4),
            "w_cost_mean": round(g["w_cost"].mean(), 4),
            "w_cost_std":  round(g["w_cost"].std(ddof=1), 4),
            "w_gen_mean":  round(g["w_generation"].mean(), 4),
            "w_gen_std":   round(g["w_generation"].std(ddof=1), 4),
            "fit_mean":    round(g["fit_mean"].iloc[0], 4),
            "req_compute": int(g["req_compute"].iloc[0]),
            "req_memory":  int(g["req_memory"].iloc[0]),
            "req_network": int(g["req_network"].iloc[0]),
            "req_price":   float(g["req_price"].iloc[0]),
        })
    return pd.DataFrame(rows)


def verdict(summary):
    HI, LO = 0.05, 0.02
    W = [("w_fit","w_fit_mean"), ("w_cost","w_cost_mean"), ("w_gen","w_gen_mean")]

    print("\n" + "=" * 100)
    print("VERDICT TABLE — BO WORKLOAD SENSITIVITY")
    print("=" * 100)
    print(f"{'Profile':<22} {'pool':>5}  "
          f"{'w_fit':>14}  {'w_cost':>14}  {'w_gen':>14}  "
          f"{'fit_mean':>9}  {'vcpu':>5}  {'mem':>5}  {'net_k':>5}")
    print("-" * 100)

    for _, r in summary.iterrows():
        print(f"{r['profile']:<22} {r['pool_size']:>5}  "
              f"{r['w_fit_mean']:.4f}±{r['w_fit_std']:.4f}  "
              f"{r['w_cost_mean']:.4f}±{r['w_cost_std']:.4f}  "
              f"{r['w_gen_mean']:.4f}±{r['w_gen_std']:.4f}  "
              f"{r['fit_mean']:>9.4f}  "
              f"{int(r['req_compute']/CPM):>5}  "
              f"{int(r['req_memory']):>5}  "
              f"{int(r['req_network']//1000):>5}")

    print("-" * 100)
    print("WEIGHT RANGES ACROSS PROFILES:")
    ranges = {}
    for label, col in W:
        lo = summary[col].min()
        hi = summary[col].max()
        rng = hi - lo
        ranges[col] = rng
        v = ("ADAPTING     [OK]" if rng > HI else
             "MARGINAL     [~] " if rng > LO else
             "NOT ADAPTING [!!]")
        print(f"  {label:<8}  {lo:.4f} -> {hi:.4f}  delta={rng:.4f}  {v}")
    print("=" * 100)

    max_rng = max(ranges.values())
    print("\nOVERALL CONCLUSION:")
    if max_rng > HI:
        print(f"  BO IS ADAPTING  (max delta={max_rng:.4f} > threshold={HI})")
        print("  Weights shift meaningfully across workload profiles.")
        print("  BO is earning its latency cost — keep it in the pipeline.")
    elif max_rng > LO:
        print(f"  BO MARGINALLY ADAPTS  (max delta={max_rng:.4f}, range 0.02-0.05)")
        print("  Weak adaptation. Test on more diverse real-world datasets.")
        print("  Consider replacing BO with a lightweight grid search.")
    else:
        print(f"  BO IS NOT ADAPTING  (max delta={max_rng:.4f} < threshold={LO})")
        print("  Weights converge to the same values regardless of workload.")
        print("  The BO objective surface is flat for this dataset.")
        print()
        print("  RECOMMENDATION:")
        print("    Replace optimize_weights() with fixed weights tuned offline:")
        print(f"    w_fit={summary['w_fit_mean'].mean():.3f}  "
              f"w_cost={summary['w_cost_mean'].mean():.3f}  "
              f"w_gen={summary['w_gen_mean'].mean():.3f}")
        print("    This removes 3-9s of latency per request with no quality loss.")

    print()
    for label, col in W:
        rng = ranges[col]
        if rng < LO:
            print(f"  {label}: FLAT — signal weight does not respond to "
                  f"requirement changes. Possibly dominated by pool structure.")
        elif rng < HI:
            print(f"  {label}: WEAK — some variation (delta={rng:.4f}) but "
                  f"not practically meaningful.")
        else:
            print(f"  {label}: RESPONSIVE — shifts with workload (delta={rng:.4f}).")


def correlations(df):
    s = summarise(df)
    print("\n" + "=" * 72)
    print("CORRELATION ANALYSIS")
    print("Pearson r between mean weight and pool/requirement variables")
    print("across profiles. Strong |r|>0.6 means the weight responds.")
    print("=" * 72)

    pvars = {
        "fit_mean":    "pool fit_score mean",
        "req_compute": "required compute",
        "req_memory":  "required memory (GiB)",
        "req_network": "required network (Mbps)",
        "req_price":   "max price constraint",
    }
    for wv in ["w_fit_mean","w_cost_mean","w_gen_mean"]:
        for pv, label in pvars.items():
            if pv not in s.columns or s[pv].std() == 0:
                continue
            r = s[wv].corr(s[pv])
            if np.isnan(r): continue
            strength = ("STRONG  <- responds"  if abs(r) > 0.6 else
                        "MODERATE"             if abs(r) > 0.3 else
                        "WEAK    <- flat")
            print(f"  {wv:<14}  vs  {label:<28}  r={r:+.3f}  {strength}")

    print("=" * 72)
    print()
    print("  STRONG  (|r|>0.6): weight adapts to this variable — BO is doing work.")
    print("  WEAK    (|r|<0.3): weight ignores this variable — BO is not adapting.")


# ─────────────────────────────────────────────────────────────────────────────
# G. ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data",      default="combined_vms.csv")
    p.add_argument("--synthetic", action="store_true")
    p.add_argument("--n-seeds",   type=int, default=5)
    p.add_argument("--n-calls",   type=int, default=30)
    args = p.parse_args()

    print(f"BO backend  : {'skopt' if _SKOPT else 'built-in GP (skopt not installed)'}")

    if args.synthetic:
        print("Data        : synthetic (800 instances)")
        df_raw = make_synthetic(800, 0)
    elif os.path.exists(args.data):
        print(f"Data        : {args.data}")
        df_raw = pd.read_csv(args.data)
    else:
        print(f"'{args.data}' not found — using synthetic data.")
        df_raw = make_synthetic(800, 0)

    print(f"Profiles    : {len(REQS)}")
    print(f"Seeds/profile: {args.n_seeds}")
    print(f"BO calls    : {args.n_calls}\n")

    results = run(df_raw, n_seeds=args.n_seeds, n_calls=args.n_calls)

    if results.empty:
        print("\n[ERROR] All profiles were skipped. Use --synthetic or a larger CSV.")
        return

    s = summarise(results)

    out = os.path.dirname(os.path.abspath(__file__))
    results.to_csv(os.path.join(out, "bo_workload_sensitivity.csv"), index=False)
    s.to_csv(os.path.join(out, "bo_workload_sensitivity_summary.csv"), index=False)
    print(f"\nSaved: bo_workload_sensitivity.csv")
    print(f"Saved: bo_workload_sensitivity_summary.csv")

    verdict(s)
    correlations(results)


if __name__ == "__main__":
    main()