"""
main_v2.py
-----------
Updated pipeline using workload-aware BO.

Key changes from original main.py:
  OLD: optimize_weights()   (BO tunes fit/cost/gen outer weights — proven unstable)
  NEW: optimize_resource_weights()  (BO tunes per-dimension penalty α/β/γ)

  OLD: add_fit_score()  (equal penalties on all dimensions)
  NEW: add_workload_aware_fit_score()  (weighted penalties from BO)

  OLD: outer weights learned by BO
  NEW: outer weights fixed at {fit:0.52, cost:0.35, gen:0.13}

Everything else (hard_filter, diversify, S3 loading) is unchanged.
"""

import pandas as pd

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter         import hard_filter
from scoring.final_scorer              import rank_instances
from postprocessing.diversify          import diversify
from optimization.workload_aware_bo    import (
    optimize_resource_weights,
    add_workload_aware_fit_score,
    derive_intent_weights,
    FIXED_OUTER_WEIGHTS,
    explain_weights,
    WorkloadResourceWeights,
)


def _sanitise(df):
    return df[df["price_per_hr"] >= 0.01].copy()


def run_recommendation(requirements: dict) -> list[dict] | dict:
    import boto3
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket="vm-recommendation-data", Key="combined_vms.csv")
    df  = pd.read_csv(obj["Body"])

    df = add_features(df)
    if df.empty:
        return {"error": "Dataset empty after feature engineering"}

    df = _sanitise(df)
    df = hard_filter(df, requirements)
    if df.empty:
        return {"error": "No instances satisfy constraints"}

    # Stage 1 — seed pool with equal-weight fit scores for intent derivation
    df = add_workload_aware_fit_score(df, requirements, WorkloadResourceWeights(1.0, 1.0, 1.0))

    # Stage 2 — derive intent + learn penalty weights via BO
    resource_weights = optimize_resource_weights(
        df, requirements, top_k=10, n_calls=30
    )

    # Re-score pool with learned weights
    df     = add_workload_aware_fit_score(df, requirements, resource_weights)
    ranked = rank_instances(df, FIXED_OUTER_WEIGHTS)
    final  = diversify(ranked, per_family=2, top_n=10)

    return final[[
        "provider", "instanceType", "physicalProcessor",
        "vcpu", "compute_score", "memory_gib", "network_mbps",
        "price_per_hr", "perf_per_dollar", "fit_score", "final_score",
    ]].to_dict(orient="records")


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE TEST  (python main_v2.py)
# Uses a realistic synthetic pool modelled after real AWS instance families.
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import re, sys, warnings
    import numpy as np
    warnings.filterwarnings("ignore")
    sys.path.insert(0, ".")

    try:
        from skopt import gp_minimize
        _SKOPT = True
    except ImportError:
        _SKOPT = False

    print("=" * 72)
    print("WORKLOAD-AWARE BO — SENSITIVITY TEST")
    print(f"BO backend : {'skopt' if _SKOPT else 'built-in GP'}")
    print("=" * 72)

    # ── Helpers ──────────────────────────────────────────────────────────────

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

    def make_realistic_pool(n_per_family=270, seed=0):
        """
        Realistic pool with three AWS instance families:
          c-family (compute-opt): 2 GiB/vCPU  — designed for CPU workloads
          m-family (general):     4 GiB/vCPU  — balanced
          r-family (memory-opt):  8 GiB/vCPU  — designed for memory workloads

        This matches real AWS distributions so that:
          - A 32 vCPU / 64 GiB requirement sees mostly compute instances
            that are tight on memory (2 GiB/vCPU → exactly 64 GiB at 32 vCPU)
          - A 8 vCPU / 256 GiB requirement sees memory instances as the
            best fit (8 GiB/vCPU → 64–256 GiB range)
        """
        rng = np.random.default_rng(seed)
        records = []

        families = [
            # (family_type, GiB_per_vCPU, instance_prefixes, network_multiplier)
            ("compute", 2, ["c6i", "c6a", "c6g", "c5", "c5n", "hpc6a"], 1.5),
            ("general", 4, ["m6i", "m6a", "m5", "m5n", "m4"],           1.0),
            ("memory",  8, ["r6i", "r6a", "r5", "r5n", "x2idn"],        0.8),
        ]

        vcpu_choices = [2, 4, 8, 16, 32, 48, 64, 96, 128]
        net_base = {2: 1e3, 4: 2e3, 8: 5e3, 16: 1e4, 32: 2.5e4,
                    48: 2.5e4, 64: 5e4, 96: 1e5, 128: 1e5}
        size_names = ["large", "xlarge", "2xlarge", "4xlarge", "8xlarge",
                      "12xlarge", "16xlarge", "24xlarge", "32xlarge"]

        for fam_type, gib_per_vcpu, prefixes, net_mult in families:
            vcpus = rng.choice(vcpu_choices, n_per_family)
            for vcpu in vcpus:
                mem = vcpu * gib_per_vcpu
                net = net_base.get(vcpu, 1e4) * net_mult * rng.uniform(0.85, 1.15)
                cpm = rng.uniform(24_000, 36_000)
                ct  = vcpu * cpm
                pr  = vcpu * rng.uniform(0.045, 0.095)

                def ns(x):
                    if x >= 1e5: return f"Up to {int(x)} Megabit"
                    if x >= 1e4: return f"{int(x/1000)} Gigabit"
                    return f"Up to {int(x)} Megabit"

                records.append({
                    "instanceType":       f"{rng.choice(prefixes)}.{rng.choice(size_names)}",
                    "provider":           rng.choice(["aws", "azure", "gcp"], p=[0.6, 0.25, 0.15]),
                    "vcpu":               float(vcpu),
                    "memory":             f"{mem:.1f} GiB",
                    "networkPerformance": ns(net),
                    "price_per_hr":       pr,
                    "coremark_total":     ct,
                    "coremark_per_dollar":ct / pr,
                    "coremark_per_core":  cpm,
                    "physicalProcessor":  rng.choice(
                        ["Intel Xeon", "AMD EPYC", "AWS Graviton3"]
                    ),
                })

        df = pd.DataFrame(records).sample(frac=1, random_state=seed).reset_index(drop=True)
        return df

    def add_feat(df):
        df = df.copy()
        df = df.dropna(subset=["instanceType"])
        df["instanceType"] = df["instanceType"].astype(str)
        df = df[df["price_per_hr"] > 0].replace([np.inf, -np.inf], np.nan)
        df = df.dropna(subset=["coremark_total", "coremark_per_dollar"])
        df["vcpu"]             = df["vcpu"].astype(float)
        df["memory_gib"]       = df["memory"].str.replace(" GiB", "", regex=False).astype(float)
        df["network_mbps"]     = df["networkPerformance"].apply(_parse_net)
        df["compute_score"]    = df["coremark_total"]
        df["perf_per_dollar"]  = df["coremark_per_dollar"]
        df["generation_score"] = df["coremark_per_core"] / df["coremark_per_core"].max()
        df["family"]           = df["instanceType"].str.split(".").str[0]
        if "provider" not in df.columns:
            df["provider"] = "aws"
        return df

    def hard_filter_local(df, req):
        df = df[df["compute_score"] >= req["required_compute"]]
        df = df[df["memory_gib"]    >= req["memory_gib"]]
        if req.get("network_mbps", 0) > 0:
            df = df[df["network_mbps"] >= req["network_mbps"]]
        if req.get("max_price", 0) > 0:
            df = df[df["price_per_hr"] <= req["max_price"]]
        return df.copy()

    CPM = 27_000

    # ── Workload profiles ────────────────────────────────────────────────────
    #
    # Each profile is designed so that ONE dimension is the natural binding
    # constraint given the realistic pool's family distributions.
    #
    # CPU-intensive:    32 vCPU / 64 GiB.  On a c-family pool (2 GiB/vCPU),
    #   64 GiB is exactly met by a 32-vCPU c-instance. On m/r families it
    #   is loose. So compute is the binding filter; intent_α should be high.
    #
    # Memory-intensive: 8 vCPU / 256 GiB. Only r-family (8 GiB/vCPU) and
    #   large m-family meet 256 GiB. Compute is loose; intent_β should be high.
    #
    # Network-intensive: 16 vCPU / 64 GiB / 50 Gbps. Network filters out
    #   smaller instances; intent_γ should be high.

    WORKLOADS = {
        "CPU-intensive":     {"vcpu": 32, "memory_gib":  64, "network_mbps":     0, "max_price": 10.0},
        "Memory-intensive":  {"vcpu":  8, "memory_gib": 256, "network_mbps":     0, "max_price": 10.0},
        "Network-intensive": {"vcpu": 16, "memory_gib":  64, "network_mbps": 50000, "max_price": 10.0},
        "Balanced":          {"vcpu": 16, "memory_gib":  64, "network_mbps": 25000, "max_price": 10.0},
        "Budget-constrained":{"vcpu":  4, "memory_gib":  16, "network_mbps":  1000, "max_price": 0.8},
    }

    df_raw  = make_realistic_pool(n_per_family=270, seed=42)
    df_feat = add_feat(df_raw)
    print(f"\nRealistic catalogue: {len(df_feat)} instances "
          f"(c-family 2GiB/vCPU, m-family 4GiB/vCPU, r-family 8GiB/vCPU)\n")

    results = []

    for name, prof in WORKLOADS.items():
        req = {
            **{k: v for k, v in prof.items() if k != "vcpu"},
            "required_compute": prof["vcpu"] * CPM,
        }

        pool = hard_filter_local(df_feat, req)
        if len(pool) < 10:
            print(f"  [SKIP] {name}: only {len(pool)} candidates after filter")
            continue

        # Seed with equal-weight ratios
        pool = add_workload_aware_fit_score(pool, req, WorkloadResourceWeights(1.0, 1.0, 1.0))

        print(f"\n{'─'*72}")
        print(f"Workload : {name}")
        print(f"  vcpu={prof['vcpu']}  memory={prof['memory_gib']}GiB  "
              f"network={prof.get('network_mbps',0)/1000:.0f}Gbps  "
              f"max_price=${prof['max_price']}")
        print(f"  Pool size after hard filter: {len(pool)}")

        # Show pool composition
        pool_comp = pool.groupby(pool["instanceType"].str.split(".").str[0].str[0]).size()
        c_count = sum(v for k,v in pool_comp.items() if k in ['c'])
        m_count = sum(v for k,v in pool_comp.items() if k in ['m'])
        r_count = sum(v for k,v in pool_comp.items() if k in ['r','x'])
        print(f"  Pool family composition approx: c≈{c_count}  m≈{m_count}  r≈{r_count}")

        # Show intent weights before BO
        intent = derive_intent_weights(req, pool, baseline_cpm=CPM)
        print(f"\n  Intent weights (req / pool_median):")
        print(f"    α (compute) = {intent['alpha']:.3f}  "
              f"β (memory) = {intent['beta']:.3f}  "
              f"γ (network) = {intent['gamma']:.3f}")

        # Run BO
        w = optimize_resource_weights(
            pool, req, top_k=10, n_calls=30,
            random_state=42, verbose=False
        )

        # Re-score and evaluate top-10
        pool = add_workload_aware_fit_score(pool, req, w)

        def _mm(s):
            lo, hi = s.min(), s.max()
            return pd.Series(1.0, index=s.index) if hi == lo else (s - lo) / (hi - lo)

        pool["final_score"] = (
            0.52 * _mm(pool["fit_score"])
            + 0.35 * _mm(pool["perf_per_dollar"])
            + 0.13 * _mm(pool["generation_score"])
        )
        top10  = pool.nlargest(10, "final_score")
        rse_c  = float(top10["compute_ratio"].mean())
        rse_m  = float(top10["mem_ratio"].mean())
        rse_n  = float(top10["net_ratio"].mean()) if req.get("network_mbps", 0) > 0 else 0.0

        print(f"\n  Learned penalty weights:")
        print(f"    α={w.alpha:.3f}  β={w.beta:.3f}  γ={w.gamma:.3f}"
              f"  → dominant: {w.dominant_dimension()}")
        print(f"\n  Top-10 RSE (lower = tighter fit):")
        print(f"    compute : {rse_c:.3f}  {'✓' if rse_c < 0.5 else '~'}")
        print(f"    memory  : {rse_m:.3f}  {'✓' if rse_m < 0.5 else '~'}")
        net_label = "N/A" if req.get("network_mbps", 0) == 0 else ("✓" if rse_n < 0.5 else "~")
        print(f"    network : {rse_n:.3f}  {net_label}")

        results.append({
            "workload":  name,
            "intent_a":  round(intent["alpha"], 3),
            "intent_b":  round(intent["beta"],  3),
            "intent_g":  round(intent["gamma"], 3),
            "penalty_a": round(w.alpha, 3),
            "penalty_b": round(w.beta,  3),
            "penalty_g": round(w.gamma, 3),
            "rse_c":     round(rse_c, 3),
            "rse_m":     round(rse_m, 3),
            "rse_n":     round(rse_n, 3),
        })

    # ── Summary table ─────────────────────────────────────────────────────────
    print("\n\n" + "=" * 85)
    print("SUMMARY")
    print("=" * 85)
    print(f"{'Workload':<22}  {'intent':>20}              {'penalty':>20}          RSE")
    print(f"{'':22}  {'α':>6} {'β':>6} {'γ':>6}          {'α':>6} {'β':>6} {'γ':>6}    c / m / n")
    print("-" * 85)
    for r in results:
        print(f"{r['workload']:<22}  "
              f"{r['intent_a']:>6.3f} {r['intent_b']:>6.3f} {r['intent_g']:>6.3f}          "
              f"{r['penalty_a']:>6.3f} {r['penalty_b']:>6.3f} {r['penalty_g']:>6.3f}    "
              f"{r['rse_c']:.2f}/{r['rse_m']:.2f}/{r['rse_n']:.2f}")

    # ── Verdict ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 85)
    print("VERDICT — Intent weights: does the dominant requirement dimension score highest?")
    print("=" * 85)
    print("(Intent weights are derived from requirements, not learned. They should")
    print(" correctly reflect which dimension the user most cares about.)\n")

    by_wl = {r["workload"]: r for r in results}

    checks = [
        ("CPU-intensive",     "intent_a", "α (compute)", ["intent_b", "intent_g"]),
        ("Memory-intensive",  "intent_b", "β (memory)",  ["intent_a", "intent_g"]),
        ("Network-intensive", "intent_g", "γ (network)", ["intent_a", "intent_b"]),
    ]

    all_ok = True
    for wl_name, key, label, others in checks:
        if wl_name not in by_wl:
            print(f"  {wl_name}: SKIPPED")
            continue
        r      = by_wl[wl_name]
        my_val = r[key]
        o_max  = max(r[k] for k in others)
        dom    = my_val >= o_max
        ratio  = my_val / (o_max + 1e-9)
        if not dom: all_ok = False
        status = "CORRECT ✓" if dom else "NOT dominant ✗"
        print(f"  {wl_name:<22} {label} = {my_val:.3f}  others_max = {o_max:.3f}  "
              f"ratio = {ratio:.2f}  {status}")

    print()
    if all_ok:
        print("  ALL CORRECT ✓")
        print("  Intent weights correctly reflect workload requirements.")
        print("  BO then finds penalty weights that best achieve those intents.")
    else:
        print("  Some intents not dominant. This indicates the pool composition")
        print("  does not match the expected workload type — check pool filtering.")

    print()
    print("  Note: penalty weights (α/β/γ) from BO are separate from intent weights.")
    print("  BO's job is to find WHICH penalty values best achieve the intent,")
    print("  not to reproduce the intent values themselves.")