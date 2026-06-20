import numpy as np

def add_fit_score(df, req):
    df = df.copy()

    # Oversize ratio — how many times larger than needed (0 = exact fit)
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
        net_ratio = 0

    # Squared penalty: a VM 10x oversized gets penalised far more than 2x oversized.
    # This collapses the score of M192 (47x oversized compute) to near-zero
    # even before cost/generation weights come into play.
    penalty = compute_ratio**2 + mem_ratio**2 + (net_ratio**2 if not isinstance(net_ratio, int) else 0)

    df["fit_score"] = 1 / (1 + penalty)

    return df