import numpy as np

def add_fit_score(df, req):
    df = df.copy()

    # Oversize penalties only (underpowered already removed by hard_filter)
    compute_penalty = (
        (df["compute_score"] - req["required_compute"]) / req["required_compute"]
    ).clip(lower=0)

    mem_penalty = (
        (df["memory_gib"] - req["memory_gib"]) / req["memory_gib"]
    ).clip(lower=0)

    # Avoid divide-by-zero if network_mbps requirement is 0
    if req.get("network_mbps", 0) > 0:
        net_penalty = (
            (df["network_mbps"] - req["network_mbps"]) / req["network_mbps"]
        ).clip(lower=0)
    else:
        net_penalty = 0

    df["fit_score"] = 1 / (1 + compute_penalty + mem_penalty + net_penalty)

    return df
