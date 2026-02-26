import numpy as np


def add_fit_score(df, req):
    df = df.copy()

    # Oversize penalties only
    compute_penalty = (
        (df["compute_score"] - req["required_compute"])
        / req["required_compute"]
    )

    mem_penalty = (
        (df["memory_gib"] - req["memory_gib"])
        / req["memory_gib"]
    )

    net_penalty = (
        (df["network_mbps"] - req["network_mbps"])
        / req["network_mbps"]
    )

    # Clip negative values (underpowered already removed)
    compute_penalty = compute_penalty.clip(lower=0)
    mem_penalty = mem_penalty.clip(lower=0)
    net_penalty = net_penalty.clip(lower=0)

    df["fit_score"] = 1 / (
        1 + compute_penalty + mem_penalty + net_penalty
    )

    return df