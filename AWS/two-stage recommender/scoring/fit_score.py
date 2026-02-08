# scoring/fit_score.py
import numpy as np

def add_fit_score(df, req):
    df = df.copy()

    df["cpu_penalty"] = abs(df["vcpu"] - req["vcpu"]) / req["vcpu"]
    df["mem_penalty"] = abs(df["memory_gib"] - req["memory_gib"]) / req["memory_gib"]
    df["net_penalty"] = abs(df["network_mbps"] - req["network_mbps"]) / req["network_mbps"]

    df["fit_score"] = 1 / (
        1 + df["cpu_penalty"] + df["mem_penalty"] + df["net_penalty"]
    )

    return df
