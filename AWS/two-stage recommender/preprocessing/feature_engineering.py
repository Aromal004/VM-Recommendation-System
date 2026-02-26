import pandas as pd
import re
import numpy as np


def parse_network_mbps(val):
    if pd.isna(val):
        return 0.0
    m = re.search(r"([\d\.]+)", str(val))
    return float(m.group(1)) if m else 0.0


def add_features(df):
    df = df.copy()

    # Remove invalid pricing
    df = df[df["price_per_hr"] > 0]

    # Remove infinite / missing performance values
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["coremark_total", "coremark_per_dollar"])

    # Convert numeric
    df["vcpu"] = df["vcpu"].astype(float)

    df["memory_gib"] = (
        df["memory"]
        .str.replace(" GiB", "", regex=False)
        .astype(float)
    )

    df["network_mbps"] = df["networkPerformance"].apply(parse_network_mbps)

    # Use CoreMark as compute signal
    df["compute_score"] = df["coremark_total"]

    # Performance per dollar
    df["perf_per_dollar"] = df["coremark_per_dollar"]

    # Since no generation column exists,
    # we approximate generation via CoreMark per core
    df["generation_score"] = (
        df["coremark_per_core"] / df["coremark_per_core"].max()
    )

    # Instance family
    df["family"] = df["instanceType"].str.split(".").str[0]

    return df