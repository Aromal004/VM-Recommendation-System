import pandas as pd
import re
import numpy as np


def parse_network_mbps(val):
    """
    Handles all formats in the dataset:
      'Up to 12500 Megabit'  -> 12500.0
      '10 Gigabit'           -> 10000.0
      '150000 Megabit'       -> 150000.0
    """
    if pd.isna(val):
        return 0.0
    s = str(val).lower()
    m = re.search(r"([\d\.]+)\s*gigabit", s)
    if m:
        return float(m.group(1)) * 1000
    m = re.search(r"([\d\.]+)\s*megabit", s)
    if m:
        return float(m.group(1))
    m = re.search(r"([\d\.]+)\s*gbps", s)
    if m:
        return float(m.group(1)) * 1000
    m = re.search(r"([\d\.]+)\s*mbps", s)
    if m:
        return float(m.group(1))
    m = re.search(r"([\d\.]+)", s)
    return float(m.group(1)) if m else 0.0


def add_features(df):
    df = df.copy()

    # Drop rows with no instanceType
    df = df.dropna(subset=["instanceType"])
    df["instanceType"] = df["instanceType"].astype(str)

    # Remove invalid pricing
    df = df[df["price_per_hr"] > 0]

    # Remove inf / missing coremark values
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["coremark_total", "coremark_per_dollar"])

    # Core specs
    df["vcpu"] = df["vcpu"].astype(float)

    df["memory_gib"] = (
        df["memory"]
        .str.replace(" GiB", "", regex=False)
        .astype(float)
    )

    df["network_mbps"] = df["networkPerformance"].apply(parse_network_mbps)

    # Compute signals
    df["compute_score"]   = df["coremark_total"]
    df["perf_per_dollar"] = df["coremark_per_dollar"]

    # Generation score — use coremark_per_core as proxy (newer chips score higher)
    df["generation_score"] = (
        df["coremark_per_core"] / df["coremark_per_core"].max()
    )

    # Instance family
    df["family"] = df["instanceType"].str.split(".").str[0]

    return df