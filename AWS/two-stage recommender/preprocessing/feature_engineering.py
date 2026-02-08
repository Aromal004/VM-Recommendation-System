# preprocessing/feature_engineering.py
import pandas as pd
import re
import numpy as np

def parse_network_mbps(val):
    if pd.isna(val):
        return 0.0
    m = re.search(r"([\d\.]+)", val)
    return float(m.group(1)) if m else 0.0


def add_features(df):
    df = df.copy()

    df["vcpu"] = df["vcpu"].astype(float)
    df["memory_gib"] = (
        df["memory"]
        .str.replace(" GiB", "", regex=False)
        .astype(float)
    )

    df["network_mbps"] = df["networkPerformance"].apply(parse_network_mbps)

    # Cost efficiency (bounded)
    df["cost_eff"] = 1 / (df["price_per_hr"] + 1e-6)

    # Generation score
    df["generation_score"] = (df["currentGeneration"] == "Yes").astype(int)

    # Instance family buckets
    df["family"] = df["instanceType"].str.split(".").str[0]

    return df
