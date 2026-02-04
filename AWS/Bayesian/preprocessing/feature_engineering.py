import pandas as pd
import re

def parse_network(network_str):
    if pd.isna(network_str):
        return 0.0

    match = re.search(r"([\d\.]+)", network_str)
    return float(match.group(1)) if match else 0.0


def add_features(df):
    # Compute power
    df["compute_score"] = df["vcpu"]

    # Memory (GiB)
    df["memory_score"] = df["memory"].str.replace(" GiB", "", regex=False).astype(float)

    # Network (Mbps approx)
    df["network_score"] = df["networkPerformance"].apply(parse_network)

    # Cost efficiency
    df["cost_efficiency"] = 1 / (df["price_per_hr"] + 1e-6)

    return df
