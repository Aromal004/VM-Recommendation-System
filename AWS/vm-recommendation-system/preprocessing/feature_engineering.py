import pandas as pd
import numpy as np
import re

def extract_network(val):
    if pd.isna(val):
        return 0
    match = re.search(r"\d+", str(val))
    return float(match.group()) if match else 0


def feature_engineering(df):
    df = df.copy()

    df["ecu"] = df["ecu"].fillna(0)
    df["normalizationSizeFactor"] = df["normalizationSizeFactor"].fillna(1)
    df["physicalCores"] = df["physicalCores"].fillna(df["vcpu"])

    df["compute_score"] = (
        df["physicalCores"] * df["clockSpeed"].fillna(0)
    ) + df["ecu"]

    df["memory_score"] = df["memory"] * df["normalizationSizeFactor"]

    df["network_score"] = (
        df["networkPerformance"].apply(extract_network) +
        df["enhancedNetworkingSupported"].map({"Yes": 1, "No": 0}).fillna(0)
    )

    df["cost_efficiency"] = df["compute_score"] / df["price_per_hr"].replace(0, np.nan)

    return df
