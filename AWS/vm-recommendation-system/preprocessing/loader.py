import pandas as pd
import re

def extract_number(val):
    if pd.isna(val):
        return None
    match = re.search(r"[\d.]+", str(val))
    return float(match.group()) if match else None


def load_data(csv_path):
    df = pd.read_csv(csv_path, low_memory=False)

    numeric_cols = [
        "vcpu",
        "memory",
        "clockSpeed",
        "physicalCores",
        "price_per_hr",
        "ecu",
        "normalizationSizeFactor",
        "gpu"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(extract_number)

    return df
