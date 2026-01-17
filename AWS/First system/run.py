import pandas as pd
import re

from workload import get_workload
from recommender import recommend_vms

# ---------------- PARSERS ----------------

def parse_memory(val):
    """
    Handles:
    - '192 GiB'
    - '8 GB'
    - numeric values
    """
    if pd.isna(val):
        return 0.0

    if isinstance(val, (int, float)):
        return float(val)

    nums = re.findall(r"\d+\.?\d*", str(val))
    return float(nums[0]) if nums else 0.0

def parse_clock(val):
    """
    Handles:
    - '3.5 GHz'
    - 'Up to 3.7 GHz'
    - numeric values
    """
    if pd.isna(val):
        return 0.0

    if isinstance(val, (int, float)):
        return float(val)

    nums = re.findall(r"\d+\.?\d*", str(val))
    return float(nums[0]) if nums else 0.0


def parse_network(val):
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    nums = re.findall(r"\d+", str(val))
    return float(nums[0]) if nums else 0.0

def parse_storage(val):
    if pd.isna(val):
        return "SSD"
    v = str(val).upper()
    return "SSD" if ("SSD" in v or "NVME" in v) else "HDD"

# ---------------- LOAD DATA ----------------

df = pd.read_csv("C:/Users/USER/Desktop/Final Project/AWS/aws_with_coremark.csv")

# ---------------- NORMALIZATION ----------------

df["memory"] = df["memory"].apply(parse_memory)
df["clockSpeed"] = df["clockSpeed"].apply(parse_clock)
df["networkPerformance"] = df["networkPerformance"].apply(parse_network)
df["storageMedia"] = df["storage"].apply(parse_storage)

numeric_cols = [
    "vcpu",
    "physicalCores",
    "ecu",
    "normalizationSizeFactor",
    "price_per_hr",
    "coremark_per_core",
    "coremark_total",
    "coremark_per_dollar"
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# Remove zero-price rows (important)
df = df[df["price_per_hr"] > 0]

# ---------------- RUN MODEL ----------------

workload = get_workload("network_intensive")

recommended = recommend_vms(
    df=df,
    workload=workload,
    min_memory=8,
    max_price=5.0,
    top_k=5
)

print("\nRecommended VMs:\n")
print(recommended)
