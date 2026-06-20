import pandas as pd
import boto3

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter         import hard_filter
from scoring.fit_score                 import add_fit_score
from optimization.bayesian_ranker      import optimize_weights
from scoring.final_scorer              import rank_instances
from postprocessing.diversify          import diversify

S3_BUCKET = "vm-recommendation-data"
S3_KEY    = "combined_vms.csv"


def load_dataset() -> pd.DataFrame:
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    return pd.read_csv(obj["Body"])


def _sanitise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows with obviously bad pricing data.
    A real VM costs at least $0.01/hr; anything below that is a dataset
    artefact (e.g. the $0.002 Azure M192 rows) that explodes perf_per_dollar.
    """
    return df[df["price_per_hr"] >= 0.01].copy()


def run_recommendation(requirements: dict) -> list[dict] | dict:
    df = load_dataset()

    df = add_features(df)
    if df.empty:
        return {"error": "Dataset empty after feature engineering"}

    df = _sanitise(df)          # ← remove price outliers before scoring
    if df.empty:
        return {"error": "Dataset empty after price sanity filter"}

    df = hard_filter(df, requirements)
    if df.empty:
        return {"error": "No instances satisfy constraints"}

    df      = add_fit_score(df, requirements)
    weights = optimize_weights(df)
    ranked  = rank_instances(df, weights)
    final   = diversify(ranked, per_family=2, top_n=10)

    return final[[
        "provider",
        "instanceType",
        "physicalProcessor",
        "vcpu",
        "compute_score",
        "memory_gib",
        "network_mbps",
        "price_per_hr",
        "perf_per_dollar",
        "final_score",
    ]].to_dict(orient="records")