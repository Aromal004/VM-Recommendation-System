import pandas as pd
import boto3

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter         import hard_filter
from scoring.fit_score                 import add_fit_score
from optimization.bayesian_ranker      import optimize_weights
from scoring.final_scorer              import rank_instances
from postprocessing.diversify          import diversify

BASELINE_COREMARK_PER_CORE = 27000


def load_dataset():
    s3  = boto3.client("s3")
    obj = s3.get_object(
        Bucket="vm-recommendation-data",
        Key="aws_with_coremark.csv"
    )
    return pd.read_csv(obj["Body"])


def run_recommendation(requirements):
    df = load_dataset()

    # Stage 1: feature engineering + data cleaning
    df = add_features(df)
    if df.empty:
        return {"error": "Dataset empty after feature engineering"}

    # Stage 2: hard filter
    df = hard_filter(df, requirements)
    if df.empty:
        return {"error": "No instances satisfy constraints"}

    # Stage 3: fit scoring
    df = add_fit_score(df, requirements)

    # Stage 4: bayesian weight optimisation
    weights = optimize_weights(df)

    # Stage 5: rank
    ranked = rank_instances(df, weights)

    # Stage 6: diversify across families
    final = diversify(ranked, per_family=2, top_n=10)

    return final[[
        "instanceType",
        "physicalProcessor",
        "vcpu",
        "compute_score",
        "memory_gib",
        "network_mbps",
        "price_per_hr",
        "perf_per_dollar",
        "final_score"
    ]].to_dict(orient="records")
