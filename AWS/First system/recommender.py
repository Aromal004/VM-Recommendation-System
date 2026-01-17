import pandas as pd
from scorer import score_vm

def recommend_vms(
    df: pd.DataFrame,
    workload,
    min_memory=0,
    max_price=None,
    top_k=5
):
    results = []

    for _, row in df.iterrows():
        if row["memory"] < min_memory:
            continue

        if max_price is not None and row["price_per_hr"] > max_price:
            continue

        scores = score_vm(row, workload)
        if scores is None:
            continue

        results.append({
            "instance": row["servicename"],
            "price_per_hr": row["price_per_hr"],
            **scores
        })

    return (
        pd.DataFrame(results)
        .sort_values("final_score", ascending=False)
        .head(top_k)
    )
