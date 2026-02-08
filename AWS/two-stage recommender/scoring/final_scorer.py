# scoring/final_scorer.py
def rank_instances(df, weights):
    df = df.copy()

    df["final_score"] = (
        weights["fit"] * df["fit_score"]
        + weights["cost"] * df["cost_eff"]
        + weights["generation"] * df["generation_score"]
    )

    return df.sort_values("final_score", ascending=False)
