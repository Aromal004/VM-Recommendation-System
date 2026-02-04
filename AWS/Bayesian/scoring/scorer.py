def score_vms(df, weights):
    df["final_score"] = (
        weights["compute"] * df["compute_score"] +
        weights["memory"] * df["memory_score"] +
        weights["network"] * df["network_score"] +
        weights["cost"] * df["cost_efficiency"]
    )

    return df.sort_values("final_score", ascending=False)
