def recommend_vms(df, top_k=5):
    return (
        df.sort_values("final_score", ascending=False)
        .head(top_k)
        [["instanceType", "vcpu", "memory", "price_per_hr", "final_score"]]
    )
