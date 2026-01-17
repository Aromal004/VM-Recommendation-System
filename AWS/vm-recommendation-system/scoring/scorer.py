from config.weights import WEIGHTS

def score_vms(df, workload_type):
    w = WEIGHTS[workload_type]

    df = df.copy()
    df["final_score"] = (
        w["compute"] * df["compute_score_norm"] +
        w["memory"]  * df["memory_score_norm"] +
        w["network"] * df["network_score_norm"] +
        w["cost"]    * df["cost_efficiency_norm"]
    )
    return df
