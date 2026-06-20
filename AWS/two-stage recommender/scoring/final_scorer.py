import pandas as pd

def _minmax(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series(1.0, index=series.index)
    return (series - lo) / (hi - lo)

def rank_instances(df, weights):
    df = df.copy()

    # Normalise each signal to [0, 1] so no single column dominates
    # purely because of its unit magnitude (e.g. perf_per_dollar >> fit_score)
    fit_norm   = _minmax(df["fit_score"])
    cost_norm  = _minmax(df["perf_per_dollar"])
    gen_norm   = _minmax(df["generation_score"])

    df["final_score"] = (
        weights["fit"]        * fit_norm
        + weights["cost"]     * cost_norm
        + weights["generation"] * gen_norm
    )

    return df.sort_values("final_score", ascending=False)