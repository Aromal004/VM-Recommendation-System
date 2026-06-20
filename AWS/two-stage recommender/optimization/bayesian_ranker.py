from skopt import gp_minimize
from skopt.space import Real
import pandas as pd


def _minmax(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series(1.0, index=series.index)
    return (series - lo) / (hi - lo)


def optimize_weights(df, top_k=10, n_calls=30, random_state=42):
    # Normalise signals once so the objective sees balanced scales
    fit_n  = _minmax(df["fit_score"]).values
    cost_n = _minmax(df["perf_per_dollar"]).values
    gen_n  = _minmax(df["generation_score"]).values

    space = [
        Real(0.3, 0.7, name="fit"),
        Real(0.1, 0.4, name="cost"),
        Real(0.05, 0.2, name="generation"),
    ]

    def objective(params):
        w = dict(zip(["fit", "cost", "generation"], params))
        s = sum(w.values())
        w = {k: v / s for k, v in w.items()}

        scores = w["fit"] * fit_n + w["cost"] * cost_n + w["generation"] * gen_n
        top_scores = sorted(scores, reverse=True)[:top_k]
        return -sum(top_scores) / len(top_scores)

    res = gp_minimize(objective, space, n_calls=n_calls, random_state=random_state)

    weights = dict(zip(["fit", "cost", "generation"], res.x))
    s = sum(weights.values())
    return {k: v / s for k, v in weights.items()}