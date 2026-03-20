# optimization/bayesian_ranker.py
from skopt import gp_minimize
from skopt.space import Real


def optimize_weights(df, top_k=10, n_calls=30, random_state=42):

    space = [
        Real(0.3, 0.7, name="fit"),
        Real(0.1, 0.4, name="cost"),
        Real(0.05, 0.2, name="generation"),
    ]

    def objective(params):
        w = dict(zip(["fit", "cost", "generation"], params))
        s = sum(w.values())
        w = {k: v / s for k, v in w.items()}

        score = (
            w["fit"] * df["fit_score"]
            + w["cost"] * df["perf_per_dollar"]
            + w["generation"] * df["generation_score"]
        )

        return -score.nlargest(top_k).mean()

    # n_initial_points must be <= n_calls - 1 (skopt requirement).
    # Default is 10, which breaks when n_calls < 12 (e.g. sensitivity study).
    n_initial = min(max(n_calls - 2, 1), 10)

    res = gp_minimize(objective, space, n_calls=n_calls,
                      n_initial_points=n_initial, random_state=random_state)

    weights = dict(zip(["fit", "cost", "generation"], res.x))
    s = sum(weights.values())
    return {k: v / s for k, v in weights.items()}