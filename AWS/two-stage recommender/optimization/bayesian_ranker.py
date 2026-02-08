# optimization/bayesian_ranker.py
from skopt import gp_minimize
from skopt.space import Real

def optimize_weights(df, top_k=10, n_calls=30):

    space = [
        Real(0.2, 0.6, name="fit"),
        Real(0.1, 0.4, name="cost"),
        Real(0.05, 0.2, name="generation"),
    ]

    def objective(params):
        w = dict(zip(["fit", "cost", "generation"], params))
        s = sum(w.values())
        w = {k: v / s for k, v in w.items()}

        score = (
            w["fit"] * df["fit_score"]
            + w["cost"] * df["cost_eff"]
            + w["generation"] * df["generation_score"]
        )

        return -score.nlargest(top_k).mean()

    res = gp_minimize(objective, space, n_calls=n_calls, random_state=42)

    weights = dict(zip(["fit", "cost", "generation"], res.x))
    s = sum(weights.values())
    return {k: v / s for k, v in weights.items()}
