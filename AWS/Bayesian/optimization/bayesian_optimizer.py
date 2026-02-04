from skopt import gp_minimize
from skopt.space import Real

def optimize_weights(df, scorer_fn, top_k=5, n_calls=30):

    space = [
        Real(0.01, 1.0, name="compute"),
        Real(0.01, 1.0, name="memory"),
        Real(0.01, 1.0, name="network"),
        Real(0.01, 1.0, name="cost"),
    ]

    def objective(params):
        weights = dict(zip(["compute", "memory", "network", "cost"], params))

        # Normalize weights
        s = sum(weights.values())
        weights = {k: v / s for k, v in weights.items()}

        ranked = scorer_fn(df.copy(), weights)
        score = ranked.head(top_k)["final_score"].mean()

        # Minimize negative score
        return -score

    result = gp_minimize(
        objective,
        space,
        n_calls=n_calls,
        random_state=42
    )

    best_weights = dict(zip(["compute", "memory", "network", "cost"], result.x))
    s = sum(best_weights.values())
    best_weights = {k: v / s for k, v in best_weights.items()}

    return best_weights
