from config import ALPHA, BETA
from performance_model import compute_performance
from energy_model import compute_energy

def score_vm(row, workload):
    energy = compute_energy(row, workload)
    if energy <= 0:
        return None

    performance = compute_performance(row)
    ppr = performance / energy
    cost_eff = row.get("coremark_per_dollar", 0)

    final_score = (ALPHA * ppr) + (BETA * cost_eff)

    return {
        "performance": performance,
        "energy": energy,
        "ppr": ppr,
        "final_score": final_score
    }
