from main import run_recommendation

# Baseline coremark_per_core for a modern CPU
# Used to convert observed vCPU count into required_compute
BASELINE_COREMARK_PER_CORE = 27000


def lambda_handler(event, context):

    vcpu         = event["vcpu"]
    memory_gib   = event["memory_gib"]
    network_mbps = event["network_mbps"]
    max_price    = event.get("max_price", 10.0)

    # Convert vCPU requirement → coremark_total requirement
    required_compute = vcpu * BASELINE_COREMARK_PER_CORE

    requirements = {
        "required_compute": required_compute,
        "memory_gib":       memory_gib,
        "network_mbps":     network_mbps,
        "max_price":        max_price,
    }

    results = run_recommendation(requirements)

    return {"recommended_instances": results}
