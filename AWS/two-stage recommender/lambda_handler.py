from main import run_recommendation

# Baseline CoreMark score per core used to translate a vCPU requirement
# into an absolute compute score the ranker can filter on.
BASELINE_COREMARK_PER_CORE = 27000


def lambda_handler(event, context):

    vcpu         = event["vcpu"]
    memory_gib   = event["memory_gib"]
    network_mbps = event["network_mbps"]
    max_price    = event.get("max_price", 10.0)

    required_compute = vcpu * BASELINE_COREMARK_PER_CORE

    requirements = {
        "required_compute": required_compute,
        "memory_gib":       memory_gib,
        "network_mbps":     network_mbps,
        "max_price":        max_price,
    }

    results = run_recommendation(requirements)

    # run_recommendation returns either a list of dicts (success) or a
    # single dict with an "error" key.  Pass both through unchanged so
    # the Step Functions state machine can surface errors cleanly.
    return {"recommended_instances": results}