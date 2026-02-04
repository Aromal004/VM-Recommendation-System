def filter_vms_by_workload(df, constraints):
    if "min_vcpu" in constraints:
        df = df[df["vcpu"] >= constraints["min_vcpu"]]

    if "min_memory" in constraints:
        df = df[df["memory_score"] >= constraints["min_memory"]]

    if "min_network" in constraints:
        df = df[df["network_score"] >= constraints["min_network"]]

    return df
