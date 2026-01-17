from config import STORAGE_FACTOR, ENHANCED_NET_FACTOR

def compute_energy(row, workload):
    cpu_energy = (
        row["vcpu"]
        * row["clockSpeed"]
        * workload["cpu_util"]
    )

    storage_factor = STORAGE_FACTOR.get(row["storageMedia"], 1.0)
    network_factor = ENHANCED_NET_FACTOR.get(
        row.get("enhancedNetworkingSupported", False),
        1.0
    )

    norm_factor = row.get("normalizationSizeFactor", 1.0)

    return cpu_energy * storage_factor * network_factor * norm_factor
