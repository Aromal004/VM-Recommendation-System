WORKLOAD_CONSTRAINTS = {
    "cpu_intensive": {
        "min_vcpu": 8,
        "min_memory": 16
    },
    "memory_intensive": {
        "min_vcpu": 4,
        "min_memory": 64
    },
    "network_intensive": {
        "min_network": 10000  # Mbps
    },
    "balanced": {
        "min_vcpu": 4,
        "min_memory": 16,
        "min_network": 5000
    }
}
