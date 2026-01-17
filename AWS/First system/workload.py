def get_workload(profile: str):
    profiles = {
        "cpu_intensive": {
            "cpu_util": 0.9,
            "memory_util": 0.5,
            "network_util": 0.3
        },
        "memory_intensive": {
            "cpu_util": 0.6,
            "memory_util": 0.9,
            "network_util": 0.3
        },
        "network_intensive": {
            "cpu_util": 0.6,
            "memory_util": 0.6,
            "network_util": 0.9
        },
        "balanced": {
            "cpu_util": 0.6,
            "memory_util": 0.6,
            "network_util": 0.6
        }
    }

    if profile not in profiles:
        raise ValueError("Invalid workload profile")

    return profiles[profile]
