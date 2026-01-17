# Scoring weights
ALPHA = 0.7   # Performance-to-Power weight
BETA = 0.3    # Cost efficiency weight

# Architecture performance multipliers
ARCH_FACTOR = {
    "64-bit": 1.0,        # x86_64
    "ARM64": 1.05,
    "ARM": 1.05
}

# Storage energy factors
STORAGE_FACTOR = {
    "SSD": 0.9,
    "HDD": 1.1
}

# Network energy factors
ENHANCED_NET_FACTOR = {
    True: 0.95,
    False: 1.05
}
