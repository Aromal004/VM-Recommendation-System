from config import ARCH_FACTOR

def compute_performance(row):
    base_perf = row["coremark_total"]

    arch = str(row.get("processorArchitecture", "64-bit")).upper()
    arch_factor = 1.05 if "ARM" in arch else 1.0

    avx_factor = 1.0
    if row.get("intelAvx2Available", False):
        avx_factor = 1.10
    elif row.get("intelAvxAvailable", False):
        avx_factor = 1.05

    turbo_factor = 1.05 if row.get("intelTurboAvailable", False) else 1.0

    return base_perf * arch_factor * avx_factor * turbo_factor
