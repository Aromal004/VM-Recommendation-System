import math

BASELINE_VCPU    = 2      # t3.large
BASELINE_MEM_GIB = 8.0    # t3.large
PROFILING_SECS   = 240


def lambda_handler(event, context):

    cpu_p95  = event["cpu_p95"]
    mem_p95  = event.get("mem_p95", 50.0)        # % of baseline 8 GiB
    disk_avg = event.get("disk_avg", 10.0)
    network_bytes = (
        event["network_in_total_bytes"] +
        event["network_out_total_bytes"]
    )
    latency_p99 = event.get("latency_p99_ms", 0)

    # ── 1. vCPU ─────────────────────────────────────────────
    cpu_ratio     = cpu_p95 / 100
    required_vcpu = max(math.ceil(cpu_ratio * BASELINE_VCPU * 2), 2)

    # ── 2. Memory — from real observation ───────────────────
    # mem_p95 is % used of 8 GiB baseline; add 25% headroom
    observed_mem_gib  = (mem_p95 / 100) * BASELINE_MEM_GIB
    required_mem_gib  = max(math.ceil(observed_mem_gib * 1.25), 4)

    # Round to standard EC2 memory sizes (4, 8, 16, 32 …)
    standard_sizes    = [4, 8, 16, 32, 64, 128]
    required_mem_gib  = next(
        (s for s in standard_sizes if s >= required_mem_gib),
        required_mem_gib
    )

    # ── 3. Network ──────────────────────────────────────────
    network_mbps = max(
        math.ceil(((network_bytes * 8) / PROFILING_SECS) / 1_000_000 * 1.3),
        1000
    )

    # ── 4. Latency sensitivity ──────────────────────────────
    # p99 > 100ms → workload is latency-sensitive
    latency_sensitive = latency_p99 > 100
    max_price         = 20.0 if latency_sensitive else 10.0

    # ── 5. Disk-heavy workload signal ───────────────────────
    needs_ssd = disk_avg > 60.0

    return {
        "vcpu":              required_vcpu,
        "memory_gib":        required_mem_gib,
        "network_mbps":      network_mbps,
        "max_price":         max_price,
        "latency_sensitive": latency_sensitive,
        "needs_ssd":         needs_ssd,
        # Pass through for scoring context
        "observed_mem_p95":  mem_p95,
        "observed_cpu_p95":  cpu_p95,
        "latency_p99_ms":    latency_p99
    }
