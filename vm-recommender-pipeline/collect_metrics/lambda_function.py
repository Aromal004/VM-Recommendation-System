import boto3
import csv
import io
import time
import statistics
from datetime import datetime, timedelta

cloudwatch = boto3.client("cloudwatch")
s3         = boto3.client("s3")

S3_BUCKET = "vm-recommendation-data"


# ── CloudWatch helpers ──────────────────────────────────────

def get_metric(namespace, instance_id, metric_name,
               statistic="Average", extra_dims=None, period=10):
    end_time   = datetime.utcnow()
    start_time = end_time - timedelta(seconds=300)

    dims = [{"Name": "InstanceId", "Value": instance_id}]
    if extra_dims:
        dims += extra_dims

    resp = cloudwatch.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dims,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
        Statistics=[statistic]
    )
    pts = resp.get("Datapoints", [])
    return [p[statistic] for p in pts] if pts else []


def safe_p95(values):
    if len(values) >= 2:
        return statistics.quantiles(values, n=100)[94]
    return values[0] if values else 0


# ── S3 ab-results helpers ───────────────────────────────────

def wait_for_ab_results(instance_id, retries=8, delay=20):
    """Poll S3 for the done marker before reading ab CSVs."""
    key = f"profiling/{instance_id}/done.txt"
    for attempt in range(retries):
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except Exception:
            print(f"Waiting for ab results… attempt {attempt+1}/{retries}")
            time.sleep(delay)
    return False


def parse_ab_csv(instance_id, name):
    """
    Parse ab -e CSV for a specific named endpoint.
    ab -e produces:
      Percentage served, Time in ms
      50,12.3
      95,45.1
      99,89.7
      100,102.0
    Returns dict with latency percentiles and throughput.
    """
    defaults = {
        "latency_p50_ms":     0,
        "latency_p95_ms":     0,
        "latency_p99_ms":     0,
        "ab_requests_per_sec": 0,
    }
    try:
        obj    = s3.get_object(
            Bucket=S3_BUCKET,
            Key=f"profiling/{instance_id}/ab_{name}.csv"
        )
        reader = csv.DictReader(io.StringIO(obj["Body"].read().decode()))
        pct_map = {}
        for row in reader:
            pct = row.get("Percentage served", "").strip()
            ms  = row.get("Time in ms", "0").strip()
            if pct:
                pct_map[pct] = float(ms)

        defaults["latency_p50_ms"] = pct_map.get("50", 0)
        defaults["latency_p95_ms"] = pct_map.get("95", 0)
        defaults["latency_p99_ms"] = pct_map.get("99", 0)
    except Exception as e:
        print(f"ab CSV parse failed for '{name}': {e}")

    # Also parse gnuplot TSV for requests/sec
    try:
        obj   = s3.get_object(
            Bucket=S3_BUCKET,
            Key=f"profiling/{instance_id}/ab_{name}.tsv"
        )
        lines = obj["Body"].read().decode().splitlines()
        data_rows = [l for l in lines if l and not l.startswith("starttime")]
        if data_rows:
            total_time_s = float(data_rows[-1].split("\t")[1])
            if total_time_s > 0:
                defaults["ab_requests_per_sec"] = round(
                    len(data_rows) / total_time_s, 2)
    except Exception as e:
        print(f"ab TSV parse failed for '{name}': {e}")

    return defaults


# ── main handler ────────────────────────────────────────────

def lambda_handler(event, context):

    instance_id = event.get("instance_id")
    endpoints   = event.get("endpoints", [{"name": "default", "path": "/"}])

    if not instance_id:
        return {
            "cpu_avg": 0, "cpu_p95": 0,
            "mem_avg": 0, "mem_p95": 0,
            "disk_avg": 0,
            "network_in_total_bytes": 0,
            "network_out_total_bytes": 0,
            "latency_p50_ms": 0, "latency_p95_ms": 0, "latency_p99_ms": 0,
            "ab_requests_per_sec": 0, "ab_failed_requests": 0,
            "ab_results": {},
            "datapoints_collected": 0,
            "warning": "instance_id not provided"
        }

    # ── 1. CloudWatch metrics (CWAgent + AWS/EC2) ───────────
    # CPU — use CWAgent cpu_usage_active at 10s resolution
    cpu_values = get_metric(
        "CWAgent", instance_id, "cpu_usage_active",
        statistic="Average",
        extra_dims=[{"Name": "cpu", "Value": "cpu-total"}],
        period=10
    )
    if not cpu_values:
        cpu_values = get_metric("AWS/EC2", instance_id,
                                "CPUUtilization", "Average", period=60)
    if not cpu_values:
        cpu_values = [10.0]

    # Memory — only available via CWAgent
    mem_values = get_metric(
        "CWAgent", instance_id, "mem_used_percent",
        statistic="Maximum", period=10
    )
    if not mem_values:
        mem_values = [50.0]

    # Disk
    disk_values = get_metric(
        "CWAgent", instance_id, "disk_used_percent",
        statistic="Average",
        extra_dims=[
            {"Name": "device", "Value": "xvda1"},
            {"Name": "fstype", "Value": "xfs"},
            {"Name": "path",   "Value": "/"}
        ],
        period=60
    )
    if not disk_values:
        disk_values = [10.0]

    # Network (standard EC2, Sum over window)
    network_in  = get_metric("AWS/EC2", instance_id,
                             "NetworkIn",  "Sum", period=60)
    network_out = get_metric("AWS/EC2", instance_id,
                             "NetworkOut", "Sum", period=60)
    if not network_in:  network_in  = [0]
    if not network_out: network_out = [0]

    # ── 2. ab results from S3 — one CSV per endpoint ────────
    ab_ready = wait_for_ab_results(instance_id)

    ab_results = {}
    if ab_ready:
        for ep in endpoints:
            name = ep["name"]
            ab_results[name] = parse_ab_csv(instance_id, name)
    else:
        for ep in endpoints:
            ab_results[ep["name"]] = {
                "latency_p50_ms": 0,
                "latency_p95_ms": 0,
                "latency_p99_ms": 0,
                "ab_requests_per_sec": 0,
            }

    # Derive headline latency — worst-case p99 across all endpoints
    latency_p99 = max(
        (r["latency_p99_ms"] for r in ab_results.values()), default=0
    )
    latency_p95 = max(
        (r["latency_p95_ms"] for r in ab_results.values()), default=0
    )
    latency_p50 = max(
        (r["latency_p50_ms"] for r in ab_results.values()), default=0
    )
    total_rps = sum(
        r["ab_requests_per_sec"] for r in ab_results.values()
    )

    # ── 3. Aggregate ────────────────────────────────────────
    return {
        # CPU
        "cpu_avg":  round(statistics.mean(cpu_values), 2),
        "cpu_p95":  round(safe_p95(cpu_values), 2),
        # Memory
        "mem_avg":  round(statistics.mean(mem_values), 2),
        "mem_p95":  round(safe_p95(mem_values), 2),
        # Disk
        "disk_avg": round(statistics.mean(disk_values), 2),
        # Network
        "network_in_total_bytes":  sum(network_in),
        "network_out_total_bytes": sum(network_out),
        # Headline latency (worst-case across endpoints)
        "latency_p50_ms":      latency_p50,
        "latency_p95_ms":      latency_p95,
        "latency_p99_ms":      latency_p99,
        "ab_requests_per_sec": round(total_rps, 2),
        "ab_failed_requests":  0,
        # Per-endpoint A/B breakdown
        "ab_results":          ab_results,
        # Meta
        "datapoints_collected": len(cpu_values),
        "ab_results_found":     ab_ready
    }
