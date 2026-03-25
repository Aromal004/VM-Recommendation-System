# VM Recommender

An AWS-native tool that profiles your containerised workload live on EC2, A/B tests multiple endpoints, and returns ranked EC2 instance recommendations using Bayesian optimisation.

---

## How It Works

1. You submit a container image and endpoint configuration via the web UI
2. A `t3.large` EC2 instance is launched, your container is started, and Apache Bench runs against each endpoint
3. CloudWatch metrics (CPU, memory, disk, network) are collected during the run
4. Resource requirements are inferred from the observations
5. A recommendation engine scores and ranks all EC2 instances from a pre-built dataset
6. Results are returned to the UI — including per-endpoint A/B latency comparison and ranked VM recommendations

---

## Folder Structure

```
vm-recommender/
├── index.html                              # Frontend — upload to S3
├── state_machine.json                      # Step Functions definition
│
├── start_pipeline/
│   └── lambda_function.py                  # POST /recommend — starts Step Functions
│
├── launch_profiling_ec2/
│   └── lambda_function.py                  # Launches t3.large, runs ab per endpoint
│
├── collect_metrics/
│   └── lambda_function.py                  # Reads CloudWatch + ab CSVs from S3
│
├── infer_requirements/
│   └── lambda_function.py                  # Converts observations → vCPU/RAM/network requirements
│
├── recommend_vm/
│   ├── lambda_handler.py                   # Lambda entry point
│   ├── main.py                             # Orchestrates the recommendation pipeline
│   ├── optimization/
│   │   └── bayesian_ranker.py              # Bayesian weight optimisation (scikit-optimize)
│   ├── postprocessing/
│   │   └── diversify.py                    # Ensures family diversity in results
│   ├── preprocessing/
│   │   ├── feature_engineering.py          # Parses and enriches EC2 dataset
│   │   └── hard_filter.py                  # Eliminates instances below requirements
│   └── scoring/
│       ├── fit_score.py                    # Oversize penalty scoring
│       └── final_scorer.py                 # Applies weighted final score
│
├── terminate_ec2/
│   └── lambda_function.py                  # Terminates profiling instance after run
│
└── get_pipeline_result/
    └── lambda_function.py                  # GET /result — polls Step Functions status
```

---

## AWS Services Used

| Service | Purpose |
|---|---|
| **S3** | Hosts the frontend, stores EC2 dataset (`aws_with_coremark.csv`), stores ab profiling results |
| **API Gateway (HTTP API)** | Exposes `/recommend` (POST) and `/result` (GET) endpoints |
| **Lambda** | Runs all pipeline stages |
| **Step Functions** | Orchestrates the pipeline as a state machine |
| **EC2** | Launches a `t3.large` profiling instance per analysis run |
| **CloudWatch** | Collects CPU, memory, disk, and network metrics via CWAgent |
| **IAM** | `ProfilingEC2Role2` — EC2 instance profile with S3 write + CloudWatch agent permissions |

---

## Deployment

### 1. S3 — Frontend

Upload `index.html` to your static website bucket:

```bash
aws s3 cp index.html s3://vm-recommender-ui/ --acl public-read
```

Ensure the bucket has:
- Static website hosting enabled (`index.html` as index document)
- Block Public Access **disabled**
- Bucket policy granting `s3:GetObject` to `"Principal": "*"`

### 2. Lambda Functions

Deploy each subfolder as its own Lambda function. The folder name matches the function name:

| Folder | Lambda Function Name |
|---|---|
| `start_pipeline` | `start_pipeline` |
| `launch_profiling_ec2` | `launch_profiling_ec2` |
| `collect_metrics` | `collect_metrics` |
| `infer_requirements` | `infer_requirements` |
| `recommend_vm` | `recommend_vm` |
| `terminate_ec2` | `terminate_ec2` |
| `get_pipeline_result` | `get_pipeline_result` |

For `recommend_vm`, the handler is `lambda_handler.lambda_handler` (not the default `lambda_function.lambda_handler`).

The `recommend_vm` Lambda requires the following Python packages in its deployment package or a Lambda Layer:
```
pandas
scikit-optimize
numpy
boto3
```

### 3. Step Functions

- Go to **AWS Step Functions → State machines → Create**
- Choose **Standard** workflow
- Paste the contents of `state_machine.json` as the definition
- Assign an IAM role with `lambda:InvokeFunction` permission on all 5 task Lambdas

### 4. API Gateway

Create an **HTTP API** with two routes:

| Method | Route | Integration |
|---|---|---|
| `POST` | `/recommend` | `start_pipeline` Lambda |
| `GET` | `/result` | `get_pipeline_result` Lambda |

Enable CORS on the API:
- Allow origins: `*`
- Allow headers: `Content-Type`
- Allow methods: `GET, POST, OPTIONS`

Update `API_BASE` in `index.html` with your API Gateway URL before uploading.

### 5. S3 — Dataset

Upload your EC2 dataset to:
```
s3://vm-recommendation-data/aws_with_coremark.csv
```

Expected columns: `instanceType`, `vcpu`, `memory`, `networkPerformance`, `price_per_hr`, `physicalProcessor`, `coremark_total`, `coremark_per_dollar`, `coremark_per_core`

### 6. IAM — EC2 Instance Profile

The profiling EC2 instance needs an instance profile named `ProfilingEC2Role2` with these permissions:
- `s3:PutObject` on `vm-recommendation-data/profiling/*`
- `cloudwatch:PutMetricData`
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

---

## Usage

1. Open the S3 website URL in your browser
2. Enter your container image URI (must be publicly pullable, e.g. from Docker Hub)
3. Set the port your container listens on
4. Add one or more endpoints to A/B test (name + path)
5. Set total requests and concurrency for the load test
6. Click **Run Analysis**
7. Wait ~5–8 minutes for the pipeline to complete
8. View the A/B latency comparison table and ranked EC2 recommendations

---

## Pipeline Timeout Reference

| Stage | Duration |
|---|---|
| EC2 launch + Docker pull | ~2 min |
| `WaitForWorkload` (fixed wait) | 4 min |
| `ab` runs (per endpoint) | ~1–2 min total |
| CloudWatch collection + S3 parse | ~3 min |
| Inference + recommendation | ~30 sec |
| **Total** | **~10–12 min** |

The frontend polls every 15 seconds and shows elapsed time.

---

## A/B Testing

You can test multiple endpoints in a single pipeline run. Each endpoint gets its own `ab` run on the same EC2 instance, producing separate latency percentile results (p50, p95, p99) and requests/sec. The endpoint with the lowest p99 latency is highlighted in the results table.

Example payload:
```json
{
  "container_image": "youruser/your-app:latest",
  "port": 8080,
  "endpoints": [
    { "name": "control", "path": "/" },
    { "name": "variant", "path": "/v2" }
  ],
  "total_requests": 10000,
  "concurrency": 100
}
```

---

## Known Limitations

- Memory metrics require CloudWatch Agent (`CWAgent`) to be installed on the AMI. If unavailable, memory defaults to 50% of 8 GiB baseline.
- `apache bench` (`ab`) models sustained concurrent load, not realistic bursty traffic. CPU observations may be higher than production.
- The network requirement floor is set to 1000 Mbps, which may over-filter for lightweight workloads.
- Disk SSD detection (`needs_ssd`) is rarely triggered for typical web workloads in a short profiling window.
- The EC2 dataset (`aws_with_coremark.csv`) must be maintained manually as AWS releases new instance types.