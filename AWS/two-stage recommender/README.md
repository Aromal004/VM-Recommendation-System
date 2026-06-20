# Two-Stage VM Recommendation System

A sophisticated AWS EC2 instance recommendation engine that uses Bayesian Optimization and workload-aware scoring to recommend optimal virtual machines based on user requirements.

## Overview

This system implements a **two-stage recommendation pipeline** that intelligently matches workload requirements to AWS EC2 instances. It combines hard constraint filtering, workload-aware fit scoring, Bayesian optimization, and diversity-based post-processing to deliver optimal VM recommendations.

### Key Features

- **Workload-Aware Scoring**: Adapts to CPU-intensive, memory-intensive, network-intensive, and balanced workloads
- **Bayesian Optimization**: Learns optimal penalty weights for resource dimensions (compute, memory, network)
- **Two-Stage Architecture**: Separates intent derivation from penalty weight optimization
- **Cost-Performance Balance**: Optimizes for both resource fit and cost efficiency
- **Family Diversification**: Ensures recommendations span multiple instance families
- **Comprehensive Evaluation**: Includes baseline comparisons and ablation studies

## Architecture

### Pipeline Stages

```
Input Requirements
       ↓
[1. Hard Filtering] ← Remove instances that don't meet minimum requirements
       ↓
[2. Feature Engineering] ← Extract and compute derived features
       ↓
[3. Fit Score Calculation] ← Compute workload-aware fit scores
       ↓
[4. Bayesian Optimization] ← Learn optimal penalty weights
       ↓
[5. Final Scoring] ← Combine fit, cost, and generation scores
       ↓
[6. Diversification] ← Ensure family diversity in top-K
       ↓
Top-K Recommendations
```

## Directory Structure

```
AWS/two-stage recommender/
├── main.py                          # Original pipeline (outer weight BO)
├── main_v2.py                       # Updated pipeline (workload-aware BO)
├── lambda_handler.py                # AWS Lambda entry point
├── Two-stage-algo.docx              # Algorithm documentation
├── input-outputs.txt                # Sample I/O for pipeline integration
│
├── preprocessing/
│   ├── feature_engineering.py       # Extract and compute VM features
│   └── hard_filter.py               # Apply hard constraints
│
├── scoring/
│   ├── fit_score.py                 # Original equal-weight fit scoring
│   └── final_scorer.py              # Combine fit, cost, generation scores
│
├── optimization/
│   ├── bayesian_ranker.py           # Original BO (outer weights)
│   └── workload_aware_bo.py         # Workload-aware BO (penalty weights)
│
├── postprocessing/
│   └── diversify.py                 # Family-based diversification
│
├── baselines/
│   └── baseline_methods.py          # Comparison baselines (Random, Heuristic, etc.)
│
├── evaluation/
│   └── metrics.py                   # Evaluation metrics (RSE, diversity, etc.)
│
└── experiments/
    ├── ablation_runner.py           # Component ablation study
    ├── ablation_runner_v2.py        # Workload-aware ablation study
    ├── arch_comparison.py           # Architecture comparison experiments
    ├── bo_sensitivity.py            # BO parameter sensitivity analysis
    ├── bo_workload_sensitivity.py   # Workload-specific BO analysis
    ├── multi_run.py                 # Statistical significance testing
    ├── scalability.py               # Performance scalability tests
    └── *.csv                        # Experiment results
```

## Core Components

### 1. Preprocessing

#### Hard Filter ([`preprocessing/hard_filter.py`](preprocessing/hard_filter.py))
Removes instances that don't meet minimum requirements:
- Compute capacity (CoreMark total)
- Memory (GiB)
- Network bandwidth (Mbps)
- Maximum price per hour

#### Feature Engineering ([`preprocessing/feature_engineering.py`](preprocessing/feature_engineering.py))
Extracts and computes:
- Numeric memory from string format
- Network bandwidth from performance descriptions
- Compute scores (CoreMark metrics)
- Performance per dollar ratios
- Generation scores

### 2. Scoring

#### Fit Score ([`scoring/fit_score.py`](scoring/fit_score.py))
Measures how well an instance matches requirements using **oversize ratios**:

```python
compute_ratio = (vm_compute - required_compute) / required_compute
memory_ratio = (vm_memory - required_memory) / required_memory
network_ratio = (vm_network - required_network) / required_network

penalty = compute_ratio² + memory_ratio² + network_ratio²
fit_score = 1 / (1 + penalty)
```

**Key insight**: Squared penalties heavily penalize oversized instances (e.g., 10× oversized gets far worse score than 2× oversized).

#### Workload-Aware Fit Score ([`optimization/workload_aware_bo.py`](optimization/workload_aware_bo.py))
Enhanced version with **learned penalty weights**:

```python
penalty = α·compute_ratio² + β·memory_ratio² + γ·network_ratio²
```

Where α, β, γ are learned via Bayesian Optimization to match workload intent.

#### Final Score ([`scoring/final_scorer.py`](scoring/final_scorer.py))
Combines three normalized components:

```python
final_score = w_fit × fit_score + w_cost × perf_per_dollar + w_gen × generation_score
```

**Fixed weights** (v2): `{fit: 0.52, cost: 0.35, generation: 0.13}`

### 3. Optimization

#### Original Bayesian Ranker ([`optimization/bayesian_ranker.py`](optimization/bayesian_ranker.py))
- Learns outer weights (w_fit, w_cost, w_generation)
- Search space: fit ∈ [0.3, 0.7], cost ∈ [0.1, 0.4], gen ∈ [0.05, 0.2]
- Objective: Maximize average score of top-K instances
- **Issue**: Unstable, corner-snapping behavior

#### Workload-Aware BO ([`optimization/workload_aware_bo.py`](optimization/workload_aware_bo.py))
**Two-stage design**:

**Stage 1 - Intent Derivation** (deterministic):
```python
utilization_d = requirement_d / pool_median_d
intent_weights = normalize(utilization_compute, utilization_memory, utilization_network)
```

**Stage 2 - Penalty Weight Learning** (BO):
```python
objective = -Σ(intent_d × improvement_d)
where improvement_d = (baseline_RSE_d - weighted_RSE_d) / baseline_RSE_d
```

Learns penalty weights (α, β, γ) that best achieve the derived intent.

### 4. Post-processing

#### Diversification ([`postprocessing/diversify.py`](postprocessing/diversify.py))
Ensures variety in recommendations:
- Limits instances per family (default: 2)
- Maintains top-K size (default: 10)
- Preserves score-based ranking within constraints

## Usage

### Basic Usage

```python
from main_v2 import run_recommendation

requirements = {
    "required_compute": 432000,  # 16 vCPU × 27,000 CoreMark/core
    "memory_gib": 64,
    "network_mbps": 25000,
    "max_price": 5.0
}

recommendations = run_recommendation(requirements)
```

### Standalone Testing

```bash
# Test with realistic synthetic workloads
python main_v2.py
```

This runs sensitivity tests across five workload profiles:
- CPU-intensive (32 vCPU / 64 GiB)
- Memory-intensive (8 vCPU / 256 GiB)
- Network-intensive (16 vCPU / 64 GiB / 50 Gbps)
- Balanced (16 vCPU / 64 GiB / 25 Gbps)
- Budget-constrained (4 vCPU / 16 GiB / max $0.80/hr)

### AWS Lambda Deployment

```python
# lambda_handler.py
from main_v2 import run_recommendation

def lambda_handler(event, context):
    requirements = event.get("inference_result", {})
    recommendations = run_recommendation(requirements)
    return {
        "statusCode": 200,
        "body": {"recommended_instances": recommendations}
    }
```

## Experiments

### Baseline Comparisons

Run baseline evaluation:
```bash
python run_baseline_evaluation.py
```

**Baselines included**:
- **Random**: Random selection from filtered pool
- **Heuristic**: Sort by (vCPU + memory) / price
- **CherryPick-like**: Single-metric BO optimization
- **Micky-like**: Greedy multi-armed bandit

### Ablation Studies

Test component contributions:
```bash
python experiments/ablation_runner_v2.py
```

**Variants tested**:
- `full`: Complete pipeline
- `no_filter`: Skip hard filtering
- `no_fit_score`: Remove fit scoring
- `no_bo`: Use equal weights instead of BO
- `no_diversify`: Skip family diversification

### Architecture Comparison

Compare v1 vs v2:
```bash
python experiments/arch_comparison.py
```

### Sensitivity Analysis

Test BO parameter sensitivity:
```bash
python experiments/bo_sensitivity.py
python experiments/bo_workload_sensitivity.py
```

### Statistical Validation

Multi-run significance testing:
```bash
python experiments/multi_run.py
```

## Key Algorithms

### Intent Weight Derivation

```python
def derive_intent_weights(requirements, pool, baseline_cpm):
    # Compute utilization ratios
    req_compute = requirements["required_compute"]
    req_memory = requirements["memory_gib"]
    req_network = requirements.get("network_mbps", 0)
    
    pool_compute_median = pool["compute_score"].median()
    pool_memory_median = pool["memory_gib"].median()
    pool_network_median = pool["network_mbps"].median()
    
    util_compute = req_compute / pool_compute_median
    util_memory = req_memory / pool_memory_median
    util_network = req_network / pool_network_median if req_network > 0 else 0
    
    # Normalize to sum to 1
    total = util_compute + util_memory + util_network
    return {
        "alpha": util_compute / total,
        "beta": util_memory / total,
        "gamma": util_network / total
    }
```

### Bayesian Optimization Objective

```python
def objective(penalty_weights):
    # Re-score pool with candidate penalty weights
    pool_scored = add_workload_aware_fit_score(pool, requirements, penalty_weights)
    
    # Get top-K instances
    top_k = pool_scored.nlargest(k, "final_score")
    
    # Compute RSE (Relative Size Error) for each dimension
    rse_compute = top_k["compute_ratio"].mean()
    rse_memory = top_k["mem_ratio"].mean()
    rse_network = top_k["net_ratio"].mean()
    
    # Compute improvement over baseline
    improvement_compute = (baseline_rse_compute - rse_compute) / baseline_rse_compute
    improvement_memory = (baseline_rse_memory - rse_memory) / baseline_rse_memory
    improvement_network = (baseline_rse_network - rse_network) / baseline_rse_network
    
    # Weight improvements by intent
    weighted_improvement = (
        intent_alpha * improvement_compute +
        intent_beta * improvement_memory +
        intent_gamma * improvement_network
    )
    
    return -weighted_improvement  # Minimize negative = maximize improvement
```

## Evaluation Metrics

### Relative Size Error (RSE)
Measures how tightly recommendations fit requirements:
```python
RSE_d = mean(|vm_d - req_d| / req_d) for dimension d
```
Lower is better (0 = perfect fit).

### Family Diversity
```python
diversity = unique_families / total_recommendations
```
Higher is better (1.0 = all different families).

### Cost Efficiency
```python
avg_price = mean(price_per_hr for top-K)
avg_perf_per_dollar = mean(perf_per_dollar for top-K)
```

### Workload Alignment
Checks if dominant requirement dimension has highest intent weight.

## Design Rationale

### Why Two Stages?

**Problem with single-stage BO**: In cloud catalogues, compute and memory are physically coupled (fixed GiB/vCPU ratios per family). This means:
- Memory oversize ratios are always large when compute is binding
- Data-driven approaches incorrectly conclude "memory is always important"
- Cannot distinguish CPU-intensive from memory-intensive workloads

**Solution**: Separate intent derivation (from requirements) from penalty weight learning (via BO).

### Why Squared Penalties?

Linear penalties don't sufficiently penalize extreme oversizing:
- 2× oversized: penalty = 1.0
- 10× oversized: penalty = 9.0

Squared penalties create stronger differentiation:
- 2× oversized: penalty = 1.0
- 10× oversized: penalty = 81.0

This prevents extremely oversized instances from appearing in recommendations.

### Why Fixed Outer Weights in v2?

Original design (v1) learned outer weights via BO, but this was unstable:
- Flat objective surface → corner-snapping
- Weights varied wildly between runs
- No clear convergence

v2 fixes outer weights at empirically validated values and moves BO to penalty weight tuning, where the objective surface has clear gradients.

## Integration with VM Recommendation Pipeline

This recommender integrates into a larger AWS Step Functions pipeline:

1. **launch_profiling_ec2**: Launch EC2 for workload profiling
2. **collect_metrics**: Gather CPU, memory, network metrics
3. **infer_requirements**: Convert metrics to requirements
4. **recommend_vm**: **← This system** generates recommendations
5. **terminate_ec2**: Clean up profiling instance

See [`input-outputs.txt`](input-outputs.txt) for complete I/O examples.

## Requirements

```
pandas
numpy
boto3
scikit-optimize (skopt)
```

## Performance

- **Typical runtime**: 2-5 seconds for 800-instance pool
- **BO iterations**: 30 calls (configurable)
- **Memory usage**: ~50MB for standard pool size
- **Scalability**: Linear with pool size

## Future Enhancements

- Multi-cloud support (Azure, GCP)
- GPU instance recommendations
- Spot instance pricing integration
- Historical performance data incorporation
- Real-time price optimization
- Container-specific recommendations

## References

- Algorithm documentation: [`Two-stage-algo.docx`](Two-stage-algo.docx)
- Baseline evaluation: [`run_baseline_evaluation.py`](run_baseline_evaluation.py)
- Experiment results: [`experiments/*.csv`](experiments/)


