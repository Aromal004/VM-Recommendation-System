# Bayesian Optimization System - VM Recommender

An enhanced VM recommendation system that uses **Bayesian Optimization** to learn optimal scoring weights instead of relying on fixed heuristics. This is the **V2** implementation that bridges the gap between rule-based and advanced workload-aware systems.

## Overview

This system improves upon the First System by replacing fixed scoring weights with data-driven weight learning via Bayesian Optimization. It automatically discovers the optimal balance between compute, memory, network, and cost factors for different workload types.

### Key Features

- **Bayesian Optimization**: Learns optimal weights using Gaussian Process-based optimization
- **Workload-Specific Filtering**: Pre-filters instances based on workload constraints
- **Feature Engineering**: Extracts and normalizes compute, memory, network, and cost metrics
- **Normalized Scoring**: Min-max normalization ensures fair comparison across dimensions
- **Adaptive Weights**: Different optimal weights for different workload types
- **Top-K Optimization**: Maximizes average score of top-K recommendations

## Architecture

```
Input: Workload Type
    ↓
[1. Load Dataset]
    ↓
[2. Feature Engineering]
    ├─ Compute score (vCPU count)
    ├─ Memory score (GiB)
    ├─ Network score (Mbps)
    └─ Cost efficiency (1/price)
    ↓
[3. Workload Filtering]
    Apply min_vcpu, min_memory, min_network
    ↓
[4. Normalization]
    Min-max scale all features to [0, 1]
    ↓
[5. Bayesian Optimization]
    Learn weights: {compute, memory, network, cost}
    Objective: Maximize avg(top-K final_score)
    ↓
[6. Final Scoring]
    final_score = Σ(weight_i × feature_i)
    ↓
Top-K Recommendations
```

## File Structure

```
AWS/Bayesian/
├── README.md                           # This file
├── main.py                             # Main entry point
├── requirements.txt                    # Python dependencies
│
├── config/
│   └── workload_constraints.py         # Workload-specific thresholds
│
├── preprocessing/
│   ├── feature_engineering.py          # Extract and compute features
│   ├── normalization.py                # Min-max scaling
│   └── workload_filter.py              # Apply workload constraints
│
├── scoring/
│   └── scorer.py                       # Weighted scoring function
│
└── optimization/
    └── bayesian_optimizer.py           # Bayesian weight learning
```

## Components

### 1. Feature Engineering (`preprocessing/feature_engineering.py`)

Extracts and computes key metrics:

```python
def add_features(df):
    # Compute power (vCPU count)
    df["compute_score"] = df["vcpu"]
    
    # Memory (parse "16 GiB" → 16.0)
    df["memory_score"] = df["memory"].str.replace(" GiB", "").astype(float)
    
    # Network (parse "10 Gigabit" → 10.0)
    df["network_score"] = df["networkPerformance"].apply(parse_network)
    
    # Cost efficiency (inverse of price)
    df["cost_efficiency"] = 1 / (df["price_per_hr"] + 1e-6)
    
    return df
```

### 2. Workload Constraints (`config/workload_constraints.py`)

Pre-defined thresholds for each workload type:

```python
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
        "min_network": 10000  # Mbps (10 Gbps)
    },
    "balanced": {
        "min_vcpu": 4,
        "min_memory": 16,
        "min_network": 5000   # Mbps (5 Gbps)
    }
}
```

### 3. Workload Filtering (`preprocessing/workload_filter.py`)

Removes instances that don't meet minimum requirements:

```python
def filter_vms_by_workload(df, constraints):
    if "min_vcpu" in constraints:
        df = df[df["vcpu"] >= constraints["min_vcpu"]]
    
    if "min_memory" in constraints:
        df = df[df["memory_score"] >= constraints["min_memory"]]
    
    if "min_network" in constraints:
        df = df[df["network_score"] >= constraints["min_network"]]
    
    return df
```

### 4. Normalization (`preprocessing/normalization.py`)

Scales all features to [0, 1] range using Min-Max scaling:

```python
from sklearn.preprocessing import MinMaxScaler

def normalize(df, columns):
    scaler = MinMaxScaler()
    df[columns] = scaler.fit_transform(df[columns])
    return df
```

**Why normalize?** Ensures that features with different scales (e.g., vCPU: 1-96, memory: 0.5-768 GiB) contribute fairly to the final score.

### 5. Scoring (`scoring/scorer.py`)

Computes weighted sum of normalized features:

```python
def score_vms(df, weights):
    df["final_score"] = (
        weights["compute"] * df["compute_score"] +
        weights["memory"] * df["memory_score"] +
        weights["network"] * df["network_score"] +
        weights["cost"] * df["cost_efficiency"]
    )
    
    return df.sort_values("final_score", ascending=False)
```

### 6. Bayesian Optimization (`optimization/bayesian_optimizer.py`)

Learns optimal weights using Gaussian Process optimization:

```python
from skopt import gp_minimize
from skopt.space import Real

def optimize_weights(df, scorer_fn, top_k=5, n_calls=30):
    # Define search space
    space = [
        Real(0.01, 1.0, name="compute"),
        Real(0.01, 1.0, name="memory"),
        Real(0.01, 1.0, name="network"),
        Real(0.01, 1.0, name="cost"),
    ]
    
    def objective(params):
        weights = dict(zip(["compute", "memory", "network", "cost"], params))
        
        # Normalize weights to sum to 1
        s = sum(weights.values())
        weights = {k: v / s for k, v in weights.items()}
        
        # Score instances with candidate weights
        ranked = scorer_fn(df.copy(), weights)
        
        # Objective: maximize average score of top-K
        score = ranked.head(top_k)["final_score"].mean()
        
        return -score  # Minimize negative = maximize positive
    
    # Run Bayesian optimization
    result = gp_minimize(objective, space, n_calls=n_calls, random_state=42)
    
    # Extract and normalize best weights
    best_weights = dict(zip(["compute", "memory", "network", "cost"], result.x))
    s = sum(best_weights.values())
    best_weights = {k: v / s for k, v in best_weights.items()}
    
    return best_weights
```

**Key Parameters**:
- `top_k=5`: Optimize for top 5 recommendations
- `n_calls=30`: Number of BO iterations (more = better convergence, slower)
- `random_state=42`: Reproducibility

## Usage

### Basic Example

```python
import pandas as pd
from preprocessing.feature_engineering import add_features
from preprocessing.normalization import normalize
from preprocessing.workload_filter import filter_vms_by_workload
from scoring.scorer import score_vms
from optimization.bayesian_optimizer import optimize_weights
from config.workload_constraints import WORKLOAD_CONSTRAINTS

# Load dataset
df = pd.read_csv("aws_ec2_full_dataset.csv")
df = df[df["price_per_hr"] > 0]  # Remove invalid prices

# Choose workload type
workload = "cpu_intensive"

# Feature engineering
df = add_features(df)

# Filter by workload constraints
df = filter_vms_by_workload(df, WORKLOAD_CONSTRAINTS[workload])

# Normalize features
df = normalize(df, ["compute_score", "memory_score", "network_score", "cost_efficiency"])

# Learn optimal weights via Bayesian Optimization
best_weights = optimize_weights(df, score_vms)

print("Optimized Weights:")
for k, v in best_weights.items():
    print(f"  {k}: {v:.3f}")

# Get final recommendations
recommendations = score_vms(df, best_weights)
print("\nTop 10 Recommendations:")
print(recommendations[["instanceType", "vcpu", "memory", "price_per_hr", "final_score"]].head(10))
```

### Running the System

```bash
cd AWS/Bayesian
python main.py
```

**Output Example**:
```
🔍 Optimized Weights (learned via Bayesian Optimization)
compute: 0.412
memory: 0.089
network: 0.156
cost: 0.343

🏆 Top EC2 Recommendations
   instanceType  vcpu  memory  networkPerformance  price_per_hr  final_score
0  c5.4xlarge      16  32 GiB  Up to 10 Gigabit         0.680        0.892
1  c5.2xlarge       8  16 GiB  Up to 10 Gigabit         0.340        0.875
2  m5.2xlarge       8  32 GiB  Up to 10 Gigabit         0.384        0.863
...
```

### Customizing Workload Constraints

Edit `config/workload_constraints.py`:

```python
WORKLOAD_CONSTRAINTS = {
    "custom_workload": {
        "min_vcpu": 16,
        "min_memory": 128,
        "min_network": 25000  # 25 Gbps
    }
}
```

### Adjusting Optimization Parameters

In `main.py` or when calling `optimize_weights()`:

```python
# More iterations for better convergence
best_weights = optimize_weights(df, score_vms, top_k=10, n_calls=50)

# Optimize for top-10 instead of top-5
best_weights = optimize_weights(df, score_vms, top_k=10, n_calls=30)
```

## Key Algorithms

### Bayesian Optimization Process

1. **Initialize**: Sample random weight combinations
2. **Evaluate**: Score instances with each weight set, compute avg(top-K)
3. **Model**: Fit Gaussian Process to (weights → score) mapping
4. **Acquire**: Use acquisition function (Expected Improvement) to select next weights
5. **Repeat**: Steps 2-4 for `n_calls` iterations
6. **Return**: Best weights found

### Weight Normalization

Ensures weights sum to 1 for interpretability:

```python
normalized_weight_i = weight_i / Σ(all_weights)
```

### Objective Function

```python
objective = -mean(final_score for top-K instances)
```

Negative because `gp_minimize` minimizes, but we want to maximize scores.

## Advantages Over First System

| Feature | First System | Bayesian System |
|---------|-------------|-----------------|
| **Weight Selection** | Manual/fixed | Learned from data |
| **Workload Adaptation** | Basic profiles | Constraint-based filtering |
| **Optimization** | None | Bayesian Optimization |
| **Normalization** | ❌ | ✅ Min-max scaling |
| **Reproducibility** | ✅ | ✅ (with random_state) |
| **Interpretability** | High | Medium |

## Limitations

### 1. Single-Objective Optimization
- Optimizes only for average top-K score
- Doesn't consider diversity, fairness, or other objectives
- Cannot explore trade-offs

### 2. No Oversize Penalties
- Treats all instances above threshold equally
- Doesn't penalize excessive resource allocation
- Can recommend oversized instances

### 3. Flat Objective Surface
- In practice, many weight combinations yield similar scores
- BO may not converge to a clear optimum
- Weights can vary significantly between runs

### 4. Workload Modeling
- Constraints are coarse (min thresholds only)
- Doesn't capture workload intensity or patterns
- No distinction between "barely sufficient" and "ideal"

### 5. No Family Diversification
- May recommend multiple instances from same family
- Reduces variety in recommendations

## Comparison with Other Systems

| Feature | First System | Bayesian | Two-Stage |
|---------|-------------|----------|-----------|
| **Weight Learning** | ❌ | ✅ BO | ✅ BO (penalty) |
| **Workload Awareness** | Profiles | Constraints | Intent-derived |
| **Oversize Penalties** | ❌ | ❌ | ✅ Squared |
| **Normalization** | ❌ | ✅ | ✅ |
| **Diversification** | ❌ | ❌ | ✅ |
| **Evaluation** | ❌ | ❌ | ✅ Comprehensive |
| **Runtime** | < 1s | ~5-10s | ~2-5s |

## Performance

- **BO Runtime**: ~5-10 seconds for 30 calls on 800-instance pool
- **Memory Usage**: ~30MB
- **Convergence**: Typically within 20-30 iterations
- **Scalability**: Linear with pool size, quadratic with n_calls

## Evolution Path

This system represents the second stage in the evolution:

1. **First System** → Fixed heuristics (α=0.7, β=0.3)
2. **Bayesian System** (this) → Learned weights via BO
3. **Two-Stage System** → Workload-aware scoring with intent derivation
4. **Production Pipeline** → Integrated with live profiling

## Future Improvements

The limitations of this system led to the development of:

- **Two-Stage System**: Separates intent derivation from weight learning
- **Oversize Penalties**: Squared penalties for excessive resources
- **Family Diversification**: Ensures variety in recommendations
- **Comprehensive Evaluation**: Baselines, ablation studies, metrics

See [../two-stage recommender/README.md](../two-stage%20recommender/README.md) for details.

## Requirements

```bash
pip install pandas numpy scikit-optimize scikit-learn
```

Or use `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Dataset Requirements

The system expects a CSV file with the following columns:

**Required**:
- `instanceType`: Instance name (e.g., "m5.xlarge")
- `vcpu`: Number of virtual CPUs
- `memory`: Memory size (e.g., "16 GiB")
- `networkPerformance`: Network bandwidth (e.g., "10 Gigabit")
- `price_per_hr`: On-demand price per hour

**Optional** (for enhanced filtering):
- Any additional columns for custom constraints

## Troubleshooting

### Empty DataFrame After Filtering

```python
if df.empty:
    raise ValueError("No VM satisfies the workload requirements")
```

**Solution**: Relax constraints in `config/workload_constraints.py`

### BO Not Converging

**Symptoms**: Weights vary wildly between runs

**Solutions**:
- Increase `n_calls` (e.g., 50 or 100)
- Check if objective surface is flat (many weights give similar scores)
- Consider switching to Two-Stage system for better convergence

### Normalization Issues

**Symptoms**: All scores are very similar

**Solution**: Ensure features have sufficient variance before normalization

## References

- Main repository README: [../../README.md](../../README.md)
- First system: [../First system/README.md](../First%20system/README.md)
- Two-stage system: [../two-stage recommender/README.md](../two-stage%20recommender/README.md)
- Production pipeline: [../../vm-recommender-pipeline/readme.md](../../vm-recommender-pipeline/readme.md)
- Scikit-Optimize documentation: https://scikit-optimize.github.io/
