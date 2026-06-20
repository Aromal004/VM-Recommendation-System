# First System - Rule-Based VM Recommender

A foundational VM recommendation system that uses fixed heuristics and performance-to-power ratio (PPR) scoring to recommend AWS EC2 instances based on workload profiles.

## Overview

This is the **V1** implementation of the VM recommendation system, serving as a baseline for comparison with more advanced approaches. It uses rule-based filtering and fixed scoring weights to rank EC2 instances.

### Key Features

- **Hard Constraint Filtering**: Memory and price thresholds
- **Performance Modeling**: CoreMark-based performance with architecture adjustments
- **Energy Modeling**: Workload-aware energy consumption estimation
- **Performance-to-Power Ratio (PPR)**: Primary scoring metric
- **Cost Efficiency**: Secondary scoring factor using CoreMark per dollar
- **Workload Profiles**: Pre-defined profiles for CPU, memory, network, and balanced workloads

## Architecture

```
Input: Workload Profile + Constraints
    ↓
[1. Data Loading & Parsing]
    ↓
[2. Hard Filtering] (memory, price)
    ↓
[3. Performance Calculation]
    ├─ Base: CoreMark total
    ├─ Architecture factor (ARM vs x86)
    ├─ AVX/AVX2 bonus
    └─ Turbo boost factor
    ↓
[4. Energy Calculation]
    ├─ CPU energy (vCPU × clock × utilization)
    ├─ Storage factor (SSD vs HDD)
    ├─ Network factor (enhanced networking)
    └─ Normalization factor
    ↓
[5. Scoring]
    PPR = Performance / Energy
    Final = α·PPR + β·CostEfficiency
    ↓
Top-K Recommendations
```

## File Structure

```
AWS/First system/
├── README.md                    # This file
├── run.py                       # Main entry point
├── recommender.py               # Recommendation orchestration
├── scorer.py                    # Scoring logic
├── performance_model.py         # Performance calculation
├── energy_model.py              # Energy consumption model
├── config.py                    # Configuration and weights
└── workload.py                  # Workload profile definitions
```

## Components

### 1. Data Loading & Parsing (`run.py`)

Handles various data formats from AWS EC2 dataset:

```python
# Memory: "192 GiB" → 192.0
# Clock Speed: "Up to 3.7 GHz" → 3.7
# Network: "25 Gigabit" → 25.0
# Storage: "EBS only" → "SSD"
```

### 2. Workload Profiles (`workload.py`)

Pre-defined utilization patterns:

| Profile | CPU Util | Memory Util | Network Util |
|---------|----------|-------------|--------------|
| **cpu_intensive** | 0.9 | 0.5 | 0.3 |
| **memory_intensive** | 0.6 | 0.9 | 0.3 |
| **network_intensive** | 0.6 | 0.6 | 0.9 |
| **balanced** | 0.6 | 0.6 | 0.6 |

### 3. Performance Model (`performance_model.py`)

Calculates adjusted performance score:

```python
performance = coremark_total × arch_factor × avx_factor × turbo_factor

where:
  arch_factor = 1.05 (ARM) or 1.0 (x86)
  avx_factor = 1.10 (AVX2) or 1.05 (AVX) or 1.0 (none)
  turbo_factor = 1.05 (if available) or 1.0
```

### 4. Energy Model (`energy_model.py`)

Estimates energy consumption:

```python
energy = cpu_energy × storage_factor × network_factor × norm_factor

where:
  cpu_energy = vCPU × clock_speed × cpu_utilization
  storage_factor = 0.9 (SSD) or 1.1 (HDD)
  network_factor = 0.95 (enhanced) or 1.05 (standard)
```

### 5. Scoring (`scorer.py`)

Combines performance, energy, and cost:

```python
ppr = performance / energy
cost_efficiency = coremark_per_dollar
final_score = α·ppr + β·cost_efficiency

# Default weights (config.py):
α = 0.7  # Performance-to-power weight
β = 0.3  # Cost efficiency weight
```

### 6. Recommendation (`recommender.py`)

Applies filters and ranks instances:

```python
def recommend_vms(df, workload, min_memory=0, max_price=None, top_k=5):
    # 1. Filter by memory threshold
    # 2. Filter by price threshold
    # 3. Score each instance
    # 4. Sort by final_score descending
    # 5. Return top-K
```

## Usage

### Basic Example

```python
from workload import get_workload
from recommender import recommend_vms
import pandas as pd

# Load dataset
df = pd.read_csv("aws_with_coremark.csv")

# Preprocess data (see run.py for full parsing)
# ... (memory, clock, network parsing)

# Get workload profile
workload = get_workload("balanced")

# Get recommendations
recommendations = recommend_vms(
    df=df,
    workload=workload,
    min_memory=8,      # Minimum 8 GiB RAM
    max_price=5.0,     # Maximum $5/hour
    top_k=5            # Top 5 recommendations
)

print(recommendations)
```

### Running the System

```bash
cd "AWS/First system"
python run.py
```

**Output**:
```
Recommended VMs:

   instance         price_per_hr  performance    energy      ppr  final_score
0  m5.2xlarge            0.384      864000.0   43200.0   20.00      14.52
1  c5.2xlarge            0.340      972000.0   54000.0   18.00      13.85
2  r5.2xlarge            0.504      864000.0   43200.0   20.00      14.20
...
```

### Customizing Workload Profiles

Edit `workload.py` to add custom profiles:

```python
def get_workload(profile: str):
    profiles = {
        "custom_profile": {
            "cpu_util": 0.8,
            "memory_util": 0.7,
            "network_util": 0.4
        },
        # ... existing profiles
    }
    return profiles[profile]
```

### Adjusting Scoring Weights

Edit `config.py`:

```python
# Emphasize cost efficiency over performance
ALPHA = 0.5   # Performance-to-power weight
BETA = 0.5    # Cost efficiency weight
```

## Configuration

### Scoring Weights (`config.py`)

```python
ALPHA = 0.7   # Performance-to-Power weight
BETA = 0.3    # Cost efficiency weight
```

### Architecture Factors

```python
ARCH_FACTOR = {
    "64-bit": 1.0,    # x86_64
    "ARM64": 1.05,    # ARM (Graviton)
    "ARM": 1.05
}
```

### Storage Energy Factors

```python
STORAGE_FACTOR = {
    "SSD": 0.9,       # Lower energy consumption
    "HDD": 1.1        # Higher energy consumption
}
```

### Network Energy Factors

```python
ENHANCED_NET_FACTOR = {
    True: 0.95,       # Enhanced networking (lower overhead)
    False: 1.05       # Standard networking
}
```

## Dataset Requirements

The system expects a CSV file with the following columns:

**Required**:
- `servicename` or `instanceType`: Instance name (e.g., "m5.xlarge")
- `vcpu`: Number of virtual CPUs
- `memory`: Memory size (e.g., "16 GiB" or 16)
- `clockSpeed`: Processor clock speed (e.g., "3.1 GHz" or 3.1)
- `price_per_hr`: On-demand price per hour
- `coremark_total`: Total CoreMark score
- `coremark_per_dollar`: CoreMark per dollar ratio

**Optional** (for enhanced scoring):
- `processorArchitecture`: "64-bit", "ARM64", etc.
- `intelAvxAvailable`: Boolean for AVX support
- `intelAvx2Available`: Boolean for AVX2 support
- `intelTurboAvailable`: Boolean for Turbo Boost
- `enhancedNetworkingSupported`: Boolean for enhanced networking
- `storage`: Storage type description
- `networkPerformance`: Network bandwidth description
- `normalizationSizeFactor`: AWS normalization factor

## Limitations

### 1. Fixed Weights
- Scoring weights (α, β) are hardcoded
- Cannot adapt to different optimization objectives
- No learning from data

### 2. Simple Energy Model
- Linear relationship between resources and energy
- Doesn't account for dynamic power management
- Ignores idle power consumption

### 3. Limited Workload Modeling
- Only 4 pre-defined profiles
- Fixed utilization ratios
- No temporal variation or burstiness

### 4. No Multi-Objective Optimization
- Single final score combines all factors
- Cannot explore Pareto-optimal solutions
- No trade-off visualization

### 5. Coarse Filtering
- Only memory and price thresholds
- No network or compute requirements
- Cannot handle complex constraints

## Comparison with Advanced Systems

| Feature | First System | Bayesian | Two-Stage |
|---------|-------------|----------|-----------|
| **Weight Learning** | ❌ Fixed | ✅ BO | ✅ BO (penalty) |
| **Workload Awareness** | Basic profiles | Constraint-based | Intent-derived |
| **Energy Modeling** | ✅ PPR-based | ❌ | ❌ |
| **Oversize Penalties** | ❌ | ❌ | ✅ Squared |
| **Diversification** | ❌ | ❌ | ✅ Family-based |
| **Evaluation Framework** | ❌ | ❌ | ✅ Comprehensive |

## Evolution Path

This system served as the foundation for more advanced approaches:

1. **First System** (this) → Fixed heuristics, PPR scoring
2. **Bayesian System** → Learned weights via Bayesian optimization
3. **Two-Stage System** → Workload-aware scoring with intent derivation
4. **Production Pipeline** → Integrated with live profiling and AWS services

## Performance

- **Runtime**: < 1 second for 800-instance pool
- **Memory**: ~20MB
- **Scalability**: Linear with pool size

## Future Improvements

The limitations of this system led to the development of:

- **Bayesian System**: Learns optimal weights instead of fixed α, β
- **Two-Stage System**: Separates intent derivation from weight learning
- **Production Pipeline**: Adds live profiling and automated deployment

See the main [README.md](../../README.md) for details on advanced systems.

## Requirements

```bash
pip install pandas
```

## References

- Main repository README: [../../README.md](../../README.md)
- Bayesian system: [../Bayesian/README.md](../Bayesian/README.md)
- Two-stage system: [../two-stage recommender/README.md](../two-stage%20recommender/README.md)
- Production pipeline: [../../vm-recommender-pipeline/readme.md](../../vm-recommender-pipeline/readme.md)

