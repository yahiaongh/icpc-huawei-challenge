# ICPC Huawei Challenge 2026 - Edge-Cloud Collaborative Scheduling

This repository contains solutions for the ICPC 2026 Online Challenge 1 powered by Huawei.

## Problem Overview

The challenge involves scheduling tasks in an edge-cloud collaborative inference system for LLM services. The system has:
- 1 edge computer (E)
- K cloud computers (C0, C1, ..., CK-1)

Each request goes through:
1. **Input Stage**: P PRE → UP transfer → P PROC → DOWN transfer → P POST
2. **Output Steps** (repeated per token): D PRE → UP transfer → D PROC → DOWN transfer → D POST

The goal is to maximize throughput while meeting latency SLOs (TDR and TPOT).

## Solutions

### 1. Basic Solution (`src/solution.cpp`)
- Correctness-first baseline
- Score-aware scheduling with adaptive batching
- Load-balanced remote assignment
- Urgency-based priority decisions

### 2. Advanced Solution (`src/solution_advanced.cpp`)
- All features from basic solution
- Memory-aware scheduling
- Enhanced load balancing with memory pressure consideration
- Improved urgency calculations

## Building

```bash
make
```

This builds both solutions:
- `build/solution` - Basic solution
- `build/solution_advanced` - Advanced solution

## Running

The solutions are interactive and communicate with the judge via stdin/stdout:

```bash
./build/solution < input.txt
```

## Key Features

### Scoring
The score is calculated as:
```
Score = 1000 × (w_tp × throughput_component + w_c × waiting_component)
```

Where:
- `throughput_component` = clamp(total_tokens / total_time, tp_base, tp_UB)
- `waiting_component` = clamp(dist, dist_base, 0)
- `dist` = sqrt(excess_tdr² + excess_tpot²)

### Scheduling Strategy

1. **Adaptive Batching**: Dynamically adjusts batch size based on:
   - Current wait pressure
   - Throughput weight (w_tp)
   - Urgency of pending requests

2. **Load-Aware Remote Assignment**: Considers:
   - Reserved compute load
   - Queue depth
   - Memory pressure
   - Remote availability

3. **Urgency-Based Prioritization**:
   - P POST urgency: (now - arrival_time) / SLO1
   - Decode urgency: (now - token_due_time) / SLO2
   - Local action scoring combines urgency with efficiency

4. **Phase-Safe Event Processing**:
   - Events processed in dependency order: ARR → TDN → XDN → FIN
   - Ensures correct state transitions

## Constraints

- 1 ≤ K ≤ 8
- 1 ≤ R ≤ 2000 requests
- 1 ≤ Lin[i] ≤ 4096
- 1 ≤ Lout[i] ≤ 512
- Σ Lout[i] ≤ 200,000 per test
- 1 ≤ num_layers ≤ 64
- 15 seconds time limit per test
- 256 MB memory limit

## References

- [ICPC Challenge powered by Huawei](https://codeforces.com/blog/entry/155646)
- [Problem Statement](https://codeforces.com/contest/2251/problem/A)
- [GitHub: frankthinker/huawei-llm-scheduling-challenge](https://github.com/frankthinker/huawei-llm-scheduling-challenge)
- [GitHub: neutron420/ICPC-Huawei-2026](https://github.com/neutron420/ICPC-Huawei-2026)
