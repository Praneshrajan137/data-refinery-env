# Reward Design Document

## Overview

This document formalizes the reward structure of the Data Quality RL environment, providing the theoretical grounding, design rationale, and empirical calibration behind every reward signal.

## 1. Markov Decision Process Formulation

The Data Quality environment is formulated as a finite-horizon MDP:

- **State space S**: (dataset snapshot, inspection history, diagnosis history, fix history, step count)
- **Action space A**: {inspect, diagnose, fix, finalize} with structured parameters
- **Transition T(s'|s,a)**: Deterministic given the ground truth; stochastic in noisy mode
- **Reward R(s,a,s')**: Dense, multi-component signal detailed below
- **Horizon H**: Task-dependent (30 / 50 / 65 steps)
- **Discount γ**: Effectively 1.0 (finite horizon, no discounting)

### Partial Observability

The agent operates under partial observability (POMDP):
- Only 10 rows visible per inspect action (window over full dataset)
- Column statistics are aggregates, not row-level data
- Ground truth is hidden; the agent infers issues from data patterns
- In stochastic mode (`noisy=True`), observed values may be perturbed

## 2. Reward Components

### 2.1 Positive Rewards (Correct Actions)

| Signal | Value | Condition |
|--------|-------|-----------|
| `R_DIAGNOSE` | +0.10 | Correct issue identification (row + column match ground truth) |
| `R_TYPE_BONUS` | +0.05 | Correct issue type classification on top of diagnosis |
| `R_FIX` | +0.15 | Exact fix value match (case-insensitive string equality) |
| `R_FIX_PARTIAL` | +0.075 | Numerically within 1% or string similarity ≥ 85% |
| `R_JUSTIFY_BONUS` | +0.05 | Justification provided with fix action |
| `R_EXPLORE` | +0.01 | Per undiscovered issue in newly-inspected rows |

### 2.2 Negative Penalties (Incorrect Actions)

| Signal | Value | Condition |
|--------|-------|-----------|
| `P_FALSE_POS` | -0.05 | Diagnosis at row/column with no ground truth issue |
| `P_WRONG_FIX` | -0.08 | Incorrect fix value for a real issue |
| `P_LATE_STEP` | -0.02 | Per step after 80% of budget consumed |
| `P_INVALID` | -0.01 | Malformed action or out-of-bounds access |
| `P_REINSPECT` | -0.01 | All requested rows already inspected (no new information) |

### 2.3 Coverage Exploration Bonus

Beyond the per-issue `R_EXPLORE`, a coverage bonus rewards expanding row coverage:

```
coverage_bonus = |new_rows| × R_EXPLORE × 0.5 × (1 - coverage_ratio)
```

where `coverage_ratio = |inspected_rows| / |total_rows|`. This implements a pseudo-count exploration bonus (Bellemare et al., 2016) that naturally decays as the agent explores more of the dataset, preventing infinite reward farming while encouraging early broad exploration.

### 2.4 Late-Step Penalty

After 80% of the step budget is consumed:

```
penalty_per_step = P_LATE_STEP = -0.02
```

This creates urgency pressure that encourages efficient exploration strategies and penalizes agents that waste steps on unproductive actions.

## 3. Terminal Score (Final Evaluation)

The episode-terminal score combines detection and fix metrics:

```
score = detection_rate × 0.40 + fix_rate × 0.60 - false_positives × fp_rate
```

Where:
- `detection_rate = |found_issues| / |total_issues|`
- `fix_rate = |fixed_issues| / |fixable_issues|`
- `fp_rate = 0.05` (doubled to `0.10` when total diagnoses > 2× ground truth count)

The 40/60 weighting reflects the real-world priority: fixing data is more valuable than merely detecting problems, but detection without fix still has value (alerting humans).

### Spam Deterrent

When `total_diagnoses > SPAM_THRESHOLD × total_issues` (threshold = 2.0), the false-positive penalty rate doubles. This prevents a degenerate strategy of diagnosing every cell.

## 4. Design Rationale

### 4.1 Why Dense Rewards?

Sparse terminal-only rewards make credit assignment intractable for the 30-65 step horizons in this environment. Dense per-action rewards provide:
- Immediate learning signal for correct/incorrect diagnoses and fixes
- Exploration guidance via the information-theoretic bonus
- Anti-degenerate-strategy pressure via false-positive penalties

### 4.2 Potential-Based Reward Shaping

The exploration bonus is potential-based in the sense of Ng et al. (1999): it depends only on the state transition (inspected rows before → after), not on the action itself. This preserves the optimal policy — the bonus guides exploration without distorting the final ranking of policies.

### 4.3 Why Separate Diagnose and Fix?

Separating detection from correction allows:
1. Partial credit for detection-only issues (no derivable fix)
2. Measurement of detection recall vs. fix precision independently
3. Curriculum learning: agents can learn to detect first, then fix

### 4.4 Adversarial Clean Rows

Tasks include adversarial clean rows — data points at boundary values that look suspicious but are valid. These test false-positive discipline and prevent overly aggressive agents from achieving high scores through indiscriminate flagging.

## 5. Grader Diagnostics

On episode termination (`finalize` or auto-finalize at max steps), the observation includes a `grader_diagnostics` field with:

- **Formula decomposition**: detection_rate, fix_rate, fp_penalty, raw and clamped scores
- **Per-issue hit/miss**: Which ground truth entries were detected and/or fixed
- **Summary statistics**: Counts of total, fixable, detection-only issues; steps used

This enables:
- RL researchers to debug reward attribution
- Curriculum learning systems to identify which issue types the agent struggles with
- Ablation studies on reward component contributions

## 6. Calibration Notes

The reward magnitudes were calibrated empirically:

| Property | Design Target | Achieved |
|----------|--------------|----------|
| Perfect episode score | 1.0 | 1.0 (verified by test) |
| Random baseline score | ~0.0 | 0.0 (verified by benchmark) |
| Heuristic baseline score | ~0.4 | 0.434 (verified by benchmark) |
| Single correct diagnose + fix | +0.30 | +0.30 (R_DIAGNOSE + R_TYPE_BONUS + R_FIX + R_JUSTIFY_BONUS) |
| Break-even FP threshold | 6 FPs | 6 × 0.05 = 0.30 = one correct diagnose+fix |

## References

- Ng, A. Y., Harada, D., & Russell, S. (1999). Policy invariance under reward transformations. ICML.
- Bellemare, M. G., Srinivasan, S., Ostrovski, G., et al. (2016). Unifying count-based exploration and intrinsic motivation. NeurIPS.
