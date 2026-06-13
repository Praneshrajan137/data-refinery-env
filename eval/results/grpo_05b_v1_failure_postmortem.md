# DataForge 0.5B-GRPO v1 Failure Postmortem

## Baseline

- Benchmark: `DataForge-Bench-light-verified` seeds `[0, 1, 2]`
- SFT per-dataset F1: `beers`=0.0061, `flights`=0.0, `hospital`=0.0098
- GRPO per-dataset F1: `beers`=0.0, `flights`=0.2121, `hospital`=0.2059
- GRPO parse success: `1.0`
- GRPO schema-case errors: `0`

## Active Repair

- GRPO active-repair precision/recall/F1: `0.0` / `0.0` / `0.0`
- Empty predictions on truth-positive tasks: `86`
- GRPO failure taxonomy: `missed_repair`=576

## Paired Comparison

- Common tasks: `100`
- Improved/regressed/unchanged: `14` / `2` / `84`

## Findings

- GRPO v1 fixed parse/schema discipline and removed SFT overrepair.
- GRPO v1 mostly learned safe abstention; active repair recall remains the v2 bottleneck.
- GRPO per-dataset strict F1: beers=0.0, flights=0.2121, hospital=0.2059.
- GRPO failure taxonomy is dominated by missed_repair=576.

## V2 Target

- Posture: `balanced_recall`
- Strict macro F1: `>=0.25`
- Parse success: `>=0.99`
- Schema-case errors: `0`
- Not-inferable slice: `>=0.95`
