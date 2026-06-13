# DataForge 0.5B-GRPO v2 Failed Candidate Failure Postmortem

## Baseline

- Benchmark: `DataForge-Bench-light-verified` seeds `[0, 1, 2]`
- SFT per-dataset F1: `beers`=0.0061, `flights`=0.0, `hospital`=0.0098
- GRPO per-dataset F1: `beers`=0.0061, `flights`=0.1515, `hospital`=0.2059
- GRPO parse success: `0.99`
- GRPO schema-case errors: `0`
- Candidate status: `quality_gate_failed_no_upload`
- Gate failures: `['grpo_f1>=0.25', 'not_inferable_from_prompt_f1>=0.95']`
- GPU hours: `4.5318`

## Active Repair

- GRPO active-repair precision/recall/F1: `0.0101` / `0.0017` / `0.003`
- Empty predictions on truth-positive tasks: `62`
- GRPO failure taxonomy: `missed_repair`=550, `overrepair`=73, `schema_error`=1, `wrong_value`=25

## Paired Comparison

- Common tasks: `100`
- Improved/regressed/unchanged: `12` / `1` / `87`

## Findings

- GRPO v2 Failed Candidate fixed parse/schema discipline and removed SFT overrepair.
- GRPO v2 Failed Candidate mostly learned safe abstention; active repair recall remains the next bottleneck.
- GRPO per-dataset strict F1: beers=0.0061, flights=0.1515, hospital=0.2059.
- GRPO failure taxonomy is dominated by missed_repair=550, overrepair=73, schema_error=1, wrong_value=25.

## V2 Target

- Posture: `balanced_recall`
- Strict macro F1: `>=0.25`
- Parse success: `>=0.99`
- Schema-case errors: `0`
- Not-inferable slice: `>=0.95`
