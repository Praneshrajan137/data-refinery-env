# SFT-v7 Smoke v2 Postmortem

Status: `quality_gate_failed_no_upload`

The SFT-v7 parse-latch smoke completed on Kaggle instead of crashing. It trained
20 configured steps on a Tesla T4, ran the strict held-out eval, wrote reports,
and correctly blocked upload/promotion.

## Metrics

- Strict macro F1: `0.0`
- Parse success: `0.01`
- Schema-case errors: `7`
- Deterministic-normalization F1: `0.0`
- Not-inferable F1: `0.0`
- Active-repair precision/recall: `0.0 / 0.0`
- Code-fence rate: `0.0`
- Reason-text rate: `0.0`

## Failure Shape

- `schema_error`: `71`
- `finish_with_repairs`: `28`
- `missed_repair`: `574`
- `overrepair`: `112`
- `wrong_cell`: `71`
- `wrong_value`: `2`

SFT-v7 smoke fixed the wrapper/no-reason problem in the curriculum itself, but
20 steps were not enough for the model to latch onto the strict action envelope.
Typical completions still copy row objects or emit `finish` with non-empty
repairs.

## Decision

Do not promote to GRPO and do not upload a model. The next wise move is the
planned SFT-v7 no-upload diagnostic at `120` steps. If that diagnostic remains
below parse `0.90`, revise the contract/curriculum again instead of running a
candidate.

Artifacts:

- `eval/results/kaggle_sft_v7_smoke_v2/kaggle_sft_v7_candidate_report.json`
- `eval/results/kaggle_sft_v7_smoke_v2/sft_v7_candidate_eval_report.json`
- `eval/results/kaggle_sft_v7_smoke_v2/DataForge-0.5B-SFT-v7-candidate-merged/eval_diagnostics.json`
- `eval/results/kaggle_sft_v7_smoke_v2.log`
