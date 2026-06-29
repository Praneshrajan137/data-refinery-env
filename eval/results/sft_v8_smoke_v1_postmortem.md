# DataForge SFT-v8 Smoke Postmortem

- Status: `failed_diagnostic_evidence`
- Training stage: `smoke`
- Attempted steps: `40`
- GPU hours: `0.2543`
- Strict macro F1: `0.0`
- Parse success: `0.03`
- Schema-case errors: `26`
- Promote to GRPO: `false`

## Decision

Do not run SFT-v8 diagnostic/candidate rungs and do not launch GRPO-v4 from this predecessor.

## Findings

- Prompt-completion shape and label-mask audit passed, so prompt-token supervision leakage is no longer the primary explanation.
- The model still emits row-object completions and `finish` actions with non-empty repairs.
- Active repair remains absent: dataset F1 is `0.0` for beers, flights, and hospital.
- Product constrained-decoding metadata is present, but the decoder was not run; raw research metrics remain the only quality result for this smoke.

## Next Move

Revise the next SFT/product cycle around action-envelope imitation and constrained product decoding. Keep raw research ability and product pipeline reliability reported separately.
