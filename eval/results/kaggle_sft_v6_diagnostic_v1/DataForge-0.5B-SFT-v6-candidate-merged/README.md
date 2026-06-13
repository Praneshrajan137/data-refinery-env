---
license: apache-2.0
base_model: Qwen/Qwen2.5-0.5B-Instruct
library_name: transformers
private_candidate: true
---

# DataForge-0.5B-SFT-v6-candidate

Private SFT-v6 contract-first candidate for DataForge 0.5B. This checkpoint is eligible to seed GRPO-v3 only when `sft_v6_candidate_eval_report.json` has `promote_to_grpo: true`.

## Evidence

- Strict macro F1: `0.002`
- Base strict macro F1: `0.002`
- Parse success: `0.59`
- Schema-case errors: `105`
- Promotion gate passed: `False`

## Limits

This is private predecessor evidence, not a public release and not production-quality autonomous data repair.
