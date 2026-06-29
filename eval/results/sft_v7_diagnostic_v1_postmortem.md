# SFT-v7 Diagnostic v1 Postmortem

SFT-v7 is failed diagnostic evidence, not a model win.

The 120-step Kaggle diagnostic completed on Tesla T4 and correctly blocked upload. It reached strict macro F1 `0.0`, parse success `0.0`, schema-case errors `35`, active-repair recall `0.0`, and `promote_to_grpo: false`.

The useful finding is the shifted root cause. Earlier runs leaked `reason` fields and code fences; v7 largely fixed that surface. The remaining failure is action-envelope learning: the model emits invalid `finish` actions with repairs and copies prompt row objects instead of producing one compact action JSON.

Key failure taxonomy:

- `finish_with_repairs`: `38`
- `schema_error`: `61`
- `truncated_json`: `1`
- `missed_repair`: `575`
- `overrepair`: `57`
- `schema_case_error`: `35`
- `wrong_cell`: `127`
- `wrong_value`: `1`

Decision: do not run more v7 steps and do not run GRPO-v3. Move to SFT-v8 Schema-Distill: prompt-completion records, completion-only loss, label-mask audit, and separate raw-research versus product-constrained reporting.

Public claim policy: no public model claim is upgraded. SFT-v7 remains private failed diagnostic evidence.
