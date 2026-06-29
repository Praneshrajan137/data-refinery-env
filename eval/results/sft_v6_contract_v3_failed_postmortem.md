# SFT-v6 Contract-v3 Diagnostic Postmortem

SFT-v6 completed as diagnostic evidence, not a training win.

- Strict macro F1: `0.0202`
- Parse success: `0.19`
- Schema-case errors: `0`
- Active-repair recall: `0.0`
- Reason-text leakage: `0 / 100`
- Code-fence outputs: `19 / 100`

The contract-v3 correction fixed the old `reason` field and schema-case drift, but exposed the next bottleneck: the model often emits `finish` with non-empty repairs, which strict parsing rejects. It also mostly abstains on truth-positive deterministic repair tasks.

Decision: do not run GRPO, do not upload a candidate, and do not update public claims. The next move is SFT-v7 parse-latch training focused on action/repair consistency.
