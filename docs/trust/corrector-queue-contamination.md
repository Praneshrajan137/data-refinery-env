# The corrector precision number was mostly measuring queue contamination

**Date:** 2026-08-20
**Model:** Azure OpenAI `gpt-5.6-sol` (version 2026-07-09), structured-enum corrector
**Spend:** see `eval/results/spend_ledger.json`, run ids prefixed `calibrate-propose`,
`reasoning-effort-`, `flagship-throughput-probe`

## The finding

Every committed corrector report puts precision between **0.059 and 0.297**, and the project
read that as the corrector being weak. On a per-table calibration session the same corrector,
same model, same structured-enum mode, measured **0.9913** (114 correct of 115 proposals).

The difference is not the model, not `k`, and not reasoning effort. It is **what fraction of
the queue being corrected contains real errors**:

| queue | cells | real errors | queue precision |
| --- | --- | --- | --- |
| hospital, inferred-FD regime (what the bench samples) | 10,373 | 455 | **0.0439** |
| hospital, default detection | 549 | 308 | 0.5610 |
| local 400-row session, default detection | 240 sampled | 114 | 0.4750 |

A corrector cannot correct a cell that was never wrong. If a flagged cell is not an error,
**any** value the corrector proposes scores as incorrect against ground truth. So corrector
precision measured on a queue that is 95.6% false positives is bounded near 0.05 no matter how
good the model is.

This is the same denominator artifact the repo already caught for F1
(`corrector_gpt56sol_certified_coverage.json` warns that sampled corrections scored against
full-class support cap recall near 0.06), but it was never applied to *precision*.

## What the corrector actually does

The mechanism is abstention, and it is measurable:

```
local session: 240 cells sampled, 114 of them real errors (detector precision 0.4750)
               115 cells received a proposal
               114 of those 115 were real errors  -> 0.9913
```

The structured corrector **declines to propose** on cells that are not errors. It is behaving
as a high-precision filter over the detector's queue, not as a blind rewriter. That is exactly
the behaviour auto-apply requires, and the benchmark number hid it.

Precision comes with reduced coverage: 115 proposals from 240 sampled cells (48%), and the
pool-constrained design only proposes when the candidate pool contains a plausible answer. So
the honest summary is **precise but partial**, which is the correct trade for writing to a
user's file.

## What this does NOT establish

- It does **not** overturn the pre-registered global certification attempt
  (`eval/preregistration/api_phase_certification.md`). That remains a **NULL** at
  `alpha = 0.05`, and the arithmetic below is why.
- The "user labels" here are RAHA ground truth used as a stand-in. A real user labelling by
  hand would be noisier and possibly biased in ways this cannot see.
- The corruptions in hospital are synthetic character substitutions (`al_axi-1` ->
  `al_ami-1`). Real-world corruption is more varied, and pool-constrained repair is unusually
  well suited to this particular shape.

## Why the global attempt stays NULL

Measured on the pre-registered flagship arm (`B_structured_k9`, sol):

| quantity | measured |
| --- | --- |
| cost | $0.0525 / issue |
| wall clock | 44.4 s / issue (the loop is serial: k=9 sequential calls) |
| proposal rate | 12% |

`min_samples_for_certification(0.05, 0.05) = 59` **all-correct** accepted samples. With even
one error in 60 the Clopper-Pearson upper bound is 0.077 > 0.05, so this is a perfection
requirement. The remaining budget bought ~157 proposals over ~16 hours of serial calling; at
the best precision ever measured on that distribution (0.2973) that is ~47 correct in total,
short of 59 before requiring zero errors above a threshold.

The pre-registration forbids rescuing this by moving `alpha`, `min_support`, or the split, and
that instruction is respected here.

## Reasoning effort does not help

`reasoning_effort` is the one capability lever `gpt-5-mini` did not have. `gpt-5.6-sol`
supports `none | low | medium | high | xhigh` and **rejects `minimal`** -- the value every
committed gpt-5-mini reproduction command sets, so reusing those commands with sol fails every
call. Tested paired on the pre-registered sweep slice, k=3, 80 issues each
(`corrector_reasoning_effort_probe.json`):

| effort | proposals | precision | s/issue | cost |
| --- | --- | --- | --- | --- |
| `none` | 14 | 0.2857 (4/14) | 9.95 | $0.93 |
| `xhigh` | 12 | 0.3333 (4/12) | 14.63 | $1.37 |

Both arms produced **exactly 4 correct proposals**. The precision difference is one fewer
proposal in the denominator, not more correct answers, and `xhigh` costs 47% more for it. At
n=14 this is far from significant either way; the honest reading is that no large effect is
visible, and the burden of proof was on the expensive setting.

## Consequence for the product

The lever that matters is not the model. It is **which queue the corrector is pointed at**.
Running the corrector over an inferred-FD-flooded queue wastes calls on cells that were never
wrong and produces a precision number that describes the detector, not the corrector. This is
the same conclusion `docs/trust/constraint-circularity.md` reached for human review effort,
now shown to apply to paid LLM correction as well.
