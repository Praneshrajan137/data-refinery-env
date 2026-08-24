# Pre-registration: does per-table certification survive a real human labeller?

**Status: committed before any labelling and before any spend on this question.**

This document exists because the answer is convenient in one direction. Per-table certification is
the project's one durable asset — exchangeability holds by construction there and cannot hold for a
benchmark-to-user-table transfer. If it survives contact with a human labeller, the product thesis
stands. If it does not, the honest product is much smaller. A result obtained by choosing a
stopping rule after seeing the labels would be worthless in exactly the way six already-retracted
claims were worthless.

Everything below is fixed **in advance**. Deviations must be recorded as deviations.

---

## Why this question and not another

The [next-phase brief](../../.snowflake/cortex/plans/next-phase-brief.md) listed seven charges.
Six of them are downstream of this one:

- **The moat** is "ordering is not a guarantee" — the corrector's structured self-confidence
  already discriminates well (ROC-AUC 0.862 at k=3, 0.948 at k=9), so the defensible asset is the
  *bound*, not the ranking. A bound that dissolves under human labels is not an asset.
- **Warehouse-native certification** is a distribution surface for a guarantee that must first
  exist. Snowflake DMFs cannot express cell-level correction at all (`RETURNS NUMBER`,
  deterministic, scalar, cannot reference UDFs so cannot call Cortex), and are Enterprise-only.
- **Building a real error corpus** is a multi-month artifact that would not make the guarantee true.
- **Whether conformal is the right formalism** is largely a wrong question: the implementation is
  correct Learn-then-Test selective risk control and `repeated_split_certification` confirms
  validity empirically. The defects were the candidate *ordering* (fixed) and this missing term.

Every certification in this project so far used RAHA ground truth **standing in for** a human.
An oracle has `beta = 0` by construction, so it cannot show anything about this. That is the gap.

## The quantity

Write `beta` for the labeller's false-accept rate (they mark a wrong repair `correct`) and `gamma`
for the false-reject rate. Certification measures `p_tilde`, not `p`:

```
p_tilde = p (1 - beta) + (1 - p) gamma      =>      p <= p_tilde / (1 - beta)
```

taking `gamma = 0`, which is the conservative choice and is *derived* — `dp/dgamma` has the sign of
`p_tilde - 1 + beta`, negative throughout this regime. Full derivation and the literature position
(Einbinder et al., arXiv:2209.14295) in [`docs/trust/human-label-noise.md`](../../docs/trust/human-label-noise.md).

Consequence: certifying a measured 0.05 delivers **0.10 at `beta = 0.5`**. Automation bias pushes
the noise in exactly that direction, because a labeller shown the machine's answer and asked "is
this right?" is performing an acquiescence-biased ratification.

## Fixed analysis parameters

| Parameter | Value | Source |
| --- | --- | --- |
| `alpha` (primary) | `0.05` | existing default; also reported at 0.10 and 0.20 |
| `delta` (total) | `0.05` | existing default, split `delta/2` across the two bounds |
| `min_support` | `30` | `certify_from_session` default |
| Grid | `CERTIFICATION_GRID`, 16 points 0.99..0.60 | existing module constant, label-independent |
| `prune_infeasible` | `True` | fixed here; reads confidences only, never labels |
| Dataset | `hospital` | ground truth is unambiguous (injected `x` substitutions) |
| Planted controls `k` | **30** | the frontier optimum: `n=82, k=30` totals 112 judgements |
| Labels `n` per class | **>= 82** for alpha=0.05, `>= 40` for alpha=0.10 | `min_samples_under_label_noise` |
| Corrector | Azure `gpt-5.6-sol` (`2026-07-09`), structured-enum, k=3 | free-text confidence is at chance (ROC-AUC 0.5536) |
| Split seed | `20260821` | fixed here |

**Why hospital, given that its `x`-substitutions are a known-easy artifact.** We are measuring the
*human*, not a detector. The `x` artifact inflates *corrector* precision and would contaminate a
detector claim; it does not contaminate an estimate of how often a person ratifies a wrong value.
What matters here is that ground truth be **uncontested**, and hospital's is. `flights` is
explicitly excluded: its labels encode an arbitrary convention (the same flight's arrival time
appears as 10:30/10:31/10:28/10:39 across sources, per Cocoon arXiv:2410.15547), so a human
disagreeing with RAHA there is not evidence the human is wrong.

## Arms

Both arms label the **same** items in the **same** order, so the comparison is paired.

| Arm | Elicitation | Purpose |
| --- | --- | --- |
| `A_ratify` | "Cell shows `X`. Proposed replacement: `Y`. Is `Y` correct?" (accept/reject) | The current interface. Measures `beta` as the product actually ships. |
| `B_blind` | "Cell shows `X`. What is the correct value?" — proposal **hidden**; agreement computed afterwards | Removes the anchor. Measures whether the interface, not the person, is the problem. |

## Planted controls

30 per arm, mixed into the stream, indistinguishable from real items at presentation time.
Two classes, deliberately kept side by side:

- **15 `column_distribution`** — the true value replaced by another value **resampled from the same
  column's empirical distribution**, so the plant is as plausible as a genuine corrector mistake.
- **15 `corrector_generated`** — an actual wrong proposal the corrector made on a cell whose truth
  is known, so at least one control class is *distributionally identical* to real corrector errors.

`beta` is reported **separately for each class**, never pooled into one headline.

## Primary endpoint

> `beta_upper = CP_upper(false_accepts, 30, delta/2)` on the `A_ratify` arm, and whether
> `min_samples_under_label_noise(0.05, controls=30, false_accepts=observed)` is finite and
> `<= 200`.

## Falsifiers, fixed now

| Hypothesis | Falsifier |
| --- | --- |
| **H1** Human-labelled certification at alpha=0.05 is reachable at a realistic labelling budget | `beta_upper > 0.35`, i.e. alpha=0.05 unreachable inside ~200 judgements |
| **H2** Blind elicitation reduces `beta` | paired CI on `beta_A - beta_B` straddles zero |
| **H3** Plant class does not matter | `beta` differs materially between the two control classes; if it does, **the smaller-`beta` class is discarded** and only the harder one is used |

## Kill criterion

**If `beta_upper > 0.35`: human-labelled per-table certification at alpha=0.05 is DEAD.** The
honest product is then (a) soundness plus reversibility, which needs no labels at all and is
already measured — 0 of 14 constraint-violating attacks written under a tight schema — and
(b) advisory triage at alpha=0.20. That result is published in `DECISIONS.md` and
`docs/trust/human-label-noise.md` either way, with the same prominence as a positive one.

## Stopping rule

**Fixed n, declared here: 82 real items + 30 controls per arm.** Labelling stops at that count
regardless of what the running numbers look like.

This is not a formality. `certify_from_session` currently has **no CLI caller**, so a
label-check-label-check loop is not reachable today — but wiring it (which the product needs) would
introduce **optional stopping**, and a fixed-n Clopper-Pearson bound is invalid under optional
stopping. Until that is replaced by an anytime-valid confidence sequence (Waudby-Smith & Ramdas
betting bounds), the stopping rule must be external and pre-committed. Recorded here so the
constraint is not rediscovered after the fact.

## Guards against a false positive

1. **Nothing may substitute for the human.** RAHA ground truth would return `beta = 0` and
   manufacture the guarantee under test. An LLM asked to ratify its own proposal measures a model's
   acquiescence — a legitimate and cheap secondary probe, reported under its own name, which
   **must never enter a certificate**.
2. **The plant-distribution caveat travels in the artifact**
   (`SessionCertification.beta_scope_note`), because `beta` estimated on plants is not "the
   labeller's error rate" and quoting it as such would be the seventh retraction.
3. **Certification fails closed** when `label_source == "human"` and no control is labelled.
   Enforced, mutation-tested (6 mutants, all caught) in
   `tests/unit/test_label_noise_certification.py`.
4. **The raw `(confidence, repair_decision)` pairs and every control verdict are committed.** The
   2026-08-20 session was never committed and its numbers are consequently unverifiable — its grid
   walk reports `n=40` while its own histogram implies 92. That is not repeated.

## Budget

**<= $8.** The only spend is regenerating repair proposals so labelling is not performed on stale
output (~$6 at the measured rate of 229 proposals for $5.42). Foreground, bounded, receipts
upserted by run id, never appended — appending cumulative snapshots once inflated a $14.35 run to
$74.49.

The binding constraint here is **a human's attention, not tokens.** Spending more would not make
the answer arrive sooner.

## Pre-flight, completed 2026-08-21

One chat completion against `praneshrajan15-3599-resource` returned HTTP 200,
`model: gpt-5.6-sol-2026-07-09`, 13 prompt + 5 completion tokens, ~$0.0002. This matters because
`DECISIONS.md` recorded that gpt-5.6-sol "cannot be reproduced on this subscription" — true of the
older `...-9819` resource under a Free Trial with zero premium quota, and **stale** for the current
one. Retracted in place there.

## Amendment 1, 2026-08-24: H2 carries a labeller-independence precondition

**Nothing above is edited.** This section is appended after the fact and is not part of the
pre-registration; it records a precondition the original H2 definition omitted, discovered by
attempting to measure H2.

`B_blind` above is specified entirely in terms of **when** the proposal is revealed: hidden at
elicitation, compared afterwards. It says nothing about **who** labels. A separate pre-registration,
`eval/preregistration/blind_elicitation.md`, ran that arm with an LLM standing in for the labeller
and returned VOID on its own capability floor -- see `docs/trust/blind-elicitation-result.md`. The
informative part was not the VOID:

> the blind arm reproduced the corrector's **exact wrong value** on 81.94% of items.

Ordering cannot help when the labeller independently emits the same wrong value. Hiding the proposal
removes the anchor, which is what `B_blind` claims to do, and still leaves `beta` high because the
anchor was never the only path to agreement. So H2 as written is **incomplete**, not wrong:

> **H2 (amended)** Blind elicitation reduces `beta` **provided the labeller's errors are independent
> of the corrector's.** Where that independence fails, `B_blind` and `A_ratify` converge and the
> ordering condition buys nothing.

Consequences for this pre-registration, if it is ever executed:

1. **The falsifier in the table is unchanged** and remains binding. A human labeller satisfies the
   independence precondition by construction, which is why H2 stays live and why the amendment costs
   this design nothing.
2. **H2 may not be pre-tested with the corrector's own model.** Measuring `beta` conditions on the
   proposal being wrong; if the labeller is the same process that generated the proposal, that
   condition collapses into "the labeller already got this wrong" and the arm runs only on items it
   has already failed. That is an instrument defect, not low power, and no increase in `n` repairs it.
3. **Report the labeller's unconditional accuracy on the same cells** alongside `beta`. Had the
   earlier design carried that number, the collapse in point 2 would have been visible before
   spending anything rather than after.

The kill criterion and H1 are untouched. H1 has since been **decided independently** of this
document: stratifying `beta` by control class puts the binding bound at 0.8712 against the 0.35
threshold, so human-labelled per-table certification at alpha=0.05 is dead and this pre-registration
is no longer on the critical path for it. See `docs/trust/stratified-label-noise-result.md`, whose
limitation 1 also records that the `corrector_generated` class supplied 8 controls rather than the
15 specified above.