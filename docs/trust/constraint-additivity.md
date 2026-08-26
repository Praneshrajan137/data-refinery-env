# Constraint acceptance is not additive, and two signals that do not separate

**Status**: measured 2026-08-26. Artifact: `eval/results/constraint_additivity.json`. Reproduce with
`python scripts/bench/measure_constraint_additivity.py --artifact eval/results/constraint_additivity.json`.

## Why this was measured

`docs/trust/shipped-premise-result.md` established that corruption is a **conjunction**: a mined
dependency damages a cell only when it is false **and** the cell's determinant group holds visible
disagreement for the repairer to resolve. Two of the four dependencies that took hospital from 86 to
116 corruptions are equally false and corrupted **nothing**.

That has a direct consequence for the interface. `constraints review` asks a human for N independent
accept/reject decisions. If per-candidate harm composed, the product could show "this dependency would
overwrite K correct cells" and a reviewer could reason about a budget. This measures whether it
composes.

## The result: it does not compose, and it errs high

Hospital, 85 mined candidates, shipped `majority` rule, per distinct cell:

| quantity | value |
| --- | --- |
| corruption summed over candidates measured **alone** | **330** |
| corruption when **all 85 accepted together** | **116** |
| non-additivity factor | **2.84** |
| candidates | 85 |
| false dependencies | 16 |
| harmful in isolation | 15 |

The cause is masking. When several accepted dependencies could act on one cell, `_acting_group` picks
the first match, so only one acts. Overlapping dependencies therefore hide one another's damage, and
the sum over singletons overcounts.

**A per-candidate harm figure would overstate harm by a factor that depends on what else the reviewer
accepts.** That is why the write-authorisation warning in `dataforge/cli/constraints.py` deliberately
carries **no** per-dependency number. The absence is a finding, not an omission.

Note the direction carefully. Non-additivity here means the aggregate is *smaller* than the sum, so a
per-candidate number would frighten a reviewer away from a dependency whose marginal contribution is
lower than advertised. Being wrong in the cautious direction is still being wrong: it trains a
reviewer to discount the numbers, which costs exactly the trust the number was for.

## What does not separate true from false dependencies

Two hypotheses tested and refuted on the corpus that has ground truth. Recorded so they are not
re-chased, and stated as **no signal in this quantity on hospital** rather than as *no signal exists* —
the distinction this project has had to make before, because the broader claim forecloses work that is
merely unfinished.

### Harm is not concentrated

The hope was that a reviewer could be pointed at the worst few candidates. Measured:

| | share of the 330 isolated corruptions |
| --- | --- |
| worst 1 candidate | 0.1576 |
| worst 3 candidates | 0.2970 |
| worst 5 candidates | 0.4333 |

**15 of the 16 false dependencies are harmful in isolation.** There is no short list. A reviewer who
inspects the five worst has addressed under half the exposure, and the remaining harm is spread across
ten more candidates that look no different.

### Determinant cardinality does not separate

The hypothesis was structural and looked strong: a determinant with very few distinct values produces
enormous groups, and an FD over an enormous group degenerates into a column-wide majority vote rather
than a dependency — the same failure mode measured for `categorical_normalization`, whose reference is
a majority vote over its column's own values.

Measured on hospital's 85 candidates:

| | median distinct determinant values | range |
| --- | --- | --- |
| **false** dependencies | 66.5 | 6 to 334 |
| **true** dependencies | 72 | 4 to 334 |

Fully overlapping, with the counterexamples in both directions: `State -> HospitalType` has **4**
distinct determinant values and is **true**; `Sample -> State` has **334** and is **false**. The
quantity carries no signal here.

This also matters because the miner *does* guard the other end of the same axis — it rejects a
determinant that is constant or near-unique — so the guard looks asymmetric and inviting to complete.
It should not be completed on this evidence.

## What this authorises

- Withholding a per-candidate harm figure from the reviewer, on measurement rather than on taste.
- Citing 330 and 116 together as the honest pair, with the factor, wherever the aggregate is discussed.
- Closing two lines of work on premise triage.

## What this does NOT authorise

- **Any claim that per-candidate harm is uninformative.** It is not additive; that is different from
  useless. A reviewer might still benefit from an *ordering*, and whether they do is unmeasured and
  belongs with `eval/preregistration/reviewer_decision_quality.md`.
- **Generalising 2.84 beyond hospital.** It is a function of how much this corpus's dependencies
  overlap. flights and rayyan mine nothing, so there is no second measurement.
- **Reading "no signal in determinant cardinality" as "no in-table signal exists."** `tested_confidence`
  separates perfectly on hospital and was declined as a gate for want of a validating corpus. The
  blocker throughout remains a second corpus with naturally-occurring false dependencies.

## Method note, and a discipline failure worth recording

The first attempt at this measurement reimplemented the write loop inline rather than importing
`_write_exposure`. It omitted the no-change filter the vetted path applies, because `_rule_choice`'s
docstring states it returns values *"before the no-change check"*. Write counts came out **959 where
the truth is 74**, and it nearly produced a published finding that writes are 95% no-ops — entirely an
artifact of the bug. It was caught because the number looked implausible, not by any gate.

The rule is recorded in `PRODUCT.md`: **a reimplementation of a measurement reproduces the defect the
vetted path exists to avoid.** It is the generalisation of the rule added hours earlier — that an arm
modelling a user journey must be built from the code that journey runs — and it was violated within
the hour by the person who wrote it.
