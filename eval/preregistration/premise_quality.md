# Pre-registration: can the FD miner tell a real dependency from a coincidence?

Written 2026-08-25, **before** implementing or measuring anything. Nothing below is edited
afterwards; results and deviations are appended as amendments.

## The finding this responds to

`docs/trust/bypass-allowlist-evidence.md` and `docs/trust/deductive-coverage-result.md`
measured that premise quality, not the write gate, determines corruption. Under a premise
whose dependencies all hold, the FD repairer corrupted **0** already-correct cells on
hospital. Under the product's own mined premise it corrupted **86**, a harmful write rate of
0.1601, and **all 25 sampled corruptions traced to mined dependencies that are false on
ground truth** -- 23 of them to `ZipCode -> HospitalName`. A zip code does not determine a
hospital name.

Of 115 mined dependencies, 103 hold exactly on clean: an FD-set precision of **0.8957**.

## The objection this must answer

`docs/trust/constraint-circularity.md`:32-41 forecloses the obvious response:

> both hold at ~0.9-1.0 with some violations. **No in-table signal separates** "these
> violations are errors to fix" from "these violations are correct variation to keep" [...]
> a confidence threshold cannot separate them; tuning one to fit two datasets would be
> overfitting, which the honesty doctrine forbids. The defense must therefore be
> **architectural, not a smarter score**.

That is correct, and it answers a different question than the one the 86 corruptions raise.
Two questions are being conflated:

- **Q1.** Given a dependency that *holds on the true data*, are these particular violations
  errors to fix or legitimate variation to keep? **Undecidable in-table. Conceded in full.
  This pre-registration does not touch Q1.**
- **Q2.** Does the dependency hold on the true data *at all*? `ZipCode -> HospitalName` is
  not an approximate FD violated by dirty cells; it is a **false dependency**. That is a
  different question, and the miner already stakes two guards on it being partly decidable:
  `_MAX_DETERMINANT_UNIQUE_FRACTION = 0.9` and `_MIN_FD_SUPPORT_GROUPS = 2`.

So the absolute claim "no in-table signal separates" is inconsistent with the file's own
implementation. What is measured is narrower and harder to dispute: **the existing Q2 guards
are insufficient.** `ZipCode -> HospitalName` passes both, scores at or above 0.95, is false,
and caused nearly every observed corruption.

## The defect: the miner does not apply its own oracle's rule

| | guards the **determinant** against triviality | guards the **dependent** against triviality |
| --- | --- | --- |
| product miner, `_fd_candidates` | yes: constant and near-superkey both rejected (`schema_inference.py`:581-588) | **no check exists** |
| this project's own bench oracle | n/a | **yes**: `if clean[dependent].nunique() <= 1: continue`, rationale "a single-valued column is determined by everything" |

**10 of the 12 measured false dependencies have `State` or `Stateavg` as the dependent** --
`Score -> State`, `Sample -> State`, `EmergencyService -> State`, `MeasureCode -> Stateavg`
and so on. In a 1000-row hospital subset those columns are near-constant, so *everything*
determines them. The bench oracle excludes exactly this and the product miner does not.

## The two corrections, and why neither is a tunable threshold

This is the part that answers the overfitting objection. **Neither correction introduces a
free parameter.** There is nothing to fit, so there is nothing to overfit.

### C1. A dependency must beat the dependent's own majority baseline

`confidence = 1 - g3` measures how well `X` predicts `Y`. The trivial predictor that ignores
`X` entirely and always emits `Y`'s global modal value has error `1 - majority_share(Y)`. A
dependency that does not beat that predictor is not evidence of a dependency; it is evidence
that `Y` is skewed.

> **Require `confidence > majority_share(dependent)`.**

No constant appears. It is the standard majority-class baseline comparison, and it
generalises the bench oracle's `nunique <= 1` rule: a constant dependent has
`majority_share = 1.0`, which no confidence can exceed, so the oracle's rule falls out as the
limiting case rather than being bolted on.

### C2. Confidence must be measured on the rows that can actually violate

`confidence = 1 - violations / total_rows` (`schema_inference.py`:612). But a violation can
only occur inside a determinant group of two or more rows -- a singleton group is consistent
with *any* dependent value and tests nothing. So every singleton row inflates the score
without supplying evidence.

> **Measure `tested_confidence = 1 - violations / rows_in_multi_row_groups`.**

Again no constant: this is a correction to a denominator that was counting rows which cannot
falsify the claim. It subsumes `_MIN_FD_SUPPORT_GROUPS` as a special case -- an FD with almost
no repeated determinant values now scores low on its own terms instead of needing a separate
floor -- and it is the reason a high-cardinality determinant like `ZipCode` currently survives.

The existing absolute floor (0.9) and the existing determinant guards are **retained
unchanged**. C1 and C2 are additional, and both are strictly conservative: they can only
reject candidates the current miner emits, never admit new ones.

## Predictions

| # | Quantity | Prediction |
| --- | --- | --- |
| **P1** | `State` and `Stateavg` as dependents on hospital | `majority_share >= confidence`, so C1 excludes them. This should remove **10 of the 12** false dependencies. |
| **P2** | `ZipCode -> HospitalName` and `ZipCode -> HospitalOwner` | survive C1, and fall to C2: a small fraction of rows live in multi-row `ZipCode` groups, so `tested_confidence` drops below the retained 0.9 floor. |
| **P3** | mined FD-set precision on hospital | rises above the incumbent **0.8957**, and hospital corruption falls below **86**. |
| **P4** | coverage | falls. Some true dependencies will be rejected. `missing_value`'s 427 flights writes and `fd_violation`'s 393 hospital-oracle / 451 hospital-mined writes are the figures most at risk, and all are reported before and after. |
| **P5** | parameters | zero new constants introduced, verifiable by inspecting the diff. |

**Uncertainty stated plainly.** P1 and P2 are structural arguments about specific columns and
I have not yet computed either quantity. P4 is the one I expect to hurt: a rule strict enough
to exclude `ZipCode -> HospitalName` may also exclude genuine dependencies with few repeated
determinant values.

## Kill criterion, fixed now

**Revert, do not retune, if any of:**

* **K1** mined FD-set precision does not improve on 0.8957 on hospital; or
* **K2** hospital corruption under the mined premise does not fall below 86; or
* **K3** the change requires introducing a tunable constant to satisfy K1 or K2. A parameter
  added after seeing the data is the overfitting `constraint-circularity.md` forbids, and the
  correct response is to abandon C1 and C2 rather than to fit them.

**Net-harm rule.** Precision bought by writing nothing is not an improvement. If
`repaired_a_real_error` falls by more than the reduction in harmful writes -- that is, if
`net_cells_improved` decreases on any corpus -- the change is reported as a trade for the
user to accept explicitly, not shipped as a win.

## Scope and reporting

All four dirty/clean corpora are measured: hospital, flights, rayyan, **tax**. `tax` is the
corpus `constraint-circularity.md` itself used for its 696-708 false-positive result, and it
is reported per-corpus like the others. Because C1 and C2 have no parameters, no corpus is
used to *choose* anything -- there is nothing to choose. That is the strongest available
answer to the overfitting objection, and it is why the criteria were derived before any
number was computed.

**Does not** claim to solve Q1. **Does not** claim the miner becomes safe -- an accepted
dependency still authorises calibration-bypassing writes, and the remedy for that is the
measured warning at review time. **Does not** touch `_MAX_DETERMINANT_UNIQUE_FRACTION`,
`_MIN_FD_SUPPORT_GROUPS`, or the 0.9 floor.
