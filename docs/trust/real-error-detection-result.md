# Real-error detection result: no detector has a safe operating point

> **CORRECTED 2026-08-23, same day.** Parts of this document are retracted. The adapter
> deduplicates values, so three frequency-dependent detectors -- `Outlier`, `DecimalShift`,
> `CategoricalNormalization` -- were measured on a distribution that does not exist.
> Re-measured with them excluded: **RT-bench precision is unchanged at 0.0285, ST-bench rises
> from 0.0113 to 0.0215.** Read `frequency-dependence-correction.md` first; it states the
> mechanism, the measured proof, and exactly which sentences are withdrawn. Retracted claims
> are marked RETRACTED inline below and left in place rather than deleted.
>
> What survives: everything resting on the per-value detectors (`TypeMismatch`,
> `MissingValue`, `FormatViolation`, `SemanticDomain`), including the flat frontier, the
> finding that nothing certifies at `alpha = 0.05`, and the headline that 97 to 98 of every
> 100 flags on real data are wrong.

Measured 2026-08-23. Artifacts: `eval/results/detection_rt_bench.json`,
`eval/results/detection_st_bench.json`.
Corpora: Auto-Test `RT-bench`/`ST-bench` @ `4acf65cf37a506206bf2888dbd45f17e58dce2e2`.
Rule: `specs/SPEC_abstention_scoring.md`. Scope and limits: `column-benchmark-scope.md`.

Reproduce with `python scripts/bench/measure_detection.py`.

## Headline

Across **2,397 real table columns and 166,387 real distinct values**, with debatable
values scored neutrally so principled abstention is not penalised:

| | RT-bench | ST-bench |
| --- | --- | --- |
| ensemble precision as published (SUPERSEDED) | 0.0255 | 0.0113 |
| **corrected `evaluable_ensemble` precision** | **0.0285** | **0.0215** |
| corrected tp / fp / fn | 8 / 273 / 33 | 14 / 637 / 33 |
| corrected recall | 0.1951 | 0.2979 |
| corrected F1 | 0.0497 | 0.0401 |
| flags excluded as not evaluable | 13 | 620 |

Both corrected rows include `SemanticDomainDetector`. The exclusion changed **RT by nothing**
-- all 13 invalid flags were also flagged by a valid detector -- and changed **ST by 1.90x**,
because roughly 590 of the 620 were unique to `Outlier`. The two corpora sat at opposite
extremes of the overlap from the same detectors on the same run, which is why an ensemble
precision cannot be corrected by subtracting per-detector counts.

Roughly **97 to 99 of every 100 flags on real data are wrong**, and the neutral zone was
already granted before counting.

## The finding that matters most: the frontier is flat

The risk-coverage frontier exists to locate a threshold where selective risk is low
enough to act on. There is no such threshold, and worse, **risk barely moves with the
threshold at all**:

| detector | corpus | risk @ t=0.95 | risk @ t=0.00 | `risk_upper` best case |
| --- | --- | --- | --- | --- |
| TypeMismatch | RT | 0.9714 | 0.9748 | 0.9922 |
| TypeMismatch | ST | 0.9380 | 0.9628 | 0.9681 |
| FormatViolation | RT | 0.9623 | 0.9867 | 0.9933 |
| FormatViolation | ST | 0.9713 | 0.9785 | 0.9873 |
| DecimalShift | both | 1.0000 | 1.0000 | 1.0000 |
| Outlier | both | 1.0000 | 1.0000 | 1.0000 |
| DateTransposition | ST | — | 1.0000 | 1.0000 |
| **MissingValue** | RT | 0.7143 | 0.6667 | **0.9023** |
| **MissingValue** | ST | 0.5000 | 0.6000 | **0.8071** |

A flat frontier is a *calibration* result, not an accuracy result, and it is the more
damaging of the two. If risk does not fall as the threshold rises, then no threshold can
buy safety, and the confidence a detector emits carries almost no information about
whether it is right. TypeMismatch on RT-bench moves from 0.9714 risk at the strictest
grid point to 0.9748 at the loosest — a 0.3 percentage-point spread across the entire
confidence range.

### This is why nothing certifies, and why that is correct

`conformal.certify_threshold` certifies a threshold when the one-sided Clopper-Pearson
upper bound on selective risk clears `alpha`. At `alpha = 0.05`, the **best** detector in
the suite bounds at `risk_upper = 0.8071`, sixteen times the budget. Every other detector
bounds at 0.97 or 1.00.

So the write gate holding everything is not conservatism, and it is not a placeholder
awaiting a better model. On this evidence it is the only defensible behaviour, and
`release/corrector_gate.py` reporting `enabled_classes == []` is a correct reading of
reality rather than an unfinished feature.

## Per detector

Applicability is declared in `dataforge.bench.detection.DECLARED_APPLICABILITY` and asserted
against observation. Four classes, corrected 2026-08-23 from an earlier two:

- `per_value` -- a predicate on one value. **Valid** on a distinct-value corpus.
- `proportion_gated` -- per-value behind a column-type fraction gate. Valid, gate shifts.
- `frequency_dependent` -- needs value multiplicities. **Not evaluable here.**
- `row_context` -- needs other rows or columns. Not evaluable here.

`not_evaluable` is **not** recall 0 and **not** precision 0. It means this corpus cannot
answer the question.

| detector | applicability | RT precision | ST precision | RT fp | ST fp | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| SemanticDomain | per_value | 0.3333 | **0.5333** | 6 | 7 | best in suite; see `semantic-domain-result.md` |
| MissingValue | per_value | 0.3333 | 0.4000 | 6 | 6 | external reference; still far from usable |
| TypeMismatch | per_value | 0.0252 | 0.0372 | 116 | 233 | internal reference; see below |
| FormatViolation | per_value | 0.0133 | 0.0215 | 148 | 364 | internal reference; high volume, near-zero precision |
| TimeFormatCruft | per_value | never fired | never fired | 0 | 0 | valid measurement, unexplained silence: OPEN QUESTION |
| DateTransposition | proportion_gated | — | 0.0000 | 0 | 30 | 30 flags, 30 wrong; gate shifted by dedup |
| DecimalShift | frequency_dependent | RETRACTED | RETRACTED | 8 | 10 | not evaluable: uses median + log-IQR |
| Outlier | frequency_dependent | RETRACTED | RETRACTED | 5 | 609 | not evaluable: `mad == 0` abstention destroyed by dedup |
| CategoricalNormalization | frequency_dependent | not evaluable | not evaluable | 0 | 0 | structurally excluded, not silent: 5 flags with frequencies, 0 without |
| FDViolation | row_context | not evaluable | not evaluable | — | — | needs a declared FD |
| DuplicateRow | row_context | not evaluable | not evaluable | — | — | unrepresentable: `dist_val` is distinct |
| EntityConsensus | row_context | not evaluable | not evaluable | — | — | needs several columns |

The false-positive columns for the frequency-dependent detectors are retained because they
are what the corrected ensemble bound subtracts, not because they measure anything about
those detectors.

"Never fired" is reported as an absence of evidence, not as a measured zero:
`DetectorMeasurement.score` is `None`, and `None` is excluded from the aggregate
denominator rather than contributing a 0.0.

**The four `per_value` rows are the substantive result**, and they separate by 15x on one
variable: whether the reference being checked against is external to the column. See
`reference-externality.md`.

## Three claims this retires

### 1. The 92/92 `type_mismatch` result was a benchmark artifact

`docs/trust/corrector-queue-contamination.md` already suspected the local precision of
0.9389 was inflated by construction, because `hospital`'s errors are a single injected
character (`birminghxm` -> `birmingham`). This quantifies it.

`cli/calibrate.py:440-447` records measured detector precision of **0.561 on hospital**,
0.947 on flights and 0.342 on rayyan. On real wild tables the same detector family runs
at **0.025 (RT) and 0.037 (ST)**.

Unit caveat, stated because the comparison is otherwise abused: those are cell-level
precisions and these are distinct-value precisions, so this is a same-quantity
comparison in different units, not a protocol-controlled one. The direction and the order
of magnitude are the finding; the exact ratio is not.

A perfect record on `x`-substitutions was never evidence of competence at detecting real
errors. It is now measured not to be.

### 2. `Outlier` and `DecimalShift` were not wronged by injected data

**RETRACTED 2026-08-23.** See `frequency-dependence-correction.md`. Both detectors are
frequency-dependent and were measured on a deduplicated column. `Outlier` returns `[]` when
`mad == 0`, which is common on a real column with a dominant value and never happens after
deduplication -- measured: 0 flags with frequencies preserved, 1 flag on the identical
distinct values deduplicated. Their 614 combined false positives are substantially an
artifact of the adapter.

Their dispositions -- no repairer for `outlier`, `decimal_shift` off the allowlist -- may
still be correct on the RAHA evidence that originally justified them, including the 263,428
false rewrites on TPC-H money columns. That is a different and weaker claim than the one
originally made here, which was that this corpus vindicated them on better evidence. It did
not; it produced invalid evidence.

The original text follows, retained because deleting a retracted claim hides that it was
ever made.

> Both carry 0.0000 precision from the RAHA corpora, and `decimal_shift` was excluded from
> `CONSTRAINT_CHECKABLE_DETECTORS` after **263,428 false rewrites** on TPC-H money columns
> with zero true errors. A reasonable objection was that injected corpora were unfair to
> them.
>
> They reproduce at 0.0000 on real data, at every threshold, with `risk_upper = 1.0`.
> `Outlier` alone contributes **609 of ST-bench's 1,223 false positives** -- half the
> ensemble's entire error volume from one detector with no true positive anywhere. The
> existing dispositions (no repairer for `outlier`; `decimal_shift` off the allowlist) were
> right, and are now right on better evidence.

### 3. Two detectors have no measured coverage at all

**PARTLY RETRACTED 2026-08-23.** `CategoricalNormalization` is `frequency_dependent` and was
**structurally excluded** by the adapter: two independent guards
(`categorical_normalization.py:68` and `:88`) return `[]` for every deduplicated column
before any work is done. Measured: 5 flags with frequencies preserved, 0 deduplicated. It
correctly declined to fire, and the recommendation below to exclude it from claims of
ensemble breadth was unjustified.

`TimeFormatCruft` appears to be per-value, so its silence is **not** explained by this
correction and remains an open question rather than a finding.

> `CategoricalNormalization` and `TimeFormatCruft` fired **zero times across 166,387 real
> values**. Not imprecisely -- not at all. Whatever they contribute on RAHA, they contribute
> nothing here, and any claim of ensemble breadth should exclude them until that changes.

## The one positive result

`MissingValue` is the most precise detector in the suite by an order of magnitude, and
its frontier is the only non-flat one: risk 0.5000 at `t=0.85` on ST-bench against
0.6000 at the floor.

It is also the detector whose applicability declaration was **wrong in this repository
until this measurement**. It was declared `row_context` on the reasoning that
`missing_value` needs a declared functional dependency — which is true of *repair*, where
the FD derives the fill value, and false of *detection*, where noticing an empty or
placeholder cell needs nothing but the column. The harness records declared and observed
applicability separately and flagged the contradiction; the declaration was corrected, not
the measurement.

Two lessons kept: a declaration derived from observed behaviour cannot classify a detector
that never fires, so applicability must be declared explicitly; and the distinction
between what a detector needs to *detect* and what it needs to *repair* is load-bearing,
not pedantic.

## What this does not say

- **Not a recall claim.** 88 unambiguous error values total. Recall rests on double-digit
  support and must always be reported with its bound.
- **Not comparable to `BENCHMARK_REPORT.md`.** Those are cell-level correction numbers;
  these are distinct-value detection numbers. `hospital` F1 0.8352 and RT-bench F1 0.0444
  are not two points on one scale. The 0.8352 is additionally a **proposal-stage** figure and
  is **not** an end-to-end result: through the shipped write path a declared premise on that
  corpus writes nothing, and the ground-truth-admitted ceiling does not exceed F1 0.1918. See
  [declared-premise-capability.md](declared-premise-capability.md).
- **Not a verdict on the write gate.** Detection has no write path. Nothing here may be
  used to add a detector to `CONSTRAINT_CHECKABLE_DETECTORS`; that allowlist asks whether
  a proposed value can be *checked*, which detection cannot answer.
- **Not a statement that DataForge corrupts data.** Precision of 0.02 on the *review
  queue* is a usability problem. It becomes a corruption problem only where a flag can
  reach a write, and of the detectors measured here exactly one — `type_mismatch` — is on
  the allowlist, gated additionally by premise and strength per
  `specs/SPEC_autoapply_decision.md`. The gate is what stops 0.02 precision becoming 0.98
  corruption, and this result is the strongest evidence yet for why it must not be
  relaxed.
