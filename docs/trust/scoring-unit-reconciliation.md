# The scoring unit is not a detail, and it breaks the reference-externality thesis

Measured 2026-08-23 on `rayyan` (natural errors, full 1000-row table, 948 ground-truth error
cells, frequencies present). Reproduces the same detectors under two scoring units on the
**same** data, so the unit is the only variable.

This document reports a result that contradicts a thesis I was about to publish. Recording
that is the point.

## The measurement

| detector | applicability | CELL precision | DISTINCT-VALUE precision | direction |
| --- | --- | --- | --- | --- |
| DateTransposition | proportion_gated | **1.0000** (tp 722, fp 0) | 1.0000 | stable |
| FormatViolation | per_value | **0.2388** (tp 85, fp 271) | **0.0000** | dedup **destroys** it |
| MissingValue | per_value | **0.0649** (tp 75, fp 1080) | **0.2000** | dedup **inflates** 3.1x |
| TypeMismatch | per_value | 0.0312 (tp 2, fp 62) | 0.0328 | stable |
| Outlier | frequency_dependent | 0.0000 (fp 118) | 0.0000 | stable at zero |
| CategoricalNormalization | frequency_dependent | 0.0000 (fp 25) | silent | dedup silences |

## Finding 1: hypothesis H1 is falsified, and the real situation is worse

H1 predicted that cell-level precision would be **systematically worse** than distinct-value
precision, reasoning that errors are rare-valued while false positives land on common values.

That holds for `MissingValue` (0.0649 cell against 0.2000 deduplicated, a 3.1x
overstatement by the distinct-value view) and **fails completely for `FormatViolation`**,
which goes the other way and by more: 0.2388 at cell level against 0.0000 deduplicated.

So there is no systematic direction and therefore **no conversion factor**. The gap is
detector-specific and non-monotone, ranging from negligible (`TypeMismatch`, 0.0312 against
0.0328) to total (`FormatViolation`, a real 0.2388 reported as zero).

This is the third time in two days that the same shape has appeared: `head`-versus-random
sampling distorted two error classes in opposite directions, deduplication distorted
`Outlier` and `CategoricalNormalization` in opposite directions, and now the scoring unit
distorts `FormatViolation` and `MissingValue` in opposite directions. **A biased view of data
is not a weaker view of the same population. It is a view of a different one.** That
generalisation now has three independent confirmations and should be treated as the
governing prior for any new measurement in this repository.

## Finding 2: the four-way taxonomy is necessary but not sufficient

`FormatViolation` is `per_value` -- its predicate reads one value with no reference to the
distribution -- and its precision still moves from 0.2388 to 0.0000 when the unit changes.

So the applicability taxonomy answers **"can this detector fire validly on this corpus?"**
and does **not** answer **"is this number comparable to that one?"** Those are separate
questions and conflating them was the residual error after the first correction.

The reason is arithmetic, not semantic: at cell level a single erroneous value repeated forty
times contributes forty true positives, and at value level it contributes one. Any detector,
however per-value its predicate, is scored against a different denominator.

**Consequence for `RT-bench` and `ST-bench`: their precision figures are not convertible to
product-relevant cell-level precision in either direction.** Limit L1 in
`specs/SPEC_abstention_scoring.md` said this qualitatively; it is now measured, and the
measured gap is up to total.

## Finding 3: the reference-externality thesis does not survive the unit change

I was about to publish this as a unifying architectural finding, on the ST-bench
distinct-value numbers:

| detector | reference | ST distinct-value precision |
| --- | --- | --- |
| SemanticDomain | external | 0.5333 |
| MissingValue | external | 0.4000 |
| TypeMismatch | internal | 0.0372 |
| FormatViolation | internal | 0.0215 |

A clean 15x separation on whether the reference is external to the column. The proposed
consequence was substantial: that reference-externality predicts precision as well as
soundness, that it should be encoded once, and that the roadmap should shift from
certification power to reference acquisition.

**The rayyan cell-level measurement inverts the ordering.** External `MissingValue` scores
0.0649; internal `FormatViolation` scores 0.2388 -- the opposite of the prediction, by 3.7x.

Two confounds prevent calling the thesis false: `rayyan` is a different corpus from
`ST-bench`, and the unit differs too. So this is a **non-replication under changed
conditions**, not a refutation. But it is decisive about the epistemic status:

> **Reference-externality is an open hypothesis with one supporting measurement and one
> non-replication. It is not a finding, and nothing may be built on it yet.**

### What this changes

The plan for this work called for encoding `reference_provenance` as a first-class detector
property and **deriving `CONSTRAINT_CHECKABLE_DETECTORS` from it**. That is now withdrawn.
Deriving the write allowlist from an unreplicated hypothesis would make the soundness gate
depend on a correlation measured once, on one corpus, in one unit -- which is a more
dangerous version of exactly the mistake being corrected throughout this document.

`CONSTRAINT_CHECKABLE_DETECTORS` stays a hand-maintained allowlist justified by measured
corruption incidents, not by a precision correlation.

### The pre-registered test that would settle it

Recorded before running, so the result cannot be reinterpreted afterwards:

1. Hold the corpus fixed and vary only the unit, on **three** frequency-preserving corpora
   (`rayyan`, `hospital`, `flights`). If externality predicts precision, the external
   detectors must rank above the internal ones **at cell level** on at least two of three.
2. Hold the unit fixed at cell level and vary the corpus. Rank correlation of the
   externality ordering against the measured ordering must be positive on at least two of
   three.
3. Failure of either is publishable as a refutation and closes the reference-acquisition
   roadmap.

Note the honest asymmetry: `SemanticDomain` cannot enter test 1 as written, because the
learned SDCs are pattern constraints over column value sets and their pre-conditions were
tuned on corpora unlike `rayyan`. That weakens the test, and a weakened test must not be
reported as a passed one.

## Finding 4: something good, which the correction framing was hiding

On `rayyan`, `DateTranspositionDetector` finds **722 of 948 error cells at precision
1.0000** -- 722 true positives, zero false positives.

`heuristic_rayyan_full.json` records `rayyan` as correction P/R/F1 = 0/0/0, and
`sampling-bias-measured.md` describes it as "detect-and-abstain". Both are accurate and both
obscure this: a single detector achieves perfect precision on 76% of that corpus's errors.
It is detection-only by design (no repairer; the correct canonical date form needs an
authority the table lacks), so it cannot raise correction F1 above zero and therefore never
appeared in a headline.

That is the detection/correction split working exactly as intended, and it is the strongest
single detector result measured anywhere in this project. It deserved to be reported the
first time and was not, because the measurement pipeline reports correction F1 and the
per-class detection recalls, and nothing surfaced per-detector cell-level precision.

## What to trust from all of this

- **Trust**: the four-way applicability taxonomy and the refusal to score what a corpus
  cannot support. That rests on two directly measured mechanisms.
- **Trust**: that distinct-value and cell-level precision are not interchangeable, with a
  measured gap up to total.
- **Do not trust**: any cross-unit or cross-corpus precision comparison in this repository
  that predates this document.
- **Do not trust**: reference-externality as an explanation, until the pre-registered test
  above runs.
