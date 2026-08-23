# SPEC: Error-injection fidelity

Status: **normative**. Adopted 2026-08-23.
Executable counterpart: `tests/integration/test_error_fidelity_gate.py`.
Implementation: `dataforge/datasets/inject.py`.
Pre-registration: `eval/preregistration/error_fidelity.md`.

## The refusal this spec exists to state

> An injected corpus is admissible as evidence only if the vector of per-detector metrics
> measured on it agrees, within a pre-registered bound, with the same vector measured on a
> real-error reference. A corpus that fails is **refused, not downgraded**.

Refused rather than downgraded because a downgraded corpus is still cited. `tax` and
`hospital` were both nominally caveated and both were nonetheless the source of published
numbers for months; `hospital` was the declared flagship and hard regression anchor while
its entire error model was a single substituted character.

## Why injection is allowed at all

Two facts have to be held at once.

**Injected errors have been actively misleading here.** Measured 2026-08-23: the detector
family that scores 92/92 and precision 0.561 on `hospital` runs at 0.025-0.037 on real
wild columns (`docs/trust/real-error-detection-result.md`). That is not a caveat, it is a
different system being described.

**And there is no real-error corpus for the correction axis, and there will not be one.**
REIN's authors (EDBT 2023, arXiv:2302.04702) built a benchmark specifically to fix this and
reported finding exactly two usable real-error datasets out of fourteen; they injected the
other twelve. `RT-bench`/`ST-bench` ship **no clean values**, so they can score detection
and never a repair. Per-table certification (`dataforge calibrate --certify`) sidesteps the
problem for a user's own table but produces no shareable corpus.

So the choice is not "real or injected". It is "injected with a measured fidelity, or
injected with an assumed one". TableEG (arXiv:2507.10934, Tsinghua/Beihang) establishes
that the first is achievable: it reports that detector metrics on its generated errors
align with metrics on real errors across nearly all datasets and algorithms. **The
transferable contribution is the admissibility criterion, not the model.** This spec
implements the criterion as a gate.

## Structural precedent

This is deliberately the same shape as `dataforge/release/corrector_gate.py`, which admits
an LLM error class to auto-apply only on a committed measurement clearing a canonical
verdict, and fails closed on a malformed artifact. Currently every class sits at the 1.01
sentinel and every committed record rejects, so nothing auto-applies -- and the gate
passes, because refusing is a valid outcome.

An error generator is the same kind of object: a fallible source asking for evidential
standing. It earns it the same way.

## The criterion

Let `D` be the ordered detector list, and for a corpus `C` let `m(C) in R^|D|` be the
vector of per-detector precisions measured by
`dataforge.bench.detection.measure_column_benchmark`.

Given a generated corpus `G` and a real-error reference `R`, `G` is **admissible** iff all
four hold:

| # | Condition | Threshold |
| --- | --- | --- |
| F1 | Spearman rank correlation of `m(G)` against `m(R)` over detectors that fired on either | `rho >= 0.70` |
| F2 | Maximum absolute per-detector gap | `max_d abs(m(G)_d - m(R)_d) <= 0.25` |
| F3 | Detectors firing on `G` but never on `R`, or the reverse | `<= 1` |
| F4 | Detectors that fired on at least one of the two | `>= 3` |

**F4 is the non-vacuity condition and it is not optional.** A corpus on which no detector
fires has a trivially perfect rank correlation with any other such corpus. This project has
already shipped exactly that bug in the other direction: a corruption oracle that
"generated clean columns so tightly clustered no correct cell could be flagged", producing
a fixture that proved nothing. Without F4, the fidelity gate would certify it.

**F1 is rank, not level, correlation.** The claim being tested is that the generated corpus
*orders detectors the way real errors do* -- that a detector which is best on real data is
best here. Absolute agreement is F2's job, with a loose bound, because reproducing real
precision levels exactly is not achievable and demanding it would reject every generator.

## Reference corpus

`RT-bench` is the default reference. It is the only real-error corpus in the registry with
enough distinct values (93,291) to estimate a false-positive rate, which is the axis the
correction gate actually needs.

Its limits transfer to every fidelity verdict and must be carried in the artifact:

- Detection only. A fidelity verdict about *detection* metrics says nothing directly about
  whether a generated corpus reproduces real *correction* difficulty. Stated because this
  is the gap most likely to be forgotten: passing this gate does not make a generated
  corpus a valid correction benchmark, only a plausibly-shaped detection one.
- Distinct values, not cells (`SPEC_abstention_scoring.md` L1).
- 88 unambiguous error values, so per-detector precision on `R` rests on small numerators
  even though the negative set is large.

## Thresholds are pre-registered

`rho >= 0.70`, gap `<= 0.25`, coverage mismatch `<= 1`, minimum 3 firing detectors.
Recorded in `eval/preregistration/error_fidelity.md` **before** any generator was run, for
the same reason `CERTIFICATION_GRID` is a module constant: a threshold chosen after seeing
the result is a validity problem, not merely a power one.

Raising a threshold to admit a specific generator is forbidden. The remedy for a failing
generator is a better generator.

## What a passing verdict does and does not authorise

**Does:** use the generated corpus as a *diagnostic* instrument -- multi-table and
relational error coverage that no public corpus provides, a debatable class with known
labels so the neutral zone can be validated against ground truth rather than trusted, and
scale beyond 1,000-row toys.

**Does not:**

- promote the corpus above `diagnostic` tier. Fidelity is evidence that a corpus is
  *shaped* like real data, not that it *is* real data, and `DatasetMetadata.tier` is
  enforced separately by `scripts/ci/readme_truth.py`.
- authorise a headline claim. No generated corpus may source one.
- touch any write gate. Nothing measured on a generated corpus may add a detector to
  `CONSTRAINT_CHECKABLE_DETECTORS`.
- transfer across generators, seeds or reference corpora. A verdict is bound to the
  `(generator_id, seed, reference)` triple recorded in the artifact.

## Fail-closed obligations

1. An **unmeasured** generator is inadmissible. Absence of a verdict is not a pass.
2. A **malformed** fidelity artifact is inadmissible, never ignored.
3. F4 violated is `REFUSED`, never `PASSED` on a vacuous correlation.
4. A verdict whose reference corpus digest does not match the registry is inadmissible:
   fidelity against unverified bytes is not fidelity.
