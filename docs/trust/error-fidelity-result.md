# The fidelity gate refused its first generator, and the number is 66x

Measured 2026-08-23. Artifact: `eval/results/error_fidelity_character_noise_v1_rt_bench.json`.
Spec: `specs/SPEC_error_fidelity.md`. Pre-registration: `eval/preregistration/error_fidelity.md`.

Reproduce with `python scripts/bench/measure_error_fidelity.py`.

## Result

`character_noise_v1` -- uniform single-character `x` substitution, the `hospital` error
model -- injected 2,398 errors across 1,194 real columns and was **REFUSED**.

| condition | measured | threshold | verdict |
| --- | --- | --- | --- |
| F1 rank correlation | **0.0000** | `>= 0.70` | FAILED |
| F2 max precision gap | **0.8667** | `<= 0.25` | FAILED |
| F3 coverage mismatch | 0 | `<= 1` | passed |
| F4 firing detectors | 5 | `>= 3` | passed |

Per-detector precision, injected against real:

| detector | on injected `x` errors | on real errors | ratio |
| --- | --- | --- | --- |
| **FormatViolation** | **0.8800** | **0.0133** | **66x** |
| MissingValue | 0.0000 | 0.3333 | inverted |
| TypeMismatch | 0.0000 | 0.0252 | inverted |
| DecimalShift | 0.0000 | 0.0000 | — |
| Outlier | 0.0000 | 0.0000 | — |

## Why this matters more than the refusal

`FormatViolationDetector` looks **66 times more precise** on injected errors than on real
ones. This is the `hospital` pathology, isolated and quantified on demand rather than
argued about.

`docs/trust/corrector-queue-contamination.md` has argued for some time that local precision
of 0.9389 was inflated by construction because `hospital`'s errors are a single substituted
character. `docs/trust/real-error-detection-result.md` established the endpoint. This
establishes the **mechanism**: it is not that `hospital` is small, or old, or saturated. It
is that a substituted character leaves a value that pattern-based detection finds trivially
and real errors are not like that.

The gate now catches that class of corpus before it can source a number, which is the whole
point of building it.

## Where the pre-registration was right, and where it was wrong

The pre-registered directional prediction was:

> a naive uniform-random character corruption will **fail** F1, because it will produce
> errors that `TypeMismatchDetector` and `FormatViolationDetector` find far more readily
> than real errors are found

**Right on the verdict and on FormatViolation. Wrong on TypeMismatch**, which scored 0.0000
on the injected corpus against 0.0252 on real errors -- the opposite direction.

The reason is worth keeping, because it is a fact about the detector rather than about the
corpus: substituting a character inside a *text* value leaves a text value, so type
inference has nothing to object to. `TypeMismatch` earns its real-corpus precision on
columns where the anomalous value breaks an inferred *type* -- a placeholder in a numeric
column -- and character noise never creates one of those.

So the naive generator fails F1 for a reason more interesting than "it is too easy". It
does not merely shift the precision *level*; it **reorders which detectors work**, sending
rank correlation to exactly 0.0. `MissingValue` is the clearest case: the best detector on
real data by a factor of ten, and completely silent on the generated corpus, because
character noise never produces a missing value.

An injected corpus can therefore be wrong in a way no amount of caveating repairs. It is
not a weaker sample of the same population. It is a different population, on which a
different set of detectors works.

## What the gate is, and is not

**Is:** a floor. Four conditions, thresholds fixed before the first generator existed, and
a refusal that names which condition failed.

**Is not** a certificate of realism, and the artifact says so in five recorded limitations:

1. The reference measures **detection only**. A detection-fidelity verdict is not evidence
   about correction difficulty. This is the limit most likely to be quietly forgotten.
2. Reference precision rests on 88 unambiguous error values, so `m(R)` is itself noisy and
   F2's loose bound absorbs part of that noise rather than isolating generator error.
3. Rank correlation over 3-8 points is low-powered. It rejects grossly wrong error
   populations; it does not confirm right ones.
4. `PASSED` does not promote a corpus above `diagnostic` tier, does not authorise a headline
   claim, and may not add a detector to `CONSTRAINT_CHECKABLE_DETECTORS`.
5. A verdict is bound to `(generator_id, seed, reference_sha256)`. Varying any of the three
   to find a passing combination is threshold-shopping by another route.

## Two things this changes

**A gate that has never refused anything is not known to be a gate.** This one has, on its
first use, against a real 1,200-column corpus, for a pre-registered reason. `F4` -- the
non-vacuity condition -- passed here rather than firing, which matters: the refusal came
from a genuine disagreement between two non-empty measurements, not from an empty corpus
being caught.

**The bar for the next generator is now concrete.** To be admissible, a generator must
produce errors that `MissingValue` can find, that `FormatViolation` finds at roughly real
rates rather than 66x them, and that preserve detector ordering. That specification came
out of a measurement rather than out of taste, and the honest expectation is that reaching
it is hard. The remedy for a failing generator is a better generator, never a lower
threshold -- the thresholds are constants in `dataforge/datasets/inject.py` and asserted
against the pre-registration in `tests/integration/test_error_fidelity_gate.py`.
