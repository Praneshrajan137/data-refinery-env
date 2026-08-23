# Pre-registration: error-injection fidelity thresholds

Registered 2026-08-23, **before any generator was implemented or run**.
Spec: `specs/SPEC_error_fidelity.md`. Implementation: `dataforge/datasets/inject.py`.

## Why this file exists before the code

A threshold chosen after seeing the result is a validity problem, not a power problem. This
project already carries the analogous lesson in `dataforge/conformal.py:215-223`, where the
certification grid is a module constant precisely because a label-derived grid invalidates
the guarantee rather than merely weakening it.

An error generator is a fallible source asking for evidential standing. If its admission
bar can move to accommodate it, the bar is not a bar.

## Hypothesis

An error generator can produce a corpus on which DataForge's detectors rank in
approximately the order they rank on real errors, without reproducing real precision levels
exactly.

Directional prediction, recorded so it can be wrong: a naive uniform-random character
corruption will **fail** F1, because it will produce errors that `TypeMismatchDetector` and
`FormatViolationDetector` find far more readily than real errors are found -- the
`hospital` pathology, where an injected `x` yields precision 0.561 against 0.025 on real
data.

## Thresholds

| id | quantity | threshold | rationale |
| --- | --- | --- | --- |
| F1 | Spearman rank correlation of per-detector precision, generated vs real | `>= 0.70` | Tests ordering, which is the transferable property. 0.70 admits one adjacent swap among four detectors without admitting an unrelated ordering. |
| F2 | max abs per-detector precision gap | `<= 0.25` | Loose on purpose. Real precisions here are 0.01-0.40; demanding tight absolute agreement would reject every generator and make the gate decorative. |
| F3 | detectors firing on exactly one of the two corpora | `<= 1` | A generator that silences a detector real data exercises, or wakes one real data never does, is producing a different error population. One is tolerated as sampling noise at 88 positives. |
| F4 | detectors firing on at least one corpus | `>= 3` | **Non-vacuity.** A corpus nothing fires on correlates perfectly with any other such corpus. Without F4 the gate would certify the empty-oracle bug this project already shipped once. |

## Reference

`RT-bench` @ Auto-Test `4acf65cf37a506206bf2888dbd45f17e58dce2e2`,
sha256 `57cc995d15275fced84d19abaaa46802dd990492052c08d0cbc7fe76b49cb623`.
1200 columns, 41 unambiguous error values, 93,291 distinct values.

Chosen over `ST-bench` because it has the larger negative set, and the false-positive rate
is the axis a write gate needs. A verdict is bound to the reference; re-running against
`ST-bench` produces a separate verdict, not a confirmation.

## Committed in advance

- Thresholds above. **Raising one to admit a specific generator is forbidden.** The remedy
  for a failing generator is a better generator.
- A `REFUSED` verdict is a publishable result. If the first generator fails, that is
  recorded as a finding, not iterated away silently.
- Verdicts are bound to `(generator_id, seed, reference_sha256)`. None of the three may be
  varied post hoc to find a passing combination; doing so is threshold-shopping by another
  route.
- Passing does **not** promote a corpus above `diagnostic` tier, and does not make it a
  valid *correction* benchmark -- the reference measures detection only.

## Known limits of the design, recorded now

1. **The reference is detection-only.** Fidelity of detection metrics is not evidence of
   fidelity of correction difficulty. This is the gap most likely to be quietly forgotten,
   so it is also asserted in the spec and carried in the artifact.
2. **88 positives.** Per-detector precision on the reference has small numerators, so `m(R)`
   is itself noisy and F2's bound absorbs some of that noise rather than isolating
   generator error.
3. **Rank correlation over 3-8 points is low-powered.** F1 at `rho >= 0.70` on four firing
   detectors is a weak test. It is a floor against grossly wrong error populations, not a
   certificate of realism, and must not be described as one.
