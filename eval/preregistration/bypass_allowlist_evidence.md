# Pre-registration: does the calibration-bypass allowlist meet its own standard?

Written 2026-08-25, **before** running the measurement. Nothing below is edited afterwards; results
and deviations are appended as amendments.

## The gap

`dataforge/domain/vocabulary.py`:181-184 states the rule the allowlist sets for itself:

> **This is an allowlist, and that is deliberate.** [...] A new detector is calibration-bound until
> it earns an entry here **with a committed measurement**.

A `deterministic` fix from a member of `CONSTRAINT_CHECKABLE_DETECTORS` bypasses the calibration
threshold entirely in `partition_auto_apply` -- no threshold, no confidence, no labels, nothing
downstream. Against that standard, as of today:

| member | committed unconditional write measurement |
| --- | --- |
| `fd_violation` | **Yes.** `docs/trust/deductive-coverage-result.md`, 2026-08-25 |
| `missing_value` | **None.** Detection recall 1.000 with correction recall 0.000; queue precision 0.000. No write number exists |
| `type_mismatch` | **None.** The 92/92 sometimes cited is the **LLM corrector**, a different component, formally retired by `real-error-detection-result.md`:125-141 |

Nine trust documents disclaim authority to *add* a member. None records what did. `decimal_shift` was
*removed* on measurement -- precision 0.0000 on three datasets, 263,428 false rewrites on an
error-free table -- so the standard is enforced on exit and not on entry.

## Why `type_mismatch` is the sharp case

Three properties, read from `dataforge/repairers/type_mismatch.py`, each of which the repository's own
reasoning already treats as disqualifying:

1. **Its trigger is a distributional inference.** `_is_predominantly_numeric` applies a hardcoded
   `>= 0.65` to the column's own values (`:30`). `vocabulary.py`:176-179 defines membership as
   "checkable against a reference [...] rather than **inferred from the shape of the column's own
   distribution**", and names the latter as requiring a calibrated threshold.
2. **It has no premise to grade.** `del schema` on `:44`. Decision-table row 6 writes `'N/A' -> ''`
   with `schema_text=None`. The premise-power rule shipped in
   `eval/preregistration/entailment_strength.md` -- which took the adversarial corpus from 10 of 14
   to 0 of 14 -- **cannot reach this path**, because there is no premise to be weak.
3. **It erases rather than copies.** `new_value=""` (`:64`), always `provenance="deterministic"`
   (`:72`). `fd_violation` and `missing_value` copy a value that exists elsewhere in the table; this
   one destroys the value present.

That is `decimal_shift`'s profile.

## Method, fixed now

Extend the harness in `scripts/bench/measure_deductive_coverage.py`. For each detector in
`CONSTRAINT_CHECKABLE_DETECTORS`, on each of hospital, rayyan and flights:

* run the **real detector** to produce the queue, then the **real repairer** on each flag -- no
  reimplementation of either;
* classify every proposal **unconditionally** against retained ground truth into
  `repaired_a_real_error` / `wrong_value_on_a_real_error` / `corrupted_a_clean_cell` /
  `no_op_on_a_clean_cell`;
* account per **distinct cell**, never per detector flag;
* report `write_precision` = repaired / proposals, and `harmful_write_rate` = (wrong + corrupted) /
  proposals.

Premise arms, per detector, chosen to match how each one actually runs:

| detector | arms |
| --- | --- |
| `type_mismatch` | **no premise** only. That is its shipped configuration and the widest one; row 7 shows a premise can only subtract writes |
| `missing_value` | `oracle` and `mined`, as for `fd_violation`: it requires a declared FD and reads the same dependency set |
| `fd_violation` | unchanged, as already committed |

**Regression guard on the refactor:** the committed `fd_violation` figures must reproduce exactly --
393/393/0/0 on hospital-oracle, 537/451/0/86 on hospital-mined, 1807/1193/270/344 on
flights-oracle-majority. Any drift means the harness moved, not the product.

## Predictions

| # | Quantity | Prediction |
| --- | --- | --- |
| **P1** | `type_mismatch`, `wrong_value_on_a_real_error` | **> 0 on at least one corpus.** It writes `""` and never a real value, so any cell whose dirty value is a missing sentinel in a mostly-numeric column and whose truth is a non-empty value receives a wrong value by construction. This **contradicts** `docs/trust/repair-failure-decomposition.md`:35-37, which states the quantity is zero on all three corpora for the pooled deterministic repairers. |
| **P2** | `type_mismatch`, `write_precision` | Low, and plausibly 0.0000 on at least one corpus. Stated as a direction rather than a number because the sentinel-in-numeric-column population size is unknown to me before running. |
| **P3** | `missing_value` | Fires on few cells. Its `_lookup` requires the determinant group to contain exactly **one** distinct non-missing dependent value, which is strictly stricter than `fd_violation`'s majority. Where it fires, precision should be higher than `fd_violation`'s and coverage far lower. |
| **P4** | `missing_value`, mined arm | Corrupts more than its oracle arm, for the same reason `fd_violation` does: a false dependency makes the detector flag clean cells. |
| **P5** | At least one member | Fires on **zero** cells on at least one corpus. rayyan already yields no dependencies at all, so both FD-driven repairers should be silent there. |

**Uncertainty stated plainly.** I do not know the outcome of P1 or P2. P1 is a structural argument, so
if it fails I have misread either the repairer or the corpora, and that is itself worth knowing. A
confirmed zero would weaken the case for removal, and I would report it as such.

## Kill criterion, fixed now

Applied per detector, after the measurement and before any code change:

**A member is REMOVED from `CONSTRAINT_CHECKABLE_DETECTORS` if either:**

* **(K1)** `write_precision` is **0.0000** on every corpus where it proposes at all -- the
  `decimal_shift` criterion, applied unchanged; or
* **(K2)** `corrupted_a_clean_cell + wrong_value_on_a_real_error` **exceeds**
  `repaired_a_real_error`, summed across corpora -- i.e. the detector damages more cells than it
  fixes, so bypassing calibration is net negative and not merely imprecise.

**A member that proposes on zero cells across all three corpora is marked `unmeasured` in the
allowlist comment and remains calibration-bound in effect**, rather than being quietly retained as
though measured. Silence is not evidence.

**If neither criterion fires**, the measurement is published as the membership justification the
allowlist has never had. That is the outcome I expect for `fd_violation` (1193 repaired against 614
harmful on flights) and it is a real improvement with no code change.

## What is deliberately not being decided here

**No new threshold is invented.** K1 and K2 are the criteria already applied to `decimal_shift` and a
sign test. Introducing a tunable precision floor would make membership depend on a number nobody
measured, which is the defect under audit.

**The 0.65 in `_is_predominantly_numeric` is not tuned.** If `type_mismatch` fails, the response is
removal from the bypass allowlist -- which leaves the detector running and its repairs
calibration-bound -- not a search for a threshold that makes the numbers acceptable.

## Consequence to be accepted, not avoided

If `type_mismatch` is removed, decision-table row 6 stops writing and the **schema-free auto-apply
path may become empty**. `docs/trust/deterministic-is-not-sound.md`:189 already claims it is empty
(falsely -- row 6 occupies it), so removal would make that sentence true for the first time. The cost
is the only zero-configuration write in the product.

`tests/integration/test_autoapply_decision_table.py` must still contain at least one write row
afterwards. **If it contains none, that is published as a finding, not resolved by relaxing the
test.** The pre-registered response to "the product now writes nothing without a declared premise" is
to say so.

## Reconciliation obligation

`docs/trust/repair-failure-decomposition.md` measures the pooled deterministic repairers: hospital
(injected) 594 writes / 143 overwrote a clean cell / damage rate 0.2407; rayyan 7/7; flights 9/9; and
`wrong_value_on_a_real_error` **zero on all three**. Its own limits section says "deterministic
repairers only" with no per-detector split.

If the per-detector numbers disagree with that artifact -- and P1 predicts they will -- the
disagreement must be **explained in writing**, not averaged away. The candidate explanations, listed
now so the choice is not made to suit the result: a different hospital corpus variant (injected
versus RAHA), a different scoring unit, a different premise, or a defect in one of the two
measurements. Whichever it is, one of the two documents is then wrong and must be corrected in place.
