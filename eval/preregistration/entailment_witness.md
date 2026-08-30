# Pre-registration: can a per-fix entailment witness predict where a premise does harm?

Written 2026-08-29, **before** implementing or measuring anything. Nothing below is edited
afterwards; results and deviations are appended as amendments.

## The finding this responds to

`PRODUCT.md`:186-190 records the mechanism that determines corruption, and no shipped
surface exposes it:

> **Premise precision does not predict corruption.** Two of the four added dependencies are
> equally false and corrupted **nothing**, because a false dependency is inert where its
> determinant group holds no visible disagreement. So FD-set precision is the wrong single
> quantity to optimise [...] What determines harm is whether a false premise meets a group
> that disagrees.

`docs/trust/shipped-premise-result.md`:46-62 measures it. The premise a zero-config user
actually accepts -- the miner's full output at its 0.90 emission floor, through the real
artifact and merge -- corrupts **116** already-correct hospital cells, and the corruptions
decompose by column as `ProviderNumber` 23, `HospitalOwner` 30, `HospitalName` 23, `State`
20, `Stateavg` 20. Meanwhile `ZipCode -> Address1` and `ZipCode -> PhoneNumber` are
*equally false* and corrupt **nothing**.

So falseness does not predict harm; **meeting a disagreeing group** does.

## The claim under test

Both conjuncts of that harm condition are computable from the user's own table, with **no
ground truth, no solver, and no fitted threshold**:

1. "a determinant group that disagrees" is a groupby over the dirty table;
2. the cells such a group would rewrite are exactly the cells the FD repairer would write.

If that is true, then at the moment a human grants write authority -- the single keystroke
in `dataforge constraints review`, whose artifact writer is *deliberately ungated* because
it changes the authority rather than acting under it -- the reviewer can be shown the
enumerated consequence of the keystroke instead of a statistic about somebody else's table.
Today they are shown hospital's 116 and 0.2046 (`dataforge/cli/constraints.py`:358-378).

This introduces no free parameter, so there is nothing to fit and nothing to overfit. It is
therefore **not** the `tested_confidence` gate that `PRODUCT.md`:213-221 refused: that
refusal stands, and this makes the gate unnecessary rather than overturning the reasoning.

## What is measured

An `entailment_witness_v1` per proposed fix: the entailing constraint, the determinant
group key, the group's value distribution (majority value and support, and the minority
values a write would destroy), and the derived **blast radius** -- for a *candidate*
constraint, before acceptance, the set of cells it would rewrite.

The oracle is the committed measurement above. The witness computes a **prediction** by
groupby alone, without running the detector or the repairer, and the prediction is compared
against measured corruption. This is deliberately cheaper than the arm it validates against:
`shipped-premise-result.md`:99 records ~23 minutes for one arm and hours for three.

## Pre-committed kill criteria

**F1 -- universality.** The witness must be computable for every kind in
`REPAIR_SUPPORTED_CONSTRAINT_KINDS` (`column_type`, `domain_bound`,
`functional_dependency`). If it is not, it ships labelled per-kind and the reviewer preview
is scoped to the kinds it covers. It must **not** ship as a general guarantee over a subset.

**F2 -- the decisive one: does the witness capture the harm mechanism?** Run witness blast
radius over hospital's 85 `shipped_accept_all` dependencies. Required, all four:

| # | prediction | oracle |
| --- | --- | --- |
| F2a | total predicted rewrites on already-correct cells equals the measured corruption count | **116** |
| F2b | per-column decomposition reproduces the measured one | `ProviderNumber` 23, `HospitalOwner` 30, `HospitalName` 23, `State` 20, `Stateavg` 20 |
| F2c | `ZipCode -> ProviderNumber` is attributed 23 and `City -> HospitalOwner` +7 | `shipped-premise-result.md`:52-53 |
| F2d | **`ZipCode -> Address1` and `ZipCode -> PhoneNumber` are attributed ZERO** | `shipped-premise-result.md`:58 |

F2d is the criterion that makes this a test rather than a restatement. Any predictor that
merely flags false dependencies passes F2a-F2c on aggregate and fails F2d. If the witness
cannot separate the two equally-false dependencies that did harm from the two that did none,
it has not captured the mechanism, the reviewer preview is decoration, and **the dependent
work does not proceed on this design.**

**F3 -- verdict preservation.** The witness is evidence *about* a write, never an input to
whether it happens. `scripts/bench/measure_deductive_coverage.py --corpus hospital` must
hold exactly: FD counts **53/81/85**, repairs **393/451/451**, majority corruptions
**0/86/116**, `replication_mismatches` **0** on all arms. Any movement means the witness
changed behaviour, and it reverts.

**F4 -- non-circularity.** A third party holding an attestation must be able to check the
witness by recomputing the stated group distribution from the data. If the only available
check is re-running `verification_strength_for` -- the same function object the engine used
to stamp the field -- then the circularity in `_check_strength` is not broken and the
attestation work in the dependent task is cosmetic. Stated here because it is the criterion
most easily satisfied on paper and not in fact.

**F5 -- cost.** Witness computation must be bounded on `tax` (200,000 rows). Measured by
**counted work**, not wall clock: the same verifier code has measured 42 to 352 ms/fix on
the development machine within one afternoon, so wall clock cannot gate here. Group
enumeration is capped at top-k values plus `group_size` with a `truncated` flag; an
unbounded witness on a large corpus will grow until it breaks something.

## What this does not claim

- **Not that an accepted dependency is correct.** The witness shows consequence, not truth.
  A reviewer who accepts a dependency after seeing 23 destroyed values has made an informed
  choice, not a verified one.
- **Not that the reviewer will decide differently.** Whether the preview changes any real
  acceptance decision is unmeasured until a design partner uses it, and claiming otherwise
  would be claiming a behavioural result from a code change.
- **Nothing about Q1.** `docs/trust/constraint-circularity.md`:32-41 forecloses deciding,
  in-table, whether a violation of a *true* dependency is an error to fix or legitimate
  variation to keep. That remains undecidable and is conceded in full. The witness reports
  what would be overwritten; it does not adjudicate whether overwriting is right.
- **Not generalisable beyond hospital.** flights and rayyan mine no candidates and tax
  mines four true ones, so hospital is the only corpus that can test F2 at all. A pass is
  evidence on one corpus, which is the same limit `shipped-premise-result.md`:129 records.

## Expected outcome, stated in advance

I expect F1, F2a-F2c, F3 and F5 to pass, and I am genuinely unsure about **F2d**. The
per-column oracle sums to 116 exactly, so the aggregate arithmetic is likely to reproduce.
F2d requires the witness to be sensitive to *within-group disagreement* rather than to
dependency falseness, and if my model of the repairer's acting-group selection
(`_applicable_groups` / `_acting_group`) is wrong in any detail, the two inert dependencies
will show non-zero predicted rewrites.

If F2d fails, the honest result is that blast radius over-predicts harm, and the reviewer
preview would show alarming numbers for dependencies that are in fact inert -- which would
make it worse than no preview, because it would train reviewers to dismiss it. In that case
the finding gets published and the dependent tasks stop.
