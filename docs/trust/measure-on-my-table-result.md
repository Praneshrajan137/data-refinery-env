# Measuring on a table with no clean copy

**Date:** 2026-08-30
**Instrument:** `dataforge measure-on-my-table`
**Code:** `dataforge/measure_on_my_table.py`, `dataforge/cli/measure.py`
**Validation:** `scripts/bench/validate_measure_on_my_table.py`
**Data:** `eval/results/measure_on_my_table_validation.json`

## What this closes, and what it does not

Every published number in this project comes from four public corpora that retain a clean column
beside the dirty one. A customer table has no clean column, so none of the harnesses can run on
it: `classify_writes`, `_write_exposure` and `fd_holds_on_clean` all take `dataset.clean_df`.
`docs/trust/design-partner-instrumentation.md` ends by stating the consequence plainly — no real
customer table has ever been tested, and this project cannot say what it would do to one.

This builds the instrument that would test one. It does not test one. **No real external table
has been measured, and `design_partner_evidence` in `dataforge release full-vision` remains
unsatisfied.** That check requires a table this repository is not allowed to see, held by a party
under signed consent, and it cannot be manufactured here by any amount of engineering. What
changed is that the blocker is now the consent, not the tooling.

## The mechanism

Planted controls, repointed from the labeller to the write path. Pick a cell **no detector
flagged**, so its current value `V` is the best available truth. Corrupt it into `V'` ourselves.
Because we performed the corruption, `V` is known by construction. Then compute what the write
path would do to the planted table, and compare.

The corruption mechanic is imported from `calibration_session._corrupt_like_the_table` rather
than rewritten. `PRODUCT.md`:176-185 records a reimplemented measurement reporting 959 writes
where the truth was 74.

The instrument reads. It never writes, creates no transaction, and needs no write permission —
asserted on the CLI path in `tests/integration/test_measure_on_my_table_cli.py`.

## Validation on the four corpora, and two claims it falsified

The instrument cannot be checked where it is used, so it was run on the four corpora that do
retain ground truth, with ground truth used only to grade it and never to feed it.

| corpus | rows | mined FDs | cells written | planted precision | precision on REAL errors |
|---|---|---|---|---|---|
| hospital | 1000 | 85 | 687 | 1.0000 | 0.7954 (451 repairs / 567) |
| tax | 200000 | 4 | 670 | 1.0000 | 0.9378 (603 repairs / 643) |
| flights | 2376 | 0 | 0 | not measurable | not measurable |
| rayyan | 1000 | 0 | 0 | not measurable | not measurable |

Hospital reproduces the K4 oracle fence exactly — 451 repairs, 116 corruptions — which is the
evidence that the instrument is wired to the shipped write path rather than to a proxy for it.

**Falsified claim 1: the bias direction.** The module originally argued that scoring against a
cell's pre-corruption value *understates* precision, since a repair to the genuinely correct
value of an unflagged-but-wrong cell is charged as a failure. That mechanism is real, and it is
not the net direction. Measured, `planted_write_precision` reads 1.0000 against real-error
precision of 0.7954 and 0.9378. It **overstates**, and structurally: a plant is a single-cell
perturbation dropped into an otherwise-consistent determinant group, so it is a minority of one
and the strict-majority rule restores the exact original. That is the easiest case an FD repair
can be handed. Real errors arrive correlated, several to a group, and sometimes in the majority.

The figure is therefore published as an **upper bound on precision** and never as an estimate of
it. Two biases of opposite sign and unequal, unmeasured magnitude do not net out into a safe
number.

**Falsified claim 2: the headline is not damage.** `wrote_to_a_cell_we_did_not_plant` was
introduced as the figure resting on no assumption, since a write to a cell we did not corrupt is
observable without any oracle. That much is true. What is false is reading it as damage. On
hospital, 567 such writes decompose into 451 repairs of real pre-existing errors and 116
corruptions of genuinely clean cells — reporting 567 as damage overstates it **4.9x**. On a table
with no clean copy those two cannot be separated, so the figure is published as a **ceiling on
damage**. Both corrections travel inside the report's own `limitations`, not only in this
document, and both are asserted by tests.

## The coverage limit

**On two of the four corpora the instrument measures nothing at all.** flights and rayyan mine
zero functional dependencies at the 0.90 emission floor, so nothing is written and there is no
denominator for anything. This is reported as *no measurement*, never as *no problem*: the CLI
exits non-zero with an explanation rather than printing a zero-write report, because a report of
zero writes reads as a clean bill of health for a table nothing was ever checked against.

A design partner whose table mines no dependencies gets no signal from this instrument. Half the
available evidence base is outside its reach, and there is no reason to expect real tables to be
distributed like hospital rather than like rayyan.

## What is not measurable, stated rather than omitted

An absent metric reads as a zero, so each is named in the report:

- **Recall on real errors.** No clean copy means the number of real errors is unknown and no
  denominator exists.
- **FD-set precision.** Whether a mined dependency is true cannot be decided without ground
  truth. What *is* measurable is its consequence, which is what the write counts are.
- **`cells_reviewed_per_true_error`**, for the same missing denominator.

## Privacy is the deliverable

A value-leak in a report a customer has already sent cannot be fixed afterwards — the data has
moved, and the partner discovers it, not us. So the guarantee is structural first: every field of
`MeasuredOnMyTable` is an integer, a float or a digest, `extra="forbid"` is set, and writes are
keyed by **column index** rather than column name, because a name like `hiv_status` discloses
without a single row. A cell value cannot appear by construction, not by filtering.

`assert_no_plaintext_values` is a second, independent scan over the emitted bytes, run on every
corpus in validation and not only in tests. It exists because the structural argument is true of
today's fields and one future field makes it false.

**The scan found a false positive in itself.** Its first version scanned the whole report,
including the fixed prose in `limitations`. That prose named the corpora mining zero dependencies
— and the flights corpus contains a cell whose value is the literal string `flights`, so a
correct report was refused. Scanning our own commentary for the user's values is a category
error: the scan's subject is content *derived from the table*. The two prose keys are now
excluded, and the exclusion is not a loophole — the emitted prose must equal the module constants
or the function refuses outright, so nothing can be smuggled through the exempt keys.

Three tests, because any one alone is satisfiable by a no-op: the field-type test (structural),
the sentinel test (a recognisable string planted in a cell the repair *does* rewrite, asserted
absent from the report bytes), and a test that the scan fires when a value **is** present.
Mutant `M24-egress-scan-never-refuses` removes the refusal and is killed.

## Honest status

- Instrument: built, validated on four corpora, 25 tests, one mutant, one registry entry in
  `_WRITE_PRIMITIVE_REGISTRY` naming the egress scan as its gate.
- Every figure it prints: bound to `eval/results/measure_on_my_table_validation.json` through
  `docs/quantitative_claims.yaml`, so the numbers in its own warning text cannot drift.
- Two of its own claims: falsified by that validation and corrected in code, tests and here.
- Real external table: **none**. `design_partner_evidence` stays red.
