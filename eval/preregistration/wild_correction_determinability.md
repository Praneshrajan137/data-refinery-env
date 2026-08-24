# Pre-registration: are wild-column errors correctable at all?

Registered 2026-08-24, **before inspecting any of the 88 values to be labelled**.
Implementation: `dataforge/datasets/wild_corrections.py`.
Labels: `dataforge/datasets/wild_correction_labels.json`.

## The question, and why nothing answers it today

`RT-bench` and `ST-bench` are the only corpus of real, labelled, cell-level errors at scale that
this project has found. They ship **no clean values**, so
`docs/trust/real-error-detection-result.md` can report detection and nothing else, and
`dataforge/bench/core.py` cannot score a correction without a `clean_value` to compare against.

Before building any machinery to correct wild-column errors, there is a prior question:

> **Given a real error found in a column of a table in the wild, and the rest of that column, is
> the correct value determinable at all?**

If most wild errors have no recoverable correct value, then wild-column *correction* is not a
hard problem, it is an **ill-posed** one, and effort should go to detection and abstention rather
than to repair. That would be a more valuable finding than a corrector.

The precedent that motivates the question is `flights`, where the same arrival time appears
upstream as 10:30, 10:31, 10:28 and 10:39 and the ground truth picks one. That corpus produces
correction F1 0.0000 not because a corrector is bad but because the task has no determinate
answer. `dataforge/datasets/registry.py` records this and records that it **cannot** be fixed by
rescoring.

## Hypothesis

A minority of wild-column errors are correctable from the value plus its column.

Directional prediction, recorded so it can be wrong: **fewer than half will be `correctable`.**
Wild columns have no schema, no declared domain and no sibling rows to consult, so the
information needed to reconstruct a correct value is usually absent. I expect format and cruft
cases to dominate the correctable set and semantic errors to dominate the rest.

## The label taxonomy, and the rules that decide it

Three classes. The rules are written **before** the values are seen, so a label is a rule
application rather than an impression.

### `correctable` -- a unique replacement is determined

A value is `correctable` only if exactly one specific replacement string is determined by one of:

| rule | condition |
| --- | --- |
| R1 format | The value carries the right information in the wrong form, and the column's dominant form is unambiguous. Normalising to that form yields one answer. |
| R2 cruft | The value contains the correct value plus extraneous content, and removing the extraneous part is unambiguous. |
| R3 unique typo neighbour | The value differs by a small edit from exactly **one** value in the column's evident domain, and no other in-domain value is equally close. |
| R4 placeholder | The value is a missing-value sentinel in a column whose convention for absence is visible. |

### `not_determinable` -- an error is present, the correction is not recoverable

| rule | condition |
| --- | --- |
| N1 absent fact | The correct value is a different fact that does not appear in the cell. The `flights` arrival-time case. |
| N2 multiple candidates | Two or more distinct corrections are equally plausible from the available evidence. |
| N3 external authority | The correction requires a lookup table, a real-world fact, or a convention not present in the column. |

### `ambiguous` -- the rules do not decide

Used when a `correctable` rule and a `not_determinable` rule both apply, or when the nature of
the error itself cannot be determined from the value and column. **Uncertainty resolves here, not
to a guess**, and the `ambiguous` rate is reported as a measure of the annotator's own limits
rather than hidden.

## Fixed procedure

| parameter | value |
| --- | --- |
| items | all 88 `ground_truth` values (41 RT-bench, 47 ST-bench). A census. |
| annotators | **one** (the maintainer) |
| evidence per item | the value, its column header, and the column's other distinct values |
| output per item | label, rule id, and a note that **must not quote the value** |
| storage | keyed by `corpus:column_index:sha256(value)[:16]` |

## The licence constraint shapes the artifact

Upstream publishes no licence, so `dataforge/datasets/registry.py` records `license_spdx=None` and
the bytes may live only in a fetched, hash-verified cache. **A label file containing the 88 error
values verbatim would be vendoring corpus content**, which the project forbids.

Labels are therefore keyed on a **truncated SHA-256 of the value**, never the value itself, and
notes describe values abstractly ("a comma-grouped integer in an integer column") rather than
quoting them. The loader re-fetches the corpus, verifies its digest, and joins on the hash.

This costs auditability -- a reader cannot check a label without fetching the corpus -- and that
cost is accepted because the alternative is redistributing bytes we have no grant to redistribute.

## Committed in advance

- The taxonomy and rules above. **Adding a rule to make a specific value correctable is
  forbidden.** A value the four `correctable` rules do not cover is not correctable.
- The directional prediction that fewer than half are correctable. If most turn out correctable,
  that is recorded as the prediction being wrong.
- A high `not_determinable` rate is a **publishable finding**, not a failed labelling effort.
- No label may quote a corpus value.

## What this cannot support, stated before it is built

1. **No certification.** One annotator, no second opinion, so the label-noise term `beta` in
   `dataforge/conformal.py` cannot be bounded -- that requires planted controls and independent
   judgements, per `eval/preregistration/human_label_noise.md`. `min_samples_under_label_noise`
   is therefore not applicable, and nothing measured here may certify an auto-apply threshold.
2. **No inter-annotator agreement**, so the labels carry unmeasured noise. The `ambiguous` rate is
   a lower bound on that noise, not an estimate of it.
3. **A conflict of interest, recorded.** The annotator also wrote the detectors and repairers. The
   temptation is to label a value `correctable` because a repairer *could* fire on it. The rules
   are phrased in terms of the evidence available in the column, not in terms of any repairer, and
   R3 in particular requires uniqueness rather than mere plausibility. This is a mitigation, not a
   solution: an independent annotator is the only real fix and is not available.
4. **88 items.** Enough to estimate a proportion to roughly +/- 10 points, not enough to break it
   down by error class and retain any precision.
5. **Determinable is not the same as correctable by this project.** A value whose correction is
   determinable by a careful human may still be beyond every repairer here. This measures whether
   the task is well-posed, not whether the tool solves it.
