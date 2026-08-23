# A head slice is not a sample: the tax measurement, measured both ways

Measured 2026-08-23. Artifacts: `eval/results/heuristic_tax_sampled.json` (head, legacy),
`eval/results/heuristic_tax_random_sample.json` (random, seeded),
`eval/results/heuristic_rayyan_full.json` (full).

## The claim, and why it needed testing

`tax` is 200,000 rows and schema inference is super-linear, so it has only ever been
measured on a sample. Until 2026-08-23 `sample_dataset_rows` took `head(max_rows)`, and
`tax` is a **sorted** table. A leading slice of a sorted table is a stratum, not a sample:
its error mix reflects wherever the sort key happens to start.

That was asserted as a defect in an audit. It was not measured, so it could have been a
pedantic objection about a difference of a few percent. It is not.

## Same size, same seed discipline, same scorer, different stratum

| | `head(3000)` | `random(3000)`, seed 20260823 |
| --- | --- | --- |
| true positives | **0** | **10** |
| false positives | **696** | **463** |
| false negatives | 1812 | 1807 |
| correction F1 | 0.0000 | 0.0087 |
| `numeric` detection recall | **0.1363** | **0.0060** |
| `value_format` detection recall | **0.0000** | **0.4500** |
| `text_normalization` detection recall | 0.4545 | 0.7143 |
| `numeric` support in sample | 1790 | 1783 |
| `value_format` support in sample | 11 | 20 |

Three differences worth naming separately, because they point in different directions and
a single summary metric hides all of them:

1. **The head slice found no correct repair at all; the random sample found ten.** The
   headline "F1 0.0000 on tax" was partly an artifact of which 1.5% of the table was read.
2. **The head slice overstated `numeric` detection recall by roughly 23x** (0.1363 against
   0.0060). This is the largest single distortion and it runs in the flattering direction.
3. **The head slice understated `value_format` detection recall from 0.45 to 0.00** --
   reporting a total blind spot where the detector in fact catches nearly half. Its
   `value_format` support was also 11 cells against 20, so the slice contained barely half
   as many of that class.

A biased slice is not uniformly optimistic or uniformly pessimistic. It is *unrelated* to
the population, which is worse, because a consistent bias can at least be reasoned about.

## What changed

`sample_dataset_rows(dataset, max_rows, *, strategy="random", seed=20260823)`. Notes:

- `strategy="random"` is now the default, drawing a seeded uniform sample without
  replacement.
- Ground truth is **re-indexed** with the retained rows. Under `head` the mapping was the
  identity, which is why the previous implementation could omit it; under `random` omitting
  it would attach every label to the wrong row while keeping every count plausible. That
  failure mode produces a confidently wrong precision and no error, so it is asserted
  directly in `tests/unit/test_dataset_sampling.py`.
- `strategy="head"` remains reachable and explicitly named, so
  `heuristic_tax_sampled.json` stays reproducible. Measured history is not rewritten; it
  is re-scoped.
- Artifacts now record `sampling_strategy` and `sampling_seed`, alongside
  `error_provenance` and `tier`. An unrecorded sampling method is how a stratum becomes a
  population in the retelling.

Neither number is promoted. `tax` is `synthetic`/`diagnostic`: the table is generated, not
merely its errors, so no sample of it sources a claim. The point of measuring it twice is
the methodological finding, not the score.

## rayyan, measured in full for the first time

`rayyan` was registered with detection recall floors and **no committed correction
baseline** -- the weakest position available, since it carried the cost of a registered
corpus without the evidence.

Measured at full size (1000 rows, so no sampling question arises):

| | value |
| --- | --- |
| correction P / R / F1 | 0.0000 / 0.0000 / 0.0000 |
| tp / fp / fn | 0 / 7 / 948 |
| `datetime_format` | detection recall **1.000**, correction recall 0.000, support 722 |
| `missing_value` | detection recall **1.000**, correction recall 0.000, support 75 |
| `numeric` | detection recall 1.000, correction recall 0.000, support 2 |
| `value_format` | detection recall 0.000, correction recall 0.000, support 107 |
| `other` | detection recall 0.000, correction recall 0.000, support 42 |

This is the detection/correction split at its starkest and it is the honest shape of the
product, not a failure:

- **Detection recall is 1.000 on 799 of 948 error cells.** The system sees them.
- **Correction recall is 0.000 on all of them, with only 7 false positives.** It proposes
  almost nothing, and what it does propose it gets wrong.

`datetime_format` at 722 cells is the whole story. Recognising that `2011-01-05` and
`Jan 5, 2011` disagree needs only the column; deciding which is canonical needs an
authority the table does not carry. So the correct behaviour is to flag and abstain, which
is exactly what 1.000 detection with 0.000 correction and 7 false positives looks like.

Two consequences:

- `rayyan` stays `diagnostic`. Being the most natural of the four RAHA corpora in
  provenance terms does not earn a tier; a measurement does, and this measurement earns a
  detection-and-abstain description rather than a headline.
- 948 error cells with 7 false positives is a far better precision posture than the 0.0255
  and 0.0113 measured on real wild columns
  (`real-error-detection-result.md`). The difference is that `rayyan`'s columns are
  narrow and curated. Do not read the good number here as transferable.
