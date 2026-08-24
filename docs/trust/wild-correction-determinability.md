# 59% of wild-column errors are correctable, and the split is almost perfectly bimodal

Labelled 2026-08-24. Labels: `dataforge/datasets/wild_correction_labels.json`.
Pre-registration: `eval/preregistration/wild_correction_determinability.md`.
Loader: `dataforge/datasets/wild_corrections.py`.
Reproduce the label file with `python scripts/data/build_wild_correction_labels.py`.

No API spend. This is corpus work.

## The question

`RT-bench` and `ST-bench` are the only corpus of real, labelled, cell-level errors at scale this
project has. They ship **no clean values**, so `dataforge/bench/core.py` cannot score a correction
against them and `docs/trust/real-error-detection-result.md` reports detection only.

Before building anything to correct wild-column errors, a prior question: **given a real error and
the rest of its column, is the correct value determinable at all?** If most wild errors have no
recoverable correction, wild-column repair is not a hard problem but an ill-posed one.

## Result

A census of all 88 unambiguous errors, one annotator, rules fixed before any value was seen.

| label | count | share |
| --- | --- | --- |
| `correctable` | **52** | **0.5909** |
| `not_determinable` | 35 | 0.3977 |
| `ambiguous` | 1 | 0.0114 |

Per corpus: RT-bench 27 of 41 correctable, ST-bench 25 of 47.

## The prediction was wrong

> Directional prediction, recorded so it can be wrong: **fewer than half will be `correctable`.**
> Wild columns have no schema, no declared domain and no sibling rows to consult, so the
> information needed to reconstruct a correct value is usually absent.

**Wrong.** 59% are correctable. The reasoning underestimated how much a column's *other values*
constrain a correction: a misspelling sits one edit from a term whose correct form is evident from
the header and the surrounding values, and that turns out to be the single most common kind of real
error in wild columns.

The second half of the prediction held: "format and cruft cases will dominate the correctable set
and semantic errors the rest." Format and cruft are only 5 of 52; typos dominate instead. So the
mechanism was wrong even where the direction of the sub-claim was roughly right.

## The split is almost perfectly bimodal, by rule

| rule | meaning | count |
| --- | --- | --- |
| **R3** | unique typo neighbour in the column's evident domain | **47** |
| R1 | right information, wrong form | 4 |
| R2 | correct value present plus removable cruft | 1 |
| **N1** | the correct value is a different fact, absent from the cell | **32** |
| N2 | two or more corrections equally plausible | 2 |
| N3 | needs an external authority such as a survey codebook | 1 |

**R3 accounts for 47 of 52 correctable (90%) and N1 for 32 of 35 not-determinable (91%).** Two
mechanisms explain 79 of 88 errors, and they are cleanly separable:

- **Correctable ≈ a misspelling of a recognisable domain term.** Misspelt month names, countries,
  provinces, colours, job titles, city names, medical terms.
- **Not determinable ≈ a sentinel or a wrong-field value.** A dash, a zero, a question mark, a
  not-applicable token, or a value belonging to a different column entirely -- an employment
  status in a country column, a country in a US-state-code column, a business function in a city
  column.

A sentinel is undeterminable for a structural reason worth stating plainly: **the erroneous cell
contains no information about the correct value.** No amount of model capability recovers a date
from a dash. This is the `flights` arrival-time finding generalised, and it now has a measured
share: **32 of 88 wild errors, 36%, are information-free.**

## What this implies for the corrector axis

**Wild-column correction is well-posed for a majority of errors**, which is a more encouraging
answer than expected and justifies building toward it.

But the composition matters more than the headline. The correctable majority is dominated by a
single mechanism -- unique-nearest-neighbour spelling repair against a domain inferred from the
column -- which is a narrow, tractable capability and **not** what a general-purpose corrector is
usually built for. The 36% that are information-free need **abstention**, not correction, and a
corrector that attempts them will be wrong every time.

So the architecture this suggests is a detector that separates sentinels from typos and routes
only the second class to a corrector. That separation is mechanical: a sentinel is drawn from a
small closed vocabulary, and this project already has a `MissingValueDetector` whose placeholder
list covers most of the 32. Note the tension with `docs/trust/cell-level-detection-result.md`,
where that same detector scores 0.0649 on rayyan: **detecting sentinels is easy; deciding whether
a sentinel is an error is not**, because in rayyan most absent optional fields are legitimate.

## What this cannot support

1. **No certification, and this is not a shortfall to be fixed later.** One annotator, no second
   opinion, so `beta` in `dataforge/conformal.py` cannot be bounded -- that needs planted controls
   and independent judgements per `eval/preregistration/human_label_noise.md`. Nothing here may
   certify an auto-apply threshold.
2. **No inter-annotator agreement.** The single `ambiguous` label is a lower bound on label noise,
   not an estimate of it. A 1.1% ambiguous rate should be read as "the annotator rarely felt
   blocked", not as "the labels are 98.9% reliable".
3. **A recorded conflict of interest.** The annotator also wrote the detectors and repairers, and
   the temptation is to call a value correctable because a repairer could fire on it. The rules
   are phrased in terms of column evidence rather than any repairer, and R3 requires *uniqueness*
   rather than plausibility. That is a mitigation, not a solution. An independent annotator is the
   only real fix and was not available.
4. **88 items.** The 0.5909 share carries roughly +/- 10 points at 95% confidence. It does not
   support a breakdown by error class with any precision.
5. **Determinable is not correctable by this project.** A value a careful human can repair may be
   beyond every repairer here. This measures whether the task is well-posed, not whether the tool
   solves it.
6. **No corrections are recorded, deliberately.** A label says whether a correction is
   recoverable, never what it is. Storing corrections would require storing the erroneous values,
   which the licence forbids.

## The licence shaped the artifact

Upstream publishes no licence, so `dataforge/datasets/registry.py` records `license_spdx=None` and
the bytes may live only in a fetched, hash-verified cache. **A label file containing the 88 error
values would be vendoring unlicensed corpus content.**

Labels are therefore keyed on `corpus:column_index:sha256(value)[:16]`, notes describe values
abstractly rather than quoting them, and the loader fails closed if the file does not assert
`contains_corpus_values: false`. `tests/unit/test_wild_corrections.py` enforces all three, and the
column index is part of the key because sentinels recur across columns -- the same dash is a
labelled error in five different columns, and a dash undeterminable in a date column could be
correctable elsewhere.

The cost is auditability: a reader cannot check a label without fetching the corpus. That cost is
accepted, because the alternative is redistributing bytes there is no grant to redistribute.

## What this does and does not authorise

**Authorises** building toward wild-column correction for the R3 class, and treating the N1 class
as an abstention target rather than a repair target.

**Does not authorise** any correction *metric* on these corpora. There are still no clean values,
and a repair number sourced here would be fabricated.

**Does not authorise** a claim that 59% of wild errors are correctable *by DataForge*. It is 59%
correctable by a careful human reading the column.

**Changes no write gate.** No label here may move a detector onto `CONSTRAINT_CHECKABLE_DETECTORS`.
