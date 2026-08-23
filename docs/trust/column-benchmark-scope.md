# Column benchmark scope: what RT-bench and ST-bench can and cannot measure

Measured 2026-08-23 against Auto-Test revision `4acf65cf37a506206bf2888dbd45f17e58dce2e2`.
Loader: `dataforge/datasets/column_corpus.py`. Scoring: `specs/SPEC_abstention_scoring.md`.

## Why these corpora were adopted

An internal audit concluded that *"there is no public corpus of real, labelled,
cell-level errors at scale."* That conclusion was wrong, and it had been load-bearing:
it was the stated reason for not looking for a replacement benchmark.

`RT-bench` and `ST-bench` (Chen et al., *Auto-Test*, SIGMOD 2025, arXiv:2504.10762) are
2,400 real table columns whose errors were manually inspected and labelled. They are a
direct download, hash-pinnable, and they carry a label class no RAHA-derived benchmark
has: **debatable**.

That third class is the reason for adoption, more than the realness of the errors.
DataForge's product behaviour is to refuse to invent a value it cannot prove. Under a
two-way rule that behaviour is arithmetically identical to failure — the measured
consequence being correction F1 `0.0000` on `flights`, where the upstream sources
disagree about an arrival time (10:30 / 10:31 / 10:28 / 10:39) and the ground truth
picks one. `accuracy-frontier.md` argues that zero is honest abstention. Until these
corpora, that argument was unfalsifiable.

## What was actually measured, and why it is less than the headline

The published description is "two 1200-column benchmarks". Both numbers are true and
both are misleading about the *evidential* weight, so the measured shape is recorded
here in full:

| | RT-bench | ST-bench | total |
| --- | --- | --- | --- |
| columns published | 1200 | 1200 | 2400 |
| columns admitted by the loader | 1200 | 1197 | **2397** |
| columns quarantined | 0 | 3 | 3 |
| columns carrying any unambiguous error | 40 | 46 | **86** |
| columns carrying any debatable value | 30 | 62 | 92 |
| **unambiguous error values (`G`)** | **41** | **47** | **88** |
| debatable values (`D`) | 35 | 77 | 112 |
| distinct values (the negative set) | 93,291 | 73,096 | **166,387** |
| `dist_val_count` disagreements | 6 | 4 | 10 |

**88 labelled error values against 166,387 real distinct values: an error rate of
0.053%.**

## The consequence: this is a false-positive benchmark, not a recall benchmark

This is the single most important thing to know before quoting a number from it.

- **Precision is strongly measured.** 166,387 real values from real tables in the wild
  form the negative set. Every spurious flag is counted. This is a large, genuine,
  hard-to-fake measurement of how often a detector cries wolf on data nobody curated.
- **Recall is weakly measured.** 88 positives is a small sample. A Clopper-Pearson
  interval on 88 draws is wide, and per-detector recall on a slice of those 88 will
  often rest on single digits. Recall numbers from this corpus must be reported with
  their support and their bound, never bare.

That asymmetry happens to be the right way round for this project. The measured failure
that motivated the write gate was **263,428 false money rewrites across three TPC-H
tables with zero true errors** — a precision catastrophe, not a recall one. A corpus
that measures the false-positive rate on 166,387 real values is testing the axis where
this system's known worst behaviour lives.

It is the wrong way round for a coverage claim. Nothing here supports "DataForge detects
N% of real errors".

## Four structural limits

Enumerated in `specs/SPEC_abstention_scoring.md` as L1-L4 and repeated here because they
are properties of the *corpus*, not of the scoring code:

1. **`dist_val` holds distinct values.** A value occurring 900 times counts once. Numbers
   from this corpus are not cell-level precision and must not be presented as such, nor
   compared directly against the cell-level RAHA numbers in `BENCHMARK_REPORT.md`.
2. **`G` contains only unambiguous errors.** Recall here is recall-on-easy-errors, so it
   is an *upper* bound on recall over all real errors.
3. **No clean values ship.** Detection only. Any correction or repair number sourced
   from these corpora would be fabricated, and `ColumnBenchmarkMetadata.axis` is pinned
   to `"detection"` so there is nowhere to record one.
4. **Each row is one column.** Detectors needing row or cross-column context —
   `fd_violation`, `missing_value`, `entity_consensus` — cannot fire and must be reported
   `not_applicable`. That is **not** recall 0. Conflating "cannot apply" with "failed" is
   the same category error as conflating abstention with failure, one level up.

## Upstream defects found, and how each is handled

Found by measurement. Each would silently corrupt a score if handled naively.

| # | Defect | Handling |
| --- | --- | --- |
| 1 | `rt_bench.csv` is an Excel export padded to **1,048,575** rows (2^20 - 1). Rows 1201+ are blank. | Truncate to `declared_columns`, but **raise** if any row past it is non-blank, so an upstream addition cannot silently shrink the corpus. |
| 2 | The published field name is misspelled. `benchmark_readme.md` documents `ground_truth_debateable`; the shipped header is `ground_truth_debatable`. | Required-field check raises and names both spellings. Reading the documented name yields `None` per row, which would silently empty the neutral zone and reinstate the abstention penalty. |
| 3 | Three `st_bench` rows do not parse. Row 1133 holds a leaked spreadsheet formula reference, `['refridgerator'+C1187]`; rows 1083 and 1165 have an empty `dist_val`. | **Quarantined and counted**, never dropped. A silently omitted label row inflates precision and is invisible in the denominator. |
| 4 | UTF-8 BOM; label fields are Python list literals, not JSON. | `utf-8-sig` + `ast.literal_eval`. |
| 5 | Ten rows disagree with their own `dist_val_count`. | Reported as `value_count_mismatches`, not fatal. Discarding 2,397 good columns over a stale count would be the worse error. |

Defect 1 is worth dwelling on. A loader that trusted the row count would score against
1,047,375 empty columns. The aggregate would not crash; it would report a coverage near
zero and a plausible-looking precision, and it would be meaningless.

## No licence, therefore never vendored

`GET /repos/qixuchen/AutoTest/license` returns **404**: the upstream repository publishes
no licence file. There is therefore no grant to redistribute these bytes.

`license_spdx` is `None` in the registry — recorded as absent rather than assumed
permissive — and the corpora are fetched and hash-verified at load time, exactly as RAHA
is. `tests/unit/test_column_corpus.py::test_corpora_are_not_vendored_into_the_repository`
asserts that neither CSV is ever committed. This is enforcement, not intention: a
convenience commit during a future debugging session is the realistic way this gets
violated.

## What adoption does and does not authorise

**Does:** one protocol-controlled comparison. The rule is published, the labels ship with
the corpus, and Auto-Test publishes PR curves on these exact bytes. `PRODUCT.md:129-135`
correctly forbids comparative claims because the existing hospital-vs-BClean comparison
is not protocol-controlled; this one is, and it is the first the project has been able to
make. It must be reported with L1-L4 stated alongside.

**Does not:**

- authorise a SOTA claim, or any comparison beyond the published curves on these bytes;
- authorise a repair or correction number (limit 3);
- authorise a coverage or recall claim about real errors generally (limit 2, plus 88
  positives);
- change any write gate. Detection scoring has no write path, and nothing measured here
  may be used to add a detector to `CONSTRAINT_CHECKABLE_DETECTORS`. That allowlist is
  about whether a proposed value can be *checked*, which a detection benchmark cannot
  answer.
