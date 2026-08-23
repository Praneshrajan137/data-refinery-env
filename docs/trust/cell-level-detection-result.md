# Cell-level detection: precision up to 1.0000, and a 15x corpus swing

Measured 2026-08-23. Artifacts: `eval/results/cell_detection_{rayyan,hospital,flights}.json`.
Reproduce with `python scripts/bench/measure_cell_detection.py`.

Scoring unit is the **cell** -- the unit a review queue is counted in. Not comparable to the
distinct-value figures in `real-error-detection-result.md`; see
`scoring-unit-reconciliation.md` for the measured gap.

## The measurement

| corpus | provenance | detector | applicability | precision | recall | tp | fp |
| --- | --- | --- | --- | --- | --- | --- | --- |
| flights | contested | **MissingValue** | per_value | **1.0000** | 0.4817 | 2370 | **0** |
| flights | contested | **TimeFormatCruft** | per_value | **1.0000** | 0.0256 | 126 | **0** |
| flights | contested | EntityConsensus | row_context | 0.9632 | 0.3404 | 1675 | 64 |
| rayyan | natural | **DateTransposition** | proportion_gated | **1.0000** | 0.7616 | 722 | **0** |
| rayyan | natural | FormatViolation | per_value | 0.2388 | 0.0897 | 85 | 271 |
| rayyan | natural | MissingValue | per_value | **0.0649** | 0.0791 | 75 | 1080 |
| hospital | injected | TypeMismatch | per_value | 1.0000 | 0.1807 | 92 | 0 |
| hospital | injected | EntityConsensus | row_context | 0.8800 | 0.6051 | 308 | 42 |
| hospital | injected | FormatViolation | per_value | 0.2188 | 0.0550 | 28 | 100 |

## Finding 1: I have to revise a verdict I gave

At the end of the previous session I said the honest present-tense product verdict was
**"safe, and currently not useful on unconstrained data"**, on the strength of distinct-value
precision of 0.0113-0.5333 on RT/ST-bench.

The first clause stands. The second was **too broad**, and the qualifier "on unconstrained
data" was doing more work than I acknowledged. At cell level on ordinary tables:

- `MissingValue` finds **2,370 error cells on flights with zero false positives**.
- `DateTransposition` finds **722 on rayyan with zero false positives**.
- `EntityConsensus` runs at 0.88-0.96 precision on two corpora, finding 1,983 error cells.

A detector at precision 1.0000 over 2,370 cells is not "not useful". The corrected verdict:

> **Safe. Useful on tables with recognisable structure. Not yet useful on unconstrained wild
> columns, where measured distinct-value precision is 0.02-0.53.**

The distinction is not rhetorical. RT/ST-bench columns are drawn from real tables in the wild
with no schema, no conventions, and no curation. RAHA tables are ordinary operational tables.
Most users have the second kind. The wild-column case is the hard one and it is right to keep
reporting it, but it is not the only case and I presented it as though it were.

## Finding 2: the same detector swings 15x across corpora, which settles an architectural question

`MissingValueDetector`, same unit, same code:

| corpus | precision | false positives |
| --- | --- | --- |
| flights | **1.0000** | 0 |
| rayyan | **0.0649** | 1,080 |

**A 15x swing driven entirely by the corpus.** On flights the missing values are genuine
blanks that are genuinely errors; on rayyan a great many blanks are legitimately absent
optional bibliographic fields, so flagging them is 1,080 wrong review items.

Nothing observable at runtime distinguishes these two cases. `cli/calibrate.py:440-447`
already says exactly this ("Detector precision is 0.561 on hospital, 0.947 on flights and
0.342 on rayyan, and nothing observable at runtime predicts which case your table
resembles"), and this measurement reproduces it at cell level with a wider spread.

**This is the strongest available argument for per-table certification**, and it is an
argument *for* the architecture rather than against it. Any global claim of the form
"DataForge's missing-value detection is X% precise" is unsound, because X ranges over
[0.0649, 1.0000] depending on a property of the table that no benchmark can transfer.

It also reframes the previous session's conclusion about certification. I called the
sample-size floor "the real adoption barrier" and proposed shifting to reference acquisition.
That framing assumed detector quality was roughly fixed and coverage was the variable. This
says the *table* is the dominant variable, which is what per-table calibration was built for.
Certification is expensive because the problem is genuinely hard, not because the design is
wrong.

## Finding 3: the row_context detectors RT/ST-bench cannot see are among the best

`EntityConsensusDetector` is `row_context`, so the distinct-value harness reports it
`not_evaluable` and it appeared nowhere in the previous result. At cell level it is one of the
strongest detectors in the suite: 0.9632 precision on flights (1,675 true positives) and
0.8800 on hospital (308).

So `not_evaluable` genuinely meant "this corpus cannot answer", not "weak". The four-way
taxonomy predicted that; this confirms it with numbers. Any future summary of detector
capability that draws only on RT/ST-bench will systematically omit the row-context family,
which on this evidence is where a large share of the real capability lives.

## Finding 4: `TimeFormatCruft`'s silence is explained, and it was never a defect

The previous result recorded it as firing "zero times across 166,387 real values" and left
that as an open question after the frequency correction ruled out the mechanism.

Resolved: it fires on **flights at precision 1.0000** (126 true positives, 0 false positives).
Its docstring said it targets "a valid clock time wrapped in extra content ... measured on
flights", and that family simply does not occur in RT/ST-bench. A narrow, high-precision,
per-value detector whose target family is absent from a corpus is behaving correctly. The open
question is closed and no defect existed.

## What still holds from the earlier results

- The **flat risk-coverage frontier** on RT/ST-bench, and that nothing certifies at
  `alpha = 0.05` on that evidence. Both were measured on per-value detectors and neither is
  affected by the correction or by this document.
- **Wild-column precision of 0.02-0.53.** Unchanged, and still the hard case.
- **`Outlier` and `DecimalShift` have no positive result anywhere.** Now confirmed at cell
  level, where they *are* evaluable: rayyan fp=118 and fp=7 with zero true positives, hospital
  fp=99 and fp=4 with zero true positives, flights fp=9 with zero. This is the valid version
  of the claim the correction withdrew -- and it reaches the same conclusion by a legitimate
  route. Their dispositions stand.
- **`CategoricalNormalization`**: fp=25, tp=0 on rayyan; silent on the others. Its only valid
  measurement is weak. Not retracted this time, because the corpus supports the measurement.

## Limits, carried on every artifact

1. **Two-way scoring.** RAHA ships no `ground_truth_debatable` class, so the ambiguous cells
   were resolved by whoever built the corpus and the resolution is unrecorded. Every number
   here inherits the identification problem that `SPEC_abstention_scoring.md` removes. Cell
   level buys the right unit at the cost of the neutral zone; neither harness dominates the
   other, which is why both now exist.
2. **Not comparable to the distinct-value figures**, in either direction, with a measured gap
   up to total.
3. **Detection, not correction.** A detector at precision 1.0000 is finding error cells, not
   producing correct replacements. `DateTransposition`, `TimeFormatCruft` and `Outlier` have
   no repairer at all. `rayyan`'s correction F1 remains 0.0000 while its best detector runs at
   perfect precision, and both facts are true at once.
4. **hospital is injected** (one substituted character) and **flights labels are contested**.
   Its 1.0000 for `TypeMismatch` is the 92 `x`-reversals, and that is a benchmark artifact
   whatever unit it is measured in.
