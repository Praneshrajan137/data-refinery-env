# Accuracy Frontier Map

The honest, per-dataset statement of what DataForge can and cannot do on the RAHA
benchmark suite, and *why*. It exists so accuracy work is chosen by measured
capability, not hope, and so no number is ever published past what was measured.
It is the narrative companion to the machine-checked floors in
`eval/thresholds/coverage_floors.json` (enforced by
`dataforge.bench.check_coverage_regression`).

Every cell error falls into one of four honest classes:

- **AUTO-CORRECTABLE** — the correct value is derivable and provable; the
  deterministic floor fixes it, it passes the differential verifier, and it is
  applied inside a reversible transaction.
- **REVIEWED-SUGGESTION** — the correct value is derivable but not *provable*
  per-cell; DataForge proposes it as a `suggested_fix` with a `review_reason` and
  never auto-applies it.
- **DETECTABLE-ONLY** — the error can be flagged with high precision, but the
  correct value is not derivable in-table; DataForge surfaces it for review.
- **ABSTENTION** — neither the error nor the value is separable from correct data
  using in-table signal; guessing would corrupt, so DataForge stays silent by
  design and reports the gap honestly.

## Per-dataset frontier (measured)

### hospital — the flagship / regression anchor
- Deterministic correction **F1 0.7926** under DataForge's own scoring harness.
  For reference, the Raha+Baran error-correction baseline is **0.73** (transcribed
  from BClean Table 4, arXiv:2311.06517). **Comparability caveat:** these two
  numbers are **not measured under an identical protocol** — DataForge's is
  computed by its own harness on its dirty/clean cut and scoring rules; the 0.73 is
  BClean's reported figure under BClean's protocol (possibly different dirty/clean
  versions, cell set, and denominator). So 0.7926 is best read as *competitive
  with / in the range of the Raha+Baran baseline under our scoring*, not a
  protocol-controlled head-to-head win. It is the one measured deterministic
  correction result and a hard floor: it must never regress.
- Dominated by FD/typo errors that ARE derivable (FD majority/lookup,
  decimal-shift inverse) -> **AUTO-CORRECTABLE**. `value_format` and
  `text_normalization` are well **DETECTED** (recall ~1.0 / ~0.87).

### flights — the not-inferable-in-table frontier
- Deterministic correction **F1 0.00** (DataForge harness); Raha+Baran reports 0.729
  under its own protocol (same comparability caveat as above). This is honest, not a
  bug: the residual errors are `missing_value` fills and `act_dep_time` /
  `act_arr_time` **value** errors (e.g. `9:22 a.m.` -> `9:32 a.m.`) tagged
  `text_normalization` only by string edit-distance. The correct time is not
  derivable in-table (no in-column canonical, no intra-row signal) -> **ABSTENTION**.
- `missing_value` is **DETECTED** (recall 1.00, 2370 cells); a slice of
  `value_format` is time-wrapped-in-date cruft **DETECTED** at ~0.40 (0 false
  positives) via `TimeFormatCruftDetector`. A precision-preserving normalization
  detector was measured to recover 0/1729 here and was correctly not shipped.

### rayyan — datetime/format canonicalization
- `datetime_format` (n=722, all in `article_jcreated_at`) is a Y/M/D transposition
  of the canonical M/D/YY. `DateTranspositionDetector` **DETECTS** all 722 with 0
  false positives (correct cells have day>12 so are invalid as Y/M/D). The left
  rotation reproduces clean exactly (722/722), but per-cell it is **not provable**
  (a valid date is indistinguishable from a transposed one), so it ships as a
  **REVIEWED-SUGGESTION** (`review_reason="unverified_transposition"`), never
  auto-applied (no repairer registered). This is the shipped answer for the
  frontier: schema-directed reviewed repair, not a guess.
- `value_format` (n=107 free-text title/author punctuation) -> **ABSTENTION**.

### tax — provable FD/rule-violation repair at scale (MEASURED, sampled)
- 200k rows x 15 cols. Error profile is ~97.9% `numeric` (rate/zip), not
  cross-column FD; the genuine FD-repairable slice is tiny (~800 cells).
- **Measured** on a deterministic head sample of 3,000 rows (schema inference is
  super-linear, so the full 200k does not finish; sampling makes it tractable and
  is reported as sampled). Artifact:
  [`eval/results/heuristic_tax_sampled.json`](../../eval/results/heuristic_tax_sampled.json),
  reproduce with `python scripts/bench/measure_sampled.py --dataset tax --max-rows 3000`.
  Result: **correction F1 = 0.0000** (tp=0, fp=708, fn=1812); detection recall
  `numeric` 0.14, `value_format` 0.09, `text_normalization` 0.46. The stack makes
  **zero correct corrections and proposes 708 false positives** — driven by
  spurious inferred FDs (e.g. `zip -> salary`). This is *measured-harmful* at the
  propose+score layer, not merely unhelpful.
- **Consequence:** tax is not just unmeasured — it is a **NON-VIABLE auto-apply
  target** by measurement, so **no floor is seeded** (a floor would be fabrication)
  and tax must never be added to the auto-apply path on inferred constraints. A
  real tax correction win would require (a) exact-FD / denial-constraint mining,
  (b) precision-controlled detection, and (c) a sampled measurement clearing a
  precision bar — in that order.
- **Open verification item (flagged honestly):** these 708 false positives are
  measured at the deterministic *propose+score* layer (bench). Whether any reach
  disk in the product depends on the SafetyFilter + differential SMT/Direct gate,
  since they derive from *inferred* (not authoritative) FDs. Confirming the
  provable-only gate rejects inferred-FD-derived false positives on tax-like data
  is a concrete follow-up (the corruption oracle covers clustered-numeric
  decimal-shift, not spurious-FD tables).

## The meta-conclusion (why this map matters)

Three consecutive in-table correction attempts (flights time-cruft, tax residual
FD, rayyan datetime) each measured non-viable for *auto-apply* for one shared
reason: the residual RAHA errors are **semantic value errors**, not syntactic
ones, so they are not inferable from in-table signal without a declared schema or
an external model. DataForge's deterministic in-table correction is therefore at
its **honest frontier**. Further auto-apply accuracy must come from (1)
schema-directed **reviewed** repair (shipped: `date_transposition`) or (2) the
calibrated, conformally-certified LLM path (`docs/trust/` + `dataforge.conformal`)
— never from more in-table detector hunting that guesses. Chasing a headline F1
by auto-applying unprovable values is explicitly out of scope; it would violate
the product's core guarantee.
