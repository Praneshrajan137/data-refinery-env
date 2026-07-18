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
- Deterministic correction **F1 0.7926**, which **beats the Raha+Baran baseline
  (0.73** from BClean Table 4, arXiv:2311.06517). This is the one measured
  correction win and a hard floor: it must never regress.
- Dominated by FD/typo errors that ARE derivable (FD majority/lookup,
  decimal-shift inverse) -> **AUTO-CORRECTABLE**. `value_format` and
  `text_normalization` are well **DETECTED** (recall ~1.0 / ~0.87).

### flights — the not-inferable-in-table frontier
- Deterministic correction **F1 0.00** vs Raha+Baran 0.729. This is honest, not a
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

### tax — provable FD/rule-violation repair at scale (NOT YET MEASURED)
- 200k rows x 15 cols. Error profile is ~97.9% `numeric` (rate/zip), not
  cross-column FD; the genuine FD-repairable slice is tiny (~800 cells).
- **No floor is seeded** — the full deterministic bench does not finish (schema
  inference is super-linear and does not complete on 200k rows in >8 min), and a
  measured offline probe found spurious inferred FDs (zip->salary etc.) and
  detection precision ~0.03. Seeding a floor here would be fabrication.
- **Measure-first tooling now exists**: `dataforge.datasets.real_world.sample_dataset_rows`
  gives a deterministic, ground-truth-aligned row sample so tax can be benchmarked
  at a tractable size (e.g. 3k rows) and reported honestly as *sampled*. A tax
  correction claim requires (a) exact-FD / denial-constraint mining, (b)
  precision-controlled detection, and (c) a sampled measurement recorded as such —
  in that order. Until then, tax is **UNMEASURED**, not a win.

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
