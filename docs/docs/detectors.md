# Detectors

Detectors are pure pandas-based scanners that emit typed `Issue` records. Each
issue includes row, column, issue type, severity, confidence, reason, and an
optional expected value.

## Shipped detector families

Eleven issue families ship. **Detection and write authority are separate**: only two detectors may
auto-apply, and only from a declared functional dependency. Named explicitly on one line, because a
detector that gains write authority silently is worse than one that loses it noisily:
`fd_violation` and `missing_value` may auto-apply, and no other family may.
The rest surface issues for review and their repairs are calibration-bound. "May auto-apply" below
means membership of
`CONSTRAINT_CHECKABLE_DETECTORS`, which
[dataforge/domain/vocabulary.py](https://github.com/Praneshrajan137/dataforge/blob/main/dataforge/domain/vocabulary.py)
treats as an allowlist a detector must earn with a committed measurement.

The count is the size of the closed `IssueTypeLiteral` vocabulary and is checked against it by
`scripts/ci/readme_truth.py`, so a new family cannot ship undocumented. Ten families come from the
eleven detectors in the default ensemble — `time_format_cruft` and `format_violation` both emit
`format_violation` — and `semantic_domain_violation` is the eleventh, opt-in.

| Detector | Finds | May auto-apply | Repair, when permitted |
| --- | --- | --- | --- |
| `fd_violation` | Functional dependency conflicts from schema metadata | **yes**, declared FD only | Align the dependent value when a strict majority exists |
| `missing_value` | Empty or sentinel cells in a populated column | **yes**, declared FD only | Fill from a unanimous determinant group |
| `type_mismatch` | Values that do not match the dominant column type | no | Would blank a sentinel in a numeric column; removed from the allowlist 2026-08-25 after 156 flags and zero proposals across three corpora |
| `decimal_shift` | Numeric values that appear off by powers of ten | no | Removed on measurement: precision 0.0000 on three datasets, 263,428 false rewrites on an error-free table |
| `format_violation` | Values whose shape departs from the column's dominant shape | no | Detection-only, and now on measurement: its repairer produced 10,356 writes, repaired 0 real errors and corrupted 10,356 clean cells across four corpora |
| `categorical_normalization` | Case and spacing variants of one category | no | Detection-only. Its repairer passes both removal criteria yet stays withheld: 38% of its writes are harmful, because a majority vote over the column's own values tracks the corpus's corruption rate rather than the truth |
| `outlier` | Numeric values far from the column's distribution | no | Detection-only |
| `duplicate_row` | Repeated rows | no | Detection-only |
| `date_transposition` | Dates whose day and month appear swapped | no | Detection-only |
| `entity_consensus` | Cells that disagree with other rows describing the same entity | no | Detection-only |
| `semantic_domain_violation` | Values outside an externally learned column domain | no | Detection-only, and structurally so: no repairer exists, so it has no write path on any surface. Opt-in — construct the detector explicitly with a fetched, hash-verified constraint artifact |

Per-detector write measurements, including the clean cells each would overwrite, are in
`docs/trust/bypass-allowlist-evidence.md` and, for the two withheld repairers, in
`docs/trust/withheld-repairer-result.md`. Three of the families in this table carry no committed
write measurement because they have never proposed a write; that is an absence of evidence, not
evidence of safety.

## Contract

Detectors do not mutate data. Repairers consume detector output and may propose
fixes, but the write path remains gated by safety, SMT verification, and the
transaction journal.
