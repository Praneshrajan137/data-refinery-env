# SPEC: Detectors

> Status: Draft
> Owner: @pranesh
> Last updated: 2026-04-20

## 1. Purpose (2 sentences)

Pure, LLM-free detection of data-quality issues in tabular data. Each detector
examines a pandas DataFrame (optionally with a declared schema) and returns a
list of typed `Issue` objects describing anomalies, their location, severity,
and human-readable explanations.

## 2. Outcomes (measurable, binary pass/fail)

- [x] `Issue` Pydantic model validates all required fields and rejects invalid ones.
- [x] `Severity` enum has exactly 3 members: SAFE, REVIEW, UNSAFE.
- [x] `Detector` protocol is implementable by any class with `detect(df, schema) -> list[Issue]`.
- [x] `TypeMismatchDetector.detect()` finds numeric-in-string and string-in-numeric anomalies.
- [x] `DecimalShiftDetector.detect()` flags values that are 10x/100x/0.1x the column distribution.
- [x] `FDViolationDetector.detect()` flags rows violating declared functional dependencies.
- [x] `run_all_detectors()` returns a merged, sorted list of issues from all detectors.
- [x] All toy cases in Appendix A pass.

## 3. Scope

**IN**:
- `Issue` model with row, column, issue_type, severity, confidence, expected, actual, reason
- `Severity` enum: SAFE, REVIEW, UNSAFE
- `Schema` model: column types, functional dependencies, constraints
- `Detector` protocol with `detect(df, schema) -> list[Issue]`
- `TypeMismatchDetector`: numeric/string/date type mismatches
- `DecimalShiftDetector`: power-of-10 outliers in numeric columns
- `FDViolationDetector`: rows violating declared FDs (declared FDs only; miner is Week 3+)
- `run_all_detectors()`: convenience function to run all and merge

**OUT** (explicitly excluded, to prevent scope creep):
- LLM-based detection (detectors are pure)
- Automatic FD mining/discovery (comes in later weeks)
- Repair proposals (that's the repairer layer)
- PII detection (separate detector, Week 2+)
- Outlier detection beyond decimal shifts (separate detector)
- Encoding error detection (separate detector)

## 4. Constraints

- Performance: all 3 detectors on a 10-row CSV must complete in < 2 seconds total.
- Performance: all 3 detectors on a 10,000-row CSV must complete in < 5 seconds total.
- Compatibility: Python 3.11+, works on Linux / macOS / Windows.
- Backward compatibility: must not break existing regression tests.
- No LLM calls: detectors are pure functions over data.

## 5. Prior decisions (locked — require new spec to change)

- Detectors receive `pd.DataFrame` (not raw file paths) — separation of I/O.
- CSV loading uses `dtype=str` by default to avoid pandas type coercion artifacts (CLAUDE.md).
- 3-tier severity (SAFE/REVIEW/UNSAFE) per DECISIONS.md entry.
- `issue_type` is a Literal string, not a free-form field — enforces closed vocabulary.

## 6. Task breakdown (atomic sub-tasks)

### 6.1 Issue and Severity models
- Acceptance: `Issue(row=0, column="a", issue_type="type_mismatch", severity=Severity.REVIEW, confidence=0.9, actual="foo", reason="bar")` constructs without error; invalid fields raise `ValidationError`.
- Depends on: none
- Estimated complexity: S

### 6.2 Schema model
- Acceptance: `Schema(columns={"a": "int", "b": "str"}, functional_dependencies=[{"determinant": ["zip"], "dependent": "city"}])` constructs; YAML round-trips.
- Depends on: none
- Estimated complexity: S

### 6.3 Detector protocol
- Acceptance: a class implementing `detect(df, schema) -> list[Issue]` is accepted by `isinstance` / structural subtyping checks.
- Depends on: 6.1
- Estimated complexity: S

### 6.4 TypeMismatchDetector
- Acceptance: all Appendix A type_mismatch cases pass.
- Depends on: 6.1, 6.3
- Estimated complexity: M

### 6.5 DecimalShiftDetector
- Acceptance: all Appendix A decimal_shift cases pass.
- Depends on: 6.1, 6.3
- Estimated complexity: M

### 6.6 FDViolationDetector
- Acceptance: all Appendix A fd_violation cases pass.
- Depends on: 6.1, 6.2, 6.3
- Estimated complexity: M

### 6.7 run_all_detectors convenience function
- Acceptance: returns merged, deduplicated issues sorted by severity then confidence.
- Depends on: 6.4, 6.5, 6.6
- Estimated complexity: S

## 7. Verification

- Unit tests: `tests/unit/test_base.py`, `tests/unit/test_type_mismatch.py`, `tests/unit/test_decimal_shift.py`, `tests/unit/test_fd_violation.py`
- Integration tests: `tests/unit/test_cli_profile.py` (end-to-end through CLI)
- Property tests: TBD (Week 2 — Hypothesis-based fuzzing of detector inputs)
- Benchmarks: `tests/benchmarks/bench_detectors.py` (TBD — Week 2)
- Coverage target: >= 90% line, >= 80% branch
- Mutation score target: >= 85%

## 8. Acceptance gate (ALL must be TRUE to mark SPEC complete)

- [x] All Section 2 outcomes are met.
- [x] All Section 6 tasks have "passes".
- [x] Coverage thresholds (Section 7) are met.
- [x] No test in `tests/regression/` fails.
- [x] `DECISIONS.md` has an entry for severity tiers.
- [x] `test_map.json` has entries for all detector source files.

## Appendix A — Toy cases (write the FIRST failing tests from these)

### Case A.1: type_mismatch — numeric value in string column
Input:
```python
pd.DataFrame({"name": ["Alice", "Bob", "12345", "Diana"]})
```
Expected output:
```python
[Issue(row=2, column="name", issue_type="type_mismatch", severity=Severity.REVIEW,
       confidence=0.85, actual="12345", reason="Value '12345' looks numeric in predominantly string column 'name'")]
```
Reasoning: a pure-numeric value in a column where 3/4 values are non-numeric strings signals data entry error or column misalignment.

### Case A.2: type_mismatch — string value in numeric column
Input:
```python
pd.DataFrame({"age": ["25", "30", "N/A", "40"]})
```
Expected output:
```python
[Issue(row=2, column="age", issue_type="type_mismatch", severity=Severity.REVIEW,
       confidence=0.90, actual="N/A", reason="Value 'N/A' is non-numeric in predominantly numeric column 'age'")]
```
Reasoning: `"N/A"` is a common sentinel that breaks downstream numeric operations. The detector should flag it.

### Case A.3: type_mismatch — clean column produces no issues
Input:
```python
pd.DataFrame({"score": ["95", "87", "92", "78"]})
```
Expected output: `[]`
Reasoning: all values are numeric strings — no type mismatch. A false positive here would erode trust.

### Case A.4: decimal_shift — 10x outlier in numeric column
Input:
```python
pd.DataFrame({"price": [100.0, 105.0, 98.0, 1020.0, 103.0]})
```
Expected output:
```python
[Issue(row=3, column="price", issue_type="decimal_shift", severity=Severity.REVIEW,
       confidence=...,  # >= 0.8
       actual="1020.0", expected="102.0",
       reason="Value 1020.0 in column 'price' appears to be ~10x the typical value (median ~103.0)")]
```
Reasoning: 1020 / 103 ~ 9.9, close to 10x. This is the canonical decimal-point-shift pattern.

### Case A.5: decimal_shift — 0.01x outlier (divide instead of multiply)
Input:
```python
pd.DataFrame({"salary": [50000, 48500, 52100, 501, 49800]})
```
Expected output:
```python
[Issue(row=3, column="salary", issue_type="decimal_shift", severity=Severity.REVIEW,
       confidence=...,  # >= 0.8
       actual="501", expected="50100",
       reason="Value 501 in column 'salary' appears to be ~0.01x the typical value (median ~49800)")]
```
Reasoning: 501 / 49800 ~ 0.01. The expected correction is 501 * 100 = 50100.

### Case A.6: decimal_shift — no outliers in uniform column
Input:
```python
pd.DataFrame({"score": [88, 92, 85, 90, 87]})
```
Expected output: `[]`
Reasoning: all values are within normal range. No power-of-10 shift detected.

### Case A.7: fd_violation — zip_code determines city
Input:
```python
pd.DataFrame({
    "zip_code": ["10001", "10001", "90210", "90210"],
    "city":     ["New York", "Manhattan", "Beverly Hills", "Beverly Hills"],
})
```
Schema: `functional_dependencies: [{determinant: [zip_code], dependent: city}]`
Expected output:
```python
[
    Issue(row=0, column="city", issue_type="fd_violation", severity=Severity.UNSAFE, ...),
    Issue(row=1, column="city", issue_type="fd_violation", severity=Severity.UNSAFE, ...),
]
```
Reasoning: zip_code "10001" maps to two different cities. Both rows in the violating group are flagged.

### Case A.8: fd_violation — consistent mapping produces no issues
Input:
```python
pd.DataFrame({
    "provider_id": ["P1", "P1", "P2", "P2"],
    "hospital":    ["General", "General", "St. Mary", "St. Mary"],
})
```
Schema: `functional_dependencies: [{determinant: [provider_id], dependent: hospital}]`
Expected output: `[]`
Reasoning: every provider_id maps to exactly one hospital. The FD is satisfied.
