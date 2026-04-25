# SPEC: Constitutional Safety Filter

> Status: Draft
> Owner: @pranesh
> Last updated: 2026-04-20

## 1. Purpose (2 sentences)

Ship the Week 3 constitutional safety layer that sits between proposed repairs
and applied repairs. Every candidate fix must be evaluated locally, without LLM
calls, against a named-rule constitution that can allow, escalate, or deny it.

## 2. Outcomes (measurable, binary pass/fail)

- [ ] The default constitution parses into compiled named-callable rules with no `eval()` or free-form code.
- [ ] `SafetyFilter.evaluate()` returns exactly one of `ALLOW`, `ESCALATE`, or `DENY` for a single candidate fix.
- [ ] PII writes are denied by default and only proceed when `--allow-pii` and confirmation are both present.
- [ ] Row-deletion proposals are denied.
- [ ] Aggregate-sensitive edits escalate unless explicitly confirmed.
- [ ] Minimal-edit scoring prefers the smallest Levenshtein-distance candidate when candidates are otherwise tied.
- [ ] The adversarial suite contains at least 50 hand-written attack fixtures and achieves 100% `DENY`.
- [ ] Benign near-miss fixtures produce < 3% false-positive `DENY`.

## 3. Scope

**IN**:
- Constitution YAML parser and named predicate/scorer registry
- Shipped default constitution with hard / soft-confirm / soft-prefer tiers
- Single-fix safety evaluation and batch conflict detection
- Typed safety context for PII and escalation confirmation
- Adversarial and benign fixture suites
- Benchmark for sub-millisecond evaluation latency

**OUT**:
- LLM-based policy reasoning
- Automatic constitution generation
- Warehouse-specific policy packs
- Human approval storage beyond per-command flags / prompts

## 4. Constraints

- Performance: p95 `< 1 ms` for `SafetyFilter.evaluate()` on a single fix.
- Compatibility: Python 3.11 / 3.12 on Linux, macOS, and Windows.
- Backward compatibility: existing Week 2 repair flows continue to import `SafetyFilter` from `dataforge.safety`.
- Safety invariant: no applied fix bypasses the safety layer.
- Free-tier constraint: all safety evaluation is local CPU work only.

## 5. Prior decisions (locked — require new spec to change)

- Constitution rules are named-callable-only; YAML never contains executable Python.
- `NO_PII_OVERWRITE` is a hard rule with a documented override + confirmation path.
- `NO_AGGREGATE_BREAK` uses explicit schema metadata (`aggregate_dependencies`) rather than heuristics.
- `MINIMAL_EDIT` is a scorer / tie-breaker, not a deny rule.

## 6. Task breakdown (atomic sub-tasks)

### 6.1 Constitution parser
- Acceptance: invalid predicate ids fail closed with a clear error; default constitution loads successfully.
- Depends on: none
- Estimated complexity: M

### 6.2 Typed safety evaluation
- Acceptance: `evaluate(fix, schema, context)` returns `ALLOW`, `ESCALATE`, or `DENY` with a human-readable reason.
- Depends on: 6.1
- Estimated complexity: M

### 6.3 Batch conflict detection
- Acceptance: conflicting writes to the same `(row, column)` with different values are denied.
- Depends on: 6.1
- Estimated complexity: S

### 6.4 Rule-specific behavior
- Acceptance: PII overwrite, row delete, aggregate escalation, and minimal-edit preference all pass their toy cases.
- Depends on: 6.2
- Estimated complexity: M

### 6.5 Adversarial and benchmark suites
- Acceptance: 50 attack fixtures, 50 benign fixtures, robustness gate, and p95 benchmark all pass.
- Depends on: 6.2, 6.3, 6.4
- Estimated complexity: M

## 7. Verification

- Unit tests: `tests/unit/test_safety_constitution.py`, `tests/unit/test_safety_filter.py`
- Adversarial tests: `tests/adversarial/test_constitution_robustness.py`
- Benchmarks: `tests/benchmarks/bench_safety_filter.py`
- Coverage target: >= 90% line, >= 80% branch
- Mutation score target: >= 85%

## 8. Acceptance gate (ALL must be TRUE to mark SPEC complete)

- [ ] All Section 2 outcomes are met.
- [ ] All Section 6 tasks have "passes".
- [ ] Coverage thresholds (Section 7) are met.
- [ ] No test in `tests/regression/` fails.
- [ ] `DECISIONS.md` contains any new Week 3 architectural choice.
- [ ] The default constitution ships with the required rules and fixtures.

## Appendix A — Toy cases (write the FIRST failing tests from these)

### Case A.1: PII overwrite denied by default
Input:
```python
fix = ProposedFix(
    fix=CellFix(row=0, column="phone_number", old_value="2175550101", new_value="", detector_id="type_mismatch"),
    reason="Normalize sentinel",
    confidence=0.9,
    provenance="deterministic",
)
schema = Schema(columns={"phone_number": "str"}, pii_columns={"phone_number"})
context = SafetyContext()
```
Expected output: `DENY`
Reasoning: default safety posture must block PII writes.

### Case A.2: Aggregate-sensitive edit escalates
Input:
```python
schema = Schema(
    columns={"amount": "float"},
    aggregate_dependencies=(
        AggregateDependency(source_column="amount", aggregate="sum", target_column="total_amount"),
    ),
)
```
Expected output: `ESCALATE`
Reasoning: aggregate-sensitive edits require explicit confirmation.

### Case A.3: Minimal edit wins
Input: two valid candidate fixes for the same issue, `"102"` and `"101"`, from original `"1020"`.
Expected output: the candidate with the smaller Levenshtein distance is chosen.
Reasoning: preference rules should be deterministic and local.

### Case A.4: Row delete denied
Input: a candidate fix with `operation="delete_row"`.
Expected output: `DENY`
Reasoning: Week 3 allows only cell updates.
