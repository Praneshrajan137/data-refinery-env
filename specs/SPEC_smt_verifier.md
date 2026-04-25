# SPEC: SMT Verifier

> Status: Draft
> Owner: @pranesh
> Last updated: 2026-04-20

## 1. Purpose (2 sentences)

Ship the Week 3 SMT verifier that validates candidate repairs against schema
constraints before they are applied. The verifier must operate locally on a
concrete working dataframe, return explainable results, and expose unsat cores
for retry hints.

## 2. Outcomes (measurable, binary pass/fail)

- [ ] `SchemaToSMT` maps supported schema types to Z3 sorts (`Int`, `Real`, `String`).
- [ ] Domain bounds are encoded as tracked constraints.
- [ ] Functional dependencies are encoded with `ForAll`-based implications, not Python O(n^2) row-pair loops.
- [ ] `verify_fix()` returns `ACCEPT`, `REJECT`, or `UNKNOWN`.
- [ ] `REJECT` includes a non-empty unsat core and a user-facing explanation.
- [ ] `UNKNOWN` is returned on timeout or unsupported type combinations.
- [ ] p95 verify latency is `< 200 ms` on a 1,000-row schema with 2 FDs.

## 3. Scope

**IN**:
- Canonical schema models for verifier use
- Z3 compilation for candidate-fix verification against current dataframe state
- Domain bounds and FD constraints
- Unsat-core explanation using schema-facing names
- Benchmark suite for latency

**OUT**:
- Automatic schema inference
- Repair synthesis inside the SMT layer
- Multi-fix global optimization across the entire issue list
- Non-Z3 solvers in Week 3

## 4. Constraints

- Performance: p95 `< 200 ms` on 1,000 rows / 2 FDs.
- Compatibility: Python 3.11 / 3.12, local CPU only.
- Backward compatibility: `SMTVerifier` remains importable from `dataforge.verifier`.
- Safety invariant: a fix is not applied unless the verifier returns `ACCEPT`.

## 5. Prior decisions (locked — require new spec to change)

- The verifier is bound to a concrete working dataframe, not schema-only reasoning.
- FD reasoning in Week 3 is candidate-local: unrelated pre-existing violations do not reject an otherwise-valid candidate.
- Z3 is the selected solver for Week 3.
- Unsat-core explanations must mention schema column names, never raw Z3 variable names.

## 6. Task breakdown (atomic sub-tasks)

### 6.1 Canonical schema types
- Acceptance: `Schema`, `FunctionalDependency`, `DomainBound`, and `AggregateDependency` construct and validate.
- Depends on: none
- Estimated complexity: S

### 6.2 Z3 sort compilation
- Acceptance: supported types compile, unsupported types return `UNKNOWN`.
- Depends on: 6.1
- Estimated complexity: M

### 6.3 Domain-bound verification
- Acceptance: below-min and above-max candidate values reject with tracked labels.
- Depends on: 6.2
- Estimated complexity: M

### 6.4 Functional-dependency verification
- Acceptance: a candidate that would violate an FD relative to its affected row/group rejects with an FD unsat core.
- Depends on: 6.2
- Estimated complexity: L

### 6.5 Explanation and benchmark suites
- Acceptance: unsat-core text uses schema names and benchmark gate passes.
- Depends on: 6.3, 6.4
- Estimated complexity: M

## 7. Verification

- Unit tests: `tests/unit/test_smt_verifier.py`
- Integration tests: `tests/integration/test_repair_pipeline.py`
- Benchmarks: `tests/benchmarks/bench_smt.py`
- Coverage target: >= 90% line, >= 80% branch
- Mutation score target: >= 85%

## 8. Acceptance gate (ALL must be TRUE to mark SPEC complete)

- [ ] All Section 2 outcomes are met.
- [ ] All Section 6 tasks have "passes".
- [ ] Coverage thresholds (Section 7) are met.
- [ ] No test in `tests/regression/` fails.
- [ ] `DECISIONS.md` records the Z3 choice.
- [ ] Verifier explanations do not expose internal Z3 symbol names.

## Appendix A — Toy cases (write the FIRST failing tests from these)

### Case A.1: Decimal-shift fix within bounds
Input:
```python
df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
schema = Schema(columns={"amount": "float"}, domain_bounds=(DomainBound(column="amount", min_value=0.0, max_value=5000.0),))
fix = ProposedFix(
    fix=CellFix(row=3, column="amount", old_value="1020", new_value="102", detector_id="decimal_shift"),
    reason="10x too large",
    confidence=0.9,
    provenance="deterministic",
)
```
Expected output: `ACCEPT`
Reasoning: valid numeric repair inside bounds should pass.

### Case A.2: Domain-bound rejection
Input: same shape, but candidate value `-5` against `min_value=0`.
Expected output: `REJECT` with a domain-bound unsat core.
Reasoning: tracked domain labels must explain why the fix is unsafe.

### Case A.3: FD rejection on dependent change
Input: schema contains `code -> name` and `name -> state`; candidate changes `name` so it disagrees with the `state` group it enters.
Expected output: `REJECT`
Reasoning: local FD reasoning must catch downstream consistency violations caused by a single fix.

### Case A.4: Unsupported type yields unknown
Input: schema contains a column type unsupported by Week 3.
Expected output: `UNKNOWN`
Reasoning: fail conservatively when the solver encoding is incomplete.
