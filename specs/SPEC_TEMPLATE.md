# SPEC: <component-name>

> Status: Draft | Reviewed | Frozen
> Owner: <your-handle>
> Last updated: YYYY-MM-DD

## 1. Purpose (2 sentences)

## 2. Outcomes (measurable, binary pass/fail)

- [ ] <measurable outcome 1>
- [ ] <measurable outcome 2>

## 3. Scope

**IN**:
- <explicit list of what this component must do>

**OUT** (explicitly excluded, to prevent scope creep):
- <things that sound related but are not this component's job>

## 4. Constraints

- Performance: <e.g., "p95 latency < 200ms on 10k-row schemas">
- Compatibility: <e.g., "Python 3.11+, works on Linux / macOS / WSL">
- Backward compatibility: <e.g., "must not break existing regression tests">

## 5. Prior decisions (locked — require new spec to change)

- <architectural commitment 1>
- <architectural commitment 2>

## 6. Task breakdown (atomic sub-tasks)

### 6.1 <task name>
- Acceptance: <binary pass/fail>
- Depends on: <list or "none">
- Estimated complexity: S / M / L

### 6.2 <task name>
...

## 7. Verification

- Unit tests: `tests/unit/test_<module>.py`
- Integration tests: `tests/integration/test_<module>_integration.py`
- Property tests: `tests/property/test_<module>_properties.py`
- Benchmarks: `tests/benchmarks/bench_<module>.py`
- Coverage target: ≥ 90% line, ≥ 80% branch
- Mutation score target: ≥ 85% (run with `mutmut run --paths-to-mutate dataforge/<module>`)

## 8. Acceptance gate (ALL must be TRUE to mark SPEC complete)

- [ ] All Section 2 outcomes are met.
- [ ] All Section 6 tasks have "passes".
- [ ] Coverage thresholds (Section 7) are met.
- [ ] No test in `tests/regression/` fails.
- [ ] `DECISIONS.md` has an entry for any non-obvious choice made during build.
- [ ] README's benchmark table (if applicable) is updated with reproducible numbers.

## Appendix A — Toy cases (write the FIRST failing tests from these)

### Case A.1: <short name>
Input: <concrete input>
Expected output: <concrete output>
Reasoning: <why this case matters — what bug would a failure here indicate?>

### Case A.2: <short name>
...
