# SPEC: Transactions and Repair Pipeline

> Status: Draft
> Owner: @pranesh
> Last updated: 2026-04-20

## 1. Purpose (2 sentences)

Ship reversible CSV repair for Week 2 via `dataforge repair` and
`dataforge revert`. Every applied repair must be journaled before disk
mutation and every revert must restore the exact original bytes.

## 2. Outcomes (measurable, binary pass/fail)

- [ ] `dataforge repair <path> --dry-run` renders proposed fixes and writes nothing.
- [ ] `dataforge repair <path> --apply` writes a transaction journal and source snapshot before modifying the CSV.
- [ ] `dataforge revert <txn_id>` restores the exact original bytes and verifies the original SHA-256.
- [ ] Transaction logs are append-only JSONL with a schema-version field on every event.
- [ ] `type_mismatch` and `decimal_shift` repairers are deterministic and never call the LLM provider.
- [ ] `fd_violation` repairer prefers deterministic majority rules and caches any LLM fallback under `.dataforge/cache/`.
- [ ] All Appendix A toy cases pass, including apply -> revert byte-identity round-trip.

## 3. Scope

**IN**:
- `CellFix` and `RepairTransaction` Pydantic models
- Append-only per-transaction JSONL journal at `.dataforge/transactions/<txn_id>.jsonl`
- Immutable source snapshot persisted before apply
- `repair` CLI with `--dry-run` and `--apply`
- `revert` CLI with post-state hash guard
- Repairer protocol and three Week 1 repairers
- Thin safety + verifier gate scaffolds in the apply path
- Property test proving exact byte restoration after revert

**OUT** (explicitly excluded, to prevent scope creep):
- SMT-backed semantic repair validation beyond a thin typed verifier stub
- Constitution-driven PII policy enforcement beyond a thin typed safety stub
- Warehouse / dbt / remote-table repair
- Parallel or streaming repair
- Patch-based CSV mutation that preserves applied-file byte layout

## 4. Constraints

- Performance: dry-run on a 10-row CSV completes in < 2 seconds.
- Compatibility: Python 3.11+ and 3.12 supported; Windows / macOS / Linux.
- Backward compatibility: no regressions in existing Week 1 tests.
- Safety invariant: apply path must flow through Safety -> Verifier -> Transaction -> File write.
- Journal format correction: use `.jsonl` rather than mutable single-file JSON.
- Exact restore guarantee: revert is snapshot-based, not pandas inverse-write based.

## 5. Prior decisions (locked — require new spec to change)

- Transaction-first ordering is non-negotiable for applied repairs.
- `type_mismatch` and `decimal_shift` repairers are deterministic; only `fd_violation` may use the LLM.
- Revert must refuse if the current file hash does not match the recorded post-state hash.
- Byte-identical restore is guaranteed by restoring an immutable snapshot of the source bytes.

## 6. Task breakdown (atomic sub-tasks)

### 6.1 Transaction models
- Acceptance: `RepairTransaction` validates `txn_id`, UTC timestamps, SHA-256 fields, and `CellFix` payloads.
- Depends on: none
- Estimated complexity: S

### 6.2 Append-only journal
- Acceptance: created / applied / reverted events replay into the latest transaction state without mutating older entries.
- Depends on: 6.1
- Estimated complexity: M

### 6.3 Revert flow
- Acceptance: revert restores snapshot bytes, verifies `source_sha256`, and refuses when the current file hash differs from recorded post-state.
- Depends on: 6.1, 6.2
- Estimated complexity: M

### 6.4 Repairers
- Acceptance: deterministic repairers emit `ProposedFix` values; fd-violation repairer uses majority rules first and cache-backed LLM fallback second.
- Depends on: Week 1 detectors
- Estimated complexity: M

### 6.5 CLI repair / revert
- Acceptance: dry-run shows a rich diff and writes nothing; apply writes journal+snapshot before mutation; revert resolves `txn_id` and restores bytes.
- Depends on: 6.2, 6.3, 6.4
- Estimated complexity: L

### 6.6 Week 2 gate scaffolds
- Acceptance: apply path calls typed safety and verifier interfaces before any disk mutation.
- Depends on: 6.4
- Estimated complexity: S

## 7. Verification

- Unit tests: `tests/unit/test_transactions.py`, `tests/unit/test_repairers.py`, `tests/unit/test_cli_repair.py`
- Integration tests: `tests/unit/test_cli_profile.py` remains green
- Property tests: `tests/property/test_revert_is_bytes_identical.py`
- Benchmarks: existing performance expectation for small CSV dry-run
- Coverage target: >= 90% line, >= 80% branch
- Mutation score target: >= 85%

## 8. Acceptance gate (ALL must be TRUE to mark SPEC complete)

- [ ] All Section 2 outcomes are met.
- [ ] All Section 6 tasks have "passes".
- [ ] Coverage thresholds (Section 7) are met.
- [ ] No test in `tests/regression/` fails.
- [ ] `DECISIONS.md` has an entry for transaction-first ordering.
- [ ] `ruff check`, `ruff format --check`, and `mypy --strict dataforge` pass.

## Appendix A — Toy cases (write the FIRST failing tests from these)

### Case A.1: RepairTransaction identifier format
Input:
```python
RepairTransaction(
    txn_id="txn-2026-04-20-a1b2c3",
    created_at=datetime.now(timezone.utc),
    source_path="/tmp/data.csv",
    source_sha256="a" * 64,
    source_snapshot_path="/tmp/.dataforge/snapshots/txn-2026-04-20-a1b2c3.bin",
    fixes=[],
    applied=False,
)
```
Expected output: model constructs successfully.
Reasoning: guards the public transaction identifier contract.

### Case A.2: Append-only replay
Input: a JSONL log with `created`, `applied`, and `reverted` events for one `txn_id`.
Expected output: replayed transaction has `applied=True`, `post_sha256` set, and `reverted_at` populated.
Reasoning: proves the journal is append-only and state is derived by replay.

### Case A.3: Revert refuses on post-state hash mismatch
Input: a recorded transaction whose current source file bytes do not match `post_sha256`.
Expected output: revert raises a clear error and leaves the file untouched.
Reasoning: avoids clobbering user edits made after apply.

### Case A.4: Decimal-shift repair is deterministic
Input: a `decimal_shift` Issue with `expected="102"` on a row whose current value is `"1020"`.
Expected output: `ProposedFix.fix.new_value == "102"` and `provenance == "deterministic"`.
Reasoning: free-tier quota must not be spent on deterministic repairs.

### Case A.5: FD violation majority-rule repair
Input:
```python
pd.DataFrame({"code": ["A", "A", "A"], "name": ["Alpha", "Alpha", "Beta"]})
```
Schema: `functional_dependencies: [{determinant: [code], dependent: name}]`
Expected output: row 2 proposes `"Alpha"` deterministically.
Reasoning: deterministic majority rules should handle the common case without the LLM.

### Case A.6: Apply -> revert round-trip
Input: a small CSV, at least one valid `CellFix`, and a full apply followed by revert.
Expected output: `sha256(reverted_file_bytes) == sha256(original_file_bytes)` and `reverted_file_bytes == original_file_bytes`.
Reasoning: the Week 2 headline guarantee is byte-identical restoration.
