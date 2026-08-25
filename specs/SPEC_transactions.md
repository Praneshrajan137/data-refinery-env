# SPEC: Transactions and Repair Pipeline

> Status: Draft
> Owner: @pranesh
> Last updated: 2026-05-20

## 1. Purpose (2 sentences)

Ship reversible CSV repair for Week 2 via `dataforge repair` and
`dataforge revert`. Every applied repair must be journaled before disk
mutation and every revert must restore the exact original bytes.

## 2. Outcomes (measurable, binary pass/fail)

- [ ] `dataforge repair <path> --dry-run` renders proposed fixes and writes nothing.
- [ ] `dataforge repair <path> --apply` writes a transaction journal and source snapshot before modifying the CSV.
- [ ] `dataforge.engine.repair.run_repair_pipeline()` exposes the public repair pipeline used by non-CLI backends.
- [ ] `dataforge revert <txn_id>` restores the exact original bytes and verifies the original SHA-256.
- [ ] Transaction logs are append-only JSONL with a schema-version field on every event.
- [ ] Newly written transaction logs use a v2 local hash chain and
  `dataforge audit <txn_id>` verifies event order, payload hashes, and replay.
- [ ] `type_mismatch` and `decimal_shift` repairers are deterministic and never call the LLM provider.
- [ ] `fd_violation` repairer prefers deterministic majority rules and caches any LLM fallback under `.dataforge/cache/`.
- [ ] All Appendix A toy cases pass, including apply -> revert byte-identity round-trip.

## 3. Scope

**IN**:
- `CellFix` and `RepairTransaction` Pydantic models
- Append-only per-transaction JSONL journal at `.dataforge/transactions/<txn_id>.jsonl`
- Local tamper-evident v2 event hash chain for newly written transaction logs
- Immutable source snapshot persisted before apply
- `repair` CLI with `--dry-run` and `--apply`
- Public engine API: `RepairPipelineRequest`, `RepairPipelineResult`,
  `RepairReceipt`, `CandidateFix`, `VerifiedFix`, and `RepairFailure`
- `revert` CLI with post-state hash guard
- `audit` CLI with v2 hash-chain verification and legacy v1 unverified status
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
- Apply invariant: source-path locking, stale-source detection, immutable snapshot creation, and atomic same-directory replacement are required for every source mutation.
- Journal format correction: use `.jsonl` rather than mutable single-file JSON.
- Exact restore guarantee: revert is snapshot-based, not pandas inverse-write based.

## 5. Prior decisions (locked — require new spec to change)

- Transaction-first ordering is non-negotiable for applied repairs.
- All deterministic repairers are deterministic-provenance; only `fd_violation` may use the LLM as a fallback. Deterministic provenance means the PROCEDURE is deterministic, not that the write is permitted: write authority is `CONSTRAINT_CHECKABLE_DETECTORS`, currently `fd_violation` and `missing_value`.
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
- Acceptance: revert restores snapshot bytes, verifies `source_sha256`, refuses when the current file hash differs from recorded post-state, and refuses tampered v2 transaction logs before mutation.
- Depends on: 6.1, 6.2
- Estimated complexity: M

### 6.3a Transaction audit verification
- Acceptance: new v2 events record `event_index`, `previous_event_sha256`,
  and `event_sha256`; `dataforge audit <txn_id>` exits 0 only for verified v2
  logs and reports legacy v1 logs as `legacy_unverified`.
- Depends on: 6.2
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

### 6.7 Public backend engine
- Acceptance: CLI-compatible repair behavior is available through
  `dataforge.engine.repair.run_repair_pipeline(request)` without API/MCP callers
  importing private CLI helpers.
- Depends on: 6.2, 6.4, 6.6
- Estimated complexity: M

### 6.8 Atomic apply and source locking
- Acceptance: apply rejects stale source bytes, duplicate transaction ids fail
  before mutation, journal-append crashes restore original bytes, and concurrent
  source mutations are serialized by a source-path lock.
- Depends on: 6.2, 6.3
- Estimated complexity: M

## 7. Verification

- Unit tests: `tests/unit/test_transactions.py`, `tests/unit/test_repairers.py`, `tests/unit/test_cli_repair.py`
- Engine tests: `tests/unit/test_engine_repair.py`
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

### Case A.6a: Tampered transaction log is refused
Input: apply a repair, edit the JSONL transaction payload without updating the
event hash, then run `dataforge audit <txn_id>` and `dataforge revert <txn_id>`.
Expected output: audit reports `tampered`; revert refuses before mutating the
source file.
Reasoning: the local audit claim requires tamper-evident transaction evidence.

### Case A.7: Stale-source apply is refused
Input: detect fixes against source bytes, edit the CSV before apply, then call apply with the old source bytes.
Expected output: apply raises a clear stale-source error and writes no transaction-applied event.
Reasoning: prevents a verified repair from being applied to a different file state.

### Case A.8: Apply crash restores bytes
Input: force the journal-applied append to fail after file mutation.
Expected output: source bytes equal the pre-apply bytes.
Reasoning: protects local-first trust when the process fails mid-transaction.
