"""Public repair engine for DataForge backend surfaces.

The engine is the stable boundary shared by CLI, Playground, MCP, and any
OpenEnv adapter that needs repair semantics. It keeps the core invariant in one
place: detect -> propose -> safety -> SMT verification -> journal/snapshot ->
atomic mutation -> byte-identical revert.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from dataforge.calibration import (
    AbstentionPolicy,
    CalibrationScope,
    corrector_default_policy,
    guard_policy_for_drift,
    guard_policy_for_drift_by_class,
    guard_policy_for_scope,
)
from dataforge.calibration_map import CalibrationMap
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema
from dataforge.domain.vocabulary import (
    CALIBRATED_PROVENANCE,
    CONSTRAINT_CHECKABLE_DETECTORS,
    UNTRUSTED_PROVENANCE,
    ReviewReason,
    VerificationStrength,
    type_discriminates,
)
from dataforge.domain.vocabulary import (
    verification_strength_for as _domain_verification_strength_for,
)
from dataforge.observability import repair_stage_span
from dataforge.repair_contract import CONTRACT_VERSION
from dataforge.repairers import build_repairers
from dataforge.repairers.base import ProposedFix, RepairAttempt, RetryContext
from dataforge.safety import SafetyContext, SafetyFilter, SafetyResult, SafetyVerdict
from dataforge.schema_inference import (
    ConstraintReviewArtifact,
    infer_verification_schema,
    merge_schema_with_reviewed_constraints,
)
from dataforge.table import (
    Table,
    TableLike,
    cell_value,
    column_names,
    copy_table,
    row_count,
    set_cell_value,
    table_to_csv_bytes,
)
from dataforge.table import (
    read_csv as read_table_csv,
)
from dataforge.transactions.files import (
    SourceLockError,
    atomic_write_bytes,
    lock_path_for,
)
from dataforge.transactions.files import (
    source_path_lock as transaction_source_path_lock,
)
from dataforge.transactions.log import (
    append_applied_event,
    append_created_transaction,
    cache_dir_for,
    sha256_bytes,
    sha256_file,
    snapshot_path_for,
)
from dataforge.transactions.txn import CellFix, RepairTransaction, generate_txn_id
from dataforge.verifier import SMTVerifier, VerificationResult, VerificationVerdict
from dataforge.verifier.differential import differential_verify

if TYPE_CHECKING:
    from dataforge.review import ReviewRanker

RepairMode = Literal["dry_run", "apply"]
EscalationResolver = Callable[
    [ProposedFix, Schema | None, SafetyContext, SafetyFilter, SafetyResult],
    tuple[SafetyContext, SafetyResult],
]


class RepairEngineError(RuntimeError):
    """Base exception for public repair engine failures."""


class TransactionApplyError(RepairEngineError):
    """Raised when an apply transaction cannot be completed safely."""


class UncheckableDetectorWriteError(RepairEngineError):
    """Raised when a gated mutation primitive is handed a deterministic fix whose
    detector is not in :data:`CONSTRAINT_CHECKABLE_DETECTORS`.

    This is a DIFFERENT failure from :class:`UnprovenWriteError`, and the two are kept
    separate because a caller hitting one needs a different remedy than a caller hitting
    the other. An ``UnprovenWriteError`` says "this value was never checked against an
    authority"; this says "the procedure that produced this value is deterministic, and
    may well be *proven* in the strength sense, but its correctness cannot be checked
    against anything outside the column's own distribution." Determinism is not
    soundness -- see ``docs/trust/deterministic-is-not-sound.md``.

    Why this lives at the primitive rather than only in :func:`partition_auto_apply`:
    because on 2026-08-22 it did only live there, and the agent controller -- which never
    calls :func:`partition_auto_apply` -- wrote a ``decimal_shift`` fix that the legacy
    pipeline held, on the SAME table, with no schema. Measured: ``4,1020`` became
    ``4,102`` via ``run_agent_repair`` while ``run_repair_pipeline`` refused. The
    strength gate did not catch it because ``verification_strength_for("deterministic",
    ...)`` is ``proven`` regardless of schema, so the allowlist was simply never
    consulted on that surface.

    That is the same class of defect, and the same remedy, as
    :class:`UnprovenWriteError` itself: enforce at the primitive, so a write surface
    cannot bypass the gate by forgetting to call the partitioner first.
    """


class UnprovenWriteError(RepairEngineError):
    """Raised when a gated mutation primitive is handed a ``plausibility_only`` fix.

    The proven-only invariant is enforced *inside* the mutation primitives rather than
    at each calling surface, so a caller that simply forgot the gate gets this exception
    instead of a silent unproven write. Callers that legitimately want an unproven write
    must say so explicitly via ``allow_unproven_autoapply``.

    Scope, stated exactly (an earlier version of this docstring said "the two mutation
    primitives", which was false -- the same class of scope error this gate exists to
    prevent). The gate is enforced at:

    * :func:`apply_transaction` -- the journaled CSV path.
    * ``DuckDBStore.apply_patch_plan`` -- the warehouse SQL path, via
      :func:`dataforge.stores.patch_plan.enforce_plan_proven_only`.

    It is NOT enforced at these paths, each for a stated reason:

    * :func:`_apply_fixes_to_csv` -- the raw byte-writer beneath
      :func:`apply_transaction`. It takes ``CellFix``, which carries no provenance, so
      strength is not merely unchecked but *undecidable* there. It is private and called
      only from this module; that, not a gate, is what keeps it off other surfaces.
    * ``dataforge.transactions.revert.revert_transaction`` -- restores bytes this tool
      previously recorded, so there is no new value to prove. Gated instead by audit-log
      verification and a post-state hash match.
    * ``dataforge.schema_inference.write_constraint_review_artifact_atomic`` -- rewrites
      the user's *constraints* artifact, not their data. It has no proven-only gate
      because it mutates the PREMISE of provenness rather than a value; see
      ``docs/trust/authority-is-mutable.md``.

    The authoritative registry of write primitives, with each one's classification and
    gate, is ``tests/integration/test_surface_uniformity.py::_WRITE_PRIMITIVE_REGISTRY``.
    """


class CandidateFix(BaseModel):
    """Stable public representation of a proposed cell repair."""

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    old_value: str
    new_value: str
    detector_id: str = Field(min_length=1)
    operation: Literal["update", "delete_row"] = "update"
    reason: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    provenance: str = Field(min_length=1)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @classmethod
    def from_proposed(cls, proposed_fix: ProposedFix) -> CandidateFix:
        """Create a public candidate from an internal repair proposal."""
        fix = proposed_fix.fix
        return cls(
            row=fix.row,
            column=fix.column,
            old_value=fix.old_value,
            new_value=fix.new_value,
            detector_id=fix.detector_id,
            operation=fix.operation,
            reason=proposed_fix.reason,
            confidence=proposed_fix.confidence,
            provenance=proposed_fix.provenance,
        )


RootCauseCategory = Literal[
    "data_entry_error",
    "decimal_shift",
    "sentinel_null_encoding",
    "fd_conflict",
    "domain_violation",
    "duplicate_key_violation",
    "referential_break",
    "unknown",
]

# ``ReviewReason`` (the machine-parseable, honest reason a proposed fix was NOT
# auto-applied) and ``VerificationStrength`` (how strongly a fix was verified, which
# decides whether it may auto-apply) are DEFINED in dataforge/domain/vocabulary.py and
# imported above. They are re-exported here because every caller in the codebase, the
# HTTP contract, and the tests refer to them by these names.
#
# They used to be declared here and re-listed by hand in the terminal humanizer, the
# HTTP models, and the browser. Each hand copy drifted: the humanizer once carried 12
# of 13 reasons, so a held fix rendered as a raw machine token to a user.
#
#   proven            -> deterministic (correct by construction) OR verified against an
#                        authoritative declared/reviewed schema (real SMT constraints).
#   plausibility_only -> no authoritative schema for that column AND an untrusted
#                        value, so it was only checked by the advisory inferred guard,
#                        where the known verifier-floor gaps live. Never auto-applied
#                        unless explicitly opted in, and then recorded as unproven.


class RootCause(BaseModel):
    """Public diagnosis attached to an issue before repair is applied."""

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    issue_type: str = Field(min_length=1)
    category: RootCauseCategory
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str = Field(min_length=1)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class CandidateRepair(CandidateFix):
    """Public repair candidate with verifier context."""

    verifier_reason: str = Field(min_length=1)
    review_reason: ReviewReason | None = None
    verification_strength: VerificationStrength | None = None


class VerifiedFix(CandidateRepair):
    """A candidate that passed safety and SMT verification."""


ProofStatus = Literal[
    "accepted",
    "rejected",
    "unknown",
    "denied",
    "escalated",
    "attempted_not_fixed",
    "not_run",
]


class ProofObligation(BaseModel):
    """Verifier/safety obligation emitted for a repair attempt."""

    obligation_id: str = Field(min_length=1)
    verifier: Literal["smt", "sql", "safety", "repairer"]
    status: ProofStatus
    reason: str = Field(min_length=1)
    unsat_core: tuple[str, ...] = Field(default_factory=tuple)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class RepairFailure(BaseModel):
    """Machine-readable account of an issue that could not be repaired."""

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    issue_type: str = Field(min_length=1)
    status: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    attempt_count: int = Field(ge=1)
    unsat_core: tuple[str, ...] = Field(default_factory=tuple)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @classmethod
    def from_attempts(cls, attempts: list[RepairAttempt]) -> RepairFailure:
        """Build a public failure record from one issue's attempt trace."""
        final = attempts[-1]
        issue = final.issue
        return cls(
            row=issue.row,
            column=issue.column,
            issue_type=issue.issue_type,
            status=final.status,
            reason=final.reason,
            attempt_count=len(attempts),
            unsat_core=tuple(final.unsat_core),
        )


class ReviewRankedCell(BaseModel):
    """One detected cell scored for human-review ordering (never auto-applied).

    ``triage_score`` in [0, 1] is the LLM review-ranker's likelihood that the
    cell is a genuine error; higher sorts earlier in the review queue. This is a
    presentation-only annotation: it never enters the verified apply path.
    """

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    triage_score: float = Field(ge=0.0, le=1.0)
    reason: str = Field(default="", description="The detector's finding for this cell.")

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class RepairReceipt(BaseModel):
    """Stable receipt for a dry-run or applied repair pipeline run."""

    schema_version: Literal["repair_receipt_v1"] = "repair_receipt_v1"
    receipt_version: Literal["repair_receipt_v1"] = "repair_receipt_v1"
    contract_version: str = CONTRACT_VERSION
    mode: RepairMode
    applied: bool
    reversible: bool
    source_path: str
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    post_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    txn_id: str | None = None
    allowed_columns: list[str] = Field(default_factory=list)
    authoritative_columns: list[str] = Field(
        default_factory=list,
        description=(
            "Columns an authoritative schema actually constrains, so a reader can tell "
            "WHICH column a 'proven' label was earned on. Empty means no authority was "
            "present, in which case only a deterministic fix can honestly be proven. "
            "Recorded because a certificate that claims 'proven' without naming the "
            "authority cannot be checked by anyone but the tool that wrote it."
        ),
    )
    valid_rows: list[int] = Field(default_factory=list)
    safety_verdict: str = Field(default="allow", min_length=1)
    verifier_verdict: str = Field(default="not_run", min_length=1)
    independent_verification: Literal["agreed", "not_run"] = "not_run"
    candidate_provenance: list[str] = Field(default_factory=list)
    root_causes: list[RootCause] = Field(default_factory=list)
    candidate_repairs: list[CandidateRepair] = Field(default_factory=list)
    suggested_fixes: list[CandidateRepair] = Field(default_factory=list)
    applied_fixes: list[VerifiedFix] = Field(default_factory=list)
    proof_obligations: list[ProofObligation] = Field(default_factory=list)
    accepted_constraint_ids: list[str] = Field(default_factory=list)
    constraints_artifact_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    patch_plan_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    revert_command: str | None = None
    limitations: list[str] = Field(default_factory=list)
    abstentions: list[str] = Field(default_factory=list)
    failure_reasons: list[str] = Field(default_factory=list)
    review_ranking: list[ReviewRankedCell] = Field(
        default_factory=list,
        description=(
            "Optional LLM review-queue ordering of detected cells (highest triage "
            "score first). Empty unless a review_ranker was supplied. Presentation "
            "only - never part of the applied/verified fixes."
        ),
    )
    issues_count: int = Field(ge=0)
    fixes_count: int = Field(ge=0)
    reason: str = Field(min_length=1)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class RepairPipelineRequest(BaseModel):
    """Input contract for running the public repair pipeline."""

    source_path: Path
    mode: RepairMode = "dry_run"
    repair_schema: Schema | None = Field(default=None, alias="schema")
    allow_llm: bool = False
    model: str | None = None
    allow_pii: bool = False
    confirm_pii: bool = False
    confirm_escalations: bool = False
    allow_unproven_autoapply: bool = False
    require_declared_fds_for_autoapply: bool = False
    fd_detection_source: FdDetectionSource = "accepted"
    allow_entity_consensus: bool = False
    corrector_pool_constrained: bool = False
    corrector_structured: bool = False
    require_independent_agreement: bool = True
    interactive: bool = False
    create_dry_run_transaction: bool = False
    constraints: ConstraintReviewArtifact | None = None
    constraints_artifact_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    corrector_policy: AbstentionPolicy | None = None
    calibration_map_by_class: dict[str, CalibrationMap] | None = None
    corrector_reference_confidences: dict[str, list[float]] | None = None
    #: Scope of the artifact ``corrector_policy`` came from. Guarded **fail-closed**: a
    #: policy that can auto-apply but carries no scope is downgraded, because a conformal
    #: certificate is valid only for data exchangeable with its calibration sample and an
    #: unscoped artifact cannot be shown to apply here. Set ``corrector_scope_verified`` to
    #: bypass, which is what in-process callers constructing a policy directly should do.
    corrector_calibration_scope: CalibrationScope | None = None
    #: Assert that this policy's scope was established out of band. Exists so tests and
    #: library callers that build a policy in memory are not forced to fabricate a scope,
    #: while a policy loaded from a file still has to prove it belongs to this table.
    corrector_scope_verified: bool = False

    model_config = ConfigDict(
        strict=True,
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        populate_by_name=True,
    )


class RepairPipelineResult(BaseModel):
    """Output contract for a public repair pipeline run."""

    receipt: RepairReceipt
    issues: list[Issue]
    fixes: list[VerifiedFix]
    failures: list[RepairFailure] = Field(default_factory=list)
    transaction: RepairTransaction | None = None

    model_config = ConfigDict(
        strict=True, arbitrary_types_allowed=True, extra="forbid", frozen=True
    )


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    """Write bytes to ``path`` through an atomic same-directory replacement."""
    atomic_write_bytes(path, payload)


def read_csv(path: Path) -> Table:
    """Read a CSV using conservative string-preserving defaults."""
    return read_table_csv(path)


def _csv_bytes_after_fixes(path: Path, fixes: list[CellFix]) -> bytes:
    """Validate fixes against a CSV and return the mutated CSV bytes."""
    df = read_csv(path)
    for fix in fixes:
        if fix.operation != "update":
            raise ValueError(f"Unsupported repair operation '{fix.operation}' for row {fix.row}.")
        if fix.column not in column_names(df):
            raise ValueError(f"Column '{fix.column}' not found in '{path}'.")
        if fix.row < 0 or fix.row >= row_count(df):
            raise ValueError(f"Row {fix.row} is out of bounds for '{path}'.")

        current_value = cell_value(df, fix.row, fix.column)
        if current_value != fix.old_value:
            raise ValueError(
                f"Refusing to apply stale fix for row {fix.row}, column '{fix.column}': "
                f"expected '{fix.old_value}', found '{current_value}'."
            )
        set_cell_value(df, fix.row, fix.column, fix.new_value)

    return table_to_csv_bytes(df)


def _apply_fixes_to_csv(path: Path, fixes: list[CellFix]) -> str:
    """Atomically write a CSV with ordered cell fixes applied; return post-state SHA-256.

    PRIVATE ON PURPOSE. This is the raw byte-writer beneath :func:`apply_transaction`,
    and it is deliberately not part of the public surface:

    * It takes ``CellFix``, which carries no ``provenance``, so the proven-only gate is
      not merely unchecked here but *undecidable*.
    * It performs no journalling, no source snapshot and no source lock, so a write
      through it is **irreversible** -- there is nothing to revert to.

    Both properties are fine for the inner step of a transaction that has already taken
    the snapshot and the lock. They are not fine as a public API -- and it WAS public
    (exported in ``dataforge.engine.__all__``) until 2026-08-09. ``PRODUCT.md`` forbids
    surfaces creating parallel write semantics; exporting the raw byte-writer of a
    transactional engine is exactly that. Callers want :func:`apply_transaction`.
    """
    payload = _csv_bytes_after_fixes(path, fixes)
    _atomic_write_bytes(path, payload)
    return hashlib.sha256(payload).hexdigest()


def _lock_path_for(source_path: Path) -> Path:
    """Return the filesystem lock path for a source file."""
    return lock_path_for(source_path)


@contextmanager
def source_path_lock(
    source_path: Path,
    *,
    timeout_seconds: float = 5.0,
    stale_after_seconds: float = 300.0,
) -> Iterator[None]:
    """Acquire an exclusive lock for a source path using an atomic lock file."""
    try:
        with transaction_source_path_lock(
            source_path,
            timeout_seconds=timeout_seconds,
            stale_after_seconds=stale_after_seconds,
        ):
            yield
    except SourceLockError as exc:
        raise TransactionApplyError(str(exc)) from exc


def _write_snapshot_once(snapshot_path: Path, source_bytes: bytes) -> None:
    """Write an immutable snapshot and fail if the transaction id already exists."""
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with snapshot_path.open("xb") as handle:
            handle.write(source_bytes)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise TransactionApplyError(
            f"Transaction snapshot already exists: {snapshot_path}"
        ) from exc


def create_repair_transaction(
    path: Path,
    fixes: list[ProposedFix],
    source_bytes: bytes,
    *,
    txn_id: str | None = None,
) -> tuple[RepairTransaction, Path]:
    """Create an unapplied transaction journal and immutable source snapshot."""
    resolved_path = path.resolve()
    transaction_id = txn_id or generate_txn_id()
    snapshot_path = snapshot_path_for(resolved_path, transaction_id)
    _write_snapshot_once(snapshot_path, source_bytes)

    transaction = RepairTransaction(
        txn_id=transaction_id,
        created_at=datetime.now(UTC),
        source_path=str(resolved_path),
        source_sha256=sha256_bytes(source_bytes),
        source_snapshot_path=str(snapshot_path.resolve()),
        fixes=[proposal.fix for proposal in fixes],
        applied=False,
    )
    try:
        log_path = append_created_transaction(transaction)
    except Exception:
        snapshot_path.unlink(missing_ok=True)
        raise
    return transaction, log_path


def enforce_snapshot_recoverable(transaction: RepairTransaction) -> None:
    """Refuse to mutate user bytes unless the recorded snapshot can actually restore them.

    Reversibility is the strongest promise in ``PRODUCT.md`` -- stronger than proven-only,
    because a reversible wrong write costs a revert while an irreversible one costs the
    data. On this path it was achieved by *ordering*: ``create_repair_transaction`` writes
    the snapshot and journal before ``_apply_fixes_to_csv`` runs, so a correct sequence
    yields a recoverable state. Ordering is not a precondition, though. Nothing checked
    that the snapshot reached disk intact, and the failure mode is silent in the worst
    direction: a truncated or missing snapshot is indistinguishable from a good one until
    the revert that needs it, which is exactly when the user has no other copy.

    ``transactions/revert.py`` restores by writing the snapshot bytes back and comparing
    the result against ``transaction.source_sha256``. So the precondition for that revert
    succeeding is checkable here, before any byte of user data changes: the snapshot must
    exist and must hash to the value the revert will demand of it.

    This is the CSV counterpart of :func:`dataforge.stores.patch_plan.enforce_plan_reversible`.

    Raises:
        TransactionApplyError: If the snapshot is absent, unreadable, or does not hash to
            the transaction's recorded source digest.
    """
    recorded = transaction.source_snapshot_path
    if not recorded:
        raise TransactionApplyError(
            "Refusing to apply repairs: the transaction records no source snapshot, so the "
            "write would be irreversible."
        )
    snapshot_path = Path(recorded)
    if not snapshot_path.is_file():
        raise TransactionApplyError(
            f"Refusing to apply repairs: the source snapshot '{recorded}' does not exist, so "
            "the write would be irreversible."
        )
    if sha256_file(snapshot_path) != transaction.source_sha256:
        raise TransactionApplyError(
            f"Refusing to apply repairs: the source snapshot '{recorded}' does not match the "
            "recorded source digest, so a revert could not restore the original bytes."
        )


def apply_transaction(
    path: Path,
    fixes: list[ProposedFix],
    source_bytes: bytes,
    *,
    txn_id: str | None = None,
    covered_columns: frozenset[str] = frozenset(),
    allow_unproven_autoapply: bool = False,
) -> str:
    """Journal, snapshot, atomically apply fixes, and restore bytes on failure.

    Enforces the proven-only invariant before touching disk (see
    :func:`enforce_proven_only`). The defaults are the safe ones, so an existing
    caller that passes neither keyword gets proven-only behaviour rather than a
    silent unproven write.

    ``covered_columns`` is the set of columns an authoritative schema actually
    constrains -- see :func:`authoritative_columns`. It is a set rather than a boolean
    because authority is per-column: a schema that declares one column's type says
    nothing about any other column.
    """
    enforce_proven_only(
        fixes,
        covered_columns=covered_columns,
        allow_unproven_autoapply=allow_unproven_autoapply,
    )
    enforce_constraint_checkable_only(fixes)
    resolved_path = path.resolve()
    with source_path_lock(resolved_path):
        current_bytes = resolved_path.read_bytes()
        if current_bytes != source_bytes:
            raise TransactionApplyError(
                "Refusing to apply repairs because the source file changed after detection."
            )

        with repair_stage_span("transaction_create", fixes_count=len(fixes)):
            transaction, log_path = create_repair_transaction(
                resolved_path,
                fixes,
                source_bytes,
                txn_id=txn_id,
            )
        enforce_snapshot_recoverable(transaction)
        try:
            with repair_stage_span("transaction_apply", fixes_count=len(fixes)):
                post_sha256 = _apply_fixes_to_csv(
                    resolved_path,
                    [proposal.fix for proposal in fixes],
                )
                append_applied_event(log_path, transaction.txn_id, post_sha256=post_sha256)
        except Exception as exc:
            _atomic_write_bytes(resolved_path, source_bytes)
            if sha256_file(resolved_path) != transaction.source_sha256:
                raise TransactionApplyError(
                    "Apply failed and the source file could not be restored to original bytes."
                ) from exc
            raise

    return transaction.txn_id


def _build_retry_context(issue: Issue, attempts: list[RepairAttempt]) -> RetryContext:
    """Build retry hints from previous failed attempts."""
    rejected_values = frozenset(
        attempt.fix.fix.new_value
        for attempt in attempts
        if attempt.fix is not None and attempt.status in {"denied", "rejected", "unknown"}
    )
    hints: list[str] = []
    for attempt in attempts:
        hints.append(attempt.reason)
        hints.extend(attempt.unsat_core)
    return RetryContext(
        issue=issue,
        previous_attempts=tuple(attempts),
        rejected_values=rejected_values,
        hints=tuple(hints),
    )


def _verify_fix(
    working_df: TableLike,
    fix: ProposedFix,
    schema: Schema | None,
    *,
    verifier: SMTVerifier,
    verification_schema: Schema | None,
    require_independent_agreement: bool,
) -> VerificationResult:
    """The single shared prove-gate for one candidate fix.

    This is the SAME verification that internal repairs and external
    (verify_and_apply) fixes run through -- there is exactly one prove gate:

    * Authoritative-schema path (with ``require_independent_agreement``): the
      primary z3 ``SMTVerifier`` is cross-checked against the independently-written
      ``DirectVerifier``, combined fail-closed. Only a fix both accept can pass, so
      a bug in either implementation can withhold a fix, never wave a corrupting one.
    * Schema-less path: ``SMTVerifier`` with the advisory inferred guard, which is
      engaged only for untrusted (non-deterministic) values; deterministic values
      are never second-guessed by it, keeping schema-less deterministic runs
      byte-identical.
    """
    guard_schema = (
        verification_schema if (schema is None and fix.provenance != "deterministic") else None
    )
    if schema is not None and require_independent_agreement:
        differential = differential_verify(working_df, [fix], schema)
        return VerificationResult(
            verdict=differential.verdict,
            reason=differential.reason,
            unsat_core=differential.unsat_core,
        )
    return verifier.verify(working_df, [fix], schema, verification_schema=guard_schema)


def propose_repairs(
    issues: list[Issue],
    path: Path,
    working_df: TableLike,
    schema: Schema | None,
    *,
    allow_llm: bool,
    model: str | None,
    allow_pii: bool,
    confirm_pii: bool,
    confirm_escalations: bool,
    interactive: bool,
    escalation_resolver: EscalationResolver | None = None,
    verification_schema: Schema | None = None,
    require_independent_agreement: bool = True,
    allow_entity_consensus: bool = False,
    corrector_pool_constrained: bool = False,
    corrector_structured: bool = False,
) -> tuple[list[ProposedFix], list[list[RepairAttempt]]]:
    """Run repairers and gates issue-by-issue against a working dataframe.

    ``verification_schema`` is the inferred, advisory safety net used only when
    no authoritative ``schema`` is present. It gates *untrusted* (LLM-originated)
    corrections so they are checked against inferred type/domain/regex/FD rather
    than structurally auto-accepted. Deterministic fixes are correct by
    construction and are never second-guessed by it, which keeps schema-less
    deterministic runs byte-identical.

    When an authoritative ``schema`` is present and ``require_independent_agreement``
    is set, each candidate is verified by TWO independently-written checkers (the
    z3-backed ``SMTVerifier`` and the direct-evaluation ``DirectVerifier``) and is
    accepted only if both agree (fail-closed). A bug in either checker can then only
    withhold a fix for review, never wave through a corrupting one.
    """
    with repair_stage_span("propose", step="repairers_build", allow_llm=allow_llm):
        repairers = build_repairers(
            cache_dir=cache_dir_for(path),
            allow_llm=allow_llm,
            model=model,
            allow_entity_consensus=allow_entity_consensus,
            corrector_pool_constrained=corrector_pool_constrained,
            corrector_structured=corrector_structured,
        )
    safety_filter = SafetyFilter()
    verifier = SMTVerifier()
    safety_context = SafetyContext(
        allow_pii=allow_pii,
        confirm_pii=confirm_pii,
        confirm_escalations=confirm_escalations,
    )

    accepted_fixes: list[ProposedFix] = []
    attempt_groups: list[list[RepairAttempt]] = []

    for issue in issues:
        attempts: list[RepairAttempt] = []
        repairer = repairers.get(issue.issue_type)
        if repairer is None:
            if issue.issue_type in _DETECTION_ONLY_SUGGESTION_TYPES:
                # Surfaced as an unverified review suggestion (see
                # _detection_only_suggestions), not an auto-fix abstention. Keep the
                # attempt group aligned with issues without a misleading failure line.
                attempt_groups.append([])
                continue
            attempts.append(
                RepairAttempt(
                    issue=issue,
                    attempt_number=1,
                    status="attempted_not_fixed",
                    reason="No repairer is registered for this issue type.",
                )
            )
            attempt_groups.append(attempts)
            continue

        accepted = False
        retry_context = RetryContext(issue=issue)
        for attempt_number in range(1, 4):
            candidate = repairer.propose(issue, working_df, schema, retry_context=retry_context)
            if candidate is None:
                # A repairer that abstains AFTER a real failure must not erase why. The
                # summary block below only rewrites a trailing attempt whose status is not
                # already "attempted_not_fixed", so without this the verifier's reason --
                # the only text that says WHICH constraint blocked the repair -- is
                # replaced by "no proposal available". That was unreachable while every
                # repairer re-proposed its rejected value on retry; it is reachable now
                # that abstaining is the correct response to an exhausted candidate set.
                prior_failure = attempts[-1].reason if attempts else None
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        status="attempted_not_fixed",
                        reason=(
                            "No repair proposal was available for this issue."
                            if prior_failure is None
                            else (
                                f"Issue was attempted but not fixed after "
                                f"{len(attempts)} attempt(s). No further proposal was "
                                f"available. Last failure: {prior_failure}"
                            )
                        ),
                    )
                )
                break

            preferred = safety_filter.choose_preferred([candidate], schema, safety_context)
            safety_result = safety_filter.evaluate(preferred, schema, safety_context)
            if (
                safety_result.verdict == SafetyVerdict.ESCALATE
                and interactive
                and escalation_resolver is not None
            ):
                safety_context, safety_result = escalation_resolver(
                    preferred,
                    schema,
                    safety_context,
                    safety_filter,
                    safety_result,
                )

            if safety_result.verdict == SafetyVerdict.DENY:
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        fix=preferred,
                        status="denied",
                        reason=safety_result.reason,
                    )
                )
                retry_context = _build_retry_context(issue, attempts)
                continue

            if safety_result.verdict == SafetyVerdict.ESCALATE:
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        fix=preferred,
                        status="escalated",
                        reason=safety_result.reason,
                    )
                )
                break

            with repair_stage_span(
                "smt_verify",
                issue_type=issue.issue_type,
                row=issue.row,
            ):
                verifier_result = _verify_fix(
                    working_df,
                    preferred,
                    schema,
                    verifier=verifier,
                    verification_schema=verification_schema,
                    require_independent_agreement=require_independent_agreement,
                )
            if verifier_result.verdict == VerificationVerdict.ACCEPT:
                accepted_fixes.append(preferred)
                set_cell_value(
                    working_df,
                    preferred.fix.row,
                    preferred.fix.column,
                    preferred.fix.new_value,
                )
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        fix=preferred,
                        status="accepted",
                        reason=verifier_result.reason,
                    )
                )
                accepted = True
                break

            attempts.append(
                RepairAttempt(
                    issue=issue,
                    attempt_number=attempt_number,
                    fix=preferred,
                    status=(
                        "rejected"
                        if verifier_result.verdict == VerificationVerdict.REJECT
                        else "unknown"
                    ),
                    reason=verifier_result.reason,
                    unsat_core=verifier_result.unsat_core,
                )
            )
            retry_context = _build_retry_context(issue, attempts)

        if (
            not accepted
            and attempts
            and attempts[-1].status not in {"attempted_not_fixed", "escalated"}
        ):
            last_reason = attempts[-1].reason
            attempts[-1] = attempts[-1].model_copy(
                update={
                    "status": "attempted_not_fixed",
                    "reason": (
                        f"Issue was attempted but not fixed after {len(attempts)} attempt(s). "
                        f"Last failure: {last_reason}"
                    ),
                }
            )
        attempt_groups.append(attempts)

    return accepted_fixes, attempt_groups


_LLM_PROVENANCE_HISTORICAL_NOTE = (
    # The calibration-specific paths (_calibrated_confidence, drift guard, LLM
    # escalation, the partition's "needs a calibrated threshold" branch) use the
    # narrower calibrated set, because an external fix proven against an authoritative
    # schema auto-applies directly and carries no LLM calibration map.
    "see dataforge.domain.vocabulary.CALIBRATED_PROVENANCE"
)

# Untrusted provenance = an LLM-origin value, an externally-proposed value
# (verify_and_apply), OR a cross-row entity-consensus value. Untrusted fixes are
# ``plausibility_only`` unless verified against an authoritative schema. Entity
# consensus is evidence-strong (the value already exists in sibling rows) but NOT
# proof -- a wrong majority yields a wrong consensus -- so by the proven-only
# invariant it is held by default and auto-applies only under the explicit
# ``allow_unproven_autoapply`` opt-in (or when a declared schema proves it).
# NOTE: this is a SUPERSET of _LLM_PROVENANCE. The calibration-specific paths
# (_calibrated_confidence, drift guard, LLM escalation, the partition's
# "needs a calibrated threshold" branch) keep _LLM_PROVENANCE, because an external
# fix proven against an authoritative schema auto-applies directly and does not
# carry an LLM calibration map.
# Untrusted provenance = an LLM-origin value, an externally-proposed value
# (verify_and_apply), OR a cross-row entity-consensus value. Untrusted fixes are
# ``plausibility_only`` unless verified against an authoritative schema. Entity
# consensus is evidence-strong (the value already exists in sibling rows) but NOT
# proof -- a wrong majority yields a wrong consensus -- so by the proven-only
# invariant it is held by default and auto-applies only under the explicit
# ``allow_unproven_autoapply`` opt-in (or when a declared schema proves it).
#
# Both sets are now DEFINED in dataforge/domain/vocabulary.py and re-exported here
# under their historical private names. They were previously written out by hand in
# this module, in the certificate verifier, and in the browser -- and the copies
# disagreed three times, once in the certificate a third party reads to decide whether
# to trust a write. `_UNTRUSTED_PROVENANCE` remains a strict superset of
# `_LLM_PROVENANCE`; the vocabulary module asserts that relationship at import.
_LLM_PROVENANCE = CALIBRATED_PROVENANCE
_UNTRUSTED_PROVENANCE = UNTRUSTED_PROVENANCE

# How many offending cells an ``UnprovenWriteError`` names before truncating. A
# batch can be thousands of cells wide; the message exists to identify the class of
# problem, not to dump the whole queue into a traceback.
_UNPROVEN_WRITE_REPORT_LIMIT = 5

# Detection-only issue types that carry an exact value in ``Issue.expected`` but
# have NO registered repairer (no write path). They are surfaced as unverified
# human-review suggestions, never auto-applied. See DateTranspositionDetector.
#
# Membership requires that ``Issue.expected`` hold a **substitutable value**, not a
# description of one. Added 2026-09-01: `time_format_cruft`, whose expected is a time
# stripped of surrounding cruft (`"17:15"` out of `"departs at 17:15 sharp"`). It was
# computed and discarded before this line, because the detector emitted the shared
# `format_violation` id.
#
# `format_violation` itself is deliberately ABSENT and must stay absent: its expected is a
# shape mask (`"9999-99-99"`), so admitting it here would propose writing a format
# description into a user cell. That is the whole reason the two ids were split -- the
# guard on this path is `expected is None`, which a mask satisfies just as well as a value.
# tests/unit/test_expected_value_semantics.py derives this classification and fails if a
# new member is added without one.
_DETECTION_ONLY_SUGGESTION_TYPES = frozenset({"date_transposition", "time_format_cruft"})


FdDetectionSource = Literal["declared", "accepted", "none"]


def schema_for_fd_detection(
    schema: Schema | None,
    declared_schema: Schema | None,
    source: FdDetectionSource,
) -> Schema | None:
    """Restrict which functional dependencies are allowed to raise issues.

    ``fd_violation`` is the only detector that reads ``schema.functional_dependencies``
    (see ``dataforge/detectors/fd_violation.py``), and it is tier-0 ``UNSAFE`` at
    confidence 0.95, so it wins its cell outright against every other detector. An FD
    therefore does not merely add flags -- it displaces what other detectors would have
    said about the cells it covers.

    That matters because inferred FDs are cheap to accept and expensive to live with.
    Measured on hospital (``eval/results/detector_queue_composition.json``): accepting the
    mined FDs turns a 549-cell queue that is 56% real errors into a 10,373-cell queue that
    is 4.4% real errors -- **+147 true errors bought with +9,824 false positives**, and
    review effort degrading from 1.78 to 22.80 cells per real error.

    ``require_declared_fds_for_autoapply`` does not help here: it runs after detection and
    filters *fixes*, so it stops the machine writing while leaving every flag in the human
    queue. Before this function there was no control anywhere that gated FD **detection**.

    Args:
        schema: The effective schema (declared plus accepted reviewed constraints).
        declared_schema: The hand-declared schema only, or ``None``.
        source: ``accepted`` keeps every FD in ``schema`` (the historical default);
            ``declared`` keeps only FDs also present in ``declared_schema``; ``none``
            disables FD detection entirely.

    Returns:
        A schema with ``functional_dependencies`` narrowed as requested. All other schema
        content is preserved untouched, because no other detector output depends on it.
    """
    if schema is None or source == "accepted":
        return schema
    if source == "none":
        return replace(schema, functional_dependencies=())
    declared = (
        frozenset(declared_schema.functional_dependencies)
        if declared_schema is not None
        else frozenset()
    )
    kept = tuple(fd for fd in schema.functional_dependencies if fd in declared)
    return replace(schema, functional_dependencies=kept)


def fd_flag_cost(df: TableLike, schema: Schema) -> int:
    """Return how many distinct CELLS the schema's functional dependencies would flag.

    Free: one detector, no LLM and no proof. Exists so a user can be shown the queue cost
    of accepting an FD candidate *before* accepting it, rather than discovering a 19x queue
    afterwards.

    Counts distinct ``(row, column)`` cells, not raw issues. ``FDViolationDetector`` emits
    one issue per violated dependency, so a cell covered by several FDs appears many times
    -- on hospital the raw count is 50,721 against a real queue contribution of ~9,800.
    Reporting the raw number would overstate the cost roughly fivefold, which for a feature
    whose only purpose is an honest preview would be worse than showing nothing.

    **Measured accuracy**: a slight *over*estimate. On hospital this returns 10,192 against
    a true queue delta of 9,824 (+3.7%), because some FD-covered cells were already flagged
    by another detector, so accepting the FDs displaces those flags rather than adding to
    them. Erring high is the right direction for a cost warning, but do not present the
    figure as exact.
    """
    from dataforge.detectors.fd_violation import FDViolationDetector

    if not schema.functional_dependencies:
        return 0
    return len({(issue.row, issue.column) for issue in FDViolationDetector().detect(df, schema)})


def partition_auto_apply(
    fixes: list[ProposedFix],
    policy: AbstentionPolicy,
    *,
    covered_columns: frozenset[str],
    allow_unproven_autoapply: bool,
    calibration_map_by_class: dict[str, CalibrationMap] | None = None,
) -> tuple[list[ProposedFix], list[ProposedFix], list[ProposedFix]]:
    """Split verified fixes into (auto_apply, calibration_held, plausibility_held).

    The enforced product invariant: only PROVEN fixes auto-apply. A fix is proven when
    it is deterministic or was verified against an authoritative schema. A
    ``plausibility_only`` fix (an LLM value with no authoritative schema, checked only by
    the advisory inferred guard where the verifier-floor gaps live) is NEVER auto-applied
    unless ``allow_unproven_autoapply`` is explicitly set -- and then it is recorded
    truthfully as unproven.

    **Deterministic no longer means unconditional, and that fix is the reason this
    docstring changed.** It previously read "among proven fixes, deterministic ones
    always auto-apply", and the code was ``if deterministic or policy.action_for(...)``.
    The ``or`` short-circuited the calibration gate, so ``enabled_classes == []``
    protected nothing on the deterministic path. Measured consequence: a live
    ``dataforge repair --apply`` on a 25-row table with no errors rewrote a legitimate
    ``1131.20`` as ``113120`` -- a 100x monetary inflation -- recorded ``proven``, held
    back by nothing. On error-free TPC-H the same rule would rewrite 263,428 monetary
    values, and it has found **zero** true errors on hospital, flights or rayyan.

    A deterministic fix now bypasses calibration only when its detector is in
    :data:`CONSTRAINT_CHECKABLE_DETECTORS` -- an allowlist, so a detector nobody
    classified is calibration-bound rather than exempt. Detectors that infer a repair
    from the column's own distribution go through the same threshold as any other
    fallible source, which under the shipped propose-not-apply policy means they are
    held for review.

    When ``calibration_map_by_class`` is provided, an LLM fix's raw confidence is
    first rescaled through its per-issue-type post-hoc calibration map so the score
    matches the scale the conformal thresholds were certified on. This is a monotone
    transform: it can only make confidences honest, never re-rank them, so it cannot
    wave through a fix the raw policy would have held below a raw-scale threshold.

    ``covered_columns`` scopes authority per column (:func:`authoritative_columns`): a
    fix is only schema-proven if the schema constrains ITS column.
    """
    auto: list[ProposedFix] = []
    calibration_held: list[ProposedFix] = []
    plausibility_held: list[ProposedFix] = []
    for fix in fixes:
        strength = strength_for_fix(fix, covered_columns)
        if strength == "plausibility_only" and not allow_unproven_autoapply:
            plausibility_held.append(fix)
            continue
        # NOTE the misnomer, which caused a real mistake while writing this gate:
        # ``_LLM_PROVENANCE`` is only ``{llm_cache, llm_live}``, so "not an LLM" is TRUE for
        # ``external`` and ``entity_consensus`` as well as ``deterministic``. A first version
        # of the restriction below keyed off this flag and silently blocked the schema-proven
        # external write path, which is a legitimate premised write. Test the provenance you
        # mean.
        non_llm = fix.provenance not in _LLM_PROVENANCE
        if fix.provenance == "deterministic":
            # Determinism of the procedure is not soundness of the inference. A deterministic
            # repair may skip the calibrated threshold only when its rule is checkable against
            # a reference; one that infers a value from the column's own distribution must
            # earn a threshold like any other fallible source.
            bypasses_calibration = fix.fix.detector_id in CONSTRAINT_CHECKABLE_DETECTORS
        else:
            # ``external`` and ``entity_consensus`` are unchanged: they reached this line only
            # by being schema-proven or by an explicit recorded opt-in.
            bypasses_calibration = non_llm
        confidence = _calibrated_confidence(fix, calibration_map_by_class)
        if (
            bypasses_calibration
            or policy.action_for(fix.fix.detector_id, confidence) == "auto_apply"
        ):
            auto.append(fix)
        else:
            calibration_held.append(fix)
    return auto, calibration_held, plausibility_held


def _calibrated_confidence(
    fix: ProposedFix, calibration_map_by_class: dict[str, CalibrationMap] | None
) -> float:
    """Rescale an LLM fix's confidence through its per-issue-type calibration map.

    Deterministic fixes and fixes without a fitted map for their issue type are
    returned unchanged. The map is keyed by ``CellFix.detector_id`` (the issue type),
    matching how the certified per-class thresholds are keyed.
    """
    if calibration_map_by_class is None or fix.provenance not in _LLM_PROVENANCE:
        return fix.confidence
    calibration_map = calibration_map_by_class.get(fix.fix.detector_id)
    if calibration_map is None:
        return fix.confidence
    return calibration_map.predict(fix.confidence)


def _guard_corrector_policy_for_drift(
    policy: AbstentionPolicy,
    fixes: list[ProposedFix],
    reference_confidences: dict[str, list[float]] | None,
) -> AbstentionPolicy:
    """Downgrade the corrector policy to propose-not-apply under distribution drift.

    The conformal auto-apply guarantee holds only for data exchangeable with the
    calibration sample. Drift is judged **per issue type**, because certification is
    per issue type: :func:`guard_policy_for_drift_by_class` disables only the classes whose
    own confidence distribution has shifted, leaving the rest certified. Pooling every class
    into one PSI test (the previous behaviour) both masked single-class drift behind the
    aggregate histogram and let one drifted class needlessly disable every other.

    Falls back to the pooled comparison only when no class has enough live samples to judge,
    so a small run still gets some protection rather than none. A no-op when there is no
    reference or no LLM fix.
    """
    if not reference_confidences:
        return policy
    live_by_class: dict[str, list[float]] = {}
    for fix in fixes:
        if fix.provenance in _LLM_PROVENANCE:
            live_by_class.setdefault(fix.fix.detector_id, []).append(fix.confidence)
    if not live_by_class:
        return policy

    guarded, psi_by_class = guard_policy_for_drift_by_class(
        policy, reference_confidences, live_by_class
    )
    if psi_by_class:
        return guarded
    # No class had enough live samples to judge; fall back to the pooled test so a
    # small run is not left completely unguarded.
    reference = [conf for confs in reference_confidences.values() for conf in confs]
    live = [conf for confs in live_by_class.values() for conf in confs]
    if not reference or not live:
        return policy
    return guard_policy_for_drift(policy, reference, live)


def _escalated_llm_suggestions(
    attempt_groups: list[list[RepairAttempt]],
) -> list[ProposedFix]:
    """Collect LLM proposals blocked by the unconfirmed-LLM-write escalation.

    These passed the repairer but were not auto-applied because the safety
    constitution requires explicit confirmation for LLM writes. They are surfaced
    as suggestions so the value is visible without being silently applied.
    """
    suggestions: list[ProposedFix] = []
    for attempts in attempt_groups:
        for attempt in attempts:
            if (
                attempt.fix is not None
                and attempt.fix.provenance in _LLM_PROVENANCE
                and attempt.status == "escalated"
            ):
                suggestions.append(attempt.fix)
    return suggestions


def _suggestion_candidates(
    fixes: list[ProposedFix],
    *,
    review_reason: ReviewReason,
    verifier_reason: str,
) -> list[CandidateRepair]:
    """Build public suggestion payloads with a structured, honest review reason."""
    return [
        CandidateRepair(
            **CandidateFix.from_proposed(fix).model_dump(),
            verifier_reason=verifier_reason,
            review_reason=review_reason,
        )
        for fix in fixes
    ]


def _detection_only_suggestions(issues: list[Issue]) -> list[CandidateRepair]:
    """Surface detection-only issues that carry an exact value as review suggestions.

    Some detectors (e.g. :class:`DateTranspositionDetector`) can compute an exact
    corrected value yet cannot *prove* the cell needs it from in-table signal, so
    they ship with no repairer (no write path). Their fix value is carried in
    ``Issue.expected`` and surfaced here as an honest, never-auto-applied
    suggestion with a structured review reason.
    """
    suggestions: list[CandidateRepair] = []
    for issue in issues:
        if issue.issue_type not in _DETECTION_ONLY_SUGGESTION_TYPES or issue.expected is None:
            continue
        suggestions.append(
            CandidateRepair(
                row=issue.row,
                column=issue.column,
                old_value=issue.actual,
                new_value=issue.expected,
                detector_id=issue.issue_type,
                operation="update",
                reason=issue.reason,
                confidence=issue.confidence,
                provenance="deterministic",
                verifier_reason=(
                    "Held for human review: an exact, deterministic transform is "
                    "available, but whether this cell needs it is not provable in-table "
                    "(the value is already valid), so it is never auto-applied."
                ),
                review_reason="unverified_transposition",
            )
        )
    return suggestions


def authoritative_columns(schema: Schema | None) -> frozenset[str]:
    """Return the columns an authoritative schema actually constrains.

    ``authoritative_schema_present`` used to be a table-level boolean, and that was a
    real defect, verified end-to-end on 2026-08-09: accepting ONE inferred
    ``column_type`` candidate on column ``id`` produced an effective schema of
    ``{"id": "int"}`` with no other constraints, which flipped an ``external`` garbage
    fix on the UNRELATED column ``city`` from held to applied -- and stamped it
    ``proven`` in the certificate. Narrow evidence was granting blanket authority, and
    because ``external`` is not in ``_LLM_PROVENANCE`` it also bypassed the calibration
    threshold. That is a truthfulness violation, not merely an unproven write.

    Authority is therefore scoped to the columns the schema speaks about. A column counts
    as covered when the schema declares a **discriminating** type for it or names it in any
    constraint. A functional dependency covers its determinant AND its dependent, because a value
    in either position is checked by the FD.

    **A declared type is not automatically a constraint.** Listing a column as ``str`` says
    nothing that can reject a value, and counting it as authority was the next instance of the
    same defect one level down. Measured on ``eval/results/trust_ledger_adversarial.json``:
    declaring every column ``str`` let **10 of 14** constraint-violating attacks be written and
    stamped ``proven``, against 0 of 14 under a premise that actually constrained. See
    :func:`~dataforge.domain.vocabulary.type_discriminates` and
    ``eval/preregistration/entailment_strength.md``.
    """
    if schema is None:
        return frozenset()
    covered: set[str] = {
        column for column, declared in schema.columns.items() if type_discriminates(declared)
    }
    covered |= set(schema.pii_columns)
    covered |= set(schema.primary_key_columns)
    covered |= set(schema.not_null_columns)
    covered |= set(schema.unique_columns)
    covered |= {accepted.column for accepted in schema.accepted_values}
    covered |= {regex.column for regex in schema.regex_constraints}
    covered |= {bound.column for bound in schema.domain_bounds}
    covered |= {relationship.column for relationship in schema.relationships}
    for dependency in schema.aggregate_dependencies:
        covered.add(dependency.source_column)
        covered.add(dependency.target_column)
    for fd in schema.functional_dependencies:
        covered.update(fd.determinant)
        covered.add(fd.dependent)
    return frozenset(covered)


def verification_strength_for(
    provenance: str, *, authoritative_schema_present: bool
) -> VerificationStrength:
    """Classify how strongly a fix was verified.

    A fix is ``proven`` when it is deterministic (correct by construction) or was
    checked against an authoritative declared/reviewed schema. Otherwise it was
    only checked by the advisory inferred guard -> ``plausibility_only``. Untrusted
    origins (LLM or external) are proven only with an authoritative schema.

    ``authoritative_schema_present`` must be decided FOR THE FIX'S OWN COLUMN -- use
    :func:`strength_for_fix`, which does that. Passing a table-level boolean here grants
    authority over columns the schema never mentions; see :func:`authoritative_columns`.

    Delegates to the domain vocabulary so the engine, the certificate verifier, and the
    generated browser code cannot disagree about what "proven" means. The domain
    predicate also fails CLOSED: an unrecognised provenance is untrusted, where the
    previous ``not in _UNTRUSTED_PROVENANCE`` test read it as trustworthy.
    """
    return _domain_verification_strength_for(
        provenance,
        authoritative_schema_present=authoritative_schema_present,
    )


def strength_for_fix(fix: ProposedFix, covered_columns: frozenset[str]) -> VerificationStrength:
    """Column-scoped strength: authority only extends to columns the schema constrains."""
    return verification_strength_for(
        fix.provenance,
        authoritative_schema_present=fix.fix.column in covered_columns,
    )


def enforce_proven_only(
    fixes: list[ProposedFix],
    *,
    covered_columns: frozenset[str],
    allow_unproven_autoapply: bool,
) -> None:
    """Refuse to write a ``plausibility_only`` fix without the explicit opt-in.

    This is the proven-only invariant made *structural*. It is called from inside
    both mutation primitives -- :func:`apply_transaction` (CSV) and
    ``DuckDBStore.apply_patch_plan`` (warehouse SQL) -- so a write surface cannot
    bypass the gate by forgetting to call :func:`partition_auto_apply` first.
    That is not a hypothetical: the agent controller and the table-store repair
    path both did exactly that between 2026-07-11 and 2026-08-08, which is why
    the check now lives at the primitive instead of at each caller.

    Strength is **computed** from ``provenance`` here rather than read from
    ``ProposedFix.verification_strength``. That field is stamped late (only when a
    receipt is built) and is frequently ``None`` at write time, so trusting it
    would make the gate both unreliable and spoofable by any caller that set it.

    Args:
        fixes: The fixes about to be committed.
        covered_columns: The columns an authoritative declared/reviewed schema actually
            constrains (:func:`authoritative_columns`). A fix on a column outside this
            set was checked only by the advisory inferred guard, where the known
            verifier-floor gaps live. This is a SET rather than a boolean because a
            table-level flag let one accepted constraint grant authority over every
            column -- verified as a live defect on 2026-08-09.
        allow_unproven_autoapply: The caller's explicit acknowledgement that
            evidence-strong-but-unproven writes are permitted.

    Raises:
        UnprovenWriteError: If any fix is ``plausibility_only`` and the opt-in is
            not set. The message names the offending cells and the flag, because a
            caller hitting this needs to know which of the two it wants.
    """
    if allow_unproven_autoapply:
        return
    unproven = [
        fix for fix in fixes if strength_for_fix(fix, covered_columns) == "plausibility_only"
    ]
    if not unproven:
        return
    cells = ", ".join(
        f"row {fix.fix.row} column {fix.fix.column!r} (provenance {fix.provenance})"
        for fix in unproven[:_UNPROVEN_WRITE_REPORT_LIMIT]
    )
    if len(unproven) > _UNPROVEN_WRITE_REPORT_LIMIT:
        cells += f", and {len(unproven) - _UNPROVEN_WRITE_REPORT_LIMIT} more"
    raise UnprovenWriteError(
        f"Refusing to write {len(unproven)} unproven fix(es): {cells}. "
        "These values were checked only by the advisory inferred guard, not proven "
        "against an authoritative schema. Either supply a declared schema or pass "
        "allow_unproven_autoapply=True to accept them as unproven (they stay "
        "reversible and are recorded truthfully as not-proven in the certificate)."
    )


def enforce_constraint_checkable_only(fixes: list[ProposedFix]) -> None:
    """Refuse to write a deterministic fix whose detector is not constraint-checkable.

    The second half of the primitive-level write gate. :func:`enforce_proven_only`
    enforces the STRENGTH dimension (was this value checked against an authority);
    this enforces the SOUNDNESS dimension (can the procedure that produced it be
    checked against anything at all).

    The two are genuinely separable, which is why they are two functions and two
    exception types rather than one. A ``decimal_shift`` fix on a schema-covered column
    is ``proven`` by the strength predicate -- ``verification_strength_for`` returns
    ``proven`` for every ``deterministic`` provenance, schema or no schema -- and is
    still not something the product can stand behind, because the only evidence for it
    is the shape of the column's own distribution.

    There is deliberately no ``allow_*`` opt-in parameter. ``allow_unproven_autoapply``
    exists because an unproven write is a defensible product choice: the value stays
    reversible and the certificate records it honestly as not-proven. An
    uncheckable-detector write is not a defensible choice at any confidence, because
    there is no evidence to record -- the measured false-positive rate on realistically
    dispersed money columns was 263,428 cells across three TPC-H tables with zero true
    errors. A flag here would be a flag for corrupting data on request. Callers wanting
    the value must take it from the review queue, where it is surfaced honestly.

    Args:
        fixes: The fixes about to be committed.

    Raises:
        UncheckableDetectorWriteError: If any fix has ``deterministic`` provenance and a
            detector outside :data:`CONSTRAINT_CHECKABLE_DETECTORS`. The message names
            both the detector and the allowlist, because a caller hitting this needs to
            know which of the two gates refused and why.
    """
    uncheckable = [
        fix
        for fix in fixes
        if fix.provenance == "deterministic"
        and fix.fix.detector_id not in CONSTRAINT_CHECKABLE_DETECTORS
    ]
    if not uncheckable:
        return
    cells = ", ".join(
        f"row {fix.fix.row} column {fix.fix.column!r} (detector {fix.fix.detector_id})"
        for fix in uncheckable[:_UNPROVEN_WRITE_REPORT_LIMIT]
    )
    if len(uncheckable) > _UNPROVEN_WRITE_REPORT_LIMIT:
        cells += f", and {len(uncheckable) - _UNPROVEN_WRITE_REPORT_LIMIT} more"
    allowlist = ", ".join(sorted(CONSTRAINT_CHECKABLE_DETECTORS))
    raise UncheckableDetectorWriteError(
        f"Refusing to write {len(uncheckable)} fix(es) from a non-constraint-checkable "
        f"detector: {cells}. Only these deterministic detectors may write: {allowlist}. "
        "A deterministic procedure is not a sound inference: these values are inferred "
        "from the shape of the column's own distribution, not checked against a "
        "reference, so a correct cell in a widely dispersed column is indistinguishable "
        "from an error. The fix is still surfaced for human review; it is not applied."
    )


def _verified_fixes(
    fixes: list[ProposedFix],
    attempt_groups: list[list[RepairAttempt]],
    *,
    covered_columns: frozenset[str],
) -> list[VerifiedFix]:
    """Build public verified fix payloads using accepted attempt reasons."""
    accepted_reasons: dict[tuple[int, str, str], str] = {}
    for attempts in attempt_groups:
        for attempt in attempts:
            if attempt.status == "accepted" and attempt.fix is not None:
                fix = attempt.fix.fix
                accepted_reasons[(fix.row, fix.column, fix.new_value)] = attempt.reason

    return [
        VerifiedFix(
            **CandidateFix.from_proposed(fix).model_dump(),
            verifier_reason=accepted_reasons.get(
                (fix.fix.row, fix.fix.column, fix.fix.new_value),
                "Accepted by verifier.",
            ),
            verification_strength=strength_for_fix(fix, covered_columns),
        )
        for fix in fixes
    ]


def _candidate_repairs(attempt_groups: list[list[RepairAttempt]]) -> list[CandidateRepair]:
    """Build public candidate payloads for every emitted repair proposal."""
    candidates: list[CandidateRepair] = []
    for attempts in attempt_groups:
        for attempt in attempts:
            if attempt.fix is None:
                continue
            candidates.append(
                CandidateRepair(
                    **CandidateFix.from_proposed(attempt.fix).model_dump(),
                    verifier_reason=attempt.reason,
                )
            )
    return candidates


def _failed_attempts(attempt_groups: list[list[RepairAttempt]]) -> list[RepairFailure]:
    """Return failures for issue groups whose final status was not accepted."""
    return [
        RepairFailure.from_attempts(attempts)
        for attempts in attempt_groups
        if attempts and attempts[-1].status != "accepted"
    ]


def _root_cause_category(issue: Issue, attempts: list[RepairAttempt]) -> RootCauseCategory:
    """Classify the likely repair cause from deterministic issue evidence."""
    del attempts
    issue_type = issue.issue_type.lower()
    actual = str(issue.actual or "").strip().lower()
    expected = str(issue.expected or "").strip().lower()
    sentinel_values = {"", "na", "n/a", "null", "none", "nan", "nil", "-", "unknown"}

    if issue_type == "decimal_shift":
        return "decimal_shift"
    if issue_type in {"fd_violation", "functional_dependency"} or "fd" in issue_type:
        return "fd_conflict"
    if "domain" in issue_type or "accepted" in issue_type or "regex" in issue_type:
        return "domain_violation"
    if "duplicate" in issue_type or "unique" in issue_type or "key" in issue_type:
        return "duplicate_key_violation"
    if "relationship" in issue_type or "referential" in issue_type:
        return "referential_break"
    if actual in sentinel_values or expected in sentinel_values:
        return "sentinel_null_encoding"
    if issue_type in {"type_mismatch", "outlier"}:
        return "data_entry_error"
    return "unknown"


def _root_causes(
    issues: list[Issue],
    attempt_groups: list[list[RepairAttempt]],
) -> list[RootCause]:
    """Create one public root-cause diagnosis per detected issue."""
    diagnoses: list[RootCause] = []
    for issue, attempts in zip(issues, attempt_groups, strict=False):
        category = _root_cause_category(issue, attempts)
        final_reason = attempts[-1].reason if attempts else issue.reason
        diagnoses.append(
            RootCause(
                row=issue.row,
                column=issue.column,
                issue_type=issue.issue_type,
                category=category,
                confidence=issue.confidence,
                reason=final_reason if category == "unknown" else issue.reason,
            )
        )
    return diagnoses


def _proof_obligations(attempt_groups: list[list[RepairAttempt]]) -> list[ProofObligation]:
    """Expose every safety/verifier obligation evaluated during repair."""
    obligations: list[ProofObligation] = []
    for attempts in attempt_groups:
        for attempt in attempts:
            issue = attempt.issue
            if attempt.status in {"denied", "escalated"}:
                verifier: Literal["smt", "sql", "safety", "repairer"] = "safety"
            elif attempt.status == "attempted_not_fixed" and attempt.fix is None:
                verifier = "repairer"
            else:
                verifier = "smt"
            status: ProofStatus = (
                cast(ProofStatus, attempt.status)
                if attempt.status
                in {
                    "accepted",
                    "rejected",
                    "unknown",
                    "denied",
                    "escalated",
                    "attempted_not_fixed",
                }
                else "not_run"
            )
            obligations.append(
                ProofObligation(
                    obligation_id=(
                        f"{verifier}::{issue.issue_type}::{issue.row}::"
                        f"{issue.column}::attempt::{attempt.attempt_number}"
                    ),
                    verifier=verifier,
                    status=status,
                    reason=attempt.reason,
                    unsat_core=tuple(attempt.unsat_core),
                )
            )
    return obligations


def _receipt_limitations(
    request: RepairPipelineRequest,
    failures: list[RepairFailure],
    batch_safety: SafetyResult,
    txn_id: str | None,
    scope_guard_reason: str | None = None,
) -> list[str]:
    """Describe honest limits for the exact receipt payload."""
    limitations: list[str] = []
    if scope_guard_reason is not None:
        # A silent downgrade is the failure mode to avoid: the user would believe a
        # certificate applied when it had been withdrawn. Recorded in the receipt so the
        # withdrawal is durable evidence rather than a console line that scrolls away.
        limitations.append(f"Corrector auto-apply withdrawn: {scope_guard_reason}")
    if request.mode == "dry_run":
        limitations.append("Dry run only; no source data was mutated.")
    if txn_id is None:
        limitations.append("No reversible transaction id exists for this run.")
    if failures:
        limitations.append("Some issues abstained or failed repair; see failure_reasons.")
    if batch_safety.verdict != SafetyVerdict.ALLOW:
        limitations.append(batch_safety.reason)
    if request.allow_llm:
        limitations.append(
            "LLM-originated candidates remain subordinate to safety and verifier gates."
        )
    return limitations


def _patch_plan_sha256(source_sha256: str, fixes: list[ProposedFix]) -> str | None:
    """Return a stable hash for the ordered local patch plan."""
    if not fixes:
        return None
    payload = {
        "source_sha256": source_sha256,
        "fixes": [
            {
                "row": fix.fix.row,
                "column": fix.fix.column,
                "old_value": fix.fix.old_value,
                "new_value": fix.fix.new_value,
                "operation": fix.fix.operation,
                "detector_id": fix.fix.detector_id,
                "provenance": fix.provenance,
            }
            for fix in fixes
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _receipt_verifier_verdict(
    fixes: list[ProposedFix],
    failures: list[RepairFailure],
) -> str:
    """Summarize verifier outcomes for the public repair receipt."""
    statuses = {failure.status for failure in failures}
    if "unknown" in statuses:
        return "unknown"
    if "rejected" in statuses:
        return "reject"
    if fixes:
        return "accept"
    return "not_run"


_REVIEW_RANKER_DEFAULT_MAX_CELLS = 200


def _build_review_ranking(
    ranker: ReviewRanker,
    df: TableLike,
    issues: list[Issue],
    *,
    max_cells: int,
) -> list[ReviewRankedCell]:
    """Rank the top detected cells for human review (never auto-applied).

    Only the first ``max_cells`` issues (already sorted best-first by the
    detectors) are scored, bounding LLM cost. The returned annotation is
    presentation-only: it is attached to the receipt, never applied.
    """
    candidates = issues[:max_cells]
    reason_by_cell = {(issue.row, issue.column): issue.reason for issue in candidates}
    ranked = ranker.rank([(issue.row, issue.column) for issue in candidates], df)
    return [
        ReviewRankedCell(
            row=scored.row,
            column=scored.column,
            triage_score=scored.score,
            reason=reason_by_cell.get((scored.row, scored.column), ""),
        )
        for scored in ranked
    ]


def run_repair_pipeline(
    request: RepairPipelineRequest,
    *,
    review_ranker: ReviewRanker | None = None,
    review_ranker_max_cells: int = _REVIEW_RANKER_DEFAULT_MAX_CELLS,
) -> RepairPipelineResult:
    """Run the public repair pipeline from detection through optional apply.

    ``review_ranker`` is an opt-in, presentation-only add-on: when supplied, the
    top ``review_ranker_max_cells`` detected cells are scored and attached to the
    receipt as ``review_ranking`` (a human-review ordering). It never proposes,
    verifies, or applies anything - the verified apply path is untouched. When
    ``None`` (the default) the receipt's ``review_ranking`` is empty and behavior
    is byte-identical to before.
    """
    source_path = request.source_path.resolve()
    source_bytes = source_path.read_bytes()
    source_sha256 = sha256_bytes(source_bytes)
    effective_schema, accepted_constraint_ids = merge_schema_with_reviewed_constraints(
        request.repair_schema,
        request.constraints,
        source_sha256=source_sha256,
    )
    df = read_csv(source_path)
    # Narrow which FDs may raise issues BEFORE detection. This is the only place the
    # queue-volume cost of an inferred FD can be controlled; the auto-apply guard below
    # runs too late to prevent flags.
    detection_schema = schema_for_fd_detection(
        effective_schema, request.repair_schema, request.fd_detection_source
    )
    with repair_stage_span("detect", row_count=row_count(df)):
        issues = run_all_detectors(df, detection_schema)
    review_ranking = (
        _build_review_ranking(review_ranker, df, issues, max_cells=review_ranker_max_cells)
        if review_ranker is not None
        else []
    )
    # Inferred, advisory safety net for untrusted corrections when no
    # authoritative schema exists. Never drives repairs or raises issues; only
    # gates LLM-originated values in propose_repairs.
    verification_schema = infer_verification_schema(df) if effective_schema is None else None
    with repair_stage_span("propose", issue_count=len(issues)):
        accepted_fixes, attempt_groups = propose_repairs(
            issues,
            source_path,
            copy_table(df),
            effective_schema,
            allow_llm=request.allow_llm,
            model=request.model,
            allow_pii=request.allow_pii,
            confirm_pii=request.confirm_pii,
            confirm_escalations=request.confirm_escalations,
            interactive=request.interactive,
            verification_schema=verification_schema,
            require_independent_agreement=request.require_independent_agreement,
            allow_entity_consensus=request.allow_entity_consensus,
            corrector_pool_constrained=request.corrector_pool_constrained,
            corrector_structured=request.corrector_structured,
        )

    # Route LLM-origin corrections: auto-apply only when a calibrated per-class
    # threshold is cleared; otherwise (and by default) hold them as suggestions.
    # Fixes blocked by the unconfirmed-LLM-write escalation also become
    # suggestions. Deterministic fixes are unaffected -> allow_llm=False runs
    # stay byte-identical.
    corrector_policy = request.corrector_policy or corrector_default_policy()
    authoritative_schema_present = effective_schema is not None
    # Per-column authority. This path is where the table-level flag was most dangerous:
    # ``external`` provenance is NOT in _LLM_PROVENANCE, so a fix labelled proven here
    # auto-applies immediately without needing to clear any calibration threshold.
    covered_columns = authoritative_columns(effective_schema)
    # Authority is per COLUMN, not per table: a schema that constrains one column says
    # nothing about any other. See authoritative_columns() for the defect this fixes.
    covered_columns = authoritative_columns(effective_schema)
    scope_guard_reason: str | None = None
    # Scope guard, before drift. A conformal certificate is valid only for data
    # exchangeable with its calibration sample, and the cheapest decidable necessary
    # condition is that the table still has the same shape. This runs FIRST because the
    # drift guard is a no-op without a reference histogram, so an artifact fitted on another
    # table and carrying no reference was previously guarded by nothing at all.
    #
    # Skipped under allow_unproven_autoapply because that mode does not rest on a
    # certificate: the user has explicitly opted into unproven auto-apply and the receipt
    # records the provenance as plausibility_only. There is no certificate claim to keep
    # inside its scope, so enforcing one here would be theatre.
    if not (request.corrector_scope_verified or request.allow_unproven_autoapply):
        corrector_policy, scope_reason = guard_policy_for_scope(
            corrector_policy, request.corrector_calibration_scope, df
        )
        if scope_reason is not None:
            scope_guard_reason = scope_reason
    # PSI drift guard: the conformal auto-apply guarantee is only valid for data
    # exchangeable with the calibration sample. If the live LLM-confidence
    # distribution has drifted from the calibration reference, downgrade to
    # propose-not-apply so the certificate is never claimed outside its scope.
    corrector_policy = _guard_corrector_policy_for_drift(
        corrector_policy, accepted_fixes, request.corrector_reference_confidences
    )
    # Declared-FD-only opt-in (constraint circularity, option B): in strict mode a
    # fd_violation correction auto-applies only when its dependent column is covered
    # by a HAND-DECLARED FD. A correction justified only by an inferred (reviewed or
    # not) FD is held -- because an approximate inferred FD can be coincidental and
    # its majority-repair would overwrite legitimate variation. Off by default.
    inferred_fd_held: list[ProposedFix] = []
    if request.require_declared_fds_for_autoapply:
        declared_fd_dependents = (
            frozenset(fd.dependent for fd in request.repair_schema.functional_dependencies)
            if request.repair_schema is not None
            else frozenset()
        )
        retained: list[ProposedFix] = []
        for fix in accepted_fixes:
            if (
                fix.fix.detector_id == "fd_violation"
                and fix.fix.column not in declared_fd_dependents
            ):
                inferred_fd_held.append(fix)
            else:
                retained.append(fix)
        accepted_fixes = retained
    accepted_fixes, calibration_suggestions, plausibility_suggestions = partition_auto_apply(
        accepted_fixes,
        corrector_policy,
        covered_columns=covered_columns,
        allow_unproven_autoapply=request.allow_unproven_autoapply,
        calibration_map_by_class=request.calibration_map_by_class,
    )
    escalated_suggestions = _escalated_llm_suggestions(attempt_groups)

    with repair_stage_span("safety_gate", fixes_count=len(accepted_fixes)):
        batch_safety = SafetyFilter().evaluate_batch(
            accepted_fixes,
            SafetyContext(confirm_escalations=request.confirm_escalations),
        )
    failures = _failed_attempts(attempt_groups)
    transaction: RepairTransaction | None = None
    txn_id: str | None = None
    post_sha256: str | None = None
    applied = False
    reason = "No accepted fixes were produced."

    if batch_safety.verdict != SafetyVerdict.ALLOW:
        accepted_fixes = []
        reason = batch_safety.reason
    elif request.mode == "apply" and accepted_fixes:
        txn_id = apply_transaction(
            source_path,
            accepted_fixes,
            source_bytes,
            covered_columns=covered_columns,
            allow_unproven_autoapply=request.allow_unproven_autoapply,
        )
        post_sha256 = sha256_file(source_path)
        applied = True
        reason = f"Applied {len(accepted_fixes)} fix(es)."
    elif request.create_dry_run_transaction:
        transaction, _log_path = create_repair_transaction(
            source_path, accepted_fixes, source_bytes
        )
        txn_id = transaction.txn_id
        reason = (
            "Dry run completed without mutating the source file."
            if accepted_fixes
            else "No accepted fixes were produced."
        )
    elif accepted_fixes:
        reason = "Dry run completed without mutating the source file."

    if txn_id is not None and transaction is None:
        # Replaying the log is unnecessary for the public contract here; this
        # minimal receipt is intentionally enough for API callers.
        transaction = None

    verified_fixes = _verified_fixes(
        accepted_fixes,
        attempt_groups,
        covered_columns=covered_columns,
    )
    candidate_repairs = _candidate_repairs(attempt_groups)
    suggestion_candidates = (
        _suggestion_candidates(
            calibration_suggestions,
            review_reason="failed_conformal_threshold",
            verifier_reason=(
                "Held for human review: correction is verified-plausible but its "
                "calibrated confidence did not clear the auto-apply threshold."
            ),
        )
        + _suggestion_candidates(
            [f for f in plausibility_suggestions if f.provenance != "entity_consensus"],
            review_reason="floor_cannot_verify",
            verifier_reason=(
                "Held for human review: only the advisory inferred guard could "
                "vouch for this value (no authoritative schema); it is not proven, "
                "so it is never auto-applied unless allow_unproven_autoapply is set."
            ),
        )
        + _suggestion_candidates(
            [f for f in plausibility_suggestions if f.provenance == "entity_consensus"],
            review_reason="unverified_entity_consensus",
            verifier_reason=(
                "Held for human review: the exact value is the strong cross-row "
                "consensus for this entity (it already exists in sibling rows), but "
                "a majority can be wrong, so it is not proven and is never auto-applied "
                "unless allow_unproven_autoapply is set."
            ),
        )
        + _suggestion_candidates(
            escalated_suggestions,
            review_reason="safety_escalation",
            verifier_reason=(
                "Held for human review: an LLM-origin write requires explicit "
                "confirmation (unconfirmed-LLM-write safety rule)."
            ),
        )
        + _suggestion_candidates(
            inferred_fd_held,
            review_reason="inferred_fd_not_declared",
            verifier_reason=(
                "Held for human review: this FD correction is justified only by an "
                "inferred functional dependency, and require_declared_fds_for_autoapply "
                "is set. Inferred FDs can be coincidental (constraint circularity), so "
                "strict mode never auto-applies a correction without a declared FD."
            ),
        )
        + _detection_only_suggestions(issues)
    )
    proof_obligations = _proof_obligations(attempt_groups)
    root_causes = _root_causes(issues, attempt_groups)
    limitations = _receipt_limitations(request, failures, batch_safety, txn_id, scope_guard_reason)
    patch_plan_sha256 = _patch_plan_sha256(source_sha256, accepted_fixes)
    with repair_stage_span(
        "receipt",
        issues_count=len(issues),
        fixes_count=len(accepted_fixes),
        applied=applied,
    ):
        receipt = RepairReceipt(
            mode=request.mode,
            applied=applied,
            reversible=True,
            source_path=str(source_path),
            source_sha256=source_sha256,
            post_sha256=post_sha256,
            txn_id=txn_id,
            allowed_columns=column_names(df),
            authoritative_columns=sorted(covered_columns),
            valid_rows=list(range(row_count(df))),
            safety_verdict=batch_safety.verdict.value,
            verifier_verdict=_receipt_verifier_verdict(accepted_fixes, failures),
            independent_verification=(
                "agreed"
                if (authoritative_schema_present and request.require_independent_agreement)
                else "not_run"
            ),
            candidate_provenance=sorted({fix.provenance for fix in accepted_fixes}),
            root_causes=root_causes,
            candidate_repairs=candidate_repairs,
            suggested_fixes=suggestion_candidates,
            applied_fixes=verified_fixes if applied else [],
            proof_obligations=proof_obligations,
            accepted_constraint_ids=accepted_constraint_ids,
            constraints_artifact_sha256=request.constraints_artifact_sha256,
            patch_plan_sha256=patch_plan_sha256,
            revert_command=f"dataforge revert {txn_id}" if txn_id is not None else None,
            limitations=limitations,
            abstentions=[failure.reason for failure in failures],
            failure_reasons=[failure.reason for failure in failures],
            review_ranking=review_ranking,
            issues_count=len(issues),
            fixes_count=len(accepted_fixes),
            reason=reason,
        )
    return RepairPipelineResult(
        receipt=receipt,
        issues=issues,
        fixes=verified_fixes,
        failures=failures,
        transaction=transaction,
    )


class ExternalFix(BaseModel):
    """A single cell edit proposed by an external actor (agent, tool, or human).

    ``expected_old_value`` is an optional compare-and-set precondition: when set,
    the fix is rejected as stale if the current cell value differs (preventing a
    lost update when the data changed since the actor read it).
    """

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    new_value: str
    expected_old_value: str | None = None

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class VerifyAndApplyRequest(BaseModel):
    """Input contract for verifying and applying externally-proposed fixes.

    External values are UNTRUSTED: each proposed fix runs the same safety
    constitution and prove-gate as an internal repair, is applied only inside a
    reversible, hash-chained transaction, and yields the same self-verifying
    certificate. A fix auto-applies only when it (a) clears the unconfirmed-write
    escalation (``confirm_escalations``) and (b) is proven -- verified against an
    authoritative schema. Without a schema it is held for review unless the
    explicit ``allow_unproven_autoapply`` opt-in is set.
    """

    source_path: Path
    fixes: list[ExternalFix]
    mode: RepairMode = "dry_run"
    repair_schema: Schema | None = Field(default=None, alias="schema")
    constraints: ConstraintReviewArtifact | None = None
    proposer: str = Field(default="external", min_length=1)
    confirm_escalations: bool = False
    allow_unproven_autoapply: bool = False
    require_independent_agreement: bool = True

    model_config = ConfigDict(
        strict=True,
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        populate_by_name=True,
    )


def _external_proposed(external: ExternalFix, *, old_value: str, proposer: str) -> ProposedFix:
    """Wrap an external cell edit as an untrusted ProposedFix."""
    return ProposedFix(
        fix=CellFix(
            row=external.row,
            column=external.column,
            old_value=old_value,
            new_value=external.new_value,
            detector_id="external",
        ),
        reason=f"External proposal by {proposer!r}.",
        confidence=1.0,
        provenance="external",
    )


def verify_and_apply(request: VerifyAndApplyRequest) -> RepairPipelineResult:
    """Verify externally-proposed fixes through the shared gate and apply the proven ones.

    This is the verification-layer entry: any external actor proposes cell edits
    and DataForge proves each safe (same safety constitution + ``_verify_fix``
    prove gate as internal repairs), applies only the proven ones inside a
    reversible transaction, and returns the same ``repair_receipt_v1`` certificate.
    Held and rejected fixes are surfaced with honest review reasons; nothing
    untrusted is silently written.
    """
    source_path = request.source_path.resolve()
    source_bytes = source_path.read_bytes()
    source_sha256 = sha256_bytes(source_bytes)
    effective_schema, accepted_constraint_ids = merge_schema_with_reviewed_constraints(
        request.repair_schema,
        request.constraints,
        source_sha256=source_sha256,
    )
    df = read_csv(source_path)
    working_df = copy_table(df)
    columns = set(column_names(df))
    total_rows = row_count(df)
    authoritative_schema_present = effective_schema is not None
    covered_columns = authoritative_columns(effective_schema)
    verification_schema = infer_verification_schema(df) if effective_schema is None else None

    safety_filter = SafetyFilter()
    verifier = SMTVerifier()
    safety_context = SafetyContext(confirm_escalations=request.confirm_escalations)

    verified: list[ProposedFix] = []
    invalid_target: list[ProposedFix] = []
    stale: list[ProposedFix] = []
    safety_denied: list[ProposedFix] = []
    safety_escalated: list[ProposedFix] = []
    verifier_rejected: list[ProposedFix] = []
    seen_cells: set[tuple[int, str]] = set()
    noop_count = 0

    for external in request.fixes:
        cell = (external.row, external.column)
        # Invalid or conflicting target: unknown column, out-of-range row, or a
        # duplicate edit to a cell already targeted this batch.
        if external.column not in columns or external.row >= total_rows or cell in seen_cells:
            invalid_target.append(
                _external_proposed(
                    external, old_value=external.expected_old_value or "", proposer=request.proposer
                )
            )
            continue
        seen_cells.add(cell)
        current = cell_value(working_df, external.row, external.column)
        # Compare-and-set precondition (optional): reject stale writes.
        if external.expected_old_value is not None and current != external.expected_old_value:
            stale.append(_external_proposed(external, old_value=current, proposer=request.proposer))
            continue
        # No-op: proposed value already present.
        if external.new_value == current:
            noop_count += 1
            continue

        candidate = _external_proposed(external, old_value=current, proposer=request.proposer)
        preferred = safety_filter.choose_preferred([candidate], effective_schema, safety_context)
        safety_result = safety_filter.evaluate(preferred, effective_schema, safety_context)
        if safety_result.verdict == SafetyVerdict.DENY:
            safety_denied.append(preferred)
            continue
        if safety_result.verdict == SafetyVerdict.ESCALATE:
            safety_escalated.append(preferred)
            continue
        verifier_result = _verify_fix(
            working_df,
            preferred,
            effective_schema,
            verifier=verifier,
            verification_schema=verification_schema,
            require_independent_agreement=request.require_independent_agreement,
        )
        if verifier_result.verdict != VerificationVerdict.ACCEPT:
            verifier_rejected.append(preferred)
            continue
        verified.append(preferred)
        set_cell_value(working_df, preferred.fix.row, preferred.fix.column, preferred.fix.new_value)

    # Untrusted-partition: external fixes are proven (auto-apply) only under an
    # authoritative schema; otherwise held unless the explicit opt-in is set.
    auto, calibration_held, plausibility_held = partition_auto_apply(
        verified,
        corrector_default_policy(),
        covered_columns=covered_columns,
        allow_unproven_autoapply=request.allow_unproven_autoapply,
    )

    batch_safety = safety_filter.evaluate_batch(auto, safety_context)
    txn_id: str | None = None
    post_sha256: str | None = None
    applied = False
    if batch_safety.verdict != SafetyVerdict.ALLOW:
        auto = []
        reason = batch_safety.reason
    elif request.mode == "apply" and auto:
        txn_id = apply_transaction(
            source_path,
            auto,
            source_bytes,
            covered_columns=covered_columns,
            allow_unproven_autoapply=request.allow_unproven_autoapply,
        )
        post_sha256 = sha256_file(source_path)
        applied = True
        reason = f"Applied {len(auto)} proven external fix(es) proposed by {request.proposer!r}."
    elif auto:
        reason = f"Dry run: {len(auto)} external fix(es) are proven; no source data was mutated."
    else:
        reason = "No external fix was auto-applied (see suggested_fixes for held/rejected)."

    verified_fixes = [
        VerifiedFix(
            **CandidateFix.from_proposed(fix).model_dump(),
            verifier_reason="Accepted by the safety constitution and the shared prove gate.",
            verification_strength=verification_strength_for(
                fix.provenance, authoritative_schema_present=authoritative_schema_present
            ),
        )
        for fix in auto
    ]

    suggestion_candidates = (
        _suggestion_candidates(
            plausibility_held,
            review_reason="floor_cannot_verify",
            verifier_reason=(
                "Held for human review: an external value with no authoritative schema is not "
                "proven; it is never auto-applied unless allow_unproven_autoapply is set."
            ),
        )
        + _suggestion_candidates(
            calibration_held,
            review_reason="failed_conformal_threshold",
            verifier_reason="Held for human review: verified-plausible but not proven for auto-apply.",
        )
        + _suggestion_candidates(
            safety_escalated,
            review_reason="safety_escalation",
            verifier_reason=(
                "Held for human review: an external write requires explicit confirmation "
                "(unconfirmed-write safety rule). Re-run with confirm_escalations to proceed."
            ),
        )
        + _suggestion_candidates(
            safety_denied,
            review_reason="safety_denied",
            verifier_reason="Rejected: the safety constitution denied this external write.",
        )
        + _suggestion_candidates(
            verifier_rejected,
            review_reason="verifier_rejected",
            verifier_reason="Rejected: the prove gate could not verify this external value as safe.",
        )
        + _suggestion_candidates(
            stale,
            review_reason="stale_precondition",
            verifier_reason=(
                "Rejected: expected_old_value did not match the current cell (stale write "
                "avoided). Re-read the current value and resubmit."
            ),
        )
        + _suggestion_candidates(
            invalid_target,
            review_reason="invalid_target",
            verifier_reason="Rejected: unknown column, out-of-range row, or duplicate cell edit.",
        )
    )

    receipt = RepairReceipt(
        mode=request.mode,
        applied=applied,
        reversible=True,
        source_path=str(source_path),
        source_sha256=source_sha256,
        post_sha256=post_sha256,
        txn_id=txn_id,
        allowed_columns=column_names(df),
        authoritative_columns=sorted(covered_columns),
        valid_rows=list(range(total_rows)),
        safety_verdict=batch_safety.verdict.value,
        verifier_verdict="accept" if auto else "not_run",
        independent_verification=(
            "agreed"
            if (authoritative_schema_present and request.require_independent_agreement and auto)
            else "not_run"
        ),
        candidate_provenance=sorted({fix.provenance for fix in auto}),
        suggested_fixes=suggestion_candidates,
        applied_fixes=verified_fixes if applied else [],
        accepted_constraint_ids=accepted_constraint_ids,
        revert_command=f"dataforge revert {txn_id}" if txn_id is not None else None,
        limitations=(
            [f"{noop_count} external fix(es) were no-ops (value already present)."]
            if noop_count
            else []
        ),
        issues_count=len(request.fixes),
        fixes_count=len(auto),
        reason=reason,
    )
    return RepairPipelineResult(
        receipt=receipt,
        issues=[],
        fixes=verified_fixes,
        failures=[],
        transaction=None,
    )
