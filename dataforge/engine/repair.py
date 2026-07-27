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
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from dataforge.calibration import (
    AbstentionPolicy,
    corrector_default_policy,
    guard_policy_for_drift,
)
from dataforge.calibration_map import CalibrationMap
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema
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

# Machine-parseable, honest reason a proposed fix was NOT auto-applied. This is
# the "honestly flags the rest" half of the trust promise: every held fix says
# WHY, so a human (or downstream automation) can triage without guessing.
ReviewReason = Literal[
    "failed_conformal_threshold",
    "safety_escalation",
    "not_inferable_from_data",
    "floor_cannot_verify",
    "ambiguous_fd",
    "out_of_inferred_domain",
    "unverified_transposition",
    "unverified_entity_consensus",
    "inferred_fd_not_declared",
    "stale_precondition",
    "invalid_target",
    "safety_denied",
    "verifier_rejected",
]

# How strongly a fix was verified, which decides whether it may auto-apply.
# "proven"  -> deterministic (correct by construction) OR verified against an
#              authoritative declared/reviewed schema (real SMT constraints).
# "plausibility_only" -> no authoritative schema AND an LLM-origin value, so it
#              was only checked by the advisory inferred guard (where the known
#              verifier-floor gaps live). Never auto-applied unless explicitly
#              opted in, and then recorded truthfully as unproven.
VerificationStrength = Literal["proven", "plausibility_only"]


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
    allow_entity_consensus: bool = False
    require_independent_agreement: bool = True
    interactive: bool = False
    create_dry_run_transaction: bool = False
    constraints: ConstraintReviewArtifact | None = None
    constraints_artifact_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    corrector_policy: AbstentionPolicy | None = None
    calibration_map_by_class: dict[str, CalibrationMap] | None = None
    corrector_reference_confidences: dict[str, list[float]] | None = None

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


def apply_fixes_to_csv(path: Path, fixes: list[CellFix]) -> str:
    """Atomically apply ordered cell fixes to a CSV and return post-state SHA-256."""
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


def apply_transaction(
    path: Path,
    fixes: list[ProposedFix],
    source_bytes: bytes,
    *,
    txn_id: str | None = None,
) -> str:
    """Journal, snapshot, atomically apply fixes, and restore bytes on failure."""
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
        try:
            with repair_stage_span("transaction_apply", fixes_count=len(fixes)):
                post_sha256 = apply_fixes_to_csv(
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
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        status="attempted_not_fixed",
                        reason="No repair proposal was available for this issue.",
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


_LLM_PROVENANCE = frozenset({"llm_live", "llm_cache"})

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
_UNTRUSTED_PROVENANCE = frozenset({"llm_live", "llm_cache", "external", "entity_consensus"})

# Detection-only issue types that carry an exact value in ``Issue.expected`` but
# have NO registered repairer (no write path). They are surfaced as unverified
# human-review suggestions, never auto-applied. See DateTranspositionDetector.
_DETECTION_ONLY_SUGGESTION_TYPES = frozenset({"date_transposition"})


def _partition_auto_apply(
    fixes: list[ProposedFix],
    policy: AbstentionPolicy,
    *,
    authoritative_schema_present: bool,
    allow_unproven_autoapply: bool,
    calibration_map_by_class: dict[str, CalibrationMap] | None = None,
) -> tuple[list[ProposedFix], list[ProposedFix], list[ProposedFix]]:
    """Split verified fixes into (auto_apply, calibration_held, plausibility_held).

    The enforced product invariant: only PROVEN fixes auto-apply. A fix is proven
    when it is deterministic (correct by construction) or was verified against an
    authoritative schema. A ``plausibility_only`` fix (an LLM value with no
    authoritative schema, checked only by the advisory inferred guard where the
    verifier-floor gaps live) is NEVER auto-applied unless ``allow_unproven_autoapply``
    is explicitly set -- and then it is recorded truthfully as unproven.

    Among proven fixes, deterministic ones always auto-apply; LLM-proven ones
    auto-apply only when their calibrated confidence clears the per-class
    threshold, else they are held for calibration review. With the default
    propose-not-apply policy every LLM fix is held.

    When ``calibration_map_by_class`` is provided, an LLM fix's raw confidence is
    first rescaled through its per-issue-type post-hoc calibration map so the score
    matches the scale the conformal thresholds were certified on. This is a monotone
    transform: it can only make confidences honest, never re-rank them, so it cannot
    wave through a fix the raw policy would have held below a raw-scale threshold.
    """
    auto: list[ProposedFix] = []
    calibration_held: list[ProposedFix] = []
    plausibility_held: list[ProposedFix] = []
    for fix in fixes:
        strength = _verification_strength(
            fix.provenance, authoritative_schema_present=authoritative_schema_present
        )
        if strength == "plausibility_only" and not allow_unproven_autoapply:
            plausibility_held.append(fix)
            continue
        deterministic = fix.provenance not in _LLM_PROVENANCE
        confidence = _calibrated_confidence(fix, calibration_map_by_class)
        if deterministic or policy.action_for(fix.fix.detector_id, confidence) == "auto_apply":
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
    calibration sample. We PSI-compare the live LLM-confidence distribution (raw
    confidences of LLM-provenance fixes this run) against the pooled calibration
    reference; if the shift exceeds the PSI threshold, :func:`guard_policy_for_drift`
    returns the conservative propose-not-apply policy so the certificate is never
    claimed outside its scope. A no-op when there is no reference, no LLM fix, or the
    policy is already the conservative default.
    """
    if not reference_confidences:
        return policy
    reference = [conf for confs in reference_confidences.values() for conf in confs]
    live = [fix.confidence for fix in fixes if fix.provenance in _LLM_PROVENANCE]
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


def _verification_strength(
    provenance: str, *, authoritative_schema_present: bool
) -> VerificationStrength:
    """Classify how strongly a fix was verified.

    A fix is ``proven`` when it is deterministic (correct by construction) or was
    checked against an authoritative declared/reviewed schema. Otherwise it was
    only checked by the advisory inferred guard -> ``plausibility_only``. Untrusted
    origins (LLM or external) are proven only with an authoritative schema.
    """
    if provenance not in _UNTRUSTED_PROVENANCE or authoritative_schema_present:
        return "proven"
    return "plausibility_only"


def _verified_fixes(
    fixes: list[ProposedFix],
    attempt_groups: list[list[RepairAttempt]],
    *,
    authoritative_schema_present: bool,
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
            verification_strength=_verification_strength(
                fix.provenance, authoritative_schema_present=authoritative_schema_present
            ),
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
) -> list[str]:
    """Describe honest limits for the exact receipt payload."""
    limitations: list[str] = []
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
    with repair_stage_span("detect", row_count=row_count(df)):
        issues = run_all_detectors(df, effective_schema)
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
        )

    # Route LLM-origin corrections: auto-apply only when a calibrated per-class
    # threshold is cleared; otherwise (and by default) hold them as suggestions.
    # Fixes blocked by the unconfirmed-LLM-write escalation also become
    # suggestions. Deterministic fixes are unaffected -> allow_llm=False runs
    # stay byte-identical.
    corrector_policy = request.corrector_policy or corrector_default_policy()
    authoritative_schema_present = effective_schema is not None
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
    accepted_fixes, calibration_suggestions, plausibility_suggestions = _partition_auto_apply(
        accepted_fixes,
        corrector_policy,
        authoritative_schema_present=authoritative_schema_present,
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
        txn_id = apply_transaction(source_path, accepted_fixes, source_bytes)
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
        authoritative_schema_present=authoritative_schema_present,
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
    limitations = _receipt_limitations(request, failures, batch_safety, txn_id)
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
    auto, calibration_held, plausibility_held = _partition_auto_apply(
        verified,
        corrector_default_policy(),
        authoritative_schema_present=authoritative_schema_present,
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
        txn_id = apply_transaction(source_path, auto, source_bytes)
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
            verification_strength=_verification_strength(
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
