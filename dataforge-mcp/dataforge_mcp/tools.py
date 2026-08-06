"""Structured MCP tool functions backed by DataForge's public API."""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from dataforge import (
    CONTRACT_VERSION,
    CellFix,
    ExternalFix,
    Issue,
    ProposedFix,
    RepairPipelineRequest,
    ReviewRanker,
    SafetyContext,
    SafetyFilter,
    SafetyVerdict,
    Schema,
    SMTVerifier,
    TransactionLogError,
    VerificationVerdict,
    VerifiedFix,
    VerifyAndApplyRequest,
    load_schema,
    read_csv,
    revert_transaction,
    run_all_detectors,
    run_repair_pipeline,
    verify_and_apply,
)
from dataforge.ui.trust_vocab import humanize_review_reason

_APPLY_ENABLED = False
_ALLOWED_ROOTS: tuple[Path, ...] | None = None


class IssueResult(BaseModel):
    """MCP-safe representation of a DataForge issue."""

    row: int
    column: str
    issue_type: str
    severity: str
    confidence: float
    expected: str | None
    actual: str
    reason: str


class RankedCellResult(BaseModel):
    """A review-queue triage score for one flagged cell.

    Deliberately carries no candidate value: a triage score orders a human's
    queue and must not be mistakable for something applicable.
    """

    row: int
    column: str
    score: float
    provenance: str


class FixResult(BaseModel):
    """MCP-safe representation of an accepted repair proposal."""

    row: int
    column: str
    old_value: str
    new_value: str
    detector_id: str
    operation: str
    reason: str
    confidence: float
    provenance: str


class ProfileResult(BaseModel):
    """Structured result returned by the profile tool."""

    path: str
    rows: int
    columns: int
    column_names: list[str]
    total_issues: int
    issues: list[IssueResult]


class VerifyFixResult(BaseModel):
    """Structured result returned by the fix verifier tool."""

    accept: bool
    reason: str
    safety_verdict: str | None = None
    verifier_verdict: str | None = None
    unsat_core: list[str] = Field(default_factory=list)


class TxnReceipt(BaseModel):
    """Structured receipt returned by the repair tool."""

    path: str
    schema_version: Literal["repair_receipt_v1"] = "repair_receipt_v1"
    receipt_version: Literal["repair_receipt_v1"] = "repair_receipt_v1"
    mode: Literal["dry_run", "apply"]
    contract_version: str = CONTRACT_VERSION
    applied: bool
    txn_id: str | None
    reversible: bool
    source_sha256: str
    post_sha256: str | None = None
    safety_verdict: str
    verifier_verdict: str
    patch_plan_sha256: str | None = None
    revert_command: str | None = None
    allowed_columns: list[str]
    valid_rows: list[int]
    root_causes: list[dict[str, Any]] = Field(default_factory=list)
    candidate_repairs: list[dict[str, Any]] = Field(default_factory=list)
    proof_obligations: list[dict[str, Any]] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    issues_count: int
    fixes_count: int
    reason: str
    fixes: list[FixResult]


class RevertReceipt(BaseModel):
    """Structured receipt returned by the revert tool."""

    txn_id: str
    source_path: str
    restored: bool
    reverted_at: str | None
    reason: str


def configure_mcp_security(
    *,
    enable_apply: bool = False,
    allowed_roots: Sequence[str | Path] | None = None,
) -> None:
    """Configure process-wide MCP path and apply safety settings."""
    global _APPLY_ENABLED, _ALLOWED_ROOTS
    _APPLY_ENABLED = enable_apply
    if allowed_roots is None:
        _ALLOWED_ROOTS = None
        return
    _ALLOWED_ROOTS = tuple(Path(root).expanduser().resolve() for root in allowed_roots)


def _env_flag_enabled(name: str) -> bool:
    """Return whether an environment flag is truthy."""
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _apply_is_enabled() -> bool:
    """Return whether MCP apply mode is explicitly enabled."""
    return _APPLY_ENABLED or _env_flag_enabled("DATAFORGE_MCP_ENABLE_APPLY")


def _allowed_roots() -> tuple[Path, ...]:
    """Return configured allowed filesystem roots for MCP file access."""
    raw_roots = os.environ.get("DATAFORGE_MCP_ALLOWED_ROOTS", "")
    if raw_roots.strip():
        return tuple(
            Path(root).expanduser().resolve()
            for root in raw_roots.split(os.pathsep)
            if root.strip()
        )
    if _ALLOWED_ROOTS is not None:
        return _ALLOWED_ROOTS
    return (Path.cwd().resolve(),)


def _ensure_under_allowed_root(path: Path) -> Path:
    """Reject paths outside the configured MCP allowlist."""
    resolved = path.expanduser().resolve()
    roots = _allowed_roots()
    if not roots:
        raise ValueError("At least one MCP allowed root must be configured.")
    for root in roots:
        if resolved == root or resolved.is_relative_to(root):
            return resolved
    allowed = ", ".join(str(root) for root in roots)
    raise ValueError(
        f"Path is outside configured MCP allowed roots: {resolved}. Allowed: {allowed}"
    )


def _resolve_csv_path(path: str) -> Path:
    """Resolve and validate a CSV path supplied by an MCP client."""
    resolved = _ensure_under_allowed_root(Path(path))
    if not resolved.exists():
        raise ValueError(f"CSV file does not exist: {resolved}")
    if not resolved.is_file():
        raise ValueError(f"CSV path is not a file: {resolved}")
    return resolved


def _load_optional_schema(raw_path: object) -> Schema | None:
    """Load an optional schema path from an untrusted payload."""
    if raw_path is None:
        return None
    schema_path = _ensure_under_allowed_root(Path(str(raw_path)))
    if not schema_path.exists():
        raise ValueError(f"Schema file does not exist: {schema_path}")
    return load_schema(schema_path)


def _issue_to_result(issue: Issue) -> IssueResult:
    """Convert a DataForge issue into a stable MCP payload."""
    return IssueResult(
        row=issue.row,
        column=issue.column,
        issue_type=issue.issue_type,
        severity=issue.severity.value,
        confidence=issue.confidence,
        expected=issue.expected,
        actual=issue.actual,
        reason=issue.reason,
    )


def _fix_to_result(proposed_fix: ProposedFix) -> FixResult:
    """Convert a proposed fix into a stable MCP payload."""
    fix = proposed_fix.fix
    return FixResult(
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


def _verified_fix_to_result(verified_fix: VerifiedFix) -> FixResult:
    """Convert a public engine verified fix into a stable MCP payload."""
    return FixResult(
        row=verified_fix.row,
        column=verified_fix.column,
        old_value=verified_fix.old_value,
        new_value=verified_fix.new_value,
        detector_id=verified_fix.detector_id,
        operation=verified_fix.operation,
        reason=verified_fix.reason,
        confidence=verified_fix.confidence,
        provenance=verified_fix.provenance,
    )


def _run_detection(path: Path, schema: Schema | None = None) -> tuple[Any, list[Issue]]:
    """Read a CSV and run all DataForge detectors."""
    df = read_csv(path)
    return df, run_all_detectors(df, schema)


def _proposed_fix_from_spec(fix_spec: dict[str, Any]) -> tuple[Path, Schema | None, ProposedFix]:
    """Parse a verifier payload into a CSV path, optional schema, and fix."""
    raw_path = fix_spec.get("path")
    if not raw_path:
        raise ValueError("fix_spec must include a CSV 'path'.")
    path = _resolve_csv_path(str(raw_path))
    schema = _load_optional_schema(fix_spec.get("schema_path"))
    raw_fix = fix_spec.get("fix")
    if not isinstance(raw_fix, dict):
        raw_fix = {
            key: value
            for key, value in fix_spec.items()
            if key in {"row", "column", "old_value", "new_value", "detector_id", "operation"}
        }
    cell_fix = CellFix.model_validate(raw_fix)
    proposed = ProposedFix(
        fix=cell_fix,
        reason=str(fix_spec.get("reason", "MCP-provided candidate fix.")),
        confidence=float(fix_spec.get("confidence", 1.0)),
        provenance=fix_spec.get("provenance", "deterministic"),
    )
    return path, schema, proposed


def dataforge_profile(path: str) -> ProfileResult:
    """Profile a CSV file and return detected DataForge issues."""
    csv_path = _resolve_csv_path(path)
    df, issues = _run_detection(csv_path)
    return ProfileResult(
        path=str(csv_path),
        rows=len(df.index),
        columns=len(df.columns),
        column_names=[str(column) for column in df.columns],
        total_issues=len(issues),
        issues=[_issue_to_result(issue) for issue in issues],
    )


def dataforge_detect_errors(path: str) -> list[IssueResult]:
    """Detect data-quality errors in a CSV file."""
    csv_path = _resolve_csv_path(path)
    _df, issues = _run_detection(csv_path)
    return [_issue_to_result(issue) for issue in issues]


def dataforge_verify_fix(fix_spec: dict[str, Any]) -> VerifyFixResult:
    """Verify whether one candidate fix may be accepted by DataForge gates."""
    path, schema, proposed = _proposed_fix_from_spec(fix_spec)
    df = read_csv(path)
    fix = proposed.fix
    if fix.column not in df.columns:
        return VerifyFixResult(accept=False, reason=f"Column '{fix.column}' does not exist.")
    if fix.row < 0 or fix.row >= len(df.index):
        return VerifyFixResult(accept=False, reason=f"Row {fix.row} is out of bounds.")
    current_value = str(df.at[fix.row, fix.column])
    if current_value != fix.old_value:
        return VerifyFixResult(
            accept=False,
            reason=(
                f"Refusing stale fix for row {fix.row}, column '{fix.column}': "
                f"expected '{fix.old_value}', found '{current_value}'."
            ),
        )

    safety_result = SafetyFilter().evaluate(proposed, schema, SafetyContext())
    if safety_result.verdict != SafetyVerdict.ALLOW:
        return VerifyFixResult(
            accept=False,
            reason=safety_result.reason,
            safety_verdict=safety_result.verdict.value,
        )

    verifier_result = SMTVerifier().verify(df, [proposed], schema)
    return VerifyFixResult(
        accept=verifier_result.verdict == VerificationVerdict.ACCEPT,
        reason=verifier_result.reason,
        safety_verdict=safety_result.verdict.value,
        verifier_verdict=verifier_result.verdict.value,
        unsat_core=list(verifier_result.unsat_core),
    )


def dataforge_review_rank(path: str, max_cells: int = 25) -> list[RankedCellResult]:
    """Order a review queue by how likely each flagged cell is a real error.

    Read-only triage, not repair. Detected cells are scored by a grounded
    yes/no LLM vote and returned highest-score-first, so a human reviews likely
    true errors before false positives. Measured on a flooded queue this lifted
    review precision from ~5% to ~41%; on an already-precise queue it adds
    nothing, so firing it is an explicit choice rather than an automatic one.

    Nothing here can write: a score carries no candidate value, and this tool
    never touches the transaction or apply path.

    Args:
        path: CSV file to triage.
        max_cells: Maximum number of flagged cells to score (bounds LLM spend).

    Returns:
        Ranked cells, highest triage score first.
    """
    csv_path = _resolve_csv_path(path)
    df, issues = _run_detection(csv_path)
    cells = [(issue.row, issue.column) for issue in issues][: max(0, max_cells)]
    if not cells:
        return []
    # cache_dir=None: MCP integrations may only use the root public facade, and
    # the per-file cache helper is internal. MCP calls are short-lived, so the
    # lost cache reuse is marginal; the CLI path (inside the package) caches.
    ranker = ReviewRanker(cache_dir=None)
    scored = ranker.rank(cells, df)
    ordered = sorted(scored, key=lambda s: (-s.score, s.row, s.column))
    return [
        RankedCellResult(
            row=s.row,
            column=s.column,
            score=round(s.score, 4),
            provenance=s.provenance,
        )
        for s in ordered
    ]


def dataforge_apply_repairs(path: str, mode: Literal["dry_run", "apply"]) -> TxnReceipt:
    """Detect, verify, and optionally apply DataForge repairs to a CSV file."""
    csv_path = _resolve_csv_path(path)
    if mode not in {"dry_run", "apply"}:
        raise ValueError("mode must be 'dry_run' or 'apply'.")
    if mode == "apply" and not _apply_is_enabled():
        raise ValueError(
            "MCP apply mode is disabled. Start the server with --enable-apply or set "
            "DATAFORGE_MCP_ENABLE_APPLY=1."
        )

    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=csv_path,
            mode=mode,
            schema=None,
            allow_llm=False,
        )
    )
    receipt = result.receipt
    return TxnReceipt(
        path=str(csv_path),
        mode=mode,
        applied=receipt.applied,
        txn_id=receipt.txn_id,
        reversible=receipt.reversible,
        source_sha256=receipt.source_sha256,
        post_sha256=receipt.post_sha256,
        safety_verdict=receipt.safety_verdict,
        verifier_verdict=receipt.verifier_verdict,
        patch_plan_sha256=receipt.patch_plan_sha256,
        revert_command=receipt.revert_command,
        allowed_columns=receipt.allowed_columns,
        valid_rows=receipt.valid_rows,
        root_causes=[item.model_dump() for item in receipt.root_causes],
        candidate_repairs=[item.model_dump() for item in receipt.candidate_repairs],
        proof_obligations=[item.model_dump() for item in receipt.proof_obligations],
        limitations=receipt.limitations,
        issues_count=receipt.issues_count,
        fixes_count=receipt.fixes_count,
        reason=receipt.reason,
        fixes=[_verified_fix_to_result(fix) for fix in result.fixes],
    )


class SuggestedFixResult(BaseModel):
    """A held-or-rejected external fix with an honest review reason."""

    row: int
    column: str
    old_value: str
    new_value: str
    review_reason: str
    review_reason_human: str = ""
    verifier_reason: str


class VerifyAndApplyReceipt(BaseModel):
    """Structured receipt returned by the external verify-and-apply tool."""

    path: str
    schema_version: Literal["repair_receipt_v1"] = "repair_receipt_v1"
    mode: Literal["dry_run", "apply"]
    applied: bool
    txn_id: str | None
    reversible: bool
    source_sha256: str
    post_sha256: str | None = None
    safety_verdict: str
    verifier_verdict: str
    revert_command: str | None = None
    proposer: str
    applied_fixes: list[FixResult] = Field(default_factory=list)
    suggested_fixes: list[SuggestedFixResult] = Field(default_factory=list)
    fixes_count: int
    issues_count: int
    limitations: list[str] = Field(default_factory=list)
    reason: str


def dataforge_verify_and_apply(
    path: str,
    fixes: list[dict[str, Any]],
    mode: Literal["dry_run", "apply"] = "dry_run",
    schema_path: str | None = None,
    proposer: str = "external",
    confirm: bool = False,
    allow_unproven: bool = False,
) -> VerifyAndApplyReceipt:
    """Verify externally-proposed cell fixes and apply only the proven ones.

    Each fix (``{"row", "column", "new_value", "expected_old_value"?}``) runs the
    same safety constitution and prove gate as an internal repair. A fix is
    auto-applied only when it clears the unconfirmed-write escalation (``confirm``)
    and is proven -- verified against an authoritative ``schema_path``. Without a
    schema, fixes are held for review unless ``allow_unproven`` is set. Every
    applied change is reversible and certified; held/rejected fixes are returned in
    ``suggested_fixes`` with an honest review reason. ``expected_old_value`` is an
    optional compare-and-set precondition that rejects stale writes.
    """
    csv_path = _resolve_csv_path(path)
    if mode not in {"dry_run", "apply"}:
        raise ValueError("mode must be 'dry_run' or 'apply'.")
    if mode == "apply" and not _apply_is_enabled():
        raise ValueError(
            "MCP apply mode is disabled. Start the server with --enable-apply or set "
            "DATAFORGE_MCP_ENABLE_APPLY=1."
        )
    schema = _load_optional_schema(schema_path)
    external_fixes = [
        ExternalFix(
            row=int(spec["row"]),
            column=str(spec["column"]),
            new_value=str(spec["new_value"]),
            expected_old_value=(
                None if spec.get("expected_old_value") is None else str(spec["expected_old_value"])
            ),
        )
        for spec in fixes
    ]
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=csv_path,
            fixes=external_fixes,
            mode=mode,
            schema=schema,
            proposer=proposer,
            confirm_escalations=confirm,
            allow_unproven_autoapply=allow_unproven,
        )
    )
    receipt = result.receipt
    return VerifyAndApplyReceipt(
        path=str(csv_path),
        mode=mode,
        applied=receipt.applied,
        txn_id=receipt.txn_id,
        reversible=receipt.reversible,
        source_sha256=receipt.source_sha256,
        post_sha256=receipt.post_sha256,
        safety_verdict=receipt.safety_verdict,
        verifier_verdict=receipt.verifier_verdict,
        revert_command=receipt.revert_command,
        proposer=proposer,
        applied_fixes=[_verified_fix_to_result(fix) for fix in result.fixes],
        suggested_fixes=[
            SuggestedFixResult(
                row=candidate.row,
                column=candidate.column,
                old_value=candidate.old_value,
                new_value=candidate.new_value,
                review_reason=str(candidate.review_reason),
                review_reason_human=humanize_review_reason(candidate.review_reason),
                verifier_reason=candidate.verifier_reason,
            )
            for candidate in receipt.suggested_fixes
        ],
        fixes_count=receipt.fixes_count,
        issues_count=receipt.issues_count,
        limitations=receipt.limitations,
        reason=receipt.reason,
    )


class AgentRepairRecord(BaseModel):
    """One step of the verified agent's audit trace."""

    step: int
    action_type: str
    accepted: bool | None = None
    detail: str


class AgentRepairReceipt(BaseModel):
    """Structured receipt returned by the verified agent repair tool."""

    path: str
    mode: Literal["dry_run", "apply"]
    applied: bool
    reversible: bool
    txn_id: str | None
    revert_command: str | None
    source_sha256: str
    post_sha256: str | None
    policy_name: str
    steps_used: int
    max_steps: int
    floor_fix_count: int
    agent_fix_count: int
    fixes_count: int
    residual_count: int
    issues_count: int
    safety_verdict: str
    reason: str
    fixes: list[FixResult]
    trace: list[AgentRepairRecord]


def dataforge_agent_repair(
    path: str,
    mode: Literal["dry_run", "apply"] = "dry_run",
    policy: str = "hosted",
    provider: str | None = None,
    model: str | None = None,
    max_steps: int = 30,
    confirm_escalations: bool = True,
) -> AgentRepairReceipt:
    """Run the verified autonomous agent: deterministic floor then LLM residual.

    The agent applies the deterministic repairers first (high-accuracy floor),
    then an autonomous policy resolves the remaining issues. Every write is
    gated by the safety constitution and the SMT verifier and committed through
    a reversible transaction, so the agent can only add proven-safe fixes.

    Args:
        path: CSV path (must be under an allowed MCP root).
        mode: ``dry_run`` (default) or ``apply``.
        policy: Agent backend — ``hosted`` provider (default; needs a server-side
            API key), ``local`` trained model, ``deterministic`` (floor only,
            no LLM), or ``custom:<name>`` for a registered policy.
        provider: Hosted provider override (``groq`` or ``gemini``); falls back
            to ``DATAFORGE_LLM_PROVIDER`` / key autodetect.
        model: Model id override for the hosted provider. When omitted, falls back
            to ``DATAFORGE_<PROVIDER>_MODEL`` and then the provider default.
        max_steps: Maximum agent reasoning steps.
        confirm_escalations: Acknowledge that live LLM-originated writes are
            auto-confirmed past the soft safety-escalation gate. Required for an
            LLM policy to apply fixes autonomously.

    Returns:
        A structured receipt with the verified fixes, audit trace, and revert
        command.
    """
    from dataforge.agent import AgentRepairRequest, run_agent_repair

    csv_path = _resolve_csv_path(path)
    if mode not in {"dry_run", "apply"}:
        raise ValueError("mode must be 'dry_run' or 'apply'.")
    if mode == "apply" and not _apply_is_enabled():
        raise ValueError(
            "MCP apply mode is disabled. Start the server with --enable-apply or set "
            "DATAFORGE_MCP_ENABLE_APPLY=1."
        )

    result = run_agent_repair(
        AgentRepairRequest(
            source_path=csv_path,
            mode=mode,
            schema=None,
            policy=policy,
            provider=provider,
            model=model,
            max_steps=max_steps,
            confirm_escalations=confirm_escalations,
        )
    )
    return AgentRepairReceipt(
        path=str(csv_path),
        mode=result.mode,
        applied=result.applied,
        reversible=result.reversible,
        txn_id=result.txn_id,
        revert_command=result.revert_command,
        source_sha256=result.source_sha256,
        post_sha256=result.post_sha256,
        policy_name=result.policy_name,
        steps_used=result.steps_used,
        max_steps=result.max_steps,
        floor_fix_count=result.floor_fix_count,
        agent_fix_count=result.agent_fix_count,
        fixes_count=result.fixes_count,
        residual_count=result.residual_count,
        issues_count=result.issues_count,
        safety_verdict=result.safety_verdict,
        reason=result.reason,
        fixes=[_verified_fix_to_result(fix) for fix in result.fixes],
        trace=[
            AgentRepairRecord(
                step=record.step,
                action_type=record.action_type,
                accepted=record.accepted,
                detail=record.detail,
            )
            for record in result.trace
        ],
    )


def dataforge_revert(txn_id: str) -> RevertReceipt:
    """Revert a previously applied DataForge repair transaction."""
    transaction = None
    last_error: Exception | None = None
    for root in _allowed_roots():
        try:
            transaction = revert_transaction(txn_id, search_root=root)
            break
        except TransactionLogError as exc:
            last_error = exc
            continue
    if transaction is None:
        if last_error is not None:
            raise ValueError(str(last_error)) from last_error
        raise ValueError(f"Could not find transaction '{txn_id}' under configured allowed roots.")
    return RevertReceipt(
        txn_id=transaction.txn_id,
        source_path=transaction.source_path,
        restored=transaction.reverted_at is not None,
        reverted_at=transaction.reverted_at.isoformat() if transaction.reverted_at else None,
        reason="Source restored successfully.",
    )
