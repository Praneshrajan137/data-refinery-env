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
    Issue,
    ProposedFix,
    RepairPipelineRequest,
    SafetyContext,
    SafetyFilter,
    SafetyVerdict,
    Schema,
    SMTVerifier,
    TransactionLogError,
    VerificationVerdict,
    VerifiedFix,
    load_schema,
    read_csv,
    revert_transaction,
    run_all_detectors,
    run_repair_pipeline,
)

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
