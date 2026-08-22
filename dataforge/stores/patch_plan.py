"""Backend-neutral patch plan models for verified tabular repairs."""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from dataforge.transactions.txn import CellFix

PATCH_PLAN_SCHEMA_VERSION: Literal["patch_plan_v1"] = "patch_plan_v1"


class RowIdentity(BaseModel):
    """Stable row locator required before a table-store repair can apply."""

    kind: Literal["csv_position", "column_values", "unavailable"]
    columns: tuple[str, ...] = Field(default_factory=tuple)
    values: dict[str, str] = Field(default_factory=dict)
    stable: bool = False
    reason: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid", frozen=True)


class PatchOperation(BaseModel):
    """One cell-level mutation in a backend-neutral repair plan."""

    operation: Literal["update"] = "update"
    relation: str = Field(min_length=1)
    row: int = Field(ge=0)
    row_identity: RowIdentity
    column: str = Field(min_length=1)
    old_value: str
    new_value: str
    detector_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    provenance: str = Field(min_length=1)
    precondition_sql: str | None = None
    forward_sql: str | None = None
    rollback_sql: str | None = None
    verification_sql: tuple[str, ...] = Field(default_factory=tuple)

    model_config = ConfigDict(extra="forbid", frozen=True)

    @classmethod
    def from_cell_fix(
        cls,
        fix: CellFix,
        *,
        relation: str,
        row_identity: RowIdentity,
        reason: str,
        confidence: float,
        provenance: str,
        precondition_sql: str | None = None,
        forward_sql: str | None = None,
        rollback_sql: str | None = None,
        verification_sql: tuple[str, ...] = (),
    ) -> PatchOperation:
        """Build an operation from an existing DataForge cell fix."""
        if fix.operation != "update":
            raise ValueError("PatchPlan v1 supports cell updates only.")
        return cls(
            relation=relation,
            row=fix.row,
            row_identity=row_identity,
            column=fix.column,
            old_value=fix.old_value,
            new_value=fix.new_value,
            detector_id=fix.detector_id,
            reason=reason,
            confidence=confidence,
            provenance=provenance,
            precondition_sql=precondition_sql,
            forward_sql=forward_sql,
            rollback_sql=rollback_sql,
            verification_sql=verification_sql,
        )


class CostEstimate(BaseModel):
    """Small, backend-agnostic estimate surfaced before mutation."""

    rows_scanned: int = Field(ge=0)
    rows_written: int = Field(ge=0)
    bytes_scanned: int | None = Field(default=None, ge=0)
    quota_units: float = Field(default=0.0, ge=0.0)

    model_config = ConfigDict(extra="forbid", frozen=True)


class PatchPlan(BaseModel):
    """The only write contract accepted by non-CSV DataForge stores."""

    schema_version: Literal["patch_plan_v1"] = PATCH_PLAN_SCHEMA_VERSION
    plan_id: str = Field(pattern=r"^plan-[0-9a-f]{12}$")
    created_at: datetime
    backend: str = Field(min_length=1)
    target: str = Field(min_length=1)
    relation: str = Field(min_length=1)
    row_identity_columns: tuple[str, ...] = Field(default_factory=tuple)
    stable_row_identity: bool
    operations: tuple[PatchOperation, ...] = Field(default_factory=tuple)
    forward_sql: tuple[str, ...] = Field(default_factory=tuple)
    rollback_sql: tuple[str, ...] = Field(default_factory=tuple)
    preflight_probes: tuple[str, ...] = Field(default_factory=tuple)
    verification_queries: tuple[str, ...] = Field(default_factory=tuple)
    touched_constraints: tuple[str, ...] = Field(default_factory=tuple)
    smt_obligations: tuple[str, ...] = Field(default_factory=tuple)
    cost_estimate: CostEstimate
    safety_verdict: str = Field(min_length=1)
    reversible: bool
    apply_supported: bool
    apply_requires_approval: bool = True
    # Whether an authoritative declared/reviewed schema backed verification. The
    # plan carries this so the *apply* primitive can enforce the proven-only
    # invariant from the plan alone (see ``enforce_plan_proven_only``) instead of
    # trusting each calling surface to have gated the fixes first.
    authoritative_schema_present: bool = False
    # The columns the authoritative schema actually constrains. Carried alongside the
    # boolean because authority is per-column: one accepted constraint must not grant
    # proven status to a fix on a column the schema never mentions.
    authoritative_columns: tuple[str, ...] = ()
    audit_metadata: dict[str, str] = Field(default_factory=dict)
    reason: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid", frozen=True)

    @classmethod
    def new(
        cls,
        *,
        backend: str,
        target: str,
        relation: str,
        row_identity_columns: tuple[str, ...],
        operations: tuple[PatchOperation, ...],
        safety_verdict: str,
        rows_scanned: int,
        reason: str,
        touched_constraints: tuple[str, ...] = (),
        smt_obligations: tuple[str, ...] = (),
        audit_metadata: dict[str, str] | None = None,
        apply_supported: bool | None = None,
        reversible: bool | None = None,
        authoritative_schema_present: bool = False,
        authoritative_columns: tuple[str, ...] = (),
    ) -> PatchPlan:
        """Construct a stable plan with derived SQL and support flags."""
        stable = bool(row_identity_columns) and all(op.row_identity.stable for op in operations)
        has_operations = bool(operations)
        supported = stable and has_operations if apply_supported is None else apply_supported
        is_reversible = supported if reversible is None else reversible
        return cls(
            plan_id=f"plan-{secrets.token_hex(6)}",
            created_at=datetime.now(UTC),
            backend=backend,
            target=target,
            relation=relation,
            row_identity_columns=row_identity_columns,
            stable_row_identity=stable,
            operations=operations,
            forward_sql=tuple(sql for op in operations if (sql := op.forward_sql)),
            rollback_sql=tuple(sql for op in operations if (sql := op.rollback_sql)),
            preflight_probes=tuple(sql for op in operations if (sql := op.precondition_sql)),
            verification_queries=tuple(query for op in operations for query in op.verification_sql),
            touched_constraints=touched_constraints,
            smt_obligations=smt_obligations,
            cost_estimate=CostEstimate(rows_scanned=rows_scanned, rows_written=len(operations)),
            safety_verdict=safety_verdict,
            reversible=is_reversible,
            apply_supported=supported,
            authoritative_schema_present=authoritative_schema_present,
            authoritative_columns=authoritative_columns,
            audit_metadata=audit_metadata or {},
            reason=reason,
        )

    def canonical_json(self) -> str:
        """Return deterministic JSON suitable for audit hashing."""
        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))

    def sha256(self) -> str:
        """Return a SHA-256 digest of the canonical plan."""
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def enforce_plan_proven_only(plan: PatchPlan, *, allow_unproven_autoapply: bool) -> None:
    """Refuse to apply a plan containing ``plausibility_only`` operations.

    The table-store counterpart of :func:`dataforge.engine.repair.enforce_proven_only`.
    A warehouse write does not go through ``apply_transaction`` -- it is raw SQL
    issued by the backend adapter -- so the proven-only invariant has to be enforced
    at that primitive too, or the whole gate is only as good as the calling surface
    remembering to partition first. It did not remember: ``run_table_store_repair``
    wrote LLM-derived values via SQL UPDATE with no strength check at all.

    Strength is derived from each operation's ``provenance`` plus the plan's
    ``authoritative_schema_present``, using the same single rule the CSV path uses,
    so the two primitives cannot drift apart on what "proven" means.

    Raises:
        TableStoreError: If any operation is unproven and the opt-in is not set.
    """
    from dataforge.engine.repair import verification_strength_for
    from dataforge.stores.base import TableStoreError

    if allow_unproven_autoapply:
        return
    covered = frozenset(plan.authoritative_columns)
    unproven = [
        op
        for op in plan.operations
        if verification_strength_for(
            op.provenance,
            # Per-COLUMN authority: the schema must constrain THIS operation's column.
            authoritative_schema_present=op.column in covered,
        )
        == "plausibility_only"
    ]
    if not unproven:
        return
    cells = ", ".join(
        f"row {op.row} column {op.column!r} (provenance {op.provenance})" for op in unproven[:5]
    )
    if len(unproven) > 5:
        cells += f", and {len(unproven) - 5} more"
    raise TableStoreError(
        f"Refusing to write {len(unproven)} unproven operation(s) to {plan.target}: "
        f"{cells}. These values were checked only by the advisory inferred guard, not "
        "proven against an authoritative schema. Either supply a declared schema or "
        "pass allow_unproven_autoapply=True to accept them as unproven."
    )


def enforce_plan_constraint_checkable_only(plan: PatchPlan) -> None:
    """Refuse to apply a plan containing a non-constraint-checkable deterministic op.

    The table-store counterpart of
    :func:`dataforge.engine.repair.enforce_constraint_checkable_only`, and the same
    argument for existing: the warehouse path issues raw SQL rather than going through
    ``apply_transaction``, so a gate that lives only on the CSV primitive leaves this
    surface open. Both primitives read the one allowlist in the domain vocabulary, so
    they cannot disagree about which detectors may write.

    Unlike the strength gate above there is no ``allow_unproven_autoapply`` escape: see
    :func:`dataforge.engine.repair.enforce_constraint_checkable_only` for why an opt-in
    would amount to a flag for corrupting data on request.

    Raises:
        TableStoreError: If any operation has ``deterministic`` provenance and a detector
            outside ``CONSTRAINT_CHECKABLE_DETECTORS``.
    """
    from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS
    from dataforge.stores.base import TableStoreError

    uncheckable = [
        op
        for op in plan.operations
        if op.provenance == "deterministic" and op.detector_id not in CONSTRAINT_CHECKABLE_DETECTORS
    ]
    if not uncheckable:
        return
    cells = ", ".join(
        f"row {op.row} column {op.column!r} (detector {op.detector_id})" for op in uncheckable[:5]
    )
    if len(uncheckable) > 5:
        cells += f", and {len(uncheckable) - 5} more"
    allowlist = ", ".join(sorted(CONSTRAINT_CHECKABLE_DETECTORS))
    raise TableStoreError(
        f"Refusing to write {len(uncheckable)} operation(s) from a "
        f"non-constraint-checkable detector to {plan.target}: {cells}. Only these "
        f"deterministic detectors may write: {allowlist}. A deterministic procedure is "
        "not a sound inference -- these values are inferred from the shape of the "
        "column's own distribution, not checked against a reference."
    )
