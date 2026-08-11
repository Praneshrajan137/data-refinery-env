"""Table-store abstractions shared by warehouse-capable repair paths."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from dataforge.detectors.base import Schema
from dataforge.repairers.base import ProposedFix
from dataforge.stores.patch_plan import PatchPlan
from dataforge.table import TableLike


class StoreApplyReceipt(BaseModel):
    """Receipt returned after a table-store patch plan is applied."""

    schema_version: str = "table_store_apply_receipt_v1"
    ok: bool
    txn_id: str | None = None
    backend: str
    target: str
    patch_plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    post_state_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    reason: str = Field(min_length=1)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class StoreRevertReceipt(BaseModel):
    """Receipt returned after a table-store transaction rollback."""

    schema_version: str = "table_store_revert_receipt_v1"
    ok: bool
    txn_id: str
    backend: str
    target: str
    reason: str = Field(min_length=1)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class TableStore(Protocol):
    """Minimal interface for backends that can host DataForge patch plans."""

    backend: str
    target: str
    relation: str
    row_identity_columns: tuple[str, ...]

    def read_table(self) -> TableLike:
        """Read the target relation into DataForge's string-preserving table surface."""

    def build_patch_plan(
        self,
        fixes: list[ProposedFix],
        *,
        schema: Schema | None,
        safety_verdict: str,
        touched_constraints: tuple[str, ...] = (),
        smt_obligations: tuple[str, ...] = (),
    ) -> PatchPlan:
        """Build a reversible patch plan for verified fixes."""

    def apply_patch_plan(
        self,
        plan: PatchPlan,
        *,
        state_root: Path | None = None,
        allow_unproven_autoapply: bool = False,
    ) -> StoreApplyReceipt:
        """Apply a patch plan through the backend transaction mechanism.

        Implementations MUST enforce the proven-only invariant before mutating --
        ``enforce_plan_proven_only`` for SQL backends, or ``apply_transaction``'s
        own gate for the CSV backend. ``allow_unproven_autoapply`` is the caller's
        explicit acknowledgement that unproven writes are permitted.
        """


class TableStoreError(RuntimeError):
    """Raised when a table-store operation cannot complete safely."""
