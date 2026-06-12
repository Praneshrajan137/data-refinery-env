"""Explicit cloud warehouse adapter boundaries.

These adapters intentionally support dry-run patch-plan manifests only until
their credentialed conformance suites prove apply, audit, and rollback.
"""

from __future__ import annotations

from pathlib import Path

from dataforge.detectors.base import Schema
from dataforge.repairers.base import ProposedFix
from dataforge.stores.base import StoreApplyReceipt, TableStore, TableStoreError
from dataforge.stores.patch_plan import PatchPlan
from dataforge.table import Table, TableLike


class CloudWarehouseStore(TableStore):
    """Dry-run-only boundary for cloud warehouses."""

    def __init__(
        self,
        *,
        backend: str,
        target: str,
        relation: str,
        row_identity_columns: tuple[str, ...] = (),
    ) -> None:
        self.backend = backend
        self.target = target
        self.relation = relation
        self.row_identity_columns = row_identity_columns

    def read_table(self) -> TableLike:
        """Return an empty table because credentials are not configured in OSS core."""
        return Table([], [])

    def build_patch_plan(
        self,
        fixes: list[ProposedFix],
        *,
        schema: Schema | None,
        safety_verdict: str,
        touched_constraints: tuple[str, ...] = (),
        smt_obligations: tuple[str, ...] = (),
    ) -> PatchPlan:
        """Create an explicit non-mutating plan boundary."""
        del schema, fixes
        return PatchPlan.new(
            backend=self.backend,
            target=self.target,
            relation=self.relation,
            row_identity_columns=self.row_identity_columns,
            operations=(),
            safety_verdict=safety_verdict,
            rows_scanned=0,
            reason=(
                f"{self.backend} dry-run boundary created. Apply is disabled until "
                "credentialed conformance tests prove reversible transactions."
            ),
            touched_constraints=touched_constraints,
            smt_obligations=smt_obligations,
            audit_metadata={"adapter_status": "dry_run_only"},
            apply_supported=False,
            reversible=False,
        )

    def apply_patch_plan(
        self,
        plan: PatchPlan,
        *,
        state_root: Path | None = None,
    ) -> StoreApplyReceipt:
        """Refuse mutation for unproven cloud adapters."""
        del state_root
        raise TableStoreError(
            f"{self.backend} apply is disabled until its conformance suite is enabled."
        )
