"""CSV table-store wrapper around DataForge's existing repair engine."""

from __future__ import annotations

from pathlib import Path

from dataforge.detectors.base import Schema
from dataforge.engine.repair import apply_transaction, read_csv
from dataforge.repairers.base import ProposedFix
from dataforge.stores.base import StoreApplyReceipt, TableStore
from dataforge.stores.patch_plan import PatchOperation, PatchPlan, RowIdentity
from dataforge.table import TableLike, row_count


class CSVStore(TableStore):
    """Reference table-store implementation for local CSV files."""

    backend = "csv"

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self.target = str(self.path)
        self.relation = self.path.name
        self.row_identity_columns: tuple[str, ...] = ("_row",)

    def read_table(self) -> TableLike:
        """Read the CSV using the existing string-preserving reader."""
        return read_csv(self.path)

    def build_patch_plan(
        self,
        fixes: list[ProposedFix],
        *,
        schema: Schema | None,
        safety_verdict: str,
        touched_constraints: tuple[str, ...] = (),
        smt_obligations: tuple[str, ...] = (),
    ) -> PatchPlan:
        """Describe existing CSV cell edits as a patch plan."""
        del schema
        operations = tuple(
            PatchOperation.from_cell_fix(
                fix.fix,
                relation=self.relation,
                row_identity=RowIdentity(
                    kind="csv_position",
                    columns=("_row",),
                    values={"_row": str(fix.fix.row)},
                    stable=True,
                    reason="CSV byte snapshot plus row position is reversible in the local engine.",
                ),
                reason=fix.reason,
                confidence=fix.confidence,
                provenance=fix.provenance,
            )
            for fix in fixes
        )
        return PatchPlan.new(
            backend=self.backend,
            target=self.target,
            relation=self.relation,
            row_identity_columns=self.row_identity_columns,
            operations=operations,
            safety_verdict=safety_verdict,
            rows_scanned=row_count(self.read_table()),
            reason="CSV patch plan mirrors the existing reversible transaction engine.",
            touched_constraints=touched_constraints,
            smt_obligations=smt_obligations,
            audit_metadata={"source": "csv_reference_engine"},
            apply_supported=bool(operations),
            reversible=True,
        )

    def apply_patch_plan(
        self,
        plan: PatchPlan,
        *,
        state_root: Path | None = None,
        source_bytes: bytes | None = None,
        fixes: list[ProposedFix] | None = None,
    ) -> StoreApplyReceipt:
        """Apply through the existing CSV transaction path."""
        del state_root
        if fixes is None or source_bytes is None:
            raise ValueError("CSVStore.apply_patch_plan requires source bytes and fixes.")
        txn_id = apply_transaction(self.path, fixes, source_bytes)
        return StoreApplyReceipt(
            ok=True,
            txn_id=txn_id,
            backend=self.backend,
            target=self.target,
            patch_plan_sha256=plan.sha256(),
            reason=f"Applied {len(fixes)} CSV fix(es).",
        )
