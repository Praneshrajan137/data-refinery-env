"""Repair pipeline entrypoints for table-store targets."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict

from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema
from dataforge.engine.repair import propose_repairs
from dataforge.safety import SafetyFilter, SafetyVerdict
from dataforge.stores.base import StoreApplyReceipt, TableStore, TableStoreError
from dataforge.stores.patch_plan import PatchPlan
from dataforge.table import copy_table


class TableStoreRepairResult(BaseModel):
    """Repair result for warehouse/table-store CLI calls."""

    schema_version: str = "table_store_repair_result_v1"
    mode: str
    target: str
    backend: str
    issues: list[Issue]
    fixes: list[dict[str, object]]
    patch_plan: PatchPlan
    apply_receipt: StoreApplyReceipt | None = None

    model_config = ConfigDict(
        strict=True, arbitrary_types_allowed=True, extra="forbid", frozen=True
    )


def run_table_store_repair(
    store: TableStore,
    *,
    mode: str,
    schema: Schema | None,
    allow_llm: bool = False,
    model: str = "gemini-2.0-flash",
    allow_pii: bool = False,
    confirm_pii: bool = False,
    confirm_escalations: bool = False,
    state_root: Path | None = None,
    only_column: str | None = None,
) -> TableStoreRepairResult:
    """Detect, verify, plan, and optionally apply repairs for a table store."""
    if mode not in {"dry_run", "apply"}:
        raise TableStoreError("Table-store repair mode must be dry_run or apply.")

    if store.backend in {"snowflake", "bigquery", "databricks"}:
        plan = store.build_patch_plan(
            [],
            schema=schema,
            safety_verdict="dry_run_only",
            touched_constraints=(),
            smt_obligations=(),
        )
        if mode == "apply":
            raise TableStoreError(plan.reason)
        return TableStoreRepairResult(
            mode=mode,
            target=store.target,
            backend=store.backend,
            issues=[],
            fixes=[],
            patch_plan=plan,
        )

    table = store.read_table()
    issues = run_all_detectors(table, schema)
    if only_column is not None:
        issues = [issue for issue in issues if issue.column == only_column]
    accepted_fixes, attempt_groups = propose_repairs(
        issues,
        Path.cwd() / ".dataforge" / "warehouse-target.csv",
        copy_table(table),
        schema,
        allow_llm=allow_llm,
        model=model,
        allow_pii=allow_pii,
        confirm_pii=confirm_pii,
        confirm_escalations=confirm_escalations,
        interactive=False,
    )
    batch_safety = SafetyFilter().evaluate_batch(accepted_fixes)
    if batch_safety.verdict != SafetyVerdict.ALLOW:
        accepted_fixes = []
    plan = store.build_patch_plan(
        accepted_fixes,
        schema=schema,
        safety_verdict=batch_safety.verdict.value,
        touched_constraints=(),
        smt_obligations=("SMTVerifier.verify",) if accepted_fixes else (),
    )
    apply_receipt = None
    if mode == "apply":
        apply_receipt = store.apply_patch_plan(plan, state_root=state_root)

    return TableStoreRepairResult(
        mode=mode,
        target=store.target,
        backend=store.backend,
        issues=issues,
        fixes=[
            {
                "row": fix.fix.row,
                "column": fix.fix.column,
                "old_value": fix.fix.old_value,
                "new_value": fix.fix.new_value,
                "detector_id": fix.fix.detector_id,
                "reason": fix.reason,
                "confidence": fix.confidence,
                "provenance": fix.provenance,
            }
            for fix in accepted_fixes
        ],
        patch_plan=plan,
        apply_receipt=apply_receipt,
    )
