"""Repair pipeline entrypoints for table-store targets."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from dataforge.calibration import AbstentionPolicy, corrector_default_policy
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema
from dataforge.engine.repair import (
    authoritative_columns,
    partition_auto_apply,
    propose_repairs,
)
from dataforge.safety import SafetyFilter, SafetyVerdict
from dataforge.schema_inference import infer_verification_schema
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
    held_fixes: list[dict[str, object]] = Field(default_factory=list)
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
    model: str | None = None,
    allow_pii: bool = False,
    confirm_pii: bool = False,
    confirm_escalations: bool = False,
    allow_unproven_autoapply: bool = False,
    corrector_policy: AbstentionPolicy | None = None,
    state_root: Path | None = None,
    only_column: str | None = None,
) -> TableStoreRepairResult:
    """Detect, verify, plan, and optionally apply repairs for a table store.

    ``allow_unproven_autoapply`` is off by default, so an untrusted (LLM or external)
    value with no authoritative schema is HELD rather than written -- the same
    proven-only invariant the CSV pipeline enforces. This path bypassed that gate from
    2026-07-11 until 2026-08-09: it fed ``propose_repairs`` output straight into a raw
    SQL UPDATE, which is a second mutation primitive the static write-caller test
    cannot see. Enforcement now lives inside both primitives.

    ``corrector_policy`` defaults to :func:`corrector_default_policy` (propose-not-apply),
    matching ``run_repair_pipeline``. It is a parameter rather than a hardcoded call
    because this function is the warehouse analogue of that pipeline and should offer the
    same control; an earlier version of this gate hardcoded the default and silently
    removed a knob the CSV path has.
    """
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
    # Advisory value-level net for untrusted corrections when no authoritative schema
    # exists -- mirrors run_repair_pipeline. Without it a schema-less LLM value reaches
    # the verifier's vacuous structural ACCEPT.
    verification_schema = infer_verification_schema(table) if schema is None else None
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
        verification_schema=verification_schema,
    )
    batch_safety = SafetyFilter().evaluate_batch(accepted_fixes)
    if batch_safety.verdict != SafetyVerdict.ALLOW:
        accepted_fixes = []
    # Proven-only partition before planning, so the plan describes exactly what will be
    # written and held fixes never become SQL.
    accepted_fixes, calibration_held, plausibility_held = partition_auto_apply(
        accepted_fixes,
        corrector_policy if corrector_policy is not None else corrector_default_policy(),
        covered_columns=authoritative_columns(schema),
        allow_unproven_autoapply=allow_unproven_autoapply,
    )
    held_fixes = [*calibration_held, *plausibility_held]
    plan = store.build_patch_plan(
        accepted_fixes,
        schema=schema,
        safety_verdict=batch_safety.verdict.value,
        touched_constraints=(),
        smt_obligations=("SMTVerifier.verify",) if accepted_fixes else (),
    )
    apply_receipt = None
    if mode == "apply":
        apply_receipt = store.apply_patch_plan(
            plan,
            state_root=state_root,
            allow_unproven_autoapply=allow_unproven_autoapply,
        )

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
        held_fixes=[
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
            for fix in held_fixes
        ],
        patch_plan=plan,
        apply_receipt=apply_receipt,
    )
