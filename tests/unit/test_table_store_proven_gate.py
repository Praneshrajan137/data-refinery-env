"""The table-store (warehouse) write path must enforce the proven-only invariant.

This surface had NO gate test at all -- the only reference to
``run_table_store_repair`` in the suite was an export-name assertion in
``tests/unit/test_public_api.py``. From 2026-07-11 until 2026-08-09 it fed
``propose_repairs`` output straight into ``store.apply_patch_plan``, which for DuckDB
is a raw SQL ``UPDATE``. That is a second mutation primitive, so the static
``apply_transaction``-caller allowlist in ``test_surface_uniformity.py`` structurally
could not see it.

Two things are asserted here:

1. ``enforce_plan_proven_only`` refuses an unproven plan at the primitive, so the gate
   holds no matter which surface built the plan.
2. ``PatchPlan`` carries ``authoritative_schema_present``, which is what lets the apply
   primitive make that decision from the plan alone rather than trusting its caller.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.detectors.base import Schema
from dataforge.repairers.base import ProposedFix
from dataforge.stores.base import TableStoreError
from dataforge.stores.csv import CSVStore
from dataforge.stores.patch_plan import (
    PatchOperation,
    PatchPlan,
    RowIdentity,
    enforce_plan_proven_only,
    enforce_plan_reversible,
    enforce_plan_write_gates,
)
from dataforge.transactions.txn import CellFix


def _operation(provenance: str) -> PatchOperation:
    return PatchOperation.from_cell_fix(
        CellFix(
            row=1, column="score", old_value="abc", new_value="30", detector_id="type_mismatch"
        ),
        relation="t",
        row_identity=RowIdentity(
            kind="column_values",
            columns=("id",),
            values={"id": "2"},
            stable=True,
            reason="test",
        ),
        reason="test",
        confidence=0.6,
        provenance=provenance,
    )


def _plan(provenance: str, *, authoritative_schema_present: bool) -> PatchPlan:
    # Authority is per-column, so a plan that claims an authoritative schema must also
    # say WHICH columns it covers. The operation targets 'score', so covering it is what
    # makes the fix proven; a plan with the flag set but no covered columns proves nothing.
    return PatchPlan.new(
        backend="duckdb",
        target="warehouse://duckdb/t",
        relation="t",
        row_identity_columns=("id",),
        operations=(_operation(provenance),),
        safety_verdict="allow",
        rows_scanned=1,
        reason="test plan",
        authoritative_schema_present=authoritative_schema_present,
        authoritative_columns=("score",) if authoritative_schema_present else (),
    )


def _proposed(provenance: str) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=1, column="score", old_value="abc", new_value="30", detector_id="type_mismatch"
        ),
        reason="test",
        confidence=0.6,
        provenance=provenance,
    )


class TestPlanProvenOnlyGate:
    def test_unproven_operation_is_refused(self) -> None:
        plan = _plan("llm_live", authoritative_schema_present=False)

        with pytest.raises(TableStoreError, match="unproven"):
            enforce_plan_proven_only(plan, allow_unproven_autoapply=False)

    def test_authoritative_schema_makes_the_same_value_proven(self) -> None:
        plan = _plan("llm_live", authoritative_schema_present=True)

        enforce_plan_proven_only(plan, allow_unproven_autoapply=False)

    def test_deterministic_operation_needs_no_schema(self) -> None:
        plan = _plan("deterministic", authoritative_schema_present=False)

        enforce_plan_proven_only(plan, allow_unproven_autoapply=False)

    def test_opt_in_permits_the_unproven_write(self) -> None:
        plan = _plan("llm_live", authoritative_schema_present=False)

        enforce_plan_proven_only(plan, allow_unproven_autoapply=True)

    def test_error_names_the_offending_cell_and_the_flag(self) -> None:
        plan = _plan("llm_live", authoritative_schema_present=False)

        with pytest.raises(TableStoreError) as excinfo:
            enforce_plan_proven_only(plan, allow_unproven_autoapply=False)

        message = str(excinfo.value)
        assert "row 1" in message
        assert "score" in message
        assert "allow_unproven_autoapply" in message


class TestPlanCarriesSchemaStatus:
    """The plan must record schema status so apply does not have to trust its caller."""

    def test_build_patch_plan_records_schema_presence(self, tmp_path: Path) -> None:
        path = tmp_path / "t.csv"
        path.write_text("id,score\n1,10\n2,abc\n", encoding="utf-8")
        store = CSVStore(path)
        fixes = [_proposed("llm_live")]

        without = store.build_patch_plan(fixes, schema=None, safety_verdict="allow")
        with_schema = store.build_patch_plan(
            fixes, schema=Schema(columns={"id": "str", "score": "float"}), safety_verdict="allow"
        )

        assert without.authoritative_schema_present is False
        assert with_schema.authoritative_schema_present is True

    def test_default_is_the_safe_one_for_plans_without_the_field(self) -> None:
        # Plans persisted before this field existed deserialize with the SAFE default,
        # so an old journaled plan cannot be replayed as if it had been proven.
        plan = _plan("llm_live", authoritative_schema_present=True)
        payload = plan.model_dump(mode="json")
        del payload["authoritative_schema_present"]

        assert PatchPlan.model_validate(payload).authoritative_schema_present is False


class TestPlanReversibilityIsAPrecondition:
    """Reversibility is checked by the gate, not by each adapter remembering.

    Until 2026-08-29 ``if not plan.apply_supported or not plan.reversible`` lived in
    ``DuckDBStore.apply_patch_plan``, one line above ``enforce_plan_proven_only``. A
    second backend adapter calling the two gates that *look* like "the write gates" would
    have inherited no reversibility precondition and no error saying so -- the exact
    calling-surface pattern ``docs/trust/write-surface-uniformity.md`` was written about.
    """

    def test_irreversible_plan_is_refused(self) -> None:
        plan = _plan("deterministic", authoritative_schema_present=True).model_copy(
            update={"reversible": False, "reason": "not reversible"}
        )

        with pytest.raises(TableStoreError, match="not reversible"):
            enforce_plan_reversible(plan)

    def test_unsupported_plan_is_refused(self) -> None:
        plan = _plan("deterministic", authoritative_schema_present=True).model_copy(
            update={"apply_supported": False, "reason": "apply not supported"}
        )

        with pytest.raises(TableStoreError, match="apply not supported"):
            enforce_plan_reversible(plan)

    def test_reversible_supported_plan_passes(self) -> None:
        """Non-vacuity: without this, the gate could refuse everything and look correct."""
        enforce_plan_reversible(_plan("deterministic", authoritative_schema_present=True))

    def test_composite_gate_checks_reversibility_before_strength(self) -> None:
        """A plan that cannot be undone is refused before anyone reasons about its contents.

        Both defects are present here. The assertion is that the reversibility message is
        the one raised, because refusing an irreversible write does not depend on how
        well-proven it is.
        """
        plan = _plan("llm_live", authoritative_schema_present=False).model_copy(
            update={"reversible": False, "reason": "not reversible"}
        )

        with pytest.raises(TableStoreError, match="not reversible"):
            enforce_plan_write_gates(plan, allow_unproven_autoapply=False)

    def test_composite_gate_still_refuses_unproven_when_reversible(self) -> None:
        """The composite must not have swallowed the strength gate."""
        plan = _plan("llm_live", authoritative_schema_present=False)

        with pytest.raises(TableStoreError, match="unproven"):
            enforce_plan_write_gates(plan, allow_unproven_autoapply=False)

    def test_composite_gate_still_refuses_uncheckable_detectors(self) -> None:
        """``type_mismatch`` is not constraint-checkable; the soundness gate must survive.

        This is the gate with no opt-in at any confidence, so a composite that lost it
        would be the most consequential possible regression of the three.
        """
        plan = _plan("deterministic", authoritative_schema_present=True)

        with pytest.raises(TableStoreError):
            enforce_plan_write_gates(plan, allow_unproven_autoapply=True)
