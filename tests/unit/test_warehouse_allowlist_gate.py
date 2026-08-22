"""The warehouse (SQL) write primitive enforces the same allowlist as the CSV path.

Companion to ``test_autoapply_decision_table.py``, which covers the CSV primitive. This
file exists because the warehouse gate was added without a test and a mutation run proved
it: deleting ``enforce_plan_constraint_checkable_only(plan)`` from
``DuckDBStore.apply_patch_plan`` broke nothing in the suite. An unpinned guard is
indistinguishable from no guard the moment someone refactors near it.

The two primitives are deliberately separate code paths -- the CSV path journals bytes,
the warehouse path issues SQL -- so each needs its own gate and its own test. Both read the
one allowlist in ``dataforge.domain.vocabulary`` so they cannot disagree about membership.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS
from dataforge.stores.base import TableStoreError
from dataforge.stores.duckdb import DuckDBStore
from dataforge.stores.patch_plan import (
    CostEstimate,
    PatchOperation,
    PatchPlan,
    RowIdentity,
    enforce_plan_constraint_checkable_only,
)


def _operation(detector_id: str, *, provenance: str = "deterministic") -> PatchOperation:
    return PatchOperation(
        relation="items",
        row=9,
        row_identity=RowIdentity(
            kind="column_values",
            columns=("id",),
            values={"id": "10"},
            stable=True,
            reason="declared row identity",
        ),
        column="city",
        old_value="bostonn",
        new_value="boston",
        detector_id=detector_id,
        reason="synthetic operation for the warehouse gate",
        confidence=0.99,
        provenance=provenance,
    )


def _plan(*operations: PatchOperation) -> PatchPlan:
    return PatchPlan(
        plan_id="plan-0123456789ab",
        created_at=datetime(2026, 8, 22, tzinfo=UTC),
        backend="duckdb",
        target="warehouse.duckdb",
        relation="items",
        stable_row_identity=True,
        cost_estimate=CostEstimate(
            rows_scanned=10, rows_written=len(operations), bytes_scanned=0, quota_units=0.0
        ),
        safety_verdict="allow",
        reversible=True,
        apply_supported=True,
        reason="synthetic plan for the warehouse allowlist gate",
        operations=operations,
        authoritative_columns=("id", "state", "city"),
        authoritative_schema_present=True,
    )


class TestWarehouseAllowlistGate:
    def test_an_uncheckable_detector_is_refused(self) -> None:
        with pytest.raises(TableStoreError, match="decimal_shift"):
            enforce_plan_constraint_checkable_only(_plan(_operation("decimal_shift")))

    def test_the_refusal_names_the_allowlist(self) -> None:
        """A caller needs to know which gate refused and what would pass instead."""
        with pytest.raises(TableStoreError) as caught:
            enforce_plan_constraint_checkable_only(_plan(_operation("decimal_shift")))
        message = str(caught.value)
        for allowed in CONSTRAINT_CHECKABLE_DETECTORS:
            assert allowed in message, f"{allowed} missing from the refusal message"

    @pytest.mark.parametrize("detector", sorted(CONSTRAINT_CHECKABLE_DETECTORS))
    def test_an_allowlisted_detector_passes(self, detector: str) -> None:
        """Non-vacuity: the gate must not refuse everything.

        Without this, a gate that raised unconditionally would satisfy the refusal tests
        above while blocking every warehouse write.
        """
        enforce_plan_constraint_checkable_only(_plan(_operation(detector)))

    def test_an_empty_plan_passes(self) -> None:
        enforce_plan_constraint_checkable_only(_plan())

    def test_a_non_deterministic_provenance_is_not_this_gate_s_business(self) -> None:
        """Only ``deterministic`` provenance is allowlist-checked.

        An ``external`` or LLM value is gated on STRENGTH and on its expected-old-value
        precondition instead (``enforce_plan_proven_only``). Pinning this boundary stops a
        future edit from widening the allowlist check onto provenances whose detector_id
        carries no soundness meaning.
        """
        enforce_plan_constraint_checkable_only(
            _plan(_operation("decimal_shift", provenance="external"))
        )

    def test_one_bad_operation_voids_a_mixed_plan(self) -> None:
        """A plan is refused as a whole; partial application is not offered.

        Mirrors the CSV path's batch behaviour: shipping the allowlisted subset of a
        refused plan would leave the warehouse in a state no receipt describes.
        """
        with pytest.raises(TableStoreError):
            enforce_plan_constraint_checkable_only(
                _plan(_operation("fd_violation"), _operation("decimal_shift"))
            )


class TestTheGateIsActuallyWiredIntoTheStore:
    """The call SITE, not just the function.

    Every test above calls ``enforce_plan_constraint_checkable_only`` directly, which
    verifies the gate's logic and nothing about whether anyone invokes it. A mutation run
    proved the distinction matters: deleting the call from ``DuckDBStore.apply_patch_plan``
    left all of them green. Testing a guard without testing its wiring is testing that the
    guard compiles.
    """

    @staticmethod
    def _database(tmp_path: Path) -> Path:
        import duckdb

        database_path = tmp_path / "warehouse.duckdb"
        rows = ", ".join(f"('{i}', 'MA', 'boston')" for i in range(1, 10))
        with duckdb.connect(str(database_path)) as connection:
            connection.execute("CREATE TABLE items (id VARCHAR, state VARCHAR, city VARCHAR)")
            connection.execute(f"INSERT INTO items VALUES {rows}, ('10', 'MA', 'bostonn')")
        return database_path

    @staticmethod
    def _city(database_path: Path, row_id: str) -> str:
        import duckdb

        with duckdb.connect(str(database_path), read_only=True) as connection:
            return str(
                connection.execute("SELECT city FROM items WHERE id = ?", [row_id]).fetchone()[0]
            )

    def test_apply_patch_plan_refuses_an_uncheckable_operation(self, tmp_path: Path) -> None:
        database_path = self._database(tmp_path)
        store = DuckDBStore(
            database_path=database_path, relation="items", row_identity_columns=("id",)
        )
        plan = _plan(_operation("decimal_shift"))

        with pytest.raises(TableStoreError, match="decimal_shift"):
            store.apply_patch_plan(plan, state_root=tmp_path)

        assert self._city(database_path, "10") == "bostonn", (
            "the store raised but had already issued the UPDATE -- the gate must run "
            "before any SQL is executed"
        )

    def test_the_opt_in_does_not_unlock_the_store_either(self, tmp_path: Path) -> None:
        """``allow_unproven_autoapply`` covers strength, never soundness."""
        database_path = self._database(tmp_path)
        store = DuckDBStore(
            database_path=database_path, relation="items", row_identity_columns=("id",)
        )

        with pytest.raises(TableStoreError, match="decimal_shift"):
            store.apply_patch_plan(
                _plan(_operation("decimal_shift")),
                state_root=tmp_path,
                allow_unproven_autoapply=True,
            )

        assert self._city(database_path, "10") == "bostonn"
