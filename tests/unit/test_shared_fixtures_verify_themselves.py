"""The shared fixtures assert their own premise.

``RepairableTable.verify_premise`` is the suite's guard against a fixture that silently
stops exercising what its name claims -- the failure mode that let six tests keep passing
while proving nothing. A guard that cannot fail is theatre, so this file pins two separate
things:

1. The guard's LOGIC: each inconsistency it claims to catch actually raises.
2. The guard's WIRING: the builders call it. A mutation run showed these are independent --
   deleting the ``table.verify_premise()`` calls from the builders left every other test
   green, because the fixtures the builders produce are in fact valid, so the check passed
   either way and its absence was invisible.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from tests.support import tables
from tests.support.tables import (
    RepairableTable,
    build_premised_repairable_table,
    build_unpremised_shifted_table,
)


class TestTheGuardsLogic:
    def test_claiming_applied_for_an_uncheckable_detector_raises(self, tmp_path: Path) -> None:
        """The contradiction at the heart of the original bug class.

        A fixture asserting "this writes" about a detector the product holds is exactly
        the assumption thirteen test files encoded.
        """
        valid = build_unpremised_shifted_table(tmp_path / "t.csv")
        lying = replace(valid, disposition="applied")

        with pytest.raises(AssertionError, match="CONSTRAINT_CHECKABLE_DETECTORS"):
            lying.verify_premise()

    def test_a_missing_defect_raises(self, tmp_path: Path) -> None:
        """If the table no longer contains the defect, the fixture is not a fixture."""
        table = build_premised_repairable_table(tmp_path / "t.csv")
        table.csv_path.write_text(
            table.read().replace(table.old_value, table.new_value), encoding="utf-8"
        )

        with pytest.raises(AssertionError, match="expected defective value"):
            table.verify_premise()

    def test_claiming_the_wrong_repaired_value_raises(self, tmp_path: Path) -> None:
        valid = build_premised_repairable_table(tmp_path / "t.csv")
        lying = replace(valid, new_value="cambridge")

        with pytest.raises(AssertionError, match="expected an auto-applied"):
            lying.verify_premise()

    def test_claiming_the_wrong_column_raises(self, tmp_path: Path) -> None:
        valid = build_premised_repairable_table(tmp_path / "t.csv")
        lying = replace(valid, column="state")

        with pytest.raises(AssertionError, match="expected an auto-applied"):
            lying.verify_premise()

    def test_a_held_table_that_starts_writing_raises(self, tmp_path: Path) -> None:
        """The held case's guard, which fires if a detector enters the allowlist.

        Simulated by relabelling the premised table as held: it does produce an applied
        fix, so the held assertion must reject it. Without this direction the guard would
        catch a write becoming a hold but not a hold becoming a write -- and the latter is
        the corruption direction.
        """
        valid = build_premised_repairable_table(tmp_path / "t.csv")
        lying = replace(valid, disposition="held", detector="fd_violation")

        with pytest.raises(AssertionError, match="must\n?\\s*produce no auto-applied fix"):
            lying.verify_premise()

    def test_a_valid_fixture_does_not_raise(self, tmp_path: Path) -> None:
        """Non-vacuity: a guard that raised unconditionally would pass everything above."""
        build_premised_repairable_table(tmp_path / "a.csv").verify_premise()
        build_unpremised_shifted_table(tmp_path / "b.csv").verify_premise()


class TestTheGuardIsWiredIntoTheBuilders:
    """The builders call the guard. Independent of whether the guard works."""

    @pytest.mark.parametrize(
        "builder",
        [build_premised_repairable_table, build_unpremised_shifted_table],
        ids=["premised", "unpremised"],
    )
    def test_the_builder_verifies_the_premise(
        self,
        builder: object,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[RepairableTable] = []
        original = tables.RepairableTable.verify_premise

        def spy(self: RepairableTable) -> None:
            calls.append(self)
            original(self)

        monkeypatch.setattr(tables.RepairableTable, "verify_premise", spy)

        builder(tmp_path / "t.csv")  # type: ignore[operator]

        assert len(calls) == 1, (
            "the builder returned a table without verifying its premise, so a fixture "
            "that stopped exercising its path would be handed to tests silently"
        )
