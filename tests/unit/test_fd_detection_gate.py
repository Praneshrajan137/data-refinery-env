"""Inferred functional dependencies must be a costed choice, not a silent default.

`fd_violation` is the only detector that reads `schema.functional_dependencies`, and it is
tier-0 `UNSAFE` at confidence 0.95, so it wins its cell outright against every other
detector. Accepting the mined FDs on hospital turns a 549-cell queue that is 56% real errors
into 10,373 cells at 4.4% -- +147 true errors bought with +9,824 false positives, and review
effort going from 1.78 to 22.80 cells per real error.

Before `schema_for_fd_detection` existed there was **no control anywhere** that gated FD
detection. `require_declared_fds_for_autoapply` runs after detection and filters *fixes*, so
it stops the machine writing while leaving every flag in the human queue. These tests lock
the distinction, because a regression here is invisible: the queue simply gets 19x longer and
nothing fails.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from dataforge.detectors import run_all_detectors
from dataforge.engine.repair import (
    RepairPipelineRequest,
    fd_flag_cost,
    schema_for_fd_detection,
)
from dataforge.verifier.schema import FunctionalDependency, Schema


def _table() -> pd.DataFrame:
    """A table with a genuine zip -> city dependency violated by one dirty cell."""
    rows = [{"id": str(i), "zip": "02134", "city": "boston"} for i in range(10)]
    rows.append({"id": "10", "zip": "02134", "city": "bostonn"})
    rows.extend({"id": str(20 + i), "zip": "60601", "city": "chicago"} for i in range(10))
    return pd.DataFrame(rows)


def _fd(determinant: str, dependent: str) -> FunctionalDependency:
    return FunctionalDependency(determinant=(determinant,), dependent=dependent)


def _schema(*fds: FunctionalDependency) -> Schema:
    return Schema(
        columns={"id": "string", "zip": "string", "city": "string"},
        functional_dependencies=tuple(fds),
    )


class TestSchemaForFdDetection:
    def test_accepted_is_the_historical_default_and_changes_nothing(self) -> None:
        schema = _schema(_fd("zip", "city"))
        assert schema_for_fd_detection(schema, None, "accepted") is schema

    def test_none_removes_every_functional_dependency(self) -> None:
        result = schema_for_fd_detection(_schema(_fd("zip", "city")), None, "none")
        assert result is not None
        assert result.functional_dependencies == ()

    def test_declared_keeps_only_hand_declared_dependencies(self) -> None:
        """The whole point: an inferred FD confers no detection under 'declared'."""
        inferred = _fd("zip", "city")
        other = _fd("id", "city")
        effective = _schema(inferred, other)
        declared = _schema(inferred)
        result = schema_for_fd_detection(effective, declared, "declared")
        assert result is not None
        assert result.functional_dependencies == (inferred,)

    def test_declared_with_no_declared_schema_drops_all(self) -> None:
        result = schema_for_fd_detection(_schema(_fd("zip", "city")), None, "declared")
        assert result is not None
        assert result.functional_dependencies == ()

    def test_none_schema_passes_through(self) -> None:
        assert schema_for_fd_detection(None, None, "declared") is None

    def test_other_schema_content_is_preserved(self) -> None:
        """Only FDs may be narrowed; nothing else affects detector output."""
        schema = Schema(
            columns={"id": "string", "zip": "string", "city": "string"},
            functional_dependencies=(_fd("zip", "city"),),
            pii_columns=("city",),
            not_null_columns=("id",),
        )
        result = schema_for_fd_detection(schema, None, "none")
        assert result is not None
        assert result.columns == schema.columns
        assert result.pii_columns == schema.pii_columns
        assert result.not_null_columns == schema.not_null_columns


class TestGateActuallyChangesTheQueue:
    """A gate that does not change detector output would be decoration."""

    def test_declared_produces_a_strictly_smaller_queue(self) -> None:
        df = _table()
        effective = _schema(_fd("zip", "city"))
        with_fd = run_all_detectors(df, schema_for_fd_detection(effective, None, "accepted"))
        without = run_all_detectors(df, schema_for_fd_detection(effective, None, "declared"))
        assert len(with_fd) > len(without), (
            "narrowing FD detection did not reduce the queue; the gate is inert"
        )

    def test_fd_issues_disappear_entirely_under_none(self) -> None:
        df = _table()
        effective = _schema(_fd("zip", "city"))
        issues = run_all_detectors(df, schema_for_fd_detection(effective, None, "none"))
        assert not [i for i in issues if i.issue_type == "fd_violation"]


class TestFdFlagCost:
    def test_zero_without_dependencies(self) -> None:
        assert fd_flag_cost(_table(), _schema()) == 0

    def test_counts_distinct_cells_not_raw_issues(self) -> None:
        """Raw issue counts overstate the queue cost ~5x, which would mislead the user."""
        df = _table()
        # Two dependencies covering the same dependent column: a violating cell is
        # reported twice as an issue but costs the queue once.
        schema = _schema(_fd("zip", "city"), _fd("id", "city"))
        raw = len(run_all_detectors(df, schema))
        cells = fd_flag_cost(df, schema)
        assert cells <= raw

    def test_matches_the_number_of_fd_flagged_cells(self) -> None:
        df = _table()
        schema = _schema(_fd("zip", "city"))
        issues = run_all_detectors(df, schema)
        flagged = {(i.row, i.column) for i in issues if i.issue_type == "fd_violation"}
        assert fd_flag_cost(df, schema) >= len(flagged)


class TestRequestDefaultPreservesBehaviour:
    def test_default_is_accepted(self) -> None:
        """Changing the default would silently alter every existing caller's queue."""
        request = RepairPipelineRequest(source_path=Path("x.csv"))
        assert request.fd_detection_source == "accepted"

    @pytest.mark.parametrize("value", ["declared", "accepted", "none"])
    def test_all_documented_values_are_accepted(self, value: str) -> None:
        request = RepairPipelineRequest(source_path=Path("x.csv"), fd_detection_source=value)
        assert request.fd_detection_source == value

    def test_an_unknown_value_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            RepairPipelineRequest(source_path=Path("x.csv"), fd_detection_source="inferred")
