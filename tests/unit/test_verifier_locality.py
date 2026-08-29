"""The SMT encoding must scale with each constraint's footprint, not with the table.

Until 2026-08-29 ``SchemaToSMT.verify_fix`` asserted a ground equality for every cell of every
relevant column, so one verification cost ``4 x relevant_columns x rows`` z3 AST nodes no matter
what the constraints looked at. On ``hospital`` that was 10,000 assertions and roughly 40,000 AST
nodes per fix, 1,192 ms, returning UNKNOWN on 60 of 60 real proposals.

Wall clock is the symptom; **assertion count is the mechanism**. A test that only asserted a
millisecond budget would pass again the moment someone got a faster machine, while the quadratic
came back. So these tests count what is asserted and pin its shape:

* the footprint of an FD is its determinant group, so growing the table while holding group size
  fixed must not add a single assertion;
* uniqueness only has to exclude rows that already hold the candidate value, so a value that
  collides with nothing must emit nothing;
* and the counts must be non-zero where a constraint genuinely applies, because an encoding that
  asserts nothing would satisfy every inequality above while verifying nothing at all.

``tests/property/test_verifier_equivalence.py`` remains the correctness fence -- it asserts the
two independently-written verifiers return the *same* verdict. This file only fences the cost.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from z3 import Solver as RealSolver  # type: ignore[import-untyped]

import dataforge.verifier.smt as smt_module
from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix
from dataforge.verifier.result import VerificationVerdict
from dataforge.verifier.schema import FunctionalDependency, Schema


class _CountingSolver:
    """A real z3 solver that records how much was asserted through it."""

    def __init__(self) -> None:
        self._inner = RealSolver()
        self.ground_assertions = 0
        self.tracked_assertions = 0

    def set(self, **kwargs: Any) -> None:
        self._inner.set(**kwargs)

    def add(self, *args: Any) -> None:
        self.ground_assertions += len(args)
        self._inner.add(*args)

    def assert_and_track(self, formula: Any, label: Any) -> None:
        self.tracked_assertions += 1
        self._inner.assert_and_track(formula, label)

    def check(self) -> Any:
        return self._inner.check()

    def unsat_core(self) -> Any:
        return self._inner.unsat_core()

    def reason_unknown(self) -> str:
        return str(self._inner.reason_unknown())


@pytest.fixture()
def counting_solvers(monkeypatch: pytest.MonkeyPatch) -> list[_CountingSolver]:
    """Replace the solver constructor with a counting wrapper over the real solver."""
    created: list[_CountingSolver] = []

    def factory() -> _CountingSolver:
        solver = _CountingSolver()
        created.append(solver)
        return solver

    monkeypatch.setattr(smt_module, "Solver", factory)
    return created


def _fd_frame(rows: int) -> pd.DataFrame:
    """Return a table whose violating determinant group is size 3 regardless of ``rows``.

    Rows 0-2 share ``code == 'A'`` and disagree on ``name``. Every other row gets a unique
    ``code``, so it forms a singleton group and is not a peer of the candidate. Growing ``rows``
    therefore grows the table without growing any footprint.
    """
    if rows < 3:
        raise ValueError("fixture needs at least the three-row group")
    code = ["A", "A", "A"] + [f"K{index}" for index in range(rows - 3)]
    name = ["X", "X", "Y"] + ["Z"] * (rows - 3)
    return pd.DataFrame({"code": code, "name": name})


_FD_SCHEMA = Schema(
    columns={"code": "str", "name": "str"},
    functional_dependencies=(FunctionalDependency(determinant=("code",), dependent="name"),),
)


def _fd_fix() -> ProposedFix:
    """Repair row 2's dependent value to agree with its determinant group."""
    return ProposedFix(
        fix=CellFix(
            row=2,
            column="name",
            old_value="Y",
            new_value="X",
            detector_id="fd_violation",
        ),
        reason="candidate",
        confidence=1.0,
        provenance="deterministic",
    )


def test_fd_encoding_size_is_independent_of_table_size(
    counting_solvers: list[_CountingSolver],
) -> None:
    """Ten times the rows, same determinant group, same number of assertions."""
    counts: dict[int, tuple[int, int]] = {}
    for rows in (20, 200, 2000):
        counting_solvers.clear()
        verifier = smt_module.SchemaToSMT(_FD_SCHEMA, _fd_frame(rows))
        result = verifier.verify_fix(_fd_fix())
        assert result.verdict == VerificationVerdict.ACCEPT, (
            f"the fixture must produce a decidable ACCEPT at {rows} rows, or this test is "
            f"measuring the cost of an error path. Got {result.verdict} ({result.reason})."
        )
        assert len(counting_solvers) == 1
        solver = counting_solvers[0]
        counts[rows] = (solver.ground_assertions, solver.tracked_assertions)

    distinct = set(counts.values())
    assert len(distinct) == 1, (
        "encoding size changed with table size, so it is still a function of row_count rather "
        f"than of the constraint footprint: {counts}"
    )

    ground, tracked = counts[20]
    # Footprint: the candidate row plus ONE representative peer. Rows 0 and 1 both hold name='X',
    # and every peer shares the determinant by construction, so both contribute the identical
    # conjunct `name(candidate) == 'X'`. Deduplicating them is semantics-preserving, so the
    # footprint is 2 rows over the 2 columns the FD names.
    assert ground == 4, f"expected 2 footprint rows x 2 columns, got {ground}"
    assert tracked == 1, f"expected exactly the one tracked FD assertion, got {tracked}"


def test_fd_encoding_grows_with_distinct_dependent_values_not_group_size(
    counting_solvers: list[_CountingSolver],
) -> None:
    """Non-vacuity, and the invariant that replaced group-size growth.

    Until peers were deduplicated by dependent value, this asserted that the encoding grew with the
    determinant GROUP. That is no longer true and should not be: a group of 500 rows holding 3
    distinct dependent values contributes 3 distinct conjuncts, so the encoding tracks the number of
    DISTINCT VALUES. Both halves matter -- it must not grow with group size (otherwise dedup is not
    working) and it must grow with distinct values (otherwise it is encoding nothing).
    """
    group_size = 24
    by_distinct: dict[int, int] = {}
    for distinct in (1, 2, 4):
        counting_solvers.clear()
        # The candidate is the last row and holds 'Y'; the peers hold `distinct` other values.
        peers = [f"V{index % distinct}" for index in range(group_size - 1)]
        code = ["A"] * group_size + [f"K{index}" for index in range(20)]
        name = [*peers, "Y"] + ["Z"] * 20
        frame = pd.DataFrame({"code": code, "name": name})
        fix = ProposedFix(
            fix=CellFix(
                row=group_size - 1,
                column="name",
                old_value="Y",
                new_value="V0",
                detector_id="fd_violation",
            ),
            reason="candidate",
            confidence=1.0,
            provenance="deterministic",
        )
        smt_module.SchemaToSMT(_FD_SCHEMA, frame).verify_fix(fix)
        by_distinct[distinct] = counting_solvers[0].ground_assertions

    # (candidate + distinct representatives) x 2 columns, independent of the 24-row group.
    assert by_distinct == {1: 4, 2: 6, 4: 10}, (
        "the encoding must scale with distinct dependent values, not with group size: "
        f"{by_distinct}"
    )


def test_fd_encoding_ignores_group_size_at_fixed_distinct_values(
    counting_solvers: list[_CountingSolver],
) -> None:
    """The other half: a bigger group with the same distinct values costs nothing more."""
    sizes: dict[int, int] = {}
    for group_size in (6, 24, 96):
        counting_solvers.clear()
        peers = ["V0"] * (group_size - 1)
        code = ["A"] * group_size + [f"K{index}" for index in range(10)]
        name = [*peers, "Y"] + ["Z"] * 10
        frame = pd.DataFrame({"code": code, "name": name})
        fix = ProposedFix(
            fix=CellFix(
                row=group_size - 1,
                column="name",
                old_value="Y",
                new_value="V0",
                detector_id="fd_violation",
            ),
            reason="candidate",
            confidence=1.0,
            provenance="deterministic",
        )
        smt_module.SchemaToSMT(_FD_SCHEMA, frame).verify_fix(fix)
        sizes[group_size] = counting_solvers[0].ground_assertions

    assert len(set(sizes.values())) == 1, (
        f"a 16x larger group over one distinct dependent value must cost the same: {sizes}"
    )


def test_uniqueness_encodes_nothing_when_the_value_collides_with_nothing(
    counting_solvers: list[_CountingSolver],
) -> None:
    """A disequality against a row holding a different value is a tautology, so do not assert it."""
    rows = 500
    frame = pd.DataFrame({"id": [str(index) for index in range(rows)]})
    schema = Schema(columns={"id": "str"}, unique_columns=frozenset({"id"}))
    fix = ProposedFix(
        fix=CellFix(
            row=0,
            column="id",
            old_value="0",
            new_value="not-taken-by-anyone",
            detector_id="prop",
        ),
        reason="candidate",
        confidence=1.0,
        provenance="deterministic",
    )
    result = smt_module.SchemaToSMT(schema, frame).verify_fix(fix)
    assert result.verdict == VerificationVerdict.ACCEPT

    solver = counting_solvers[0]
    assert solver.tracked_assertions == 0, (
        "a unique value that collides with nothing needs no tracked disequality; this previously "
        f"built {rows - 1} of them to reach the same conclusion"
    )
    assert solver.ground_assertions == 1, (
        f"only the candidate row should be grounded, got {solver.ground_assertions}"
    )


def test_uniqueness_still_rejects_a_real_collision(
    counting_solvers: list[_CountingSolver],
) -> None:
    """Non-vacuity for the test above: the constraint must still fire when it should."""
    frame = pd.DataFrame({"id": [str(index) for index in range(500)]})
    schema = Schema(columns={"id": "str"}, unique_columns=frozenset({"id"}))
    fix = ProposedFix(
        fix=CellFix(row=0, column="id", old_value="0", new_value="7", detector_id="prop"),
        reason="candidate",
        confidence=1.0,
        provenance="deterministic",
    )
    result = smt_module.SchemaToSMT(schema, frame).verify_fix(fix)
    assert result.verdict == VerificationVerdict.REJECT, (
        "row 7 already holds '7', so this must be rejected. If it is not, the footprint "
        "computation is dropping rows it needs."
    )
    assert counting_solvers[0].tracked_assertions == 1
