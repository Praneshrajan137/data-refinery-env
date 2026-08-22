"""Shared domain fixtures for the DataForge test suite.

Why this file exists
--------------------
Until 2026-08-22 the suite had no ``conftest.py`` and no shared notion of "a table with
a repairable defect". Instead the literal::

    id,amount\\n1,100\\n2,105\\n3,98\\n4,1020\\n5,103\\n

was copy-pasted into thirteen files under six different helper names
(``_write_repairable_csv``, ``_decimal_shift_csv``, ``_decimal_shift_case``, ``_csv``,
``_write_csv``, and bare inline ``write_text``). That literal encodes exactly ONE
detector, ``decimal_shift``. So the suite's de-facto definition of "repairable" was a
string, and every test that needed "a write happened" silently depended on that single
detector remaining auto-appliable.

When ``decimal_shift`` was correctly removed from the auto-apply allowlist -- it infers
values from the shape of a column's own distribution, which produced 263,428 flagged
cells with zero true errors across three TPC-H money columns -- thirteen tests broke
across seven files, three of which (patch-plan, watch, certificate) have nothing to do
with ``decimal_shift`` as a subject. Six more tests kept passing while silently proving
nothing.

The fix is to name the two concepts the suite actually needs, define each exactly once,
and make each fixture assert its own premise so it fails loudly rather than quietly
weakening whatever depends on it.

The two concepts
----------------
``premised_repairable_table``
    A table whose repair is checkable against a DECLARED authority -- here a functional
    dependency ``state -> city`` supplied by the operator. Its detector,
    ``fd_violation``, is in :data:`CONSTRAINT_CHECKABLE_DETECTORS`, so the product stands
    behind the write and it is auto-applied. **This is the default for any test that
    needs "a write happened".**

``unpremised_shifted_table``
    The old ``1020`` literal, now named for what it actually is: a table whose only
    candidate repair is inferred from the column's own distribution, is therefore HELD
    for human review, and is never written. Tests that want the held path ask for this
    one explicitly, so the disposition is visible at the call site instead of being an
    accident of which string got pasted.

Both come with a factory variant for tests that need to control the path.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dataforge.cli.common import load_schema
from dataforge.detectors.base import Schema
from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

Disposition = Literal["applied", "held"]


@dataclass(frozen=True)
class RepairableTable:
    """A CSV, its premise, and the single repair it is built to exercise.

    A fixture is a table PLUS its premise, not a table alone. Keeping them in one object
    is what stops a test from declaring a schema that does not hold (which manufactures
    violations) or omitting one that does (which silently changes the disposition).
    """

    csv_path: Path
    schema_path: Path | None
    schema: Schema | None
    detector: str
    row: int
    row_id: str
    column: str
    old_value: str
    new_value: str
    disposition: Disposition

    @property
    def expects_write(self) -> bool:
        """Whether applying this table's repair should change bytes on disk."""
        return self.disposition == "applied"

    def read(self) -> str:
        """Current text of the CSV."""
        return self.csv_path.read_text(encoding="utf-8")

    def cell_line(self) -> str:
        """The line carrying the defective cell, for before/after assertions.

        Matched on :attr:`row_id` (the value in the CSV's ``id`` column), not on
        :attr:`row`, because those are deliberately different numbers: ``row`` is the
        ZERO-BASED dataframe index that ``CellFix`` expects, while ``row_id`` is what the
        file actually says. Conflating them produced ``ValueError: Row 10 is out of
        bounds`` on a ten-row table.
        """
        return next(line for line in self.read().splitlines() if line.startswith(f"{self.row_id},"))

    def verify_premise(self) -> None:
        """Assert this fixture still exercises what its name claims.

        A fixture that silently stops exercising its path is worse than a missing test:
        everything depending on it keeps passing while proving nothing. This is the
        non-vacuity guard for the fixture itself, and it is called by every fixture in
        this file before the table is handed to a test.

        Checks, in order of what would break first:

        1. The disposition is consistent with the allowlist. A fixture claiming
           ``applied`` whose detector is not constraint-checkable is a contradiction --
           that combination is exactly the bug this suite failed to catch.
        2. The defect is actually present in the bytes.
        3. The pipeline reaches the claimed disposition, with the claimed detector, on
           the claimed cell.
        """
        allowlisted = self.detector in CONSTRAINT_CHECKABLE_DETECTORS
        if self.expects_write and not allowlisted:
            raise AssertionError(
                f"fixture claims disposition='applied' but detector {self.detector!r} is "
                f"not in CONSTRAINT_CHECKABLE_DETECTORS ({sorted(CONSTRAINT_CHECKABLE_DETECTORS)}). "
                "A deterministic procedure is not a sound inference; such a fix is held, "
                "not written. See docs/trust/deterministic-is-not-sound.md."
            )
        if self.old_value not in self.read():
            raise AssertionError(
                f"fixture premise broken: expected defective value {self.old_value!r} "
                f"in {self.csv_path.name}, but it is absent -- the table no longer "
                "contains the defect it exists to exercise."
            )
        result = run_repair_pipeline(
            RepairPipelineRequest(source_path=self.csv_path, mode="dry_run", schema=self.schema)
        )
        if self.expects_write:
            matching = [
                fix
                for fix in result.fixes
                if fix.detector_id == self.detector
                and fix.column == self.column
                and fix.new_value == self.new_value
            ]
            if not matching:
                got = [
                    (fix.detector_id, fix.column, fix.old_value, fix.new_value)
                    for fix in result.fixes
                ]
                raise AssertionError(
                    f"fixture premise broken: expected an auto-applied {self.detector!r} "
                    f"fix on {self.column!r} producing {self.new_value!r}, got {got}. "
                    "Any test asserting 'a write happened' would now pass or fail for "
                    "the wrong reason."
                )
        elif result.fixes:
            got = [(fix.detector_id, fix.column, fix.new_value) for fix in result.fixes]
            raise AssertionError(
                f"fixture premise broken: {self.csv_path.name} is the HELD case and must "
                f"produce no auto-applied fix, but the pipeline accepted {got}. Either a "
                "detector entered the allowlist or the table changed."
            )


# --------------------------------------------------------------------------------------
# The premised (auto-applied) table.
# --------------------------------------------------------------------------------------
# Nine rows agree that MA -> boston; row 10 says 'bostonn'. The majority is what lets the
# FD repairer name the correct value, and the DECLARED dependency is what makes it an
# authority rather than a pattern read off the column itself. A near-miss typo is used
# rather than a plausible alternative city ('springfield') because the latter is not a
# violation to be repaired -- it is a legitimate second value that would falsify the FD.
_PREMISED_ROWS = 9
_PREMISED_CSV = (
    "id,state,city\n"
    + "".join(f"{i},MA,boston\n" for i in range(1, _PREMISED_ROWS + 1))
    + f"{_PREMISED_ROWS + 1},MA,bostonn\n"
)
_PREMISED_SCHEMA = (
    "columns:\n"
    "  id: string\n"
    "  state: string\n"
    "  city: string\n"
    "functional_dependencies:\n"
    "  - determinant: [state]\n"
    "    dependent: city\n"
)

# --------------------------------------------------------------------------------------
# The unpremised (held) table.
# --------------------------------------------------------------------------------------
# The historical literal, preserved byte-for-byte so migrated tests keep their original
# subject matter, but now under a name that states the disposition. 1020 reads as a
# 10x decimal shift of 102 ONLY relative to this column's own tight clustering; in a
# realistically dispersed money column the same reasoning flags correct cells.
_UNPREMISED_CSV = "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n"


def build_premised_repairable_table(csv_path: Path) -> RepairableTable:
    """Write the premised (auto-applied) table at ``csv_path`` and verify its premise."""
    csv_path.write_text(_PREMISED_CSV, encoding="utf-8")
    schema_path = csv_path.with_suffix(".schema.yaml")
    schema_path.write_text(_PREMISED_SCHEMA, encoding="utf-8")
    table = RepairableTable(
        csv_path=csv_path,
        schema_path=schema_path,
        schema=load_schema(schema_path),
        detector="fd_violation",
        row=_PREMISED_ROWS,
        row_id=str(_PREMISED_ROWS + 1),
        column="city",
        old_value="bostonn",
        new_value="boston",
        disposition="applied",
    )
    table.verify_premise()
    return table


def build_unpremised_shifted_table(csv_path: Path) -> RepairableTable:
    """Write the unpremised (held) table at ``csv_path`` and verify its premise."""
    csv_path.write_text(_UNPREMISED_CSV, encoding="utf-8")
    table = RepairableTable(
        csv_path=csv_path,
        schema_path=None,
        schema=None,
        detector="decimal_shift",
        row=3,
        row_id="4",
        column="amount",
        old_value="1020",
        new_value="102",
        disposition="held",
    )
    table.verify_premise()
    return table
