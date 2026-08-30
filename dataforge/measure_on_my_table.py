"""Measure what DataForge would do to a table that has no clean copy.

WHY THIS EXISTS

Every published number in this project comes from four public academic corpora with retained
ground truth -- a clean column beside the dirty one. A real customer table has no clean column,
so none of the harnesses can run on it: ``classify_writes``, ``_write_exposure`` and
``fd_holds_on_clean`` all take ``dataset.clean_df``. The consequence is stated plainly at the
end of ``docs/trust/design-partner-instrumentation.md``: *no real customer table has ever been
tested, and this project cannot yet say what it would do to one.*

That sentence is what blocks ``design_partner_evidence``, the one check in
``dataforge release full-vision`` that cannot be manufactured, and therefore the only gate that
decides whether any of this matters to anybody outside the repository.

HOW TRUTH IS OBTAINED WITHOUT AN ORACLE

Planted controls, the mechanism reasoned through in
``dataforge/calibration_session.py::plant_controls``. Pick a cell **no detector flagged**, so
its current value ``V`` is the best available truth. Corrupt it ourselves into ``V'``. Because
we performed the corruption, ``V`` is known by construction.

That programme's own consumer died -- its pre-registered kill criterion fired at ``beta_upper``
0.8712 against a 0.35 criterion -- so the component survives with no caller. This repoints it
from the labeller to the write path. The corruption mechanic itself is imported rather than
rewritten: ``PRODUCT.md``:176-185 records a reimplemented measurement producing 959 writes
where the truth was 74.

WHAT THE ASSUMPTIONS ARE, AND WHICH WAY THEY ERR

"No detector flagged it" does not establish that ``V`` is correct; it establishes that no
detector noticed, and detector recall is well below 1. So suppose a selected cell's ``V`` is in
fact wrong and the repairer proposes the genuinely true ``T != V``. This scores that as a
failure, because it scores against ``V``. Taken alone, that mechanism understates precision.

**That is not the net direction, and the argument that it was is wrong.** It was checked rather
than assumed, by ``scripts/bench/validate_measure_on_my_table.py``, and the measurement
contradicts it:

    corpus     planted_write_precision   write precision on REAL errors
    hospital   1.0000                    0.7954   (451 repairs / 567 writes)
    tax        1.0000                    0.9378   (603 repairs / 643 writes)

``planted_write_precision`` **OVERSTATES**, on both corpora where it is measurable at all, and
the cause is structural rather than incidental. A plant is a single-cell perturbation dropped
into a determinant group that is otherwise already consistent, so it becomes a minority of one
and the strict-majority rule restores the exact original value. That is the easiest case a
functional-dependency repair can be handed. Real errors arrive correlated, several to a group,
and sometimes in the majority -- which is where the 0.795 comes from.

So ``planted_write_precision`` is reported as an upper bound on precision and must never be
quoted as an estimate of it. Two biases of opposite sign and unequal, unmeasured magnitude do
not net out into a safe number, and pretending otherwise would be the exact failure this
project exists to avoid.

THE METRIC THAT NEEDS NO GROUND TRUTH AT ALL

``wrote_to_a_cell_we_did_not_plant``. Any write to a cell we did not corrupt is observable
without knowing whether it was right, and it is the failure that costs a user data. It is the
column that matters here, and unlike write precision it carries no assumption whatsoever.

PRIVACY IS THE DELIVERABLE

A value-leak in a customer report is not a bug that can be fixed after shipping. So the report
is built **only** from integers, floats and digests -- a cell value cannot appear in it by
construction, not by filtering. :func:`assert_no_plaintext_values` is a second, independent
check over the emitted bytes, and ``tests/unit/test_measure_on_my_table.py`` plants a
recognisable sentinel and asserts it appears nowhere.

The instrument never needs write permission. It reads the table, plants in memory, and computes
what the write path *would* do.
"""

from __future__ import annotations

import json
import random
from hashlib import sha256
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from dataforge.detectors.base import FunctionalDependency, Schema
from dataforge.table import Table, TableLike, cell_value, column_names, row_count
from dataforge.witness import blast_radius

REPORT_SCHEMA_VERSION: Final = "measure_on_my_table_v1"

#: Types a field of the egress report may have. A ``str`` field is admitted only where its
#: value is a digest or a fixed vocabulary member, which
#: ``test_measure_on_my_table.py::TestEveryFieldIsNonValueBearing`` enforces field by field.
_EGRESS_SAFE = (int, float, bool, str, type(None))


class PlantedCell(BaseModel):
    """One planted control. Never leaves the machine: values are the whole point of it."""

    row: int
    column: str
    withheld_truth: str
    corrupted_to: str

    model_config = ConfigDict(frozen=True)


class MeasuredOnMyTable(BaseModel):
    """Counts-only report describing what a repair would do to one real table.

    Every field is an integer, a float, a digest, or a fixed string. There is no field a cell
    value could occupy, which is what makes this shareable. ``extra="forbid"`` so a future
    field cannot be added without a reviewer passing the field-type test.
    """

    schema_version: Literal["measure_on_my_table_v1"] = REPORT_SCHEMA_VERSION
    #: sha256 of the table bytes. Identifies the run without describing the data.
    table_digest: str
    rows: int
    columns: int

    plants_requested: int
    plants_placed: int
    mined_dependencies: int
    fd_covered_columns: int

    #: Outcomes on cells we corrupted ourselves, so truth is known by construction.
    repaired_a_planted_error: int
    wrong_value_on_a_planted_error: int
    missed_a_planted_error: int

    #: The metric that needs no ground truth. Any write to a cell we did not corrupt.
    wrote_to_a_cell_we_did_not_plant: int
    cells_written_total: int

    #: Writes keyed by COLUMN INDEX only -- never by name, which can itself be sensitive.
    writes_by_column_index: dict[str, int] = Field(default_factory=dict)

    planted_write_precision: float | None = None
    unrequested_write_rate: float | None = None

    limitations: list[str] = Field(default_factory=list)
    not_measurable: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", frozen=True)


def _distinct_values_by_column(table: TableLike) -> dict[str, list[str]]:
    """Columns with at least two distinct non-empty values, so a different value exists."""
    out: dict[str, list[str]] = {}
    for column in column_names(table):
        distinct = sorted(
            {
                str(cell_value(table, row, column))
                for row in range(row_count(table))
                if str(cell_value(table, row, column)).strip()
            }
        )
        if len(distinct) >= 2:
            out[column] = distinct
    return out


def plant_into_table(
    table: TableLike,
    *,
    count: int,
    flagged_cells: frozenset[tuple[int, str]],
    seed: int = 20260829,
) -> tuple[Table, list[PlantedCell]]:
    """Return a copy of the table with ``count`` cells corrupted, plus what was corrupted.

    Selection follows ``plant_controls``: skip any cell a detector flagged, skip empty cells,
    and only use columns holding at least two distinct values so a genuinely different
    replacement exists. The corruption itself is
    ``calibration_session._corrupt_like_the_table``, imported rather than rewritten so a plant
    stays indistinguishable from a real mistake.
    """
    from dataforge.calibration_session import _corrupt_like_the_table

    rng = random.Random(seed)
    columns = list(column_names(table))
    rows: list[dict[str, object]] = [
        {name: str(cell_value(table, index, name)) for name in columns}
        for index in range(row_count(table))
    ]
    by_column = _distinct_values_by_column(table)
    if not by_column:
        return Table(columns, rows), []

    candidates = [
        (row, column, rows[row][column])
        for column in by_column
        for row in range(len(rows))
        if str(rows[row][column]).strip() and (row, column) not in flagged_cells
    ]
    rng.shuffle(candidates)

    planted: list[PlantedCell] = []
    for row, column, truth in candidates:
        if len(planted) >= count:
            break
        corrupted = _corrupt_like_the_table(str(truth), rng)
        if corrupted == str(truth):
            continue
        rows[row][column] = corrupted
        planted.append(
            PlantedCell(
                row=row,
                column=column,
                withheld_truth=str(truth),
                corrupted_to=corrupted,
            )
        )
    return Table(columns, rows), planted


def measure_on_my_table(
    table: TableLike,
    *,
    table_bytes: bytes,
    schema: Schema | None,
    flagged_cells: frozenset[tuple[int, str]] = frozenset(),
    plants: int = 200,
    seed: int = 20260829,
) -> MeasuredOnMyTable:
    """Measure what the write path would do to this table, using planted controls only.

    Reads. Never writes. Requires no ground truth and no write permission.
    """
    fds: tuple[FunctionalDependency, ...] = (
        tuple(schema.functional_dependencies) if schema is not None else ()
    )
    columns = list(column_names(table))
    covered: set[str] = set()
    for fd in fds:
        covered.update(fd.determinant)
        covered.add(fd.dependent)

    planted_table, planted = plant_into_table(
        table, count=plants, flagged_cells=flagged_cells, seed=seed
    )
    truth_by_cell = {(item.row, item.column): item.withheld_truth for item in planted}

    witnesses = blast_radius(planted_table, fds) if fds else []

    repaired = 0
    wrong = 0
    unrequested = 0
    writes_by_index: dict[str, int] = {}
    written_cells: set[tuple[int, str]] = set()
    for witness in witnesses:
        key = (witness.row, witness.column)
        written_cells.add(key)
        index = columns.index(witness.column) if witness.column in columns else -1
        label = f"col:{index}"
        writes_by_index[label] = writes_by_index.get(label, 0) + 1
        if key in truth_by_cell:
            if witness.new_value == truth_by_cell[key]:
                repaired += 1
            else:
                wrong += 1
        else:
            unrequested += 1

    missed = sum(1 for key in truth_by_cell if key not in written_cells)
    written = len(witnesses)

    def _rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    return MeasuredOnMyTable(
        table_digest=sha256(table_bytes).hexdigest(),
        rows=row_count(table),
        columns=len(columns),
        plants_requested=plants,
        plants_placed=len(planted),
        mined_dependencies=len(fds),
        fd_covered_columns=len(covered & set(columns)),
        repaired_a_planted_error=repaired,
        wrong_value_on_a_planted_error=wrong,
        missed_a_planted_error=missed,
        wrote_to_a_cell_we_did_not_plant=unrequested,
        cells_written_total=written,
        writes_by_column_index=dict(sorted(writes_by_index.items())),
        planted_write_precision=_rate(repaired, repaired + wrong),
        unrequested_write_rate=_rate(unrequested, written),
        limitations=list(_LIMITATIONS),
        not_measurable=list(_NOT_MEASURABLE),
    )


def assert_no_plaintext_values(report_bytes: bytes, table: TableLike) -> None:
    """Refuse to emit a report containing any recognisable value from the table.

    A second, independent check. The report is already value-free *by construction* -- every
    field is an integer, a float or a digest -- so this cannot be the primary guarantee. It
    exists because privacy is the deliverable here, and a structural argument that is true
    today can be made false by one future field.

    Scoped to values of at least :data:`_SENTINEL_MIN_LENGTH` characters, and that bound is a
    real limit rather than a tuned parameter: short values collide with digest substrings by
    chance, so scanning for them would fail on correct reports. Any identifier, name, address
    or free-text field -- the values whose disclosure would matter -- is longer than that.

    The subject of the scan is content **derived from the table**. The two fixed-prose keys are
    excluded, because the scan found its own false positive on 2026-08-30: the prose names the
    corpora that mine zero dependencies, and one of them contains a cell whose value is that
    corpus's own name. Excluding them is not a loophole -- the prose must equal the module
    constants or this function refuses outright, so the exclusion cannot carry anything.

    Raises:
        ValueError: If a table value appears in the report, or if the fixed prose was altered.
    """
    payload = json.loads(report_bytes)
    if (
        tuple(payload.get("limitations", ())) != _LIMITATIONS
        or tuple(payload.get("not_measurable", ())) != _NOT_MEASURABLE
    ):
        raise ValueError(
            "Refusing to emit: the report's fixed prose does not match the module constants. "
            "Those keys are exempt from the value scan on the grounds that no input can reach "
            "them, and that ground no longer holds."
        )
    scanned = {key: value for key, value in payload.items() if key not in _CONSTANT_PROSE_KEYS}
    haystack = json.dumps(scanned, sort_keys=True)
    seen: set[str] = set()
    for column in column_names(table):
        for row in range(row_count(table)):
            value = str(cell_value(table, row, column))
            if len(value) >= _SENTINEL_MIN_LENGTH:
                seen.add(value)
    leaked = sorted(value for value in seen if value in haystack)
    if leaked:
        raise ValueError(
            f"Refusing to emit: {len(leaked)} table value(s) appear in the report. A "
            "value-leak in a customer report cannot be fixed after shipping."
        )


#: Minimum value length the egress scan considers. Below this, values collide with digest
#: substrings by chance and the scan would reject correct reports.
_SENTINEL_MIN_LENGTH: Final = 6

#: Report keys holding FIXED prose, identical on every run and defined as literals below. They
#: are excluded from the egress scan and the exclusion is not a loophole: no input can place a
#: value into a source literal, and :func:`assert_no_plaintext_values` refuses outright if the
#: emitted prose differs from the constants, so the exclusion cannot be used to smuggle text.
#:
#: Discovered by the scan itself, on 2026-08-30. The prose names the corpora that mine zero
#: dependencies -- including ``flights`` -- and the flights corpus contains a cell whose value
#: is the literal string ``flights``. Scanning our own commentary for the user's values is a
#: category error: the scan's subject is content DERIVED from the table, and prose is not.
_CONSTANT_PROSE_KEYS: Final = ("limitations", "not_measurable")

_LIMITATIONS: Final[tuple[str, ...]] = (
    "planted_write_precision is an UPPER BOUND on precision, never an estimate of it. "
    "A plant is a single-cell perturbation in an otherwise-consistent determinant "
    "group, so it becomes a minority of one and the strict-majority rule restores the "
    "exact original -- the easiest case there is. Real errors arrive correlated and "
    "sometimes in the majority. Measured: planted precision 1.0 on both hospital and "
    "tax, against real-error precision of 0.795 and 0.938 respectively.",
    "Only functional dependencies are exercised. missing_value writes on unanimity "
    "and so destroys nothing by construction; column_type and domain_bound constrain "
    "what a verifier accepts rather than driving a rewrite.",
    "wrote_to_a_cell_we_did_not_plant is an UPPER BOUND on damage, not damage. It "
    "counts every write to a cell we did not corrupt, and on a table with no clean "
    "copy those writes cannot be separated into repairs of real pre-existing errors "
    "and corruptions of genuinely clean cells. Measured on hospital, where truth is "
    "retained: 567 such writes were 451 real repairs and 116 real corruptions, so "
    "reading the figure as damage overstated it 4.9x. Weigh it as a ceiling.",
    "If mined_dependencies is 0 this instrument measures NOTHING -- not safety. Two "
    "of the four reference corpora mine zero dependencies at the 0.90 emission "
    "floor, so half the available evidence base is outside its reach.",
)

_NOT_MEASURABLE: Final[tuple[str, ...]] = (
    "RECALL on real errors. This table has no clean copy, so the number of real errors "
    "is unknown and no denominator exists. Reported as unmeasurable rather than "
    "omitted, because an absent metric reads as a zero.",
    "FD-set precision. Whether a mined dependency is true cannot be decided without "
    "ground truth. What IS measurable is its consequence, which is what the write "
    "counts above are.",
    "cells_reviewed_per_true_error, for the same missing denominator.",
)


def report_payload(report: MeasuredOnMyTable) -> dict[str, Any]:
    """Return the report as a JSON-ready mapping."""
    return report.model_dump(mode="json")
