"""Entailment witnesses: the evidence a constraint-derived write rests on, as data.

WHY THIS EXISTS

``PRODUCT.md`` §1.4 is titled "The premise is the product", and the safety thesis is "no
premise, no unsupervised write" -- every unpremised write path was deliberately emptied. So
the premise is now the entire load-bearing input to correctness, and the surface that
acquires it is the least engineered thing in the system. The only zero-configuration premise
is the FD miner, and ``docs/trust/shipped-premise-result.md`` measures what its shipped
output does on hospital: 567 writes, 451 real errors repaired, and **116 already-correct
cells overwritten**.

The mechanism is known, and it is not falseness. ``PRODUCT.md``:186-190:

    Premise precision does not predict corruption. [...] What determines harm is whether a
    false premise meets a group that disagrees.

Measured: ``ZipCode -> Address1`` and ``ZipCode -> PhoneNumber`` are *equally false* as the
dependencies that did the damage, and they corrupted **nothing**, because their determinant
groups held no visible disagreement to resolve.

Both conjuncts of that condition are computable from the user's own table -- no ground
truth, no solver, no fitted threshold. This module computes them. What it produces is used
four ways: the reviewer's consequence preview at the acceptance keystroke, a blast-radius
budget derived from cell counts rather than hardcoded, a checkable derivation inside the
attestation, and ground-truth-free harm accounting for a real external table.

WHAT A WITNESS IS NOT

A witness is **evidence about** a write, never an input to whether it happens. Nothing here
decides anything; the verdict path is untouched. That is pre-registered as kill criterion F3
in ``eval/preregistration/entailment_witness.md`` and pinned by the K4 oracle.

It is also not a confidence score. ``PRODUCT.md``:213-221 records the deliberate refusal to
ship ``tested_confidence`` as a gate, because its separating constant is fitted to 85
candidates from a single corpus with nothing to validate it against. That refusal stands.
A witness enumerates the actual consequence instead of scoring the likelihood of one, so it
introduces no parameter and there is nothing to overfit.

THE PRECEDENCE RULE, AND WHY BLAST RADIUS IS NOT PER-DEPENDENCY

``FDViolationRepairer._propose`` consults the dependencies naming a column in canonical
determinant order and acts on the **first** whose determinant group shows more than one
distinct dependent value. It returns on that first match: if the chosen value equals the
current one, no write happens *at all*, even where a later dependency would have written.

So a candidate dependency's consequence depends on which other dependencies are accepted --
an earlier-sorted determinant can mask it entirely. A per-candidate number computed in
isolation would therefore be wrong in both directions, which is why the reviewer-facing
quantity is the **marginal** blast radius of accepting a candidate given the current
accepted set. ``docs/trust/constraint-additivity.md`` measures the masking this precedence
causes; this module respects it rather than assuming independence.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from pydantic import BaseModel, Field

from dataforge.detectors.base import FunctionalDependency, Schema
from dataforge.table import TableLike, column_names, row_count

WITNESS_SCHEMA_VERSION: Final = "entailment_witness_v1"

#: Values retained per group in a serialized witness. A witness is embedded in receipts and
#: attestations, so it is a bounded budget rather than "whatever the group contained": an
#: unbounded witness on a 200,000-row corpus grows until it breaks something it shares a
#: payload with. `group_size` and `truncated` preserve the shape that was dropped.
MAX_WITNESS_VALUES: Final = 8


def fd_label(fd: FunctionalDependency) -> str:
    """Render a dependency as a stable, readable key.

    Matches ``scripts/bench/measure_deductive_coverage.py::_fd_label`` so witness output and
    the measurement harness name the same dependency the same way.
    """
    return f"{' + '.join(fd.determinant)} -> {fd.dependent}"


class GroupDistribution(BaseModel):
    """The value distribution of one determinant group, as the witness records it."""

    group_size: int = Field(ge=0)
    #: Value -> count, largest first, capped at :data:`MAX_WITNESS_VALUES`.
    values: tuple[tuple[str, int], ...] = ()
    truncated: bool = False

    model_config = {"frozen": True}

    @classmethod
    def from_counts(cls, counts: Counter[str]) -> GroupDistribution:
        """Build a bounded distribution from a full value counter."""
        ranked = counts.most_common()
        return cls(
            group_size=sum(counts.values()),
            values=tuple((value, count) for value, count in ranked[:MAX_WITNESS_VALUES]),
            truncated=len(ranked) > MAX_WITNESS_VALUES,
        )


class EntailmentWitness(BaseModel):
    """The minimal evidence that entails one constraint-derived write.

    Carries what a third party needs to recompute the derivation from the data alone: which
    constraint acted, which group it looked at, what that group contained, and what the
    write destroys. That is what makes the claim checkable rather than merely labelled --
    kill criterion F4. ``dataforge/attestation`` embeds constraints in full for the same
    reason: "a digest and an id list are dangling pointers".
    """

    schema_version: str = WITNESS_SCHEMA_VERSION
    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    #: The constraint that entailed the write, e.g. ``"state -> city"``.
    constraint: str = Field(min_length=1)
    constraint_kind: str = Field(min_length=1)
    #: Determinant column -> the value shared by every row in the group.
    determinant: tuple[tuple[str, str], ...] = ()
    distribution: GroupDistribution
    #: The value currently in the cell, which the write would replace.
    old_value: str
    #: The value the constraint entails.
    new_value: str
    #: Support for ``new_value`` within the group.
    support: int = Field(ge=0)

    model_config = {"frozen": True}

    @property
    def destroys(self) -> int:
        """Rows in this group holding a value the write disagrees with.

        The reviewer-facing quantity. A dependency whose group is unanimous destroys
        nothing and is inert however false it is; that asymmetry is the whole finding.
        """
        return sum(count for value, count in self.distribution.values if value != self.new_value)

    def to_attestation_payload(self) -> dict[str, Any]:
        """Project this witness into the form an attestation may carry.

        **Values are hashed, and that is load-bearing rather than defensive.** The
        attestation predicate deliberately carries no cell values -- ``build_attestation``
        projects each fix to row, column, detector, provenance and strength and drops
        ``old_value``/``new_value``. A witness stating a group's value distribution in
        plaintext would reverse that decision silently and turn a document meant for sharing
        into a data-disclosure vector.

        Hashing costs nothing that matters. A third party **holding the table** hashes their
        own group values and compares counts, so the derivation stays fully checkable -- in
        SQL, in any language, with no DataForge code. A third party **without** the table
        learns only the shape: how large the group was, how many distinct values it held, and
        what share the majority had. That is exactly the asymmetry a portable proof wants.

        The digest is ``sha256(value)[:16]``, the same construction
        ``dataforge/datasets/wild_corrections.py`` already uses for corpora whose bytes may
        not be vendored, so there is one convention rather than two.
        """
        return {
            "constraint": self.constraint,
            "constraint_kind": self.constraint_kind,
            "determinant_columns": [name for name, _value in self.determinant],
            "determinant_digests": [value_digest(value) for _name, value in self.determinant],
            "group_size": self.distribution.group_size,
            "value_digests": [
                [value_digest(value), count] for value, count in self.distribution.values
            ],
            "truncated": self.distribution.truncated,
            "support": self.support,
            "old_value_digest": value_digest(self.old_value),
            "new_value_digest": value_digest(self.new_value),
        }


def value_digest(value: str) -> str:
    """Return the published short digest of a cell value.

    ``sha256(value)[:16]``. Matches ``dataforge/datasets/wild_corrections.py`` so a witness
    digest and a corpus join key are the same construction rather than two conventions that
    look alike.
    """
    from hashlib import sha256

    return sha256(value.encode("utf-8")).hexdigest()[:16]


def _cell(table: TableLike, row: int, column: str) -> str:
    from dataforge.table import cell_value

    return str(cell_value(table, row, column))


def _strict_majority(counts: Counter[str]) -> tuple[str, int] | None:
    """Return the strictly-majority value and its support, or ``None``.

    Strict, not plurality: more than half the group. Mutant ``M16`` records the difference
    -- reverting to plurality writes on 2 votes of 5 across four distinct values, with
    ``deterministic`` provenance that bypasses calibration, and it is worse on every
    measured axis (write precision 0.5618 against 0.6602, clean cells corrupted 731
    against 344).
    """
    if not counts:
        return None
    value, count = counts.most_common(1)[0]
    return (value, count) if count * 2 > sum(counts.values()) else None


class _GroupIndex:
    """Determinant-group value counts for one dependency, built in a single pass.

    Cost is the reason this class exists rather than a per-cell scan. The obvious
    implementation -- for each cell, for each dependency, walk the table looking for rows
    that share the determinant -- is O(rows^2 x dependencies), which is 10^10 row
    comparisons on tax's 200,000 rows and would have made the instrument unusable on
    exactly the corpus that tests its limits. Grouping once per dependency is O(rows), so
    a full blast radius is O(rows x dependencies).

    Pre-registered as kill criterion F5, and measured by counted work rather than wall
    clock: the same verifier code has measured 42 to 352 ms/fix on the development machine
    within one afternoon, so a wall-clock budget here would gate on scheduler noise.
    """

    __slots__ = ("fd", "keys", "counts")

    def __init__(self, table: TableLike, fd: FunctionalDependency) -> None:
        self.fd = fd
        determinant = list(fd.determinant)
        dependent = fd.dependent
        rows = row_count(table)
        self.keys: list[tuple[str, ...]] = []
        self.counts: dict[tuple[str, ...], Counter[str]] = {}
        for row in range(rows):
            key = tuple(_cell(table, row, name) for name in determinant)
            self.keys.append(key)
            bucket = self.counts.get(key)
            if bucket is None:
                bucket = Counter()
                self.counts[key] = bucket
            bucket[_cell(table, row, dependent)] += 1

    def group_for(self, row: int) -> Counter[str]:
        return self.counts[self.keys[row]]

    def key_for(self, row: int) -> tuple[str, ...]:
        return self.keys[row]


def _build_indexes(
    table: TableLike,
    fds: tuple[FunctionalDependency, ...],
) -> list[_GroupIndex]:
    """Build one group index per usable dependency, in canonical determinant order.

    Canonical order is the precedence order ``FDViolationRepairer._propose`` uses, so the
    list can be scanned front-to-back to find the acting dependency for any cell.
    """
    columns = frozenset(column_names(table))
    usable = [
        fd
        for fd in sorted(fds, key=lambda item: tuple(item.determinant))
        if fd.dependent in columns and all(name in columns for name in fd.determinant)
    ]
    return [_GroupIndex(table, fd) for fd in usable]


def _witness_from_indexes(
    table: TableLike,
    row: int,
    column: str,
    indexes: list[_GroupIndex],
) -> EntailmentWitness | None:
    """Resolve the acting dependency for a cell and build its witness, if a write results."""
    for index in indexes:
        if index.fd.dependent != column:
            continue
        counts = index.group_for(row)
        if len(counts) <= 1:
            continue
        # First applicable dependency wins and the search STOPS here, exactly as in
        # `FDViolationRepairer._propose`. Falling through to a dependency that disagrees is
        # how a clean cell gets overwritten, so the early return is the semantics, not an
        # optimisation.
        winner = _strict_majority(counts)
        if winner is None:
            return None
        new_value, support = winner
        old_value = _cell(table, row, column)
        if new_value == old_value:
            return None
        return EntailmentWitness(
            row=row,
            column=column,
            constraint=fd_label(index.fd),
            constraint_kind="functional_dependency",
            determinant=tuple(zip(index.fd.determinant, index.key_for(row), strict=True)),
            distribution=GroupDistribution.from_counts(counts),
            old_value=old_value,
            new_value=new_value,
            support=support,
        )
    return None


def witness_for_cell(
    table: TableLike,
    row: int,
    column: str,
    fds: tuple[FunctionalDependency, ...],
) -> EntailmentWitness | None:
    """Return the witness for the write this premise would make to one cell, if any.

    ``None`` means no write: either no dependency applies, or the acting one has no strict
    majority, or its majority already equals the cell's value.

    Convenience entry point. It rebuilds the group indexes, so use :func:`blast_radius`
    when asking about more than one cell.
    """
    return _witness_from_indexes(table, row, column, _build_indexes(table, fds))


def blast_radius(
    table: TableLike,
    fds: tuple[FunctionalDependency, ...],
) -> list[EntailmentWitness]:
    """Return every write this set of dependencies would make to this table.

    The unit of account is the distinct cell, not the flag: a cell is written at most once,
    and with a mined premise the same cell is reachable through several dependencies naming
    its column. Counting flags would inflate every rate computed against them.

    Needs no ground truth, no detector and no solver -- which is what makes it usable on a
    customer table, and why the reviewer can be shown their own consequence instead of
    hospital's published statistic.
    """
    indexes = _build_indexes(table, fds)
    columns = frozenset(column_names(table))
    dependents = sorted({index.fd.dependent for index in indexes if index.fd.dependent in columns})
    rows = row_count(table)
    witnesses: list[EntailmentWitness] = []
    for column in dependents:
        for row in range(rows):
            witness = _witness_from_indexes(table, row, column, indexes)
            if witness is not None:
                witnesses.append(witness)
    return witnesses


def marginal_blast_radius(
    table: TableLike,
    accepted: tuple[FunctionalDependency, ...],
    candidate: FunctionalDependency,
) -> list[EntailmentWitness]:
    """Return the writes that accepting ``candidate`` would ADD to the current premise.

    The reviewer-facing quantity, and it is marginal rather than absolute for a measured
    reason: the repairer acts on the first applicable dependency in canonical determinant
    order, so an already-accepted determinant sorting earlier can mask a candidate
    completely, and a candidate sorting earlier can mask an accepted one. A per-candidate
    number computed in isolation is therefore wrong in both directions.

    Cells whose written value merely *changes* are included: replacing one write with a
    different one is a consequence of the acceptance, not a no-op.
    """
    before = {(w.row, w.column): w.new_value for w in blast_radius(table, accepted)}
    after = blast_radius(table, (*accepted, candidate))
    return [
        witness
        for witness in after
        if before.get((witness.row, witness.column)) != witness.new_value
    ]


def summarise(witnesses: list[EntailmentWitness]) -> dict[str, Any]:
    """Reduce witnesses to the counts a reviewer, a budget, and a report all need.

    ``cells_written`` is the budget's unit. ``values_destroyed`` is the reviewer's: it is
    the count of rows currently disagreeing with what would be written, which on a table
    with no ground truth is the closest observable analogue of harm.
    """
    by_constraint: dict[str, int] = {}
    by_column: dict[str, int] = {}
    destroyed = 0
    for witness in witnesses:
        by_constraint[witness.constraint] = by_constraint.get(witness.constraint, 0) + 1
        by_column[witness.column] = by_column.get(witness.column, 0) + 1
        destroyed += witness.destroys
    return {
        "cells_written": len(witnesses),
        "values_destroyed": destroyed,
        "by_constraint": dict(sorted(by_constraint.items())),
        "by_column": dict(sorted(by_column.items())),
    }


def witnesses_for_schema(table: TableLike, schema: Schema | None) -> list[EntailmentWitness]:
    """Return the blast radius of a schema's functional dependencies."""
    if schema is None or not schema.functional_dependencies:
        return []
    return blast_radius(table, tuple(schema.functional_dependencies))


def witnesses_for_applied_fixes(
    post_path: Path,
    applied_fixes: Sequence[Mapping[str, Any]],
    schema: Schema | None,
) -> dict[tuple[int, str], dict[str, Any]]:
    """Witness each applied fix, for embedding in an attestation.

    The attestation is over the POST-repair file, but a witness describes the group as it
    stood when the write was decided -- so the pre-repair state has to be recovered. It is
    recovered rather than carried: each applied fix records its own ``old_value``, so writing
    those back over the post-repair table reconstructs the input exactly.

    Doing it here, in the producer, is a deliberate choice over the alternative of computing
    witnesses during the repair and carrying them on the receipt. That alternative would put
    ``dataforge.witness`` on the write path, which trips the criterion-F3 tripwire in
    ``tests/unit/test_entailment_witness.py`` and would oblige a full K4 re-run -- hours of
    measurement -- to establish that a purely additive evidence field changed no verdict. A
    witness is evidence *about* a write; it has no business inside one.

    Returns an empty mapping rather than raising on any problem. This attaches optional
    evidence to a repair that already succeeded, and a completed mutation must not be
    reported as failed because its evidence could not be assembled.
    """
    if schema is None or not schema.functional_dependencies:
        return {}
    try:
        from dataforge.cli.common import read_csv

        table = read_csv(post_path)
        pre = _table_with_values_restored(table, applied_fixes)
    except Exception:  # noqa: BLE001 - optional evidence, never fatal to the repair
        return {}

    fds = tuple(schema.functional_dependencies)
    indexes = _build_indexes(pre, fds)
    out: dict[tuple[int, str], dict[str, Any]] = {}
    for fix in applied_fixes:
        row = fix.get("row")
        column = fix.get("column")
        if not isinstance(row, int) or not isinstance(column, str):
            continue
        try:
            witness = _witness_from_indexes(pre, row, column, indexes)
        except Exception:  # noqa: BLE001 - same reason as above
            continue
        if witness is not None:
            out[(row, column)] = witness.to_attestation_payload()
    return out


def _table_with_values_restored(
    table: TableLike,
    applied_fixes: Sequence[Mapping[str, Any]],
) -> TableLike:
    """Return the table with each applied fix's ``old_value`` written back."""
    from dataforge.table import Table

    columns = list(column_names(table))
    rows: list[dict[str, object]] = [
        {name: _cell(table, index, name) for name in columns} for index in range(row_count(table))
    ]
    for fix in applied_fixes:
        row = fix.get("row")
        column = fix.get("column")
        old_value = fix.get("old_value")
        if (
            isinstance(row, int)
            and isinstance(column, str)
            and 0 <= row < len(rows)
            and column in rows[row]
            and old_value is not None
        ):
            rows[row][column] = str(old_value)
    return Table(columns, rows)
