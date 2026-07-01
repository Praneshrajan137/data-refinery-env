"""Value-focused checks against inferred (advisory) constraints.

This module is the single source of truth for "does a proposed correction value
satisfy the inferred constraints for its column". It is shared by:

* :class:`dataforge.verifier.smt.SMTVerifier` -- the schema-less verification
  guard that rejects untrusted (e.g. LLM) corrections instead of structurally
  auto-accepting them when no authoritative schema exists; and
* :class:`dataforge.repairers.contract.CorrectionContract` -- the per-issue spec
  that tells a corrector what a valid value looks like and cheaply pre-filters
  candidates before the verifier/constitution gates.

Keeping the logic here guarantees the contract and the verifier never disagree:
a value the contract accepts as well-formed will not be rejected by the guard
for a value-local reason.

Design: pure Python, conservative. Value-local checks (type / numeric domain /
regex) inspect only the proposed value. The functional-dependency check is
table-relative but only fires on a *unanimous* determinant group -- the safest
signal that a dependent correction is wrong. Anything the inferred schema cannot
speak to passes through unchallenged.
"""

from __future__ import annotations

import re

from dataforge.table import TableLike, cell_value, column_names, row_count
from dataforge.verifier.schema import DomainBound, Schema

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")

# A correction may legitimately extrapolate slightly past the observed range
# (the observed range is drawn from dirty data). The guard only rejects values
# that fall well outside, controlled by this multiple of the observed span.
_DOMAIN_PAD_FRACTION = 0.5


def parse_numeric(value: str) -> float | None:
    """Parse a finite float from a string, or return ``None``."""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def type_violation(value: str, column_type: str) -> str | None:
    """Return a reason if ``value`` does not parse as ``column_type``."""
    column_type = column_type.strip().lower()
    if column_type in {"int", "integer"} and not _INT_RE.fullmatch(value):
        return f"value {value!r} is not a valid integer (inferred type int)"
    if column_type in {"float", "decimal", "real"} and not _FLOAT_RE.fullmatch(value):
        return f"value {value!r} is not a valid number (inferred type {column_type})"
    return None


def domain_violation(value: str, bound: DomainBound) -> str | None:
    """Return a reason if numeric ``value`` is far outside the inferred bound."""
    numeric = parse_numeric(value)
    if numeric is None:
        return None
    low = bound.min_value
    high = bound.max_value
    if low is None or high is None:
        return None
    span = high - low
    pad = (abs(high) * _DOMAIN_PAD_FRACTION if span == 0 else span * _DOMAIN_PAD_FRACTION) or 1.0
    if numeric < low - pad or numeric > high + pad:
        return f"value {numeric} is far outside the inferred numeric range [{low}, {high}]"
    return None


def regex_violation(value: str, pattern: str) -> str | None:
    """Return a reason if ``value`` does not match ``pattern``."""
    try:
        matches = re.fullmatch(pattern, value) is not None
    except re.error:
        return None
    if not matches:
        return f"value {value!r} does not match the inferred pattern {pattern!r}"
    return None


def value_local_violation(value: str, column: str, schema: Schema) -> str | None:
    """Return a reason if ``value`` violates a value-local inferred constraint.

    Covers type, numeric domain, and regex -- everything that depends only on
    the proposed value and the column's inferred constraints, not on other rows.
    """
    stripped = str(value).strip()
    column_type = schema.column_type(column) or "str"
    reason = type_violation(stripped, column_type)
    if reason is not None:
        return reason
    for bound in schema.domain_bounds_for(column):
        reason = domain_violation(stripped, bound)
        if reason is not None:
            return reason
    for rule in schema.regex_constraints_for(column):
        reason = regex_violation(stripped, rule.pattern)
        if reason is not None:
            return reason
    return None


def fd_consensus_violation(
    df: TableLike,
    row: int,
    column: str,
    value: str,
    schema: Schema,
) -> str | None:
    """Reject a dependent value that contradicts a unanimous determinant group.

    Only rejects when every *other* row sharing the determinant value agrees on
    a single dependent value that differs from the proposal. Mixed or empty
    groups pass through.
    """
    stripped = str(value).strip()
    columns = set(column_names(df))
    total_rows = row_count(df)

    for fd in schema.functional_dependencies:
        if fd.dependent != column:
            continue
        if any(det not in columns for det in fd.determinant):
            continue
        determinant_key = tuple(str(cell_value(df, row, det)).strip() for det in fd.determinant)
        if any(part == "" for part in determinant_key):
            continue

        peers: set[str] = set()
        for other in range(total_rows):
            if other == row:
                continue
            other_key = tuple(str(cell_value(df, other, det)).strip() for det in fd.determinant)
            if other_key != determinant_key:
                continue
            dependent_value = str(cell_value(df, other, column)).strip()
            if dependent_value:
                peers.add(dependent_value)

        if len(peers) == 1 and stripped not in peers:
            consensus = next(iter(peers))
            determinant_label = "+".join(fd.determinant)
            return (
                f"value {value!r} contradicts the inferred dependency "
                f"{determinant_label} -> {column}: rows with {determinant_key} "
                f"consistently show {consensus!r}"
            )
    return None


def inferred_value_violation(
    df: TableLike,
    row: int,
    column: str,
    value: str,
    schema: Schema,
) -> str | None:
    """Return a rejection reason if ``value`` violates any inferred constraint.

    Orchestrates the value-local checks and the table-relative FD consensus
    check. Returns ``None`` when the value is acceptable under inferred rules.
    """
    reason = value_local_violation(value, column, schema)
    if reason is not None:
        return reason
    return fd_consensus_violation(df, row, column, value, schema)
