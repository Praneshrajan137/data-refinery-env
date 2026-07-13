"""DirectVerifier: an independently-written constraint checker (N-version diversity).

This is a second, deliberately-diverse implementation of the authoritative
schema-verification specification that the z3-backed
:class:`~dataforge.verifier.smt.SMTVerifier` implements. Where the primary
verifier compiles constraints into an SMT problem and asks a solver, this one
evaluates every constraint by DIRECT table inspection -- set membership,
comparison, and enumeration in plain Python.

The two share only the *specification* (:class:`~dataforge.verifier.schema.Schema`),
the *output contract* (:class:`~dataforge.verifier.result.VerificationResult`), and
the table *accessors* (data I/O). They share NONE of their checking logic and this
module imports NO z3. Cross-checking the two (fail-closed on disagreement) turns
"trust the verifier" into "two independent verifiers agree", so a bug in either
implementation is caught rather than silently trusted.

Scope: this twins the crisp AUTHORITATIVE path (a declared/reviewed schema). The
advisory inferred guard (heuristic, and only ever gating non-auto-applying
plausibility fixes) is intentionally not re-implemented here.
"""

from __future__ import annotations

import re

from dataforge.repairers.base import ProposedFix
from dataforge.table import (
    TableLike,
    cell_value,
    column_names,
    copy_table,
    row_count,
    set_cell_value,
)
from dataforge.verifier.result import VerificationResult, VerificationVerdict
from dataforge.verifier.schema import Schema

_INT_TYPES = frozenset({"int", "integer"})
_FLOAT_TYPES = frozenset({"float", "decimal", "real"})
_STR_TYPES = frozenset({"str", "string"})


def _accept(reason: str) -> VerificationResult:
    return VerificationResult(verdict=VerificationVerdict.ACCEPT, reason=reason)


def _reject(reason: str, unsat_core: tuple[str, ...] = ()) -> VerificationResult:
    return VerificationResult(
        verdict=VerificationVerdict.REJECT, reason=reason, unsat_core=unsat_core
    )


def _unknown(reason: str) -> VerificationResult:
    return VerificationResult(verdict=VerificationVerdict.UNKNOWN, reason=reason)


def _normalize_type(column_type: str | None) -> str:
    return (column_type or "str").strip().lower()


def _typed(value: str, column_type: str) -> int | float | str:
    """Coerce a raw value to its declared type, raising ValueError if it cannot.

    Mirrors the primary verifier's z3 value factories: int columns require an
    integer literal, float columns a real, string columns pass through.
    """
    if column_type in _INT_TYPES:
        return int(value)
    if column_type in _FLOAT_TYPES:
        return float(value)
    return str(value)


class DirectVerifier:
    """Independent, z3-free constraint checker (an N-version twin of SMTVerifier)."""

    def verify(
        self,
        df: TableLike,
        fixes: list[ProposedFix],
        schema: Schema | None = None,
        *,
        verification_schema: Schema | None = None,
    ) -> VerificationResult:
        """Verify one or more fixes against the working dataframe by direct evaluation.

        Dispatch mirrors ``SMTVerifier.verify``: with no authoritative ``schema``
        only structural checks run (the advisory inferred guard is out of scope
        for this diverse checker); with a schema, fixes are verified sequentially
        against a working copy.
        """
        if schema is None:
            total_rows = row_count(df)
            columns = set(column_names(df))
            for proposed in fixes:
                if proposed.fix.row < 0 or proposed.fix.row >= total_rows:
                    return _reject(f"Row {proposed.fix.row} is out of bounds for the input file.")
                if proposed.fix.column not in columns:
                    return _reject(
                        f"Column '{proposed.fix.column}' does not exist in the input file."
                    )
            return _accept("All proposed fixes passed structural verification (direct).")

        working_df = copy_table(df)
        for proposed in fixes:
            result = self._verify_fix(working_df, schema, proposed)
            if result.verdict != VerificationVerdict.ACCEPT:
                return result
            set_cell_value(
                working_df, proposed.fix.row, proposed.fix.column, proposed.fix.new_value
            )
        return _accept("All proposed fixes passed the direct verifier.")

    def _verify_fix(
        self, df: TableLike, schema: Schema, proposed: ProposedFix
    ) -> VerificationResult:
        fix = proposed.fix
        if fix.operation != "update":
            return _reject("Only cell updates are supported by the verifier.")
        if fix.row < 0 or fix.row >= row_count(df):
            return _reject(f"Row {fix.row} is out of bounds for the input file.")
        columns = set(column_names(df))
        if fix.column not in columns:
            return _reject(f"Column '{fix.column}' does not exist in the input file.")

        column = fix.column
        new_value = fix.new_value

        relevant_fds = tuple(
            fd
            for fd in schema.functional_dependencies
            if column == fd.dependent or column in fd.determinant
        )
        relevant_columns = {column}
        for fd in relevant_fds:
            relevant_columns.update(fd.determinant)
            relevant_columns.add(fd.dependent)

        # Type-encoding parity: an unsupported declared type, or any value in a
        # relevant column that cannot be coerced to it, is UNKNOWN (the primary
        # verifier likewise cannot encode it). ``new_value`` substitutes its cell.
        def read(index: int, col: str) -> str:
            if index == fix.row and col == column:
                return new_value
            return cell_value(df, index, col)

        for col in sorted(relevant_columns):
            ctype = _normalize_type(schema.column_type(col))
            if ctype not in _INT_TYPES | _FLOAT_TYPES | _STR_TYPES:
                return _unknown(f"Unsupported schema type '{ctype}' for column '{col}'.")
            for index in range(row_count(df)):
                try:
                    _typed(read(index, col), ctype)
                except (TypeError, ValueError):
                    return _unknown(f"Could not encode value for column '{col}' as type '{ctype}'.")

        ctype = _normalize_type(schema.column_type(column))

        # NOT NULL / PRIMARY KEY not-null (string columns only, mirroring primary).
        if (column in schema.not_null_columns or column in schema.primary_key_columns) and (
            ctype in _STR_TYPES and new_value == ""
        ):
            return _reject(
                f"Value for column '{column}' must not be empty.",
                (f"not_null::{column}::row::{fix.row}",),
            )

        # UNIQUE / PRIMARY KEY uniqueness: candidate distinct from all other rows.
        if column in schema.unique_columns or column in schema.primary_key_columns:
            typed_new = _typed(new_value, ctype)
            for index in range(row_count(df)):
                if index == fix.row:
                    continue
                if _typed(cell_value(df, index, column), ctype) == typed_new:
                    return _reject(
                        f"Value for column '{column}' must be unique.",
                        (f"unique::{column}::row::{fix.row}",),
                    )

        # ACCEPTED VALUES: candidate must belong to the closed set (per declared type).
        for accepted_rule in schema.accepted_values_for(column):
            if not accepted_rule.values:
                continue
            try:
                allowed = {_typed(value, ctype) for value in accepted_rule.values}
            except (TypeError, ValueError):
                return _unknown(
                    f"Could not encode accepted values for column '{column}' as type '{ctype}'."
                )
            if _typed(new_value, ctype) not in allowed:
                return _reject(
                    f"Value for column '{column}' is not in the accepted set.",
                    (f"accepted_values::{column}::row::{fix.row}",),
                )

        # REGEX: candidate string must fully match the declared pattern.
        for regex_rule in schema.regex_constraints_for(column):
            try:
                matches = re.fullmatch(regex_rule.pattern, new_value) is not None
            except re.error as exc:
                return _unknown(f"Invalid regex constraint for column '{column}': {exc}")
            if not matches:
                return _reject(
                    f"Value for column '{column}' does not match the required pattern.",
                    (f"regex::{column}::row::{fix.row}",),
                )

        # DOMAIN BOUNDS: numeric range with inclusive/exclusive endpoints.
        for bound in schema.domain_bounds_for(column):
            if ctype in _STR_TYPES:
                continue  # a numeric bound on a string column is out of the well-defined spec
            numeric = float(new_value)
            if bound.min_value is not None:
                below = (
                    numeric < bound.min_value
                    if bound.inclusive_min
                    else (numeric <= bound.min_value)
                )
                if below:
                    return _reject(
                        f"Value {numeric} for column '{column}' is below the minimum.",
                        (f"domain::{column}::min::row::{fix.row}",),
                    )
            if bound.max_value is not None:
                above = (
                    numeric > bound.max_value
                    if bound.inclusive_max
                    else (numeric >= bound.max_value)
                )
                if above:
                    return _reject(
                        f"Value {numeric} for column '{column}' is above the maximum.",
                        (f"domain::{column}::max::row::{fix.row}",),
                    )

        # FUNCTIONAL DEPENDENCIES: for the candidate row, any other row that agrees
        # on the determinant must agree on the dependent.
        for fd in relevant_fds:
            dep_type = _normalize_type(schema.column_type(fd.dependent))
            det_types = {det: _normalize_type(schema.column_type(det)) for det in fd.determinant}
            candidate_det = {
                det: _typed(read(fix.row, det), det_types[det]) for det in fd.determinant
            }
            candidate_dep = _typed(read(fix.row, fd.dependent), dep_type)
            for index in range(row_count(df)):
                if index == fix.row:
                    continue
                same_determinant = all(
                    _typed(read(index, det), det_types[det]) == candidate_det[det]
                    for det in fd.determinant
                )
                if (
                    same_determinant
                    and _typed(read(index, fd.dependent), dep_type) != candidate_dep
                ):
                    label = "+".join(fd.determinant)
                    return _reject(
                        f"Functional dependency {label} -> {fd.dependent} violated.",
                        (f"fd::{label}::{fd.dependent}::row::{fix.row}",),
                    )

        return _accept("The candidate fix satisfied all tracked verifier constraints (direct).")
