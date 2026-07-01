"""Z3-backed candidate verifier for Week 3 repairs."""

from __future__ import annotations

import enum
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field
from z3 import (  # type: ignore[import-untyped]
    And,
    Bool,
    ForAll,
    Function,
    Implies,
    Int,
    IntSort,
    IntVal,
    Or,
    RealSort,
    RealVal,
    Solver,
    StringSort,
    StringVal,
    sat,
    unknown,
    unsat,
)

from dataforge.repairers.base import ProposedFix
from dataforge.table import (
    TableLike,
    cell_value,
    column_names,
    copy_table,
    row_count,
    set_cell_value,
)
from dataforge.verifier.explain import explain_unsat_core
from dataforge.verifier.inferred import inferred_value_violation
from dataforge.verifier.schema import DomainBound, FunctionalDependency, Schema

Z3ExprFactory = Callable[[Any], Any]
Z3ValueFactory = Callable[[str], Any]


class VerificationVerdict(enum.Enum):
    """Possible outcomes of the verifier gate."""

    ACCEPT = "accept"
    REJECT = "reject"
    UNKNOWN = "unknown"


class VerificationResult(BaseModel):
    """Typed result for the Week 3 verifier gate."""

    verdict: VerificationVerdict
    reason: str = Field(min_length=1)
    unsat_core: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}


@dataclass(frozen=True)
class _ColumnEncoding:
    """Z3 encoding helpers for one column."""

    name: str
    column_type: str
    function: Z3ExprFactory
    value_factory: Z3ValueFactory


class SchemaToSMT:
    """Compile candidate-local constraints from a schema and working dataframe."""

    def __init__(self, schema: Schema, df: TableLike, *, timeout_ms: int = 200) -> None:
        self._schema = schema
        self._df = df
        self._timeout_ms = timeout_ms

    def verify_fix(self, proposed_fix: ProposedFix) -> VerificationResult:
        """Return whether a candidate fix satisfies schema constraints."""
        if proposed_fix.fix.operation != "update":
            return VerificationResult(
                verdict=VerificationVerdict.REJECT,
                reason="Only cell updates are supported by the verifier.",
            )

        row = proposed_fix.fix.row
        column = proposed_fix.fix.column
        if row < 0 or row >= row_count(self._df):
            return VerificationResult(
                verdict=VerificationVerdict.REJECT,
                reason=f"Row {row} is out of bounds for the input file.",
            )
        if column not in column_names(self._df):
            return VerificationResult(
                verdict=VerificationVerdict.REJECT,
                reason=f"Column '{column}' does not exist in the input file.",
            )

        relevant_columns = {column}
        relevant_fds = tuple(
            fd
            for fd in self._schema.functional_dependencies
            if column == fd.dependent or column in fd.determinant
        )
        for fd in relevant_fds:
            relevant_columns.update(fd.determinant)
            relevant_columns.add(fd.dependent)

        try:
            encodings = {
                name: self._build_column_encoding(name) for name in sorted(relevant_columns)
            }
        except ValueError as exc:
            return VerificationResult(
                verdict=VerificationVerdict.UNKNOWN,
                reason=str(exc),
            )

        solver = Solver()
        solver.set(timeout=self._timeout_ms, unsat_core=True)

        try:
            self._add_value_assignments(solver, encodings, proposed_fix)
        except ValueError as exc:
            return VerificationResult(
                verdict=VerificationVerdict.UNKNOWN,
                reason=str(exc),
            )

        for column_name in sorted(
            schema_column
            for schema_column in (
                set(self._schema.not_null_columns)
                | set(self._schema.primary_key_columns)
                | set(self._schema.unique_columns)
                | {rule.column for rule in self._schema.accepted_values}
                | {rule.column for rule in self._schema.regex_constraints}
            )
            if schema_column == column
        ):
            if column_name in self._schema.not_null_columns:
                self._track_not_null(solver, encodings[column_name], proposed_fix)
            if column_name in self._schema.primary_key_columns:
                self._track_not_null(
                    solver,
                    encodings[column_name],
                    proposed_fix,
                    label_prefix="primary_key_not_null",
                )
                self._track_unique(
                    solver,
                    encodings[column_name],
                    proposed_fix,
                    label_prefix="primary_key_unique",
                )
            if column_name in self._schema.unique_columns:
                self._track_unique(solver, encodings[column_name], proposed_fix)
            for rule in self._schema.accepted_values_for(column_name):
                try:
                    self._track_accepted_values(
                        solver,
                        encodings[column_name],
                        proposed_fix,
                        rule.values,
                    )
                except ValueError as exc:
                    return VerificationResult(
                        verdict=VerificationVerdict.UNKNOWN,
                        reason=str(exc),
                    )
            regex_result = self._check_regex_constraints(column_name, proposed_fix)
            if regex_result is not None:
                return regex_result

        for bound in self._schema.domain_bounds_for(column):
            self._track_domain_bound(solver, encodings[column], proposed_fix, bound)

        for fd in relevant_fds:
            self._track_fd_constraint(solver, encodings, proposed_fix, fd)

        result = solver.check()
        if result == sat:
            return VerificationResult(
                verdict=VerificationVerdict.ACCEPT,
                reason="The candidate fix satisfied all tracked verifier constraints.",
            )
        if result == unsat:
            unsat_core = tuple(str(label) for label in solver.unsat_core())
            return VerificationResult(
                verdict=VerificationVerdict.REJECT,
                reason=explain_unsat_core(unsat_core, self._schema),
                unsat_core=unsat_core,
            )
        if result == unknown:
            return VerificationResult(
                verdict=VerificationVerdict.UNKNOWN,
                reason=f"Solver returned unknown: {solver.reason_unknown()}",
            )
        return VerificationResult(
            verdict=VerificationVerdict.UNKNOWN,
            reason="Solver returned an unrecognized status.",
        )

    def _build_column_encoding(self, column: str) -> _ColumnEncoding:
        column_type = (self._schema.column_type(column) or "str").strip().lower()
        function_name = f"col_{column.replace(' ', '_')}"
        if column_type in {"int", "integer"}:
            return _ColumnEncoding(
                name=column,
                column_type=column_type,
                function=Function(function_name, IntSort(), IntSort()),
                value_factory=lambda raw: IntVal(int(raw)),
            )
        if column_type in {"float", "decimal", "real"}:
            return _ColumnEncoding(
                name=column,
                column_type=column_type,
                function=Function(function_name, IntSort(), RealSort()),
                value_factory=lambda raw: RealVal(str(float(raw))),
            )
        if column_type in {"str", "string"}:
            return _ColumnEncoding(
                name=column,
                column_type=column_type,
                function=Function(function_name, IntSort(), StringSort()),
                value_factory=lambda raw: StringVal(str(raw)),
            )
        raise ValueError(f"Unsupported schema type '{column_type}' for column '{column}'.")

    def _add_value_assignments(
        self,
        solver: Solver,
        encodings: dict[str, _ColumnEncoding],
        proposed_fix: ProposedFix,
    ) -> None:
        for column, encoding in encodings.items():
            for index in range(row_count(self._df)):
                raw_value = cell_value(self._df, index, column)
                if index == proposed_fix.fix.row and column == proposed_fix.fix.column:
                    raw_value = proposed_fix.fix.new_value
                try:
                    z3_value = encoding.value_factory(raw_value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Could not encode value '{raw_value}' for column '{column}' "
                        f"as type '{encoding.column_type}'."
                    ) from exc
                solver.add(encoding.function(IntVal(index)) == z3_value)

    def _track_domain_bound(
        self,
        solver: Solver,
        encoding: _ColumnEncoding,
        proposed_fix: ProposedFix,
        bound: DomainBound,
    ) -> None:
        row_expr = encoding.function(IntVal(proposed_fix.fix.row))
        if bound.min_value is not None:
            label = Bool(f"domain::{bound.column}::min::row::{proposed_fix.fix.row}")
            threshold = (
                RealVal(str(bound.min_value))
                if encoding.column_type != "int"
                else IntVal(int(bound.min_value))
            )
            formula = row_expr >= threshold if bound.inclusive_min else row_expr > threshold
            solver.assert_and_track(formula, label)
        if bound.max_value is not None:
            label = Bool(f"domain::{bound.column}::max::row::{proposed_fix.fix.row}")
            threshold = (
                RealVal(str(bound.max_value))
                if encoding.column_type != "int"
                else IntVal(int(bound.max_value))
            )
            formula = row_expr <= threshold if bound.inclusive_max else row_expr < threshold
            solver.assert_and_track(formula, label)

    def _track_not_null(
        self,
        solver: Solver,
        encoding: _ColumnEncoding,
        proposed_fix: ProposedFix,
        *,
        label_prefix: str = "not_null",
    ) -> None:
        """Track a non-empty value constraint for the candidate cell."""
        if encoding.column_type not in {"str", "string"}:
            return
        label = Bool(f"{label_prefix}::{encoding.name}::row::{proposed_fix.fix.row}")
        row_expr = encoding.function(IntVal(proposed_fix.fix.row))
        empty_value = encoding.value_factory("")
        solver.assert_and_track(row_expr != empty_value, label)

    def _track_unique(
        self,
        solver: Solver,
        encoding: _ColumnEncoding,
        proposed_fix: ProposedFix,
        *,
        label_prefix: str = "unique",
    ) -> None:
        """Track that the candidate value is unique across all other rows."""
        other_rows = [
            encoding.function(IntVal(index)) != encoding.function(IntVal(proposed_fix.fix.row))
            for index in range(row_count(self._df))
            if index != proposed_fix.fix.row
        ]
        if not other_rows:
            return
        label = Bool(f"{label_prefix}::{encoding.name}::row::{proposed_fix.fix.row}")
        solver.assert_and_track(And(*other_rows), label)

    def _track_accepted_values(
        self,
        solver: Solver,
        encoding: _ColumnEncoding,
        proposed_fix: ProposedFix,
        values: tuple[str, ...],
    ) -> None:
        """Track that the candidate value belongs to a closed allowed set."""
        if not values:
            return
        row_expr = encoding.function(IntVal(proposed_fix.fix.row))
        try:
            allowed = [row_expr == encoding.value_factory(value) for value in values]
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Could not encode accepted values for column '{encoding.name}' "
                f"as type '{encoding.column_type}'."
            ) from exc
        label = Bool(f"accepted_values::{encoding.name}::row::{proposed_fix.fix.row}")
        solver.assert_and_track(Or(*allowed), label)

    def _check_regex_constraints(
        self,
        column: str,
        proposed_fix: ProposedFix,
    ) -> VerificationResult | None:
        """Conservatively evaluate declared regex constraints before solver check."""
        if column != proposed_fix.fix.column:
            return None
        for rule in self._schema.regex_constraints_for(column):
            try:
                matches = re.fullmatch(rule.pattern, proposed_fix.fix.new_value) is not None
            except re.error as exc:
                return VerificationResult(
                    verdict=VerificationVerdict.UNKNOWN,
                    reason=f"Invalid regex constraint for column '{column}': {exc}",
                )
            if not matches:
                label = f"regex::{column}::row::{proposed_fix.fix.row}"
                return VerificationResult(
                    verdict=VerificationVerdict.REJECT,
                    reason=explain_unsat_core((label,), self._schema),
                    unsat_core=(label,),
                )
        return None

    def _track_fd_constraint(
        self,
        solver: Solver,
        encodings: dict[str, _ColumnEncoding],
        proposed_fix: ProposedFix,
        fd: FunctionalDependency,
    ) -> None:
        # Use a universally-quantified implication over all valid other rows.
        other_row = Int("other_row")
        bounds_guard = And(other_row >= 0, other_row < row_count(self._df))
        candidate_row = IntVal(proposed_fix.fix.row)
        determinant_equal = And(
            *[
                encodings[column].function(candidate_row) == encodings[column].function(other_row)
                for column in fd.determinant
            ]
        )
        dependent_equal = encodings[fd.dependent].function(candidate_row) == encodings[
            fd.dependent
        ].function(other_row)
        determinant_label = "+".join(fd.determinant)
        label = Bool(f"fd::{determinant_label}::{fd.dependent}::row::{proposed_fix.fix.row}")
        solver.assert_and_track(
            ForAll([other_row], Implies(bounds_guard, Implies(determinant_equal, dependent_equal))),
            label,
        )


class SMTVerifier:
    """Compatibility wrapper over the Week 3 `SchemaToSMT` verifier."""

    def verify(
        self,
        df: TableLike,
        fixes: list[ProposedFix],
        schema: Schema | None = None,
        *,
        verification_schema: Schema | None = None,
    ) -> VerificationResult:
        """Verify one or more candidate fixes against the working dataframe.

        ``schema`` is the authoritative (declared or reviewed) schema and is
        verified rigorously with z3 when present. ``verification_schema`` is the
        advisory, inferred safety net used only when no authoritative schema
        exists: it lets the verifier reject clear violations of a proposed value
        (type / domain / regex / functional dependency) instead of structurally
        auto-accepting it. It is value-focused and never imposes inferred
        constraints on the rest of the (possibly dirty) table.
        """
        if schema is None:
            total_rows = row_count(df)
            for proposed in fixes:
                if proposed.fix.row < 0 or proposed.fix.row >= total_rows:
                    return VerificationResult(
                        verdict=VerificationVerdict.REJECT,
                        reason=f"Row {proposed.fix.row} is out of bounds for the input file.",
                    )
                if proposed.fix.column not in column_names(df):
                    return VerificationResult(
                        verdict=VerificationVerdict.REJECT,
                        reason=f"Column '{proposed.fix.column}' does not exist in the input file.",
                    )
            if verification_schema is None:
                return VerificationResult(
                    verdict=VerificationVerdict.ACCEPT,
                    reason="All proposed fixes passed structural verification.",
                )
            return self._verify_against_inferred(df, fixes, verification_schema)

        working_df = copy_table(df)
        verifier = SchemaToSMT(schema, working_df)
        for proposed in fixes:
            result = verifier.verify_fix(proposed)
            if result.verdict != VerificationVerdict.ACCEPT:
                return result
            set_cell_value(
                working_df, proposed.fix.row, proposed.fix.column, proposed.fix.new_value
            )
            verifier = SchemaToSMT(schema, working_df)
        return VerificationResult(
            verdict=VerificationVerdict.ACCEPT,
            reason="All proposed fixes passed the SMT verifier.",
        )

    def _verify_against_inferred(
        self,
        df: TableLike,
        fixes: list[ProposedFix],
        verification_schema: Schema,
    ) -> VerificationResult:
        """Value-focused advisory check against inferred constraints.

        Pure Python by design: it inspects only the *proposed value* (and, for
        functional dependencies, the determinant consensus). It never encodes
        sibling cells, so dirty rows cannot mask a violation or turn a valid
        correction UNKNOWN. Only definitive violations are rejected; anything
        the inferred schema cannot speak to passes.
        """
        working_df = copy_table(df)
        for proposed in fixes:
            if proposed.fix.operation != "update":
                # Inferred guard speaks only to cell values; defer other ops.
                continue
            violation = inferred_value_violation(
                working_df,
                proposed.fix.row,
                proposed.fix.column,
                str(proposed.fix.new_value),
                verification_schema,
            )
            if violation is not None:
                return VerificationResult(
                    verdict=VerificationVerdict.REJECT,
                    reason=(
                        f"Proposed value for column '{proposed.fix.column}' "
                        f"failed inferred-constraint verification: {violation}."
                    ),
                )
            set_cell_value(
                working_df, proposed.fix.row, proposed.fix.column, proposed.fix.new_value
            )
        return VerificationResult(
            verdict=VerificationVerdict.ACCEPT,
            reason="All proposed fixes passed inferred-constraint verification.",
        )
