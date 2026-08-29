"""Z3-backed candidate verifier for Week 3 repairs."""

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final

from z3 import (  # type: ignore[import-untyped]
    And,
    Bool,
    Function,
    Implies,
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

from dataforge.fd_index import DeterminantGroupIndex
from dataforge.repairers.base import ProposedFix
from dataforge.table import (
    TableLike,
    column_names,
    column_values,
    copy_table,
    row_count,
    set_cell_value,
)
from dataforge.verifier.explain import explain_unsat_core
from dataforge.verifier.inferred import inferred_value_violation
from dataforge.verifier.result import VerificationResult, VerificationVerdict
from dataforge.verifier.schema import DomainBound, FunctionalDependency, Schema

Z3ExprFactory = Callable[[Any], Any]
Z3ValueFactory = Callable[[str], Any]

#: Solver budget in milliseconds. 200 ms was hard-coded and unreachable: no CLI flag, no
#: environment variable, no config key, and ``SMTVerifier.verify`` does not forward the
#: parameter. That made it a coverage knob no operator could turn -- on a 1,000-row table the
#: whole-table encoding exhausted it on every fix, and a budget-exhausted UNKNOWN is collapsed to
#: REJECT by ``differential_verify``, so repairs were being dropped by a timeout nobody could see
#: or raise. The encoding is now scoped, which is the real fix; this makes the budget adjustable
#: for the cases scoping does not reach.
_TIMEOUT_ENV_VAR: Final[str] = "DATAFORGE_SMT_TIMEOUT_MS"
_DEFAULT_TIMEOUT_MS: Final[int] = 200


def _default_timeout_ms() -> int:
    """Return the configured solver budget, falling back to the documented default.

    Read per instance rather than at import, so a test or an operator can change it without
    reloading the module. A malformed or non-positive value falls back rather than raising: a
    verifier that refuses to start because an environment variable is wrong would fail *open*
    relative to the operator's intent, and the safe reading of "unset or nonsense" is "default".
    """
    raw = os.environ.get(_TIMEOUT_ENV_VAR)
    if raw is None:
        return _DEFAULT_TIMEOUT_MS
    try:
        parsed = int(raw)
    except ValueError:
        return _DEFAULT_TIMEOUT_MS
    return parsed if parsed > 0 else _DEFAULT_TIMEOUT_MS


@dataclass(frozen=True)
class _ColumnEncoding:
    """Z3 encoding helpers for one column."""

    name: str
    column_type: str
    function: Z3ExprFactory
    value_factory: Z3ValueFactory
    #: The pure-Python part of ``value_factory``: the coercion that can actually fail, with no z3
    #: involved. ``None`` means the column type cannot fail to coerce.
    #:
    #: This exists because the parity sweep below runs over EVERY row of every relevant column,
    #: and calling ``value_factory`` there built one z3 AST node per cell -- measured at 153,480
    #: ``StringVal`` calls over 15 fixes on hospital, 40% of verification cost, to answer a
    #: question no solver is needed for. For a ``str`` column it was worse than wasteful: every
    #: cell is already a string, so ``str(raw)`` cannot raise and the check was incapable of
    #: failing on all 20 of hospital's columns.
    python_coercion: Callable[[str], object] | None


class SchemaToSMT:
    """Compile candidate-local constraints from a schema and working dataframe.

    Only the rows a constraint's verdict can actually depend on are encoded. Until 2026-08-29
    every cell of every relevant column was asserted as a ground equality, which made one
    verification cost ``4 x relevant_columns x rows`` z3 AST nodes regardless of what the
    constraints looked at. Measured on ``hospital`` (1,000 rows, 53 oracle FDs): 10,000 ground
    assertions and roughly 40,000 AST nodes for a single fix, at **1,192 ms**, returning
    **UNKNOWN on 60 of 60** real proposals -- against ``DirectVerifier``'s 29.9 ms for a real
    verdict. Holding the fix constant and dropping the solver budget to 1 ms still cost 618 ms,
    so that part was never solving: it was Python-side AST construction, which no timeout can
    remove.

    The footprint of each constraint is small and knowable:

    * ``FD X -> Y``: only rows whose determinant tuple equals the candidate's **post-fix**
      tuple. Rows in other groups cannot change the verdict. When the fix targets a determinant
      column the row moves groups, so the post-fix tuple is the one that matters.
    * ``UNIQUE`` / ``PRIMARY KEY``: only rows already holding the candidate's new value. Rows
      holding anything else satisfy the disequality by construction, so asserting them adds
      ``rows``-many tautologies.
    * domain bounds, accepted values, NOT NULL, regex: the candidate row alone.

    Two further consequences, both deliberate:

    * The FD constraint was a ``ForAll`` over an unbounded integer, which leaves the decidable
      fragment -- z3's own guide warns that quantifier reasoning is undecidable and that it will
      "likely diverge" on satisfiable formulas with no finite model. Because the footprint is now
      an explicit finite row set, it is expanded into a conjunction of implications instead. That
      is the same transformation z3's documentation uses to argue decidability for bounded
      quantifiers, and it removes the UNKNOWN.
    * The tracked labels are unchanged, so ``explain_unsat_core`` and every unsat-core assertion
      in the suite keep working. This changes what is asserted, never what a verdict means.
    """

    def __init__(self, schema: Schema, df: TableLike, *, timeout_ms: int | None = None) -> None:
        self._schema = schema
        self._df = df
        self._timeout_ms = _default_timeout_ms() if timeout_ms is None else timeout_ms
        self._column_cache: dict[str, list[Any]] = {}
        # Targeted scans, not cached groupings. This class is rebuilt per fix (see
        # SMTVerifier.verify), so a full grouping over every row can never be reused, while a
        # targeted scan collects only the rows the constraint needs. That is an argument from the
        # instance's lifetime, not from a measurement.
        #
        # An earlier version of this comment cited "65.5 ms/fix with caching against 16.0 ms with
        # the scan". That comparison was invalid: the 65.5 ms figure came from a ``Table`` and the
        # 16.0 ms figure from a pandas frame, and caching is only reachable on ``Table``, so the
        # two runs differed in representation as well as in configuration. A later attempt to
        # settle it by wall clock failed too -- three repeats of identical code spanned 79.8 to
        # 352.2 ms/fix. The choice stands on the lifetime argument alone until a deterministic
        # counted measurement can decide it.
        self._group_index = DeterminantGroupIndex(
            lambda _df, column: self._column(column),
            cache_groups=False,
        )

    def _column(self, column: str) -> list[Any]:
        """Return one column's values, materialised once per verifier instance."""
        cached = self._column_cache.get(column)
        if cached is None:
            cached = column_values(self._df, column)
            self._column_cache[column] = cached
        return cached

    def _post_fix_value(self, row: int, column: str, proposed_fix: ProposedFix) -> str:
        """Return a cell as it would read after the candidate fix is applied."""
        if row == proposed_fix.fix.row and column == proposed_fix.fix.column:
            return proposed_fix.fix.new_value
        return str(self._column(column)[row])

    def _fd_peer_rows(
        self,
        fd: FunctionalDependency,
        proposed_fix: ProposedFix,
    ) -> list[int]:
        """Return one representative row per distinct dependent value in the determinant group.

        These are the rows whose dependent value can witness a violation. A row in a different
        determinant group is unconstrained by this FD relative to the candidate, so encoding it can
        only add work.

        The grouping comes from ``DeterminantGroupIndex`` so it is one pass per determinant per
        pass, not one scan per fix. Note the post-fix substitution: when the fix targets a
        determinant column the row moves groups, so the tuple that matters is the one it will have
        *after* the write, not the one it has now.

        **Peers are then deduplicated by dependent value.** Every peer shares the determinant by
        construction, so the constraint each contributes reduces to
        ``dependent(candidate) == dependent(peer)``. Two peers holding the same dependent value
        therefore assert the identical fact, and z3's documentation states that in SMT 2.0 "the goal
        is the conjunction of all assertions" -- so dropping a duplicate conjunct is
        semantics-preserving by construction, not an approximation. On hospital's oracle premise
        some determinant groups run to hundreds of rows over a handful of distinct dependent values,
        and this is where that ratio is paid back. It shrinks the footprint as well as the
        constraint, because a peer that is not encoded needs no ground value either.
        """
        candidate_row = proposed_fix.fix.row
        determinant = tuple(fd.determinant)
        target = tuple(
            self._post_fix_value(candidate_row, column, proposed_fix) for column in determinant
        )
        rows = self._group_index.rows_for_key(self._df, determinant, target)
        representatives: dict[str, int] = {}
        for index in rows:
            if index == candidate_row:
                continue
            value = self._post_fix_value(index, fd.dependent, proposed_fix)
            representatives.setdefault(value, index)
        return sorted(representatives.values())

    def _unique_peer_rows(self, column: str, proposed_fix: ProposedFix) -> list[int]:
        """Return rows already holding the candidate's new value, excluding the candidate.

        Any other row satisfies the disequality under the ground assignment, so asserting it
        would be a tautology. Dropping tautologies cannot change satisfiability.
        """
        candidate_row = proposed_fix.fix.row
        new_value = proposed_fix.fix.new_value
        values = self._column(column)
        return [
            index
            for index in range(row_count(self._df))
            if index != candidate_row and str(values[index]) == new_value
        ]

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

        # Type-encoding parity with DirectVerifier, which is NOT part of the footprint argument
        # and must not be scoped away. ``direct.py`` returns UNKNOWN when any value in a relevant
        # column cannot be coerced to its declared type, on the stated grounds that "the primary
        # verifier likewise cannot encode it". That was true while every cell was encoded. Scoping
        # the encoding made it false, and the two verifiers diverged: with an uncoercible value in
        # a row outside the footprint, SMT returned ACCEPT while Direct returned UNKNOWN. The
        # differential caught it and failed closed, so nothing unsound could be written -- but two
        # independently-written verifiers disagreeing on an input class is precisely what the
        # N-version design exists to prevent, and the equivalence property missed it because its
        # tables are 2-4 rows of well-typed values.
        #
        # So coercibility is checked over the WHOLE relevant column, deliberately, while only the
        # footprint is asserted into the solver.
        #
        # It is checked with ``python_coercion`` -- ``int`` or ``float`` -- and NOT with
        # ``value_factory``. Using the latter built a z3 AST node per cell: 153,480 ``StringVal``
        # calls over 15 fixes on hospital, 40% of verification cost, for a question that needs no
        # solver. A ``str`` column skips the loop entirely because ``str`` is total, so the check
        # could never have failed on any of hospital's 20 columns.
        #
        # Whether holding a provable repair because an unrelated row holds garbage is the RIGHT
        # semantics is a separate question. Arguably SMT's scoped ACCEPT was the better answer and
        # Direct is the over-conservative one. That is a deliberate coverage change and belongs in
        # its own pre-registered decision, not smuggled in as a side effect of a speedup.
        for name, encoding in encodings.items():
            coerce_value = encoding.python_coercion
            if coerce_value is None:
                continue
            values = self._column(name)
            for index in range(len(values)):
                raw = self._post_fix_value(index, name, proposed_fix)
                try:
                    coerce_value(raw)
                except (TypeError, ValueError):
                    return VerificationResult(
                        verdict=VerificationVerdict.UNKNOWN,
                        reason=(
                            f"Could not encode value '{raw}' for column '{name}' "
                            f"as type '{encoding.column_type}'."
                        ),
                    )

        # The footprint: the candidate row, plus each relevant FD's determinant group, plus any
        # row already holding the candidate value where uniqueness applies. Everything outside
        # this set is provably unable to change the verdict.
        fd_peers = {fd: self._fd_peer_rows(fd, proposed_fix) for fd in relevant_fds}
        uniqueness_applies = (
            column in self._schema.unique_columns or column in self._schema.primary_key_columns
        )
        unique_peers = self._unique_peer_rows(column, proposed_fix) if uniqueness_applies else []
        relevant_rows = {row}
        for peers in fd_peers.values():
            relevant_rows.update(peers)
        relevant_rows.update(unique_peers)

        solver = Solver()
        solver.set(timeout=self._timeout_ms, unsat_core=True)

        try:
            self._add_value_assignments(solver, encodings, proposed_fix, sorted(relevant_rows))
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
                    unique_peers,
                    label_prefix="primary_key_unique",
                )
            if column_name in self._schema.unique_columns:
                self._track_unique(solver, encodings[column_name], proposed_fix, unique_peers)
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
            self._track_fd_constraint(solver, encodings, proposed_fix, fd, fd_peers[fd])

        started = time.perf_counter()
        result = solver.check()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
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
            # Classify by our own wall clock rather than by parsing reason_unknown(). Upstream
            # z3 reports that string non-deterministically for the same cause -- see
            # Z3Prover/z3#445, where the reporter observed "sometimes I get `timeout` and
            # sometimes I get `unknown`" and only the separate default-value bug was fixed. There
            # is also no documented enumeration of the possible strings and no stability
            # guarantee, so branching on substrings would be building logic on undefined
            # behaviour. The budget is ours, so the measurement is ours.
            #
            # This distinction is not cosmetic: differential_verify collapses UNKNOWN into
            # REJECT, so a fix dropped because the solver ran out of budget is otherwise
            # indistinguishable from a fix that genuinely violates a constraint. One is fixable
            # by raising DATAFORGE_SMT_TIMEOUT_MS; the other means the repair was wrong.
            if elapsed_ms >= self._timeout_ms * 0.9:
                return VerificationResult(
                    verdict=VerificationVerdict.UNKNOWN,
                    reason=(
                        f"Solver budget exhausted after {self._timeout_ms} ms, so no verdict was "
                        f"reached. This is a timeout, NOT a constraint violation; the fix is held "
                        f"fail-closed. Raise {_TIMEOUT_ENV_VAR} to give the solver more time. "
                        f"(z3 reported: {solver.reason_unknown()})"
                    ),
                )
            return VerificationResult(
                verdict=VerificationVerdict.UNKNOWN,
                reason=(
                    f"Solver returned unknown in {elapsed_ms:.0f} ms, well inside its "
                    f"{self._timeout_ms} ms budget, so this is incompleteness rather than a "
                    f"timeout and more time will not help: {solver.reason_unknown()}"
                ),
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
                python_coercion=int,
            )
        if column_type in {"float", "decimal", "real"}:
            return _ColumnEncoding(
                name=column,
                column_type=column_type,
                function=Function(function_name, IntSort(), RealSort()),
                value_factory=lambda raw: RealVal(str(float(raw))),
                python_coercion=float,
            )
        if column_type in {"str", "string"}:
            return _ColumnEncoding(
                name=column,
                column_type=column_type,
                function=Function(function_name, IntSort(), StringSort()),
                value_factory=lambda raw: StringVal(str(raw)),
                # No coercion can fail: the cell is already a string and ``str`` is total.
                python_coercion=None,
            )
        raise ValueError(f"Unsupported schema type '{column_type}' for column '{column}'.")

    def _add_value_assignments(
        self,
        solver: Solver,
        encodings: dict[str, _ColumnEncoding],
        proposed_fix: ProposedFix,
        rows: list[int],
    ) -> None:
        """Assert ground values for the footprint rows only.

        ``rows`` is the union of every constraint's footprint, so a row omitted here is one no
        tracked constraint can reference. Its column function stays uninterpreted, which is
        sound: an uninterpreted application appears in no assertion, so it constrains nothing
        and cannot flip a verdict.
        """
        for column, encoding in encodings.items():
            values = self._column(column)
            for index in rows:
                raw_value = str(values[index])
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
        peer_rows: list[int],
        *,
        label_prefix: str = "unique",
    ) -> None:
        """Track that the candidate value differs from every row that already holds it.

        ``peer_rows`` is the set of rows whose current value equals the candidate's new value.
        Every other row satisfies the disequality under the ground assignment, so asserting it
        would add a tautology; dropping tautologies cannot change satisfiability. When the set is
        empty the constraint holds and nothing is asserted -- previously this built ``rows - 1``
        disequalities to reach the same conclusion.
        """
        if not peer_rows:
            return
        candidate_expr = encoding.function(IntVal(proposed_fix.fix.row))
        other_rows = [encoding.function(IntVal(index)) != candidate_expr for index in peer_rows]
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
        peer_rows: list[int],
    ) -> None:
        """Track the FD over its determinant group, expanded rather than quantified.

        This was a ``ForAll`` over an unbounded ``Int``, which took the problem out of the
        decidable quantifier-free fragment for no benefit: the row set is finite, known, and
        ground. z3's guide is explicit that quantifier reasoning is undecidable and that the
        solver will "likely diverge" on satisfiable formulas without finite models, and its own
        decidability argument for bounded quantifiers is expansion into a finite conjunction.
        That is what this does.

        ``peer_rows`` already shares the candidate's post-fix determinant tuple and has been
        deduplicated by dependent value (see ``_fd_peer_rows``), so ``determinant_equal`` is true
        for each by construction. The implication is kept anyway: it makes the assertion
        self-evidently equivalent to the quantified form, and it keeps the encoding correct if the
        peer computation is ever widened.
        """
        if not peer_rows:
            return
        candidate_row = IntVal(proposed_fix.fix.row)
        clauses = []
        for index in peer_rows:
            other_row = IntVal(index)
            determinant_equal = And(
                *[
                    encodings[col].function(candidate_row) == encodings[col].function(other_row)
                    for col in fd.determinant
                ]
            )
            dependent_equal = encodings[fd.dependent].function(candidate_row) == encodings[
                fd.dependent
            ].function(other_row)
            clauses.append(Implies(determinant_equal, dependent_equal))
        determinant_label = "+".join(fd.determinant)
        label = Bool(f"fd::{determinant_label}::{fd.dependent}::row::{proposed_fix.fix.row}")
        solver.assert_and_track(And(*clauses), label)


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
