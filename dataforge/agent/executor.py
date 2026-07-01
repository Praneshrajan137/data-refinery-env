"""Verified action executor for the DataForge agent.

The executor is the single place where an agent action touches data. Read-only
tool actions return observations; a ``FIX`` action is routed through the exact
same gates the deterministic pipeline uses — the constitutional
:class:`~dataforge.safety.SafetyFilter` and the
:class:`~dataforge.verifier.SMTVerifier` — and is staged only if BOTH accept.
Rejections return the safety reason and SMT unsat-core so the controller can
feed them back to the policy for self-correction.

This is the heart of the safety invariant: the policy proposes, the executor
disposes, and nothing unverified is ever staged for the transaction commit.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from dataforge.agent.scratchpad import Scratchpad
from dataforge.agent.tool_actions import (
    Action,
    Diagnose,
    Fix,
    Hypothesis,
    InspectRows,
    PatternMatch,
    RootCause,
    SqlQuery,
    StatTest,
)
from dataforge.detectors.base import Schema
from dataforge.repairers.base import ProposedFix
from dataforge.safety import SafetyContext, SafetyFilter, SafetyVerdict
from dataforge.table import (
    TableLike,
    cell_value,
    column_names,
    column_values,
    row_count,
    set_cell_value,
)
from dataforge.transactions.txn import CellFix
from dataforge.verifier import SMTVerifier, VerificationVerdict

__all__ = ["ActionOutcome", "VerifiedActionExecutor"]

_MAX_INSPECT_ROWS = 20
_MAX_SQL_ROWS = 50


@dataclass(frozen=True)
class ActionOutcome:
    """Result of executing a single agent action.

    Args:
        action_type: The dispatched action type.
        feedback: Human-readable result for the next observation's ``last_result``.
        accepted: For ``FIX``: whether it passed both gates. ``None`` otherwise.
        staged_fix: The verified fix staged for commit, if accepted.
        rejection_reason: Safety/verifier reason when a ``FIX`` is rejected.
        unsat_core: SMT unsat-core labels when the verifier rejected the fix.
        resolved_cell: The ``(row, column)`` resolved by an accepted fix.
    """

    action_type: str
    feedback: str
    accepted: bool | None = None
    staged_fix: ProposedFix | None = None
    rejection_reason: str | None = None
    unsat_core: tuple[str, ...] = field(default_factory=tuple)
    resolved_cell: tuple[int, str] | None = None


class VerifiedActionExecutor:
    """Execute agent actions against a working table with verified writes.

    Args:
        working_df: The post-floor working table. Accepted fixes mutate it in
            place so subsequent verification sees the corrected state.
        schema: Optional schema driving SMT verification and safety.
        safety_context: PII/escalation flags for the safety gate.
        scratchpad: Episode hypothesis tracker (created if not supplied).
        detector_id: Detector id stamped on agent-proposed cell fixes.
        provenance: Provenance label for agent-proposed fixes.
    """

    def __init__(
        self,
        working_df: TableLike,
        schema: Schema | None,
        *,
        safety_context: SafetyContext | None = None,
        scratchpad: Scratchpad | None = None,
        detector_id: str = "agent",
        provenance: str = "llm_live",
    ) -> None:
        self._df = working_df
        self._schema = schema
        self._safety = SafetyFilter()
        self._verifier = SMTVerifier()
        self._context = safety_context or SafetyContext()
        self._scratchpad = scratchpad or Scratchpad()
        self._detector_id = detector_id
        self._provenance = provenance
        self._staged: list[ProposedFix] = []
        self._resolved: set[tuple[int, str]] = set()

    @property
    def scratchpad(self) -> Scratchpad:
        """The episode scratchpad."""
        return self._scratchpad

    @property
    def staged_fixes(self) -> list[ProposedFix]:
        """Verified fixes staged for commit, in acceptance order."""
        return list(self._staged)

    @property
    def resolved_cells(self) -> set[tuple[int, str]]:
        """The ``(row, column)`` cells resolved by accepted agent fixes."""
        return set(self._resolved)

    def mark_resolved(self, row: int, column: str) -> None:
        """Reserve a cell so the agent cannot re-propose a fix for it.

        Used to lock cells the deterministic floor already fixed, preventing
        stale-value conflicts at commit time.
        """
        self._resolved.add((row, column))

    def execute(self, action: Action) -> ActionOutcome:
        """Dispatch an action to its handler."""
        if isinstance(action, Fix):
            return self._handle_fix(action)
        if isinstance(action, InspectRows):
            return self._handle_inspect(action)
        if isinstance(action, PatternMatch):
            return self._handle_pattern(action)
        if isinstance(action, StatTest):
            return self._handle_stat(action)
        if isinstance(action, SqlQuery):
            return self._handle_sql(action)
        if isinstance(action, Hypothesis):
            return self._handle_hypothesis(action)
        if isinstance(action, Diagnose):
            return self._handle_diagnose(action)
        if isinstance(action, RootCause):
            return self._handle_root_cause(action)
        return ActionOutcome(
            action_type=getattr(action, "action_type", "UNKNOWN"),
            feedback="Unsupported action type.",
        )

    # ── FIX: the verified write path ──────────────────────────────────────

    def _handle_fix(self, action: Fix) -> ActionOutcome:
        """Gate a proposed fix through safety + SMT; stage only if both accept."""
        columns = column_names(self._df)
        if action.column not in columns:
            return ActionOutcome(
                "FIX",
                f"FIX rejected: column {action.column!r} does not exist.",
                accepted=False,
                rejection_reason="column_not_found",
            )
        if action.row < 0 or action.row >= row_count(self._df):
            return ActionOutcome(
                "FIX",
                f"FIX rejected: row {action.row} is out of bounds.",
                accepted=False,
                rejection_reason="row_out_of_bounds",
            )
        if (action.row, action.column) in self._resolved:
            return ActionOutcome(
                "FIX",
                f"FIX rejected: cell ({action.row}, {action.column!r}) is already fixed.",
                accepted=False,
                rejection_reason="already_fixed",
            )

        old_value = cell_value(self._df, action.row, action.column)
        operation: Literal["update", "delete_row"] = (
            "delete_row" if action.fix_type == "delete_row" else "update"
        )
        cell_fix = CellFix(
            row=action.row,
            column=action.column,
            old_value=old_value,
            new_value=action.new_value,
            detector_id=self._detector_id,
            operation=operation,
        )
        proposed = ProposedFix(
            fix=cell_fix,
            reason=action.justification or "Agent-proposed repair.",
            confidence=0.6,
            provenance=self._provenance,  # type: ignore[arg-type]
        )

        safety_result = self._safety.evaluate(proposed, self._schema, self._context)
        if safety_result.verdict != SafetyVerdict.ALLOW:
            return ActionOutcome(
                "FIX",
                f"FIX rejected by safety constitution ({safety_result.verdict.value}): "
                f"{safety_result.reason}",
                accepted=False,
                rejection_reason=safety_result.reason,
            )

        verifier_result = self._verifier.verify(self._df, [proposed], self._schema)
        if verifier_result.verdict == VerificationVerdict.ACCEPT:
            set_cell_value(self._df, action.row, action.column, action.new_value)
            self._staged.append(proposed)
            self._resolved.add((action.row, action.column))
            return ActionOutcome(
                "FIX",
                f"FIX accepted and staged for row {action.row}, column {action.column!r}.",
                accepted=True,
                staged_fix=proposed,
                resolved_cell=(action.row, action.column),
            )

        core = list(verifier_result.unsat_core)
        return ActionOutcome(
            "FIX",
            f"FIX rejected by SMT verifier ({verifier_result.verdict.value}): "
            f"{verifier_result.reason}" + (f" unsat_core={core}" if core else ""),
            accepted=False,
            rejection_reason=verifier_result.reason,
            unsat_core=tuple(verifier_result.unsat_core),
        )

    # ── Read-only investigation tools ─────────────────────────────────────

    def _handle_inspect(self, action: InspectRows) -> ActionOutcome:
        """Return a slice of rows (optionally column-filtered)."""
        total = row_count(self._df)
        indices = [i for i in action.row_indices if 0 <= i < total][:_MAX_INSPECT_ROWS]
        columns = action.column_names or column_names(self._df)
        columns = [c for c in columns if c in column_names(self._df)]
        rows = {i: {c: cell_value(self._df, i, c) for c in columns} for i in indices}
        if not rows:
            return ActionOutcome("INSPECT_ROWS", "INSPECT_ROWS: no valid rows in range.")
        return ActionOutcome("INSPECT_ROWS", f"INSPECT_ROWS rows={rows}")

    def _handle_pattern(self, action: PatternMatch) -> ActionOutcome:
        """Report rows whose column value matches (or not) a regex."""
        if action.column not in column_names(self._df):
            return ActionOutcome(
                "PATTERN_MATCH", f"PATTERN_MATCH: column {action.column!r} not found."
            )
        try:
            pattern = re.compile(action.pattern)
        except re.error as exc:
            return ActionOutcome("PATTERN_MATCH", f"PATTERN_MATCH: invalid regex ({exc}).")
        hits: list[int] = []
        for i, value in enumerate(column_values(self._df, action.column)):
            matched = bool(pattern.fullmatch(str(value)))
            if matched == action.expect_match:
                hits.append(i)
        label = "matching" if action.expect_match else "non-matching"
        return ActionOutcome(
            "PATTERN_MATCH",
            f"PATTERN_MATCH column={action.column!r} {label} rows={hits[:_MAX_INSPECT_ROWS]} "
            f"(total {len(hits)}).",
        )

    def _handle_stat(self, action: StatTest) -> ActionOutcome:
        """Run a simple numeric outlier test on a column."""
        if action.column not in column_names(self._df):
            return ActionOutcome("STAT_TEST", f"STAT_TEST: column {action.column!r} not found.")
        numeric: list[tuple[int, float]] = []
        for i, value in enumerate(column_values(self._df, action.column)):
            try:
                numeric.append((i, float(str(value))))
            except (TypeError, ValueError):
                continue
        if len(numeric) < 3:
            return ActionOutcome(
                "STAT_TEST", f"STAT_TEST: column {action.column!r} has too few numeric values."
            )
        values = [v for _, v in numeric]
        outliers = self._outliers(action.test_type, numeric, values, action.threshold)
        return ActionOutcome(
            "STAT_TEST",
            f"STAT_TEST {action.test_type} column={action.column!r} "
            f"outlier_rows={outliers[:_MAX_INSPECT_ROWS]} (total {len(outliers)}).",
        )

    @staticmethod
    def _outliers(
        test_type: str,
        numeric: list[tuple[int, float]],
        values: list[float],
        threshold: float | None,
    ) -> list[int]:
        """Return row indices flagged as outliers by zscore or iqr."""
        n = len(values)
        mean = sum(values) / n
        if test_type == "iqr":
            ordered = sorted(values)
            q1 = ordered[n // 4]
            q3 = ordered[(3 * n) // 4]
            iqr = q3 - q1
            k = threshold if threshold is not None else 1.5
            lo, hi = q1 - k * iqr, q3 + k * iqr
            return [i for i, v in numeric if v < lo or v > hi]
        # default: zscore (also used for "ks" fallback)
        variance = sum((v - mean) ** 2 for v in values) / n
        std = variance**0.5
        if std == 0:
            return []
        k = threshold if threshold is not None else 3.0
        return [i for i, v in numeric if abs((v - mean) / std) > k]

    def _handle_sql(self, action: SqlQuery) -> ActionOutcome:
        """Execute a read-only SELECT against the working table, if duckdb is present."""
        query = action.query.strip()
        if not re.match(r"^\s*select\b", query, re.IGNORECASE) or ";" in query.rstrip(";"):
            return ActionOutcome(
                "SQL_QUERY", "SQL_QUERY rejected: only a single read-only SELECT is allowed."
            )
        try:
            import duckdb
        except ImportError:
            return ActionOutcome(
                "SQL_QUERY", "SQL_QUERY unavailable: duckdb is not installed in this environment."
            )
        try:
            records = self._df.to_dict("records")  # type: ignore[attr-defined]
            connection = duckdb.connect()
            connection.register("data", _records_relation(connection, records))
            rows = connection.execute(query).fetchmany(_MAX_SQL_ROWS)
            columns = [c[0] for c in connection.description] if connection.description else []
            connection.close()
        except Exception as exc:  # malformed query / engine error
            return ActionOutcome("SQL_QUERY", f"SQL_QUERY error: {exc}")
        payload = [dict(zip(columns, row, strict=False)) for row in rows]
        return ActionOutcome("SQL_QUERY", f"SQL_QUERY columns={columns} rows={payload}")

    def _handle_hypothesis(self, action: Hypothesis) -> ActionOutcome:
        """Record a hypothesis in the scratchpad."""
        self._scratchpad.add_hypothesis(
            action.claim,
            list(action.affected_rows),
            list(action.affected_columns),
            action.root_cause_type,
        )
        return ActionOutcome("HYPOTHESIS", f"HYPOTHESIS recorded: {action.claim}")

    def _handle_diagnose(self, action: Diagnose) -> ActionOutcome:
        """Record a confirmed issue in the scratchpad."""
        self._scratchpad.confirm_issue(action.row, action.column, action.issue_type)
        return ActionOutcome(
            "DIAGNOSE",
            f"DIAGNOSE recorded for row {action.row}, column {action.column!r} "
            f"({action.issue_type}).",
        )

    def _handle_root_cause(self, action: RootCause) -> ActionOutcome:
        """Acknowledge a root-cause analysis request over detected issues."""
        return ActionOutcome(
            "ROOT_CAUSE",
            f"ROOT_CAUSE noted for issue indices {list(action.error_indices)}; "
            "use HYPOTHESIS to record a specific claim.",
        )


def _records_relation(connection: object, records: list[dict[str, str]]) -> Any:
    """Build a duckdb-registerable relation from row records.

    Uses pandas if available (fast path); otherwise constructs an in-memory
    relation via VALUES. Returns an object suitable for ``register``.
    """
    try:
        import pandas as pd

        return pd.DataFrame(records)
    except ImportError:  # pragma: no cover - pandas usually present with duckdb
        import duckdb

        return duckdb.values(cast(Any, records))
