"""OpenEnv-compatible DataForge RL environment.

Core environment implementing reset/step/state/close for data-quality
detection, diagnosis, and repair with typed tool-use actions.

No LLM calls. No disk writes. Dataset state is in-memory per episode.
"""

from __future__ import annotations

import logging
import random
import re
import uuid
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, cast

import duckdb
import pandas as pd
import sqlglot
import sqlglot.expressions as sqlglot_exp
from pydantic import BaseModel, Field

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
    parse_action,
)
from dataforge.cli.common import load_schema
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema
from dataforge.env.observation import DataForgeObservation, ToolResult
from dataforge.env.reward import (
    P_FALSE_POS,
    P_INVALID,
    P_WRONG_FIX,
    R_EXPLORE,
    R_ROOT_CAUSE,
    EpisodeMetrics,
    RewardEngine,
)

logger = logging.getLogger("dataforge.env")

__all__ = [
    "DataForgeEnv",
    "EnvState",
    "ResetResult",
    "StepResult",
]

_FIXTURES_DIR = Path(__file__).resolve().parents[1].parent / "fixtures"
_DEFAULT_CSV = _FIXTURES_DIR / "hospital_10rows.csv"
_DEFAULT_SCHEMA = _FIXTURES_DIR / "hospital_schema.yaml"
_MAX_STEPS = 30
_MAX_RESULT_ROWS = 20
_TOOL_HISTORY_LIMIT = 5
_NOISE_EPSILON = 0.15
_BLOCKED_SQL_FRAGMENTS = (
    "attach",
    "call ",
    "copy ",
    "detach",
    "duckdb_extensions",
    "filename",
    "from_csv_auto",
    "glob(",
    "http://",
    "https://",
    "httpfs",
    "install",
    "load ",
    "mysql_scan",
    "parquet_scan",
    "postgres_scan",
    "pragma",
    "read_csv",
    "read_json",
    "read_parquet",
    "s3://",
    "sqlite_scan",
)


# ═══════════════════════════════════════════════════════════════════════════
# Result models
# ═══════════════════════════════════════════════════════════════════════════


class ResetResult(BaseModel):
    """Result of env.reset()."""

    observation: DataForgeObservation
    info: dict[str, Any] = Field(default_factory=dict)


class StepResult(BaseModel):
    """Result of env.step()."""

    observation: DataForgeObservation
    reward: float = 0.0
    done: bool = False
    info: dict[str, Any] = Field(default_factory=dict)


class EnvState(BaseModel):
    """Internal environment state snapshot."""

    episode_id: str = ""
    step_count: int = 0
    task_id: str = ""
    issues_detected: int = 0
    issues_fixed: int = 0
    false_positives: int = 0
    total_issues: int = 0
    is_done: bool = False


# ═══════════════════════════════════════════════════════════════════════════
# Environment
# ═══════════════════════════════════════════════════════════════════════════


class DataForgeEnv:
    """OpenEnv-compatible RL environment for data quality repair.

    Core API: ``reset()``, ``step()``, ``state()``, ``close()`` (no-op).

    Example::

        >>> env = DataForgeEnv()
        >>> result = env.reset(seed=42)
        >>> result.observation.done
        False
    """

    def __init__(self, max_steps: int = _MAX_STEPS) -> None:
        self._max_steps = max_steps
        self._episode_id = ""
        self._step_count = 0
        self._df: pd.DataFrame = pd.DataFrame()
        self._ground_truth: list[Issue] = []
        self._found_issues: list[dict[str, Any]] = []
        self._fixed_issues: list[dict[str, Any]] = []
        self._false_positives = 0
        self._cumulative_reward = 0.0
        self._is_done = False
        self._inspected_rows: set[int] = set()
        self._noisy = False
        self._noise_rng: random.Random | None = None
        self._scratchpad = Scratchpad()
        self._tool_history: list[ToolResult] = []
        self._reward_engine = RewardEngine()
        self._schema_info: dict[str, str] = {}
        self._schema: Schema | None = None
        self._causal_dag_cache: Any = None
        self._root_cause_labels: set[int] = set()

    # ── Core API ──────────────────────────────────────────────────────────

    def reset(self, seed: int | None = None, *, noisy: bool = False) -> ResetResult:
        """Reset the environment for a new episode.

        Args:
            seed: Optional RNG seed for deterministic episodes.
            noisy: If True, enable observation noise (epsilon=0.15).

        Returns:
            ResetResult with initial observation.
        """
        self._episode_id = str(uuid.uuid4())
        self._step_count = 0
        self._found_issues = []
        self._fixed_issues = []
        self._false_positives = 0
        self._cumulative_reward = 0.0
        self._is_done = False
        self._inspected_rows = set()
        self._scratchpad.reset()
        self._tool_history = []
        self._causal_dag_cache = None
        self._root_cause_labels = set()
        self._noisy = noisy
        self._noise_rng = random.Random(seed if seed is not None else 0) if noisy else None

        # Load fixture dataset
        self._df = pd.read_csv(_DEFAULT_CSV, dtype=str)
        self._schema_info = dict.fromkeys(self._df.columns, "str")
        self._schema = None
        if _DEFAULT_SCHEMA.exists():
            self._schema = load_schema(_DEFAULT_SCHEMA)
            self._schema_info = dict(self._schema.columns)

        # Run detectors for hidden ground truth
        self._ground_truth = run_all_detectors(self._df, self._schema)
        logger.info(
            "Episode %s: %d rows, %d ground-truth issues",
            self._episode_id[:8],
            len(self._df),
            len(self._ground_truth),
        )

        # Initial observation with first 5 rows
        initial_rows = cast(list[dict[str, Any]], self._df.head(5).to_dict(orient="records"))
        obs = DataForgeObservation(
            visible_rows=initial_rows,
            step_budget_remaining=self._max_steps,
            scratchpad_summary=self._scratchpad.summary(),
            metadata={
                "episode_id": self._episode_id,
                "total_rows": len(self._df),
                "total_columns": len(self._df.columns),
                "schema": self._schema_info,
            },
        )
        return ResetResult(observation=obs, info={"episode_id": self._episode_id})

    def step(self, action: Action | dict[str, Any]) -> StepResult:
        """Execute one agent action and return the result.

        Args:
            action: A typed Action model or raw dict to be parsed.

        Returns:
            StepResult with observation, reward, and done flag.
        """
        if self._is_done:
            return self._terminal_result(0.0)

        self._step_count += 1

        # Parse if raw dict
        if isinstance(action, dict):
            try:
                action = parse_action(action)
            except Exception as exc:
                return self._error_step(str(exc))

        # Dispatch
        try:
            tool_result, reward = self._dispatch(action)
        except Exception as exc:
            logger.exception("Action dispatch error at step %d", self._step_count)
            return self._error_step(str(exc))

        # Late-step penalty
        reward += self._reward_engine.compute_late_penalty(self._step_count, self._max_steps)

        # Accumulate
        self._cumulative_reward += reward

        # Record in history
        self._tool_history.append(tool_result)
        if len(self._tool_history) > _TOOL_HISTORY_LIMIT:
            self._tool_history = self._tool_history[-_TOOL_HISTORY_LIMIT:]

        # Check termination
        done = self._step_count >= self._max_steps
        if done:
            self._is_done = True
            terminal = self._compute_terminal()
            self._cumulative_reward = max(self._cumulative_reward, terminal)

        obs = DataForgeObservation(
            visible_rows=tool_result.data
            if tool_result.action_type == "INSPECT_ROWS" and tool_result.success
            else None,
            scratchpad_summary=self._scratchpad.summary(),
            step_budget_remaining=max(0, self._max_steps - self._step_count),
            tool_usage_history=list(self._tool_history),
            latest_result=tool_result,
            done=done,
            reward=reward,
            cumulative_reward=self._cumulative_reward,
        )
        return StepResult(observation=obs, reward=reward, done=done)

    def state(self) -> EnvState:
        """Return current internal state snapshot."""
        return EnvState(
            episode_id=self._episode_id,
            step_count=self._step_count,
            issues_detected=len(self._found_issues),
            issues_fixed=len(self._fixed_issues),
            false_positives=self._false_positives,
            total_issues=len(self._ground_truth),
            is_done=self._is_done,
        )

    def close(self) -> None:
        """No-op. Retained for OpenEnv container compatibility."""

    # ── Dispatch ──────────────────────────────────────────────────────────

    def _dispatch(self, action: Action) -> tuple[ToolResult, float]:
        """Route action to handler. Returns (tool_result, step_reward)."""
        if isinstance(action, InspectRows):
            return self._handle_inspect(action)
        if isinstance(action, SqlQuery):
            return self._handle_sql(action)
        if isinstance(action, StatTest):
            return self._handle_stat(action)
        if isinstance(action, PatternMatch):
            return self._handle_pattern(action)
        if isinstance(action, Hypothesis):
            return self._handle_hypothesis(action)
        if isinstance(action, RootCause):
            return self._handle_root_cause(action)
        if isinstance(action, Diagnose):
            return self._handle_diagnose(action)
        if isinstance(action, Fix):
            return self._handle_fix(action)
        return ToolResult(
            action_type="UNKNOWN",
            success=False,
            error={"verdict": "error", "reason": "Unknown action type"},
        ), P_INVALID

    # ── Action handlers ───────────────────────────────────────────────────

    def _handle_inspect(self, action: InspectRows) -> tuple[ToolResult, float]:
        """Handle INSPECT_ROWS: return dataset rows."""
        valid_indices = [i for i in action.row_indices if 0 <= i < len(self._df)]
        if not valid_indices:
            return ToolResult(
                action_type="INSPECT_ROWS",
                success=False,
                error={"verdict": "error", "reason": "No valid row indices"},
            ), P_INVALID

        # Apply 20-row cap
        valid_indices = valid_indices[:20]
        rows = self._df.iloc[valid_indices]
        if action.column_names:
            valid_cols = [c for c in action.column_names if c in self._df.columns]
            if valid_cols:
                rows = rows[valid_cols]

        row_dicts = cast(list[dict[str, Any]], rows.to_dict(orient="records"))
        for i, idx in enumerate(valid_indices[: len(row_dicts)]):
            row_dicts[i]["_row_index"] = idx

        # Noise injection
        if self._noisy and self._noise_rng:
            row_dicts = self._inject_noise(row_dicts)

        # Exploration bonus
        new_indices = set(valid_indices) - self._inspected_rows
        self._inspected_rows.update(valid_indices)
        gt_rows = {issue.row for issue in self._ground_truth}
        found_rows = {f["row"] for f in self._found_issues}
        bonus = self._reward_engine.compute_exploration_bonus(
            new_indices,
            self._inspected_rows,
            len(self._df),
            gt_rows,
            found_rows,
        )
        return ToolResult(action_type="INSPECT_ROWS", success=True, data=row_dicts), bonus

    def _handle_sql(self, action: SqlQuery) -> tuple[ToolResult, float]:
        """Handle SQL_QUERY: execute read-only SQL via DuckDB."""
        # Validate read-only
        try:
            parsed = [stmt for stmt in sqlglot.parse(action.query) if stmt is not None]
        except sqlglot.errors.ParseError as exc:
            return ToolResult(
                action_type="SQL_QUERY",
                success=False,
                error={
                    "verdict": "error",
                    "reason": str(exc),
                    "suggested_constraint": "Use valid SQL syntax",
                },
            ), P_INVALID

        if len(parsed) != 1:
            return ToolResult(
                action_type="SQL_QUERY",
                success=False,
                error={
                    "verdict": "rejected",
                    "reason": "Exactly one SELECT statement is allowed.",
                    "suggested_constraint": "Use a single read-only SELECT statement.",
                },
            ), P_INVALID

        normalized_query = f" {action.query.lower()} "
        blocked = next(
            (fragment for fragment in _BLOCKED_SQL_FRAGMENTS if fragment in normalized_query),
            None,
        )
        if blocked is not None:
            return ToolResult(
                action_type="SQL_QUERY",
                success=False,
                error={
                    "verdict": "rejected",
                    "reason": "SQL_QUERY may only read from the registered data relation.",
                    "suggested_constraint": "Query the in-memory data table without file, network, extension, or table functions.",
                },
            ), P_INVALID

        for stmt in parsed:
            if stmt.key not in ("select",):
                return ToolResult(
                    action_type="SQL_QUERY",
                    success=False,
                    error={
                        "verdict": "rejected",
                        "reason": f"Only SELECT queries allowed, got {stmt.key.upper()}",
                        "suggested_constraint": "Use SELECT statements only",
                    },
                ), P_INVALID

            for table in stmt.find_all(sqlglot_exp.Table):
                if table.name.lower() != "data":
                    return ToolResult(
                        action_type="SQL_QUERY",
                        success=False,
                        error={
                            "verdict": "rejected",
                            "reason": (
                                "SQL_QUERY may only reference the registered data relation; "
                                f"got '{table.name}'."
                            ),
                            "suggested_constraint": "Use FROM data for tabular queries.",
                        },
                    ), P_INVALID

        try:
            conn = duckdb.connect(":memory:")
            conn.register("data", self._df)
            result_df = conn.execute(action.query).fetchdf()
            conn.close()
            rows = result_df.head(_MAX_RESULT_ROWS).to_dict(orient="records")
            return ToolResult(action_type="SQL_QUERY", success=True, data=rows), 0.0
        except duckdb.Error as exc:
            return ToolResult(
                action_type="SQL_QUERY",
                success=False,
                error={"verdict": "error", "reason": str(exc)},
            ), P_INVALID

    def _handle_stat(self, action: StatTest) -> tuple[ToolResult, float]:
        """Handle STAT_TEST: run zscore/iqr/ks on a column."""
        if action.column not in self._df.columns:
            return ToolResult(
                action_type="STAT_TEST",
                success=False,
                error={"verdict": "error", "reason": f"Column '{action.column}' not found"},
            ), P_INVALID

        try:
            col = pd.to_numeric(self._df[action.column], errors="coerce").dropna()
            if len(col) == 0:
                return ToolResult(
                    action_type="STAT_TEST",
                    success=False,
                    error={
                        "verdict": "error",
                        "reason": f"No numeric values in column '{action.column}'",
                    },
                ), P_INVALID
        except Exception as exc:
            return ToolResult(
                action_type="STAT_TEST",
                success=False,
                error={"verdict": "error", "reason": str(exc)},
            ), P_INVALID

        from scipy import stats as scipy_stats  # type: ignore[import-untyped]

        if action.test_type == "zscore":
            zscores = scipy_stats.zscore(col)
            threshold = action.threshold or 3.0
            outliers = col.index[abs(zscores) > threshold].tolist()
            data = {
                "test": "zscore",
                "threshold": threshold,
                "outlier_indices": outliers,
                "n_outliers": len(outliers),
                "mean": float(col.mean()),
                "std": float(col.std()),
            }
        elif action.test_type == "iqr":
            q1, q3 = float(col.quantile(0.25)), float(col.quantile(0.75))
            iqr_val = q3 - q1
            factor = action.threshold or 1.5
            lower, upper = q1 - factor * iqr_val, q3 + factor * iqr_val
            outliers = col.index[(col < lower) | (col > upper)].tolist()
            data = {
                "test": "iqr",
                "q1": q1,
                "q3": q3,
                "iqr": iqr_val,
                "lower": lower,
                "upper": upper,
                "outlier_indices": outliers,
            }
        elif action.test_type == "ks":
            mean = float(col.mean())
            std = float(col.std())
            if not std > 0:
                return ToolResult(
                    action_type="STAT_TEST",
                    success=False,
                    error={
                        "verdict": "error",
                        "reason": "KS test requires at least two distinct numeric values",
                    },
                ), P_INVALID
            standardized = (col - mean) / std
            stat_val, p_val = scipy_stats.kstest(standardized, scipy_stats.norm.cdf)
            data = {
                "test": "ks",
                "statistic": float(stat_val),
                "p_value": float(p_val),
                "normal": p_val > 0.05,
            }
        else:
            return ToolResult(
                action_type="STAT_TEST",
                success=False,
                error={"verdict": "error", "reason": f"Unknown test type: {action.test_type}"},
            ), P_INVALID

        return ToolResult(action_type="STAT_TEST", success=True, data=data), 0.0

    def _handle_pattern(self, action: PatternMatch) -> tuple[ToolResult, float]:
        """Handle PATTERN_MATCH: evaluate regex against column values."""
        if action.column not in self._df.columns:
            return ToolResult(
                action_type="PATTERN_MATCH",
                success=False,
                error={"verdict": "error", "reason": f"Column '{action.column}' not found"},
            ), P_INVALID

        try:
            compiled = re.compile(action.pattern)
        except re.error as exc:
            return ToolResult(
                action_type="PATTERN_MATCH",
                success=False,
                error={"verdict": "error", "reason": f"Invalid regex: {exc}"},
            ), P_INVALID

        matches: list[dict[str, Any]] = []
        for idx, val in enumerate(self._df[action.column].astype(str)):
            is_match = bool(compiled.search(val))
            if is_match == action.expect_match:
                matches.append({"row": idx, "column": action.column, "value": val})
        return ToolResult(
            action_type="PATTERN_MATCH",
            success=True,
            data={"matches": matches[:_MAX_RESULT_ROWS], "total_matches": len(matches)},
        ), 0.0

    def _handle_hypothesis(self, action: Hypothesis) -> tuple[ToolResult, float]:
        """Handle HYPOTHESIS: record claim and award root-cause credit."""
        self._scratchpad.add_hypothesis(
            action.claim,
            action.affected_rows,
            action.affected_columns,
            action.root_cause_type,
        )
        # Check for root-cause match against ground truth
        credit = 0.0
        for issue in self._ground_truth:
            if (
                issue.row in action.affected_rows
                and issue.column in action.affected_columns
                and issue.issue_type == action.root_cause_type
            ):
                credit += R_EXPLORE
        data = {"recorded": True, "root_cause_credit": credit}
        return ToolResult(action_type="HYPOTHESIS", success=True, data=data), credit

    def _handle_root_cause(self, action: RootCause) -> tuple[ToolResult, float]:
        """Handle ROOT_CAUSE: analyze detected issues for minimal roots."""
        if not self._found_issues:
            return ToolResult(
                action_type="ROOT_CAUSE",
                success=False,
                error={"verdict": "error", "reason": "No detected issues are available"},
            ), P_INVALID

        invalid = [idx for idx in action.error_indices if idx >= len(self._found_issues)]
        if invalid:
            return ToolResult(
                action_type="ROOT_CAUSE",
                success=False,
                error={
                    "verdict": "error",
                    "reason": f"Detected issue indices out of range: {invalid}",
                },
            ), P_INVALID

        from dataforge.causal.pc import discover_causal_dag
        from dataforge.causal.root_cause import CausalRootCauseAnalyzer, evidence_from_issue

        if self._causal_dag_cache is None:
            self._causal_dag_cache = discover_causal_dag(self._df).dag

        selected = [
            evidence_from_issue(index, self._found_issues[index]) for index in action.error_indices
        ]
        result = CausalRootCauseAnalyzer(self._causal_dag_cache).analyze(selected)
        data = result.model_dump(mode="json")
        reward = self._root_cause_reward(set(result.root_indices))
        return ToolResult(action_type="ROOT_CAUSE", success=True, data=data), reward

    def _handle_diagnose(self, action: Diagnose) -> tuple[ToolResult, float]:
        """Handle DIAGNOSE: score against ground truth."""
        if action.row < 0 or action.row >= len(self._df):
            return ToolResult(
                action_type="DIAGNOSE",
                success=False,
                error={"verdict": "error", "reason": f"Row {action.row} out of bounds"},
            ), P_INVALID
        if action.column not in self._df.columns:
            return ToolResult(
                action_type="DIAGNOSE",
                success=False,
                error={"verdict": "error", "reason": f"Column '{action.column}' not found"},
            ), P_INVALID

        # Already reported?
        for found in self._found_issues:
            if found["row"] == action.row and found["column"] == action.column:
                return ToolResult(
                    action_type="DIAGNOSE", success=True, data={"result": "already_found"}
                ), 0.0

        # Match ground truth
        for issue in self._ground_truth:
            if issue.row == action.row and issue.column == action.column:
                type_match = action.issue_type == issue.issue_type
                reward = self._reward_engine.diagnose_reward(type_match)
                self._found_issues.append(
                    {"row": action.row, "column": action.column, "type": action.issue_type}
                )
                self._scratchpad.confirm_issue(action.row, action.column, action.issue_type)
                return ToolResult(
                    action_type="DIAGNOSE",
                    success=True,
                    data={"result": "correct", "type_match": type_match},
                ), reward

        # False positive
        self._false_positives += 1
        return ToolResult(
            action_type="DIAGNOSE", success=True, data={"result": "false_positive"}
        ), P_FALSE_POS

    def _root_cause_reward(self, root_indices: set[int]) -> float:
        """Return root-cause bonus only when task labels are available."""
        if not self._root_cause_labels:
            return 0.0
        return R_ROOT_CAUSE if root_indices == self._root_cause_labels else 0.0

    def _handle_fix(self, action: Fix) -> tuple[ToolResult, float]:
        """Handle FIX: validate through safety/SMT, then score."""
        if action.row < 0 or action.row >= len(self._df):
            return ToolResult(
                action_type="FIX",
                success=False,
                error={"verdict": "error", "reason": f"Row {action.row} out of bounds"},
            ), P_INVALID
        if action.column not in self._df.columns:
            return ToolResult(
                action_type="FIX",
                success=False,
                error={"verdict": "error", "reason": f"Column '{action.column}' not found"},
            ), P_INVALID

        # Already fixed?
        for fixed in self._fixed_issues:
            if fixed["row"] == action.row and fixed["column"] == action.column:
                return ToolResult(
                    action_type="FIX", success=True, data={"result": "already_fixed"}
                ), 0.0

        # Safety filter + SMT verifier (best-effort, no crash on import failure)
        try:
            safety_ok, safety_msg = self._check_safety(action)
        except Exception as exc:
            logger.warning("Safety pipeline failed closed: %s", exc)
            safety_ok = False
            safety_msg = f"Safety pipeline failed closed: {exc}"
        if not safety_ok:
            return ToolResult(
                action_type="FIX",
                success=False,
                error={"verdict": "rejected", "reason": safety_msg},
            ), P_INVALID

        # Match ground truth
        for issue in self._ground_truth:
            if issue.row == action.row and issue.column == action.column:
                if issue.expected is None:
                    return ToolResult(
                        action_type="FIX", success=True, data={"result": "detection_only"}
                    ), 0.0

                # Exact match (case-insensitive)
                if action.new_value.strip().lower() == str(issue.expected).lower():
                    reward = self._reward_engine.fix_reward(
                        exact=True, has_justification=bool(action.justification)
                    )
                    self._fixed_issues.append(
                        {"row": action.row, "column": action.column, "value": action.new_value}
                    )
                    self._auto_diagnose(action, issue)
                    return ToolResult(
                        action_type="FIX", success=True, data={"result": "correct"}
                    ), reward

                # Partial: numeric within 1%
                try:
                    prov = float(action.new_value.strip())
                    exp = float(str(issue.expected))
                    rel_err = abs(prov - exp) / abs(exp) if exp != 0 else abs(prov)
                    if rel_err < 0.01:
                        reward = self._reward_engine.fix_reward(
                            exact=False, has_justification=bool(action.justification)
                        )
                        self._fixed_issues.append(
                            {"row": action.row, "column": action.column, "value": action.new_value}
                        )
                        self._auto_diagnose(action, issue)
                        return ToolResult(
                            action_type="FIX", success=True, data={"result": "partial_numeric"}
                        ), reward
                except (ValueError, TypeError):
                    pass

                # Partial: string similarity >= 85%
                sim = SequenceMatcher(
                    None, action.new_value.lower(), str(issue.expected).lower()
                ).ratio()
                if sim >= 0.85:
                    reward = self._reward_engine.fix_reward(
                        exact=False, has_justification=bool(action.justification)
                    )
                    self._fixed_issues.append(
                        {"row": action.row, "column": action.column, "value": action.new_value}
                    )
                    self._auto_diagnose(action, issue)
                    return ToolResult(
                        action_type="FIX", success=True, data={"result": "partial_string"}
                    ), reward

                return ToolResult(
                    action_type="FIX", success=True, data={"result": "wrong_value"}
                ), P_WRONG_FIX

        return ToolResult(
            action_type="FIX", success=True, data={"result": "no_issue_at_location"}
        ), P_WRONG_FIX

    # ── Helpers ────────────────────────────────────────────────────────────

    def _check_safety(self, action: Fix) -> tuple[bool, str]:
        """Run SafetyFilter + SMTVerifier. Returns (ok, message)."""
        try:
            from dataforge.repairers.base import ProposedFix
            from dataforge.safety.filter import SafetyContext, SafetyFilter, SafetyVerdict
            from dataforge.transactions.txn import CellFix
            from dataforge.verifier.result import VerificationVerdict
            from dataforge.verifier.smt import SMTVerifier

            old_val = str(self._df.at[action.row, action.column])
            cell_fix = CellFix(
                row=action.row,
                column=action.column,
                old_value=old_val,
                new_value=action.new_value,
                detector_id="agent",
            )
            proposed = ProposedFix(
                fix=cell_fix,
                reason=action.justification,
                confidence=0.8,
                provenance="deterministic",
            )

            sf = SafetyFilter()
            ctx = SafetyContext()
            sr = sf.evaluate(proposed, self._schema, ctx)
            if sr.verdict == SafetyVerdict.DENY:
                return False, f"Safety filter denied: {sr.reason}"

            verifier = SMTVerifier()
            vr = verifier.verify(self._df, [proposed], self._schema)
            if vr.verdict == VerificationVerdict.REJECT:
                return False, f"SMT verifier rejected: {vr.reason}"
            if vr.verdict == VerificationVerdict.UNKNOWN:
                return False, f"SMT verifier returned unknown: {vr.reason}"

            return True, "Passed safety and verification"
        except ImportError as exc:
            return False, f"Safety/verifier dependency unavailable: {exc}"

    def _auto_diagnose(self, action: Fix, issue: Issue) -> None:
        """Auto-credit diagnosis when agent fixes without diagnosing first."""
        already = any(
            f["row"] == action.row and f["column"] == action.column for f in self._found_issues
        )
        if not already:
            self._found_issues.append(
                {"row": action.row, "column": action.column, "type": issue.issue_type}
            )

    def _inject_noise(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply deterministic observation noise (epsilon=0.15)."""
        if not self._noise_rng:
            return rows
        noisy = []
        for row in rows:
            row_copy = dict(row)
            if self._noise_rng.random() < _NOISE_EPSILON:
                cols = [k for k in row_copy if k != "_row_index"]
                if cols:
                    col = self._noise_rng.choice(cols)
                    val = row_copy[col]
                    if isinstance(val, str) and len(val) > 3:
                        row_copy[col] = (
                            val[: -(self._noise_rng.randint(1, 3))]
                            if self._noise_rng.random() < 0.5
                            else val.swapcase()
                        )
            noisy.append(row_copy)
        return noisy

    def _compute_terminal(self) -> float:
        """Compute terminal score."""
        fixable = [i for i in self._ground_truth if i.expected is not None]
        metrics = EpisodeMetrics(
            found_issues=len(self._found_issues),
            total_issues=len(self._ground_truth),
            fixed_issues=len(self._fixed_issues),
            fixable_issues=len(fixable),
            false_positives=self._false_positives,
        )
        return self._reward_engine.compute_terminal_score(metrics)

    def _error_step(self, message: str) -> StepResult:
        """Build error StepResult."""
        tr = ToolResult(
            action_type="ERROR", success=False, error={"verdict": "error", "reason": message}
        )
        self._tool_history.append(tr)
        self._cumulative_reward += P_INVALID
        done = self._step_count >= self._max_steps
        if done:
            self._is_done = True
        return StepResult(
            observation=DataForgeObservation(
                step_budget_remaining=max(0, self._max_steps - self._step_count),
                tool_usage_history=list(self._tool_history[-_TOOL_HISTORY_LIMIT:]),
                latest_result=tr,
                done=done,
                reward=P_INVALID,
                cumulative_reward=self._cumulative_reward,
                scratchpad_summary=self._scratchpad.summary(),
            ),
            reward=P_INVALID,
            done=done,
        )

    def _terminal_result(self, reward: float) -> StepResult:
        """Build terminal StepResult for already-done episodes."""
        return StepResult(
            observation=DataForgeObservation(
                step_budget_remaining=0,
                done=True,
                reward=reward,
                cumulative_reward=self._cumulative_reward,
                scratchpad_summary=self._scratchpad.summary(),
                tool_usage_history=list(self._tool_history[-_TOOL_HISTORY_LIMIT:]),
            ),
            reward=reward,
            done=True,
        )
