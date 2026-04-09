# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Typed Pydantic models for the Data Quality RL environment.

This module defines the **formal schema boundary** between the agent and the
environment.  Every field entering or leaving the environment is validated at
construction time by Pydantic v2 — malformed actions are rejected before they
reach environment logic, and observations carry machine-checked types that
agents and training infrastructure can rely on.

Hierarchy (all ``model_config`` inherited from openenv-core):

    DataQualityAction      ← Action      ← BaseModel   (extra="forbid")
    DataQualityObservation ← Observation ← BaseModel   (extra="forbid")
    DataQualityState       ← State       ← BaseModel   (extra="allow")

Base-class fields (DO NOT redeclare — verified against openenv-core ≥0.2.2):

    Action:       metadata: Dict[str, Any]
    Observation:  done: bool, reward: bool|int|float|None, metadata: Dict[str, Any]
    State:        episode_id: str|None, step_count: int (ge=0)

Usage::

    >>> action = DataQualityAction.inspect(row_indices=[0, 1, 2])
    >>> action = DataQualityAction.diagnose(
    ...     row_index=5, column_name="age", issue_type=IssueType.OUTLIER,
    ... )
    >>> action = DataQualityAction.fix(
    ...     row_index=5, column_name="age",
    ...     fix_type=FixType.CORRECT_VALUE, new_value="25",
    ...     justification="Value exceeds 3σ from mean; corrected to median.",
    ... )
    >>> action = DataQualityAction.finalize()

Imports:
    Base classes are resolved through ``compat.py`` (Phase 1), which handles
    openenv-core import-path variations.  Application code should import
    models from this module, not from openenv directly.

See Also:
    compat.py  — import resolution layer (Phase 1)
    server/    — environment implementation (Phase 3+)
    client.py  — EnvClient wrapper (Phase 4+)
"""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, field_validator, model_validator

try:
    from .compat import Action, Observation, State
except ImportError:
    from compat import Action, Observation, State  # type: ignore[no-redef]


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

__all__ = [
    # Constants
    "MAX_INSPECT_ROWS",
    "MAX_INSPECT_COLUMNS",
    "DEFAULT_MAX_STEPS",
    # Enums
    "IssueType",
    "FixType",
    "ActionResult",
    "RemainingHint",
    # Models
    "DataQualityAction",
    "DataQualityObservation",
    "DataQualityState",
]


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

MAX_INSPECT_ROWS: int = 10
"""Maximum number of rows the agent may request in a single inspect action."""

MAX_INSPECT_COLUMNS: int = 20
"""Maximum number of columns the agent may request in a single inspect action."""

DEFAULT_MAX_STEPS: int = 30
"""Default episode length limit (can be overridden per-task by the environment)."""


# ═══════════════════════════════════════════════════════════════════════════
# §1  Enums — constrained vocabularies for categorical fields
# ═══════════════════════════════════════════════════════════════════════════

class IssueType(str, Enum):
    """Classification of data quality issues.

    Each value maps to a distinct detection strategy in the environment.
    Using an enum (rather than a free-form string) ensures that typos like
    ``"outlire"`` are caught at validation time, not at scoring time.
    """

    FORMAT_ERROR = "format_error"
    MISSING_VALUE = "missing_value"
    DUPLICATE = "duplicate"
    NEAR_DUPLICATE = "near_duplicate"
    TYPE_MISMATCH = "type_mismatch"
    OUTLIER = "outlier"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    CROSS_FIELD = "cross_field"
    BUSINESS_RULE = "business_rule"


class FixType(str, Enum):
    """How the agent proposes to fix an identified issue.

    ``new_value`` semantics vary by fix type:
        CORRECT_VALUE   — ``new_value`` is **required** (the corrected cell value).
        DELETE_ROW      — ``new_value`` is **forbidden** (row is removed entirely).
        IMPUTE          — ``new_value`` is **optional** (environment may compute it).
        STANDARDIZE     — ``new_value`` is **optional** (environment may compute it).
    """

    CORRECT_VALUE = "correct_value"
    DELETE_ROW = "delete_row"
    IMPUTE = "impute"
    STANDARDIZE = "standardize"


class ActionResult(str, Enum):
    """Outcome of the agent's last action, as determined by the environment."""

    CORRECT = "correct"
    INCORRECT = "incorrect"
    PARTIAL = "partial"
    ALREADY_FOUND = "already_found"
    INITIAL = "initial"
    ERROR = "error"
    COMPLETE = "complete"


class RemainingHint(str, Enum):
    """Coarse hint about how many issues the agent has yet to find."""

    NONE = "none"
    FEW = "few"
    SOME = "some"
    MANY = "many"
    UNKNOWN = "unknown"


# ═══════════════════════════════════════════════════════════════════════════
# §2  DataQualityAction
# ═══════════════════════════════════════════════════════════════════════════

class DataQualityAction(Action):
    """Agent's action submitted to the Data Quality environment.

    Four action types are supported, each requiring a different subset of
    fields.  Use the **factory classmethods** (``inspect``, ``diagnose``,
    ``fix``, ``finalize``) instead of the raw constructor to avoid
    mismatched-field errors.

    Inherits from ``Action`` (provides ``metadata: Dict[str, Any]``).

    Validation Rules:
        +-----------+----------------------------------------------------+
        | type      | required fields                                    |
        +-----------+----------------------------------------------------+
        | inspect   | ≥1 of ``row_indices``, ``column_names``            |
        | diagnose  | ``row_index``, ``column_name``, ``issue_type``     |
        | fix       | ``row_index``, ``column_name``, ``fix_type``,      |
        |           | ``justification``; ``new_value`` per FixType rules |
        | finalize  | (none — all optional fields must be None)          |
        +-----------+----------------------------------------------------+
    """

    action_type: Literal["inspect", "diagnose", "fix", "finalize"] = Field(
        ..., description="Type of action to perform.",
    )

    # ── inspect fields ────────────────────────────────────────────────────
    row_indices: Optional[List[int]] = Field(
        None,
        description=(
            f"Row indices to inspect (1–{MAX_INSPECT_ROWS} per call). "
            "Each index must be ≥ 0."
        ),
    )
    column_names: Optional[List[str]] = Field(
        None,
        description=(
            f"Column names to inspect or retrieve statistics for "
            f"(1–{MAX_INSPECT_COLUMNS} per call)."
        ),
    )

    # ── diagnose / fix shared fields ──────────────────────────────────────
    row_index: Optional[int] = Field(
        None, description="Row index of the suspected or confirmed issue.",
    )
    column_name: Optional[str] = Field(
        None, description="Column name of the suspected or confirmed issue.",
    )

    # ── diagnose-specific ─────────────────────────────────────────────────
    issue_type: Optional[IssueType] = Field(
        None, description="Classified issue type (see IssueType enum).",
    )

    # ── fix-specific ──────────────────────────────────────────────────────
    fix_type: Optional[FixType] = Field(
        None, description="How to fix the issue (see FixType enum).",
    )
    new_value: Optional[str] = Field(
        None,
        description=(
            "The corrected cell value (as a string).  Required for "
            "CORRECT_VALUE, forbidden for DELETE_ROW, optional otherwise."
        ),
    )
    justification: Optional[str] = Field(
        None,
        description="Explanation of why this fix is correct.  Required for all fix actions.",
    )

    # ── cross-table (Task 3) ──────────────────────────────────────────────
    related_table: Optional[str] = Field(
        None,
        description=(
            "Name of a secondary table for cross-table inspection or "
            "diagnosis (e.g., 'orders', 'products')."
        ),
    )

    # ── field validators ──────────────────────────────────────────────────

    @field_validator("row_indices")
    @classmethod
    def _validate_row_indices(cls, v: Optional[List[int]]) -> Optional[List[int]]:
        if v is not None:
            if len(v) == 0:
                raise ValueError("row_indices must not be empty when provided")
            if len(v) > MAX_INSPECT_ROWS:
                raise ValueError(
                    f"Cannot inspect more than {MAX_INSPECT_ROWS} rows "
                    f"per call (got {len(v)})"
                )
            if any(i < 0 for i in v):
                raise ValueError("All row indices must be ≥ 0")
        return v

    @field_validator("column_names")
    @classmethod
    def _validate_column_names(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is not None:
            if len(v) == 0:
                raise ValueError("column_names must not be empty when provided")
            if len(v) > MAX_INSPECT_COLUMNS:
                raise ValueError(
                    f"Cannot inspect more than {MAX_INSPECT_COLUMNS} columns "
                    f"per call (got {len(v)})"
                )
            if any(not name.strip() for name in v):
                raise ValueError("Column names must be non-empty, non-whitespace strings")
        return v

    @field_validator("row_index")
    @classmethod
    def _validate_row_index(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError("row_index must be ≥ 0")
        return v

    @field_validator("justification")
    @classmethod
    def _validate_justification(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("justification must be a non-empty, substantive string")
        return v

    # ── model-level discriminated validator ────────────────────────────────

    @model_validator(mode="after")
    def _validate_action_consistency(self) -> "DataQualityAction":
        """Enforce that exactly the right fields are populated for each action_type."""
        t = self.action_type

        if t == "inspect":
            if self.row_indices is None and self.column_names is None:
                raise ValueError(
                    "inspect action requires at least one of "
                    "'row_indices' or 'column_names'"
                )
            self._reject_fix_fields("inspect")

        elif t == "diagnose":
            for field_name in ("row_index", "column_name", "issue_type"):
                if getattr(self, field_name) is None:
                    raise ValueError(f"diagnose action requires '{field_name}'")
            self._reject_fix_fields("diagnose")

        elif t == "fix":
            for field_name in ("row_index", "column_name", "fix_type", "justification"):
                if getattr(self, field_name) is None:
                    raise ValueError(f"fix action requires '{field_name}'")
            # new_value semantics depend on fix_type
            if self.fix_type == FixType.CORRECT_VALUE and self.new_value is None:
                raise ValueError(
                    "fix_type 'correct_value' requires 'new_value' "
                    "(the corrected cell content)"
                )
            if self.fix_type == FixType.DELETE_ROW and self.new_value is not None:
                raise ValueError(
                    "fix_type 'delete_row' must not set 'new_value' "
                    "(the entire row is removed)"
                )

        elif t == "finalize":
            _optional = [
                self.row_indices, self.column_names, self.row_index,
                self.column_name, self.issue_type, self.fix_type,
                self.new_value, self.justification, self.related_table,
            ]
            if any(f is not None for f in _optional):
                raise ValueError(
                    "finalize action must not set any optional fields"
                )

        return self

    def _reject_fix_fields(self, action_label: str) -> None:
        """Raise if fix-specific fields are populated on a non-fix action."""
        if self.fix_type is not None:
            raise ValueError(f"{action_label} action must not set 'fix_type'")
        if self.new_value is not None:
            raise ValueError(f"{action_label} action must not set 'new_value'")
        if self.justification is not None:
            raise ValueError(f"{action_label} action must not set 'justification'")

    # ── factory classmethods ──────────────────────────────────────────────

    @classmethod
    def inspect(
        cls,
        *,
        row_indices: Optional[List[int]] = None,
        column_names: Optional[List[str]] = None,
        related_table: Optional[str] = None,
    ) -> "DataQualityAction":
        """Create an inspect action to view rows or column statistics.

        Example::

            >>> DataQualityAction.inspect(row_indices=[0, 1, 2])
            >>> DataQualityAction.inspect(column_names=["age", "income"])
        """
        return cls(
            action_type="inspect",
            row_indices=row_indices,
            column_names=column_names,
            related_table=related_table,
        )

    @classmethod
    def diagnose(
        cls,
        *,
        row_index: int,
        column_name: str,
        issue_type: IssueType,
        related_table: Optional[str] = None,
    ) -> "DataQualityAction":
        """Create a diagnose action to flag a suspected data quality issue.

        Example::

            >>> DataQualityAction.diagnose(
            ...     row_index=5, column_name="email",
            ...     issue_type=IssueType.FORMAT_ERROR,
            ... )
        """
        return cls(
            action_type="diagnose",
            row_index=row_index,
            column_name=column_name,
            issue_type=issue_type,
            related_table=related_table,
        )

    @classmethod
    def fix(
        cls,
        *,
        row_index: int,
        column_name: str,
        fix_type: FixType,
        justification: str,
        new_value: Optional[str] = None,
    ) -> "DataQualityAction":
        """Create a fix action to correct an identified issue.

        Example::

            >>> DataQualityAction.fix(
            ...     row_index=5, column_name="age",
            ...     fix_type=FixType.CORRECT_VALUE,
            ...     new_value="25",
            ...     justification="Typo: was '255', clearly intended '25'.",
            ... )
        """
        return cls(
            action_type="fix",
            row_index=row_index,
            column_name=column_name,
            fix_type=fix_type,
            new_value=new_value,
            justification=justification,
        )

    @classmethod
    def finalize(cls) -> "DataQualityAction":
        """Create a finalize action to end the episode.

        The agent calls this when it believes all issues have been found
        and fixed.  The environment scores the agent's work and returns
        a terminal observation.

        Example::

            >>> DataQualityAction.finalize()
        """
        return cls(action_type="finalize")


# ═══════════════════════════════════════════════════════════════════════════
# §3  DataQualityObservation
# ═══════════════════════════════════════════════════════════════════════════

class DataQualityObservation(Observation):
    """What the agent observes after each action.

    Inherits from ``Observation`` (provides ``done``, ``reward``, ``metadata``).

    Field Groups:
        **Episode context** — ``task_id``, ``dataset_name``, ``schema_info``,
        ``total_rows``, ``total_columns``.  Set on reset, stable within episode.

        **Inspection data** — ``visible_rows``, ``column_statistics``,
        ``secondary_table_rows``.  Populated only after an inspect action;
        ``None`` otherwise.

        **Scoring** — ``action_result``, ``reward_delta``, ``cumulative_reward``,
        ``issues_found``, ``issues_remaining_hint``.  Updated every step.

        **Episode progress** — ``steps_taken``, ``max_steps``, ``message``.
        Updated every step.

    Reward Semantics:
        The inherited ``reward`` field (``bool | int | float | None``) is set
        by the environment to the step reward and consumed by the openenv
        framework for training signals.  ``reward_delta`` mirrors the same
        value as a plain ``float`` for convenient arithmetic without
        type-narrowing.  ``cumulative_reward`` is the running episode total.
    """

    # ── Episode context (stable within episode) ───────────────────────────
    task_id: str = Field("", description="Active task identifier.")
    dataset_name: str = Field("", description="Human-readable dataset name.")
    schema_info: Dict[str, str] = Field(
        default_factory=dict,
        description="Column name → data type mapping (e.g., {'age': 'int', 'email': 'str'}).",
    )
    total_rows: int = Field(0, ge=0, description="Total rows in the dataset.")
    total_columns: int = Field(0, ge=0, description="Total columns in the dataset.")

    # ── Inspection data (populated only after inspect actions) ─────────────
    visible_rows: Optional[List[Dict[str, Any]]] = Field(
        None, description="Rows returned by the last inspect action.",
    )
    column_statistics: Optional[Dict[str, Any]] = Field(
        None,
        description="Per-column statistics (mean, std, nulls, etc.) from last inspect.",
    )
    secondary_table_rows: Optional[List[Dict[str, Any]]] = Field(
        None, description="Rows from a secondary table (cross-table Task 3).",
    )

    # ── Scoring (updated every step) ──────────────────────────────────────
    action_result: ActionResult = Field(
        ActionResult.INITIAL,
        description="Outcome of the agent's last action.",
    )
    reward_delta: float = Field(
        0.0,
        description=(
            "Step reward as a float (mirrors inherited ``reward``; "
            "use this for arithmetic to avoid type-narrowing the base union)."
        ),
    )
    cumulative_reward: float = Field(
        0.0, description="Running total of rewards across the episode.",
    )
    issues_found: int = Field(
        0, ge=0, description="Number of correctly identified issues so far.",
    )
    issues_remaining_hint: RemainingHint = Field(
        RemainingHint.UNKNOWN,
        description="Coarse hint about remaining undiscovered issues.",
    )

    # ── Episode progress (updated every step) ─────────────────────────────
    steps_taken: int = Field(0, ge=0, description="Steps taken this episode.")
    max_steps: int = Field(
        DEFAULT_MAX_STEPS, ge=1, description="Maximum allowed steps this episode.",
    )
    difficulty_level: Optional[str] = Field(
        None, description="Task difficulty tier: easy, medium, or hard.",
    )
    message: str = Field("", description="Human-readable feedback from the environment.")

    # ── Clamp terminal scores — validator rejects exactly 0.0 and 1.0 ───

    @model_validator(mode="after")
    def _clamp_terminal_scores(self) -> "DataQualityObservation":
        """Ensure done=True observations have scores strictly in (0, 1)."""
        if self.done:
            if isinstance(self.reward, (int, float)) and self.reward is not None:
                object.__setattr__(
                    self, "reward",
                    max(0.0001, min(0.9999, float(self.reward))),
                )
            object.__setattr__(
                self, "cumulative_reward",
                max(0.0001, min(0.9999, self.cumulative_reward)),
            )
        return self

    # ── custom repr for readable debug logs ───────────────────────────────

    def __repr__(self) -> str:
        return (
            f"DataQualityObservation("
            f"task={self.task_id!r}, "
            f"result={self.action_result.value}, "
            f"reward_delta={self.reward_delta:+.3f}, "
            f"cumulative={self.cumulative_reward:.3f}, "
            f"issues={self.issues_found}, "
            f"step={self.steps_taken}/{self.max_steps}, "
            f"done={self.done}"
            f")"
        )


# ═══════════════════════════════════════════════════════════════════════════
# §4  DataQualityState
# ═══════════════════════════════════════════════════════════════════════════

class DataQualityState(State):
    """Internal environment state (server-side only).

    Inherits from ``State`` (provides ``episode_id``, ``step_count``).

    This model is **not** sent to the agent — it is the environment's private
    bookkeeping.  It is serializable for checkpointing and dashboard display.

    Note: ``State`` has ``extra='allow'``, so extra fields can be set
    dynamically.  However, all fields used by the environment should be
    declared here explicitly for schema introspection and IDE support.
    """

    task_id: str = Field("", description="Active task identifier.")
    current_reward: float = Field(0.0, description="Cumulative reward this episode.")
    issues_detected: int = Field(0, ge=0, description="Agent's correct diagnoses.")
    issues_fixed: int = Field(0, ge=0, description="Agent's correct fixes.")
    false_positives: int = Field(0, ge=0, description="Agent's incorrect diagnoses/fixes.")
    total_issues: int = Field(0, ge=0, description="Ground-truth issue count (for metrics).")
    is_finalized: bool = Field(False, description="Whether the agent has called finalize.")


# ═══════════════════════════════════════════════════════════════════════════
# §5  Self-test entry point
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    import json
    import traceback

    _SEP = "-" * 64
    _pass = 0
    _fail = 0

    def _check(label: str, fn) -> None:  # noqa: ANN001
        global _pass, _fail
        try:
            fn()
            print(f"  [OK]   {label}")
            _pass += 1
        except Exception as exc:
            print(f"  [FAIL] {label}")
            traceback.print_exc()
            _fail += 1

    print(f"\n{_SEP}")
    print("  Data Quality Models — Phase 2 Diagnostics")
    print(_SEP)

    # 1. Inheritance
    def assert_(cond: bool, msg: str = "") -> None:
        if not cond:
            raise AssertionError(msg or "assertion failed")

    _check("DataQualityAction  ← Action",
           lambda: assert_(issubclass(DataQualityAction, Action)))
    _check("DataQualityObservation ← Observation",
           lambda: assert_(issubclass(DataQualityObservation, Observation)))
    _check("DataQualityState ← State",
           lambda: assert_(issubclass(DataQualityState, State)))

    # 2. Base fields inherited
    _check("Action.metadata inherited",
           lambda: assert_("metadata" in DataQualityAction.model_fields))
    _check("Observation.done inherited",
           lambda: assert_("done" in DataQualityObservation.model_fields))
    _check("Observation.reward inherited",
           lambda: assert_("reward" in DataQualityObservation.model_fields))
    _check("State.episode_id inherited",
           lambda: assert_("episode_id" in DataQualityState.model_fields))
    _check("State.step_count inherited",
           lambda: assert_("step_count" in DataQualityState.model_fields))

    # 3. Factory constructors
    _check("Factory: inspect(row_indices=[0,1])",
           lambda: DataQualityAction.inspect(row_indices=[0, 1]))
    _check("Factory: inspect(column_names=['age'])",
           lambda: DataQualityAction.inspect(column_names=["age"]))
    _check("Factory: diagnose(...)",
           lambda: DataQualityAction.diagnose(
               row_index=0, column_name="age", issue_type=IssueType.OUTLIER))
    _check("Factory: fix(CORRECT_VALUE, new_value='25')",
           lambda: DataQualityAction.fix(
               row_index=0, column_name="age",
               fix_type=FixType.CORRECT_VALUE, new_value="25",
               justification="Corrected typo."))
    _check("Factory: fix(DELETE_ROW)",
           lambda: DataQualityAction.fix(
               row_index=0, column_name="age",
               fix_type=FixType.DELETE_ROW,
               justification="Duplicate row."))
    _check("Factory: finalize()",
           lambda: DataQualityAction.finalize())

    # 4. Validation rejects invalid actions
    def _must_reject(label: str, fn) -> None:  # noqa: ANN001
        global _pass, _fail
        try:
            fn()
            print(f"  [FAIL] {label}  (should have raised)")
            _fail += 1
        except (ValueError, Exception):
            print(f"  [OK]   {label}  (correctly rejected)")
            _pass += 1

    _must_reject("Reject: inspect with no fields",
                 lambda: DataQualityAction(action_type="inspect"))
    _must_reject("Reject: diagnose missing issue_type",
                 lambda: DataQualityAction(action_type="diagnose",
                                           row_index=0, column_name="a"))
    _must_reject("Reject: fix missing justification",
                 lambda: DataQualityAction(action_type="fix",
                                           row_index=0, column_name="a",
                                           fix_type=FixType.CORRECT_VALUE,
                                           new_value="x"))
    _must_reject("Reject: correct_value without new_value",
                 lambda: DataQualityAction(action_type="fix",
                                           row_index=0, column_name="a",
                                           fix_type=FixType.CORRECT_VALUE,
                                           justification="test"))
    _must_reject("Reject: delete_row with new_value",
                 lambda: DataQualityAction(action_type="fix",
                                           row_index=0, column_name="a",
                                           fix_type=FixType.DELETE_ROW,
                                           new_value="oops",
                                           justification="test"))
    _must_reject("Reject: finalize with extra fields",
                 lambda: DataQualityAction(action_type="finalize",
                                           row_index=0))
    _must_reject("Reject: row_indices > MAX",
                 lambda: DataQualityAction.inspect(
                     row_indices=list(range(MAX_INSPECT_ROWS + 1))))
    _must_reject("Reject: negative row_index",
                 lambda: DataQualityAction(action_type="diagnose",
                                           row_index=-1, column_name="a",
                                           issue_type=IssueType.OUTLIER))
    _must_reject("Reject: invalid issue_type string",
                 lambda: DataQualityAction(action_type="diagnose",
                                           row_index=0, column_name="a",
                                           issue_type="outlire"))

    # 5. Observation construction
    _check("Observation with defaults",
           lambda: DataQualityObservation())
    _check("Observation with all fields", lambda: DataQualityObservation(
        task_id="task_1", dataset_name="customers",
        schema_info={"age": "int"}, total_rows=100, total_columns=5,
        action_result=ActionResult.CORRECT, reward_delta=0.5,
        cumulative_reward=1.5, issues_found=3,
        issues_remaining_hint=RemainingHint.FEW,
        steps_taken=5, max_steps=30, message="Good find!",
        done=False, reward=0.5,
    ))
    _check("Observation __repr__ is compact", lambda: assert_(
        "DataQualityObservation(" in repr(DataQualityObservation()),
        "repr format unexpected",
    ))

    # 6. State construction
    _check("State with defaults", lambda: DataQualityState())
    _check("State with all fields", lambda: DataQualityState(
        episode_id="ep-001", step_count=5, task_id="task_1",
        current_reward=1.5, issues_detected=3, issues_fixed=2,
        false_positives=1, total_issues=5, is_finalized=False,
    ))

    # 7. JSON schema generation
    _check("Action JSON schema", lambda: assert_(
        "action_type" in json.dumps(DataQualityAction.model_json_schema())))
    _check("Observation JSON schema", lambda: assert_(
        "action_result" in json.dumps(DataQualityObservation.model_json_schema())))
    _check("State JSON schema", lambda: assert_(
        "task_id" in json.dumps(DataQualityState.model_json_schema())))

    # 8. Serialization round-trip
    def _roundtrip_check() -> None:
        action = DataQualityAction.fix(
            row_index=5, column_name="age",
            fix_type=FixType.CORRECT_VALUE, new_value="25",
            justification="Typo fix.",
        )
        d = action.model_dump()
        rebuilt = DataQualityAction(**d)
        assert_(rebuilt.action_type == action.action_type)
        assert_(rebuilt.fix_type == action.fix_type)
        assert_(rebuilt.new_value == action.new_value)

    _check("Serialization round-trip (Action)", _roundtrip_check)

    # Summary
    print(_SEP)
    total = _pass + _fail
    if _fail == 0:
        print(f"  Status: ALL {total} CHECKS PASSED [OK]")
    else:
        print(f"  Status: {_fail}/{total} CHECKS FAILED [FAIL]")
    print(f"{_SEP}\n")

    if _fail > 0:
        sys.exit(1)
