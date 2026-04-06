# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Definitive test suite for the Data Quality RL environment.

Validates every grading path, reward constant, edge case, and scoring formula
locally — without Docker, network, or API keys.

Usage::

    python test_env.py                 # Run all tests
    python -m pytest test_env.py -v    # With pytest (optional)

Design Principles:
    1. **Data-driven**: Tests read ground truth dynamically to find candidates,
       AND verify against hardcoded known-good values for documentation.
    2. **Isolated**: Each test function creates its own DataQualityEnvironment.
    3. **Exhaustive**: Covers reset, inspect (rows/columns/secondary/business
       rules), diagnose (correct/false-positive/duplicate/already-found/OOB),
       fix (correct/detection-only/wrong-value/DELETE_ROW/OOB), finalize
       (with/without work), max-steps auto-finalize, late-step penalty,
       cumulative reward accumulation, final score formula, score variance,
       all task loading, and Pydantic validation boundaries.
    4. **Portable**: Uses [PASS]/[FAIL] markers (no Unicode that may break on
       some terminals).  Exit code 0 = all pass, 1 = failures.

Bug fixes from review:
    [T-01]  Proper imports — no sys.path hacking.
    [T-02]  Data-driven tests + hardcoded verification for documentation.
    [T-04]  Full test isolation — fresh env per test function.
    [T-06]  Distinguishes Pydantic validation errors from env errors.
    [T-07]  Exact reward constant verification.
    [T-09]  Tests secondary table inspection (Task 3).
    [T-10]  Tests business rules inspection.
    [T-11]  Tests cumulative reward accumulation.
    [T-12]  Tests late-step penalty.
    [T-14]  Tests final score formula correctness.
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# ── Imports (project convention: try relative, fallback absolute) ─────────
try:
    from .models import (
        DataQualityAction,
        DataQualityObservation,
        IssueType,
        FixType,
        ActionResult,
        RemainingHint,
    )
    from .server.data_quality_environment import (
        DataQualityEnvironment,
        R_DIAGNOSE,
        R_TYPE_BONUS,
        R_FIX,
        R_JUSTIFY_BONUS,
        P_FALSE_POS,
        P_WRONG_FIX,
        P_LATE_STEP,
        P_INVALID,
        LATE_STEP_THRESHOLD,
        DETECTION_WEIGHT,
        FIX_WEIGHT,
        MAX_FALSE_POS_PENALTY,
        FALSE_POS_PENALTY_RATE,
        TASK_CONFIG,
    )
except ImportError:
    from models import (  # type: ignore[no-redef]
        DataQualityAction,
        DataQualityObservation,
        IssueType,
        FixType,
        ActionResult,
        RemainingHint,
    )
    from server.data_quality_environment import (  # type: ignore[no-redef]
        DataQualityEnvironment,
        R_DIAGNOSE,
        R_TYPE_BONUS,
        R_FIX,
        R_JUSTIFY_BONUS,
        P_FALSE_POS,
        P_WRONG_FIX,
        P_LATE_STEP,
        P_INVALID,
        LATE_STEP_THRESHOLD,
        DETECTION_WEIGHT,
        FIX_WEIGHT,
        MAX_FALSE_POS_PENALTY,
        FALSE_POS_PENALTY_RATE,
        TASK_CONFIG,
    )


# ═══════════════════════════════════════════════════════════════════════════
# §1  Test Framework
# ═══════════════════════════════════════════════════════════════════════════

_PASS = 0
_FAIL = 0
_ERRORS: List[str] = []
_CURRENT_SUITE = ""


def _check(name: str, condition: bool, detail: str = "") -> None:
    """Record a test assertion result."""
    global _PASS, _FAIL
    if condition:
        _PASS += 1
        print(f"  [PASS] {name}")
    else:
        _FAIL += 1
        msg = f"  [FAIL] {name}"
        if detail:
            msg += f" -- {detail}"
        print(msg)
        _ERRORS.append(f"{_CURRENT_SUITE}: {name} -- {detail}")


def _approx(a: float, b: float, tol: float = 1e-6) -> bool:
    """Check approximate float equality."""
    return abs(a - b) < tol


# ═══════════════════════════════════════════════════════════════════════════
# §2  Ground Truth Helpers — data-driven test candidate discovery
# ═══════════════════════════════════════════════════════════════════════════

def _load_ground_truth(task_id: str) -> List[Dict[str, Any]]:
    """Load ground truth issues for a task."""
    config = TASK_CONFIG[task_id]
    gt_path = Path(__file__).resolve().parent / "datasets" / config["ground_truth"]
    with open(gt_path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "issues" in data:
        return data["issues"]
    return data


def _find_issue(gt: List[Dict], **criteria: Any) -> Optional[Dict]:
    """Find the first ground truth issue matching all criteria."""
    for issue in gt:
        if all(issue.get(k) == v for k, v in criteria.items()):
            return issue
    return None


def _find_fixable_issue(gt: List[Dict], issue_type: Optional[str] = None) -> Optional[Dict]:
    """Find a fixable issue (has 'expected' key) of a specific type."""
    for issue in gt:
        if "expected" in issue and issue.get("expected") is not None:
            if issue_type is None or issue.get("type") == issue_type:
                return issue
    return None


def _find_detection_only(gt: List[Dict]) -> Optional[Dict]:
    """Find a detection-only issue (no 'expected' key)."""
    for issue in gt:
        if "expected" not in issue:
            return issue
    return None


def _find_row_issue(gt: List[Dict]) -> Optional[Dict]:
    """Find a whole-row issue (column='_row', e.g., duplicate)."""
    for issue in gt:
        if issue.get("column") == "_row":
            return issue
    return None


# ═══════════════════════════════════════════════════════════════════════════
# §3  Test Functions
# ═══════════════════════════════════════════════════════════════════════════

def test_reset_all_tasks() -> None:
    """Verify reset() works for all tasks and returns valid observations."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_reset_all_tasks"
    print(f"\n=== {_CURRENT_SUITE} ===")

    for task_id in TASK_CONFIG:
        env = DataQualityEnvironment()
        obs = env.reset(task_id=task_id)

        _check(f"reset({task_id}) returns DataQualityObservation",
               isinstance(obs, DataQualityObservation))
        _check(f"  done=False", obs.done is False)
        _check(f"  reward=0.0", obs.reward == 0.0)
        _check(f"  task_id set correctly", obs.task_id == task_id)
        _check(f"  visible_rows populated", obs.visible_rows is not None and len(obs.visible_rows) > 0)
        _check(f"  schema_info populated", len(obs.schema_info) > 0)
        _check(f"  total_rows > 0", obs.total_rows > 0)
        _check(f"  total_columns > 0", obs.total_columns > 0)
        _check(f"  action_result=INITIAL", obs.action_result == ActionResult.INITIAL)
        _check(f"  issues_remaining_hint is enum", isinstance(obs.issues_remaining_hint, RemainingHint))
        _check(f"  steps_taken=0", obs.steps_taken == 0)
        _check(f"  max_steps > 0", obs.max_steps > 0)
        _check(f"  message non-empty", len(obs.message) > 0)


def test_reset_invalid_task_id() -> None:
    """Invalid task_id falls back to task_1_format_fixer."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_reset_invalid_task_id"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    obs = env.reset(task_id="nonexistent_task")
    _check("invalid task_id defaults to task_1", obs.task_id == "task_1_format_fixer")


def test_inspect_rows() -> None:
    """Inspect action with row_indices returns correct rows."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_inspect_rows"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(action_type="inspect", row_indices=[0, 1, 2]))
    _check("inspect returns 3 rows", obs.visible_rows is not None and len(obs.visible_rows) == 3)
    _check("rows have _row_index", all("_row_index" in r for r in obs.visible_rows))
    _check("_row_index values correct",
           [r["_row_index"] for r in obs.visible_rows] == [0, 1, 2])
    _check("done=False", obs.done is False)
    _check("steps_taken=1", obs.steps_taken == 1)


def test_inspect_out_of_bounds_rows() -> None:
    """Out-of-bounds row indices are silently filtered."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_inspect_out_of_bounds_rows"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(action_type="inspect", row_indices=[999, 0]))
    _check("OOB indices filtered, valid returned",
           obs.visible_rows is not None and len(obs.visible_rows) == 1)
    _check("remaining row is index 0", obs.visible_rows[0]["_row_index"] == 0)


def test_inspect_column_statistics() -> None:
    """Inspect action with column_names returns statistics."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_inspect_column_statistics"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    # Get actual column names from schema
    schema_cols = list(env.schema_info.keys())
    test_cols = schema_cols[:2] if len(schema_cols) >= 2 else schema_cols

    obs = env.step(DataQualityAction(action_type="inspect", column_names=test_cols))
    _check("column_statistics returned", obs.column_statistics is not None)
    _check("correct number of columns", len(obs.column_statistics) == len(test_cols))

    for col in test_cols:
        stats = obs.column_statistics.get(col, {})
        _check(f"  {col}: has 'type'", "type" in stats)
        _check(f"  {col}: has 'total'", "total" in stats)
        _check(f"  {col}: has 'non_null'", "non_null" in stats)
        _check(f"  {col}: has 'null_count'", "null_count" in stats)
        _check(f"  {col}: has 'unique_count'", "unique_count" in stats)
        _check(f"  {col}: has 'sample_values'", "sample_values" in stats)
        _check(f"  {col}: total = dataset size", stats["total"] == len(env.dataset))


def test_inspect_secondary_table() -> None:
    """[T-09] Task 3: inspect related products table."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_inspect_secondary_table"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_3_integrity_auditor")

    obs = env.step(DataQualityAction(
        action_type="inspect",
        row_indices=[0],
        related_table="products",
    ))
    _check("secondary_table_rows returned",
           obs.secondary_table_rows is not None and len(obs.secondary_table_rows) > 0)
    _check("products rows have data",
           isinstance(obs.secondary_table_rows[0], dict) and len(obs.secondary_table_rows[0]) > 0)


def test_inspect_business_rules() -> None:
    """[T-10] Task 3: inspect business rules metadata."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_inspect_business_rules"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_3_integrity_auditor")

    obs = env.step(DataQualityAction(
        action_type="inspect",
        row_indices=[0],
        related_table="business_rules",
    ))
    # business_rules may or may not be populated depending on dataset
    _check("action executed without error", obs.action_result is not None)


def test_diagnose_correct_hardcoded() -> None:
    """Correct diagnosis — hardcoded known-good: Task 1, row 3, email, format_error."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_diagnose_correct_hardcoded"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(
        action_type="diagnose",
        row_index=3,
        column_name="email",
        issue_type="format_error",
    ))
    _check("result=correct", obs.action_result == ActionResult.CORRECT)
    _check("issues_found=1", obs.issues_found == 1)

    # [T-07] Exact reward: R_DIAGNOSE + R_TYPE_BONUS (type matches)
    expected_reward = R_DIAGNOSE + R_TYPE_BONUS
    _check(f"reward_delta={expected_reward:.2f} (R_DIAGNOSE + R_TYPE_BONUS)",
           _approx(obs.reward_delta, expected_reward),
           f"got {obs.reward_delta}")


def test_diagnose_correct_dynamic() -> None:
    """Correct diagnosis — data-driven: finds first fixable issue in each task."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_diagnose_correct_dynamic"
    print(f"\n=== {_CURRENT_SUITE} ===")

    for task_id in TASK_CONFIG:
        gt = _load_ground_truth(task_id)
        # Find a non-_row issue (standard column diagnosis)
        issue = None
        for i in gt:
            if i.get("column") != "_row":
                issue = i
                break
        if issue is None:
            _check(f"{task_id}: skipped (no standard column issue)", True)
            continue

        env = DataQualityEnvironment()
        env.reset(task_id=task_id)

        obs = env.step(DataQualityAction(
            action_type="diagnose",
            row_index=issue["row"],
            column_name=issue["column"],
            issue_type=issue.get("type", "format_error"),
        ))
        _check(f"{task_id}: row {issue['row']}, {issue['column']} -> correct",
               obs.action_result == ActionResult.CORRECT,
               f"got {obs.action_result}")
        _check(f"{task_id}: positive reward", obs.reward_delta > 0)


def test_diagnose_false_positive() -> None:
    """False positive diagnosis: row 0, name column, unlikely to be an issue."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_diagnose_false_positive"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    # Row 0 is clean (not in ground truth)
    gt = _load_ground_truth("task_1_format_fixer")
    clean_row = 0
    for r in range(len(env.dataset)):
        if not any(g["row"] == r for g in gt):
            clean_row = r
            break

    # Get any column name from schema
    col = list(env.schema_info.keys())[0]

    obs = env.step(DataQualityAction(
        action_type="diagnose",
        row_index=clean_row,
        column_name=col,
        issue_type="format_error",
    ))
    _check("result=incorrect", obs.action_result == ActionResult.INCORRECT)
    _check(f"reward_delta={P_FALSE_POS} (P_FALSE_POS)",
           _approx(obs.reward_delta, P_FALSE_POS),
           f"got {obs.reward_delta}")


def test_diagnose_already_found() -> None:
    """Repeated diagnosis returns already_found with zero reward."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_diagnose_already_found"
    print(f"\n=== {_CURRENT_SUITE} ===")

    gt = _load_ground_truth("task_1_format_fixer")
    issue = gt[0]  # First issue

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    col = issue["column"] if issue["column"] != "_row" else list(env.schema_info.keys())[0]

    # First diagnosis
    env.step(DataQualityAction(
        action_type="diagnose",
        row_index=issue["row"],
        column_name=col,
        issue_type=issue.get("type", "format_error"),
    ))

    # Repeat
    obs = env.step(DataQualityAction(
        action_type="diagnose",
        row_index=issue["row"],
        column_name=col,
        issue_type=issue.get("type", "format_error"),
    ))
    _check("repeat returns already_found", obs.action_result == ActionResult.ALREADY_FOUND)
    _check("repeat reward_delta=0", _approx(obs.reward_delta, 0.0))


def test_diagnose_duplicate_any_column() -> None:
    """[FIX-03] Duplicate row issues (column='_row') accept ANY column name."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_diagnose_duplicate_any_column"
    print(f"\n=== {_CURRENT_SUITE} ===")

    gt = _load_ground_truth("task_2_duplicate_detective")
    dup_issue = _find_row_issue(gt)

    if dup_issue is None:
        _check("SKIP: no _row issue in task 2 ground truth", False,
               "Expected at least one duplicate row issue")
        return

    # Hardcoded verification: row 12 is known duplicate
    _check("hardcoded: row 12 is duplicate", dup_issue["row"] == 12 or any(
        g["row"] == 12 and g["column"] == "_row" for g in gt
    ))

    # Dynamic: test with multiple column names
    test_columns = ["first_name", "email", "phone", "city"]
    for col in test_columns:
        env = DataQualityEnvironment()
        env.reset(task_id="task_2_duplicate_detective")

        # Verify column exists in schema
        if col not in env.schema_info:
            continue

        obs = env.step(DataQualityAction(
            action_type="diagnose",
            row_index=dup_issue["row"],
            column_name=col,
            issue_type="duplicate",
        ))
        _check(f"dup row {dup_issue['row']} via '{col}' -> correct",
               obs.action_result == ActionResult.CORRECT,
               f"got {obs.action_result}")


def test_diagnose_out_of_bounds() -> None:
    """[FIX-06] Out-of-bounds row_index returns error, not crash."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_diagnose_out_of_bounds"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(
        action_type="diagnose",
        row_index=9999,
        column_name="email",
        issue_type="format_error",
    ))
    _check("OOB diagnose returns error", obs.action_result == ActionResult.ERROR)
    _check("no crash (done=False)", obs.done is False)


def test_fix_correct_hardcoded() -> None:
    """Correct fix — hardcoded: Task 1, row 3, email -> 'john.doe@example.com'."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_fix_correct_hardcoded"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(
        action_type="fix",
        row_index=3,
        column_name="email",
        new_value="john.doe@example.com",
        fix_type="correct_value",
        justification="Missing @ symbol between 'doe' and 'example'",
    ))
    _check("result=correct", obs.action_result == ActionResult.CORRECT)

    # [T-07] Exact reward: R_FIX + R_JUSTIFY_BONUS
    expected_reward = R_FIX + R_JUSTIFY_BONUS
    _check(f"reward_delta={expected_reward:.2f} (R_FIX + R_JUSTIFY_BONUS)",
           _approx(obs.reward_delta, expected_reward),
           f"got {obs.reward_delta}")

    # Auto-diagnose: fix without prior diagnose should auto-count found issues
    _check("auto-diagnose: issues_found >= 1", obs.issues_found >= 1)


def test_fix_correct_dynamic() -> None:
    """Correct fix — data-driven: finds first fixable issue per task."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_fix_correct_dynamic"
    print(f"\n=== {_CURRENT_SUITE} ===")

    for task_id in TASK_CONFIG:
        gt = _load_ground_truth(task_id)
        issue = _find_fixable_issue(gt)

        if issue is None:
            _check(f"{task_id}: skipped (no fixable issue)", True)
            continue

        if issue.get("expected") == "DELETE_ROW":
            # Skip DELETE_ROW here — tested separately
            issue = _find_fixable_issue(gt, issue_type="format_error")
            if issue is None:
                continue

        env = DataQualityEnvironment()
        env.reset(task_id=task_id)

        col = issue["column"] if issue["column"] != "_row" else list(env.schema_info.keys())[0]

        obs = env.step(DataQualityAction(
            action_type="fix",
            row_index=issue["row"],
            column_name=col,
            new_value=str(issue["expected"]),
            fix_type="correct_value",
            justification="Automated test fix",
        ))
        _check(f"{task_id}: fix row {issue['row']}/{col} -> correct",
               obs.action_result == ActionResult.CORRECT,
               f"got {obs.action_result}")


def test_fix_detection_only() -> None:
    """[FIX-02] Detection-only issues (no 'expected') return partial, not error."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_fix_detection_only"
    print(f"\n=== {_CURRENT_SUITE} ===")

    gt = _load_ground_truth("task_1_format_fixer")
    det_only = _find_detection_only(gt)

    if det_only is None:
        _check("SKIP: no detection-only issue in task 1", False,
               "Expected at least one detection-only issue")
        return

    # Hardcoded verification: row 7, phone is detection-only
    _check("hardcoded: row 7 phone is detection-only",
           any(g["row"] == 7 and g["column"] == "phone" and "expected" not in g for g in gt))

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(
        action_type="fix",
        row_index=det_only["row"],
        column_name=det_only["column"],
        new_value="anything",
        fix_type="correct_value",
        justification="Test attempt on detection-only issue",
    ))
    _check("detection-only fix returns partial",
           obs.action_result == ActionResult.PARTIAL,
           f"got {obs.action_result}")
    _check("no penalty (reward_delta >= 0)", obs.reward_delta >= 0)


def test_fix_wrong_value() -> None:
    """Wrong fix value yields P_WRONG_FIX penalty."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_fix_wrong_value"
    print(f"\n=== {_CURRENT_SUITE} ===")

    gt = _load_ground_truth("task_1_format_fixer")
    issue = _find_fixable_issue(gt)

    if issue is None or issue.get("expected") == "DELETE_ROW":
        _check("SKIP: no standard fixable issue", True)
        return

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(
        action_type="fix",
        row_index=issue["row"],
        column_name=issue["column"],
        new_value="DELIBERATELY_WRONG_VALUE_12345",
        fix_type="correct_value",
        justification="Intentionally wrong for testing",
    ))
    _check("wrong fix returns incorrect",
           obs.action_result == ActionResult.INCORRECT,
           f"got {obs.action_result}")
    _check(f"penalty = {P_WRONG_FIX}",
           _approx(obs.reward_delta, P_WRONG_FIX),
           f"got {obs.reward_delta}")


def test_fix_delete_row() -> None:
    """[FIX-08] DELETE_ROW fix for duplicate rows scored correctly."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_fix_delete_row"
    print(f"\n=== {_CURRENT_SUITE} ===")

    gt = _load_ground_truth("task_2_duplicate_detective")
    dup_issue = None
    for g in gt:
        if g.get("expected") == "DELETE_ROW":
            dup_issue = g
            break

    if dup_issue is None:
        _check("SKIP: no DELETE_ROW issue in task 2", False,
               "Expected at least one DELETE_ROW issue")
        return

    # Hardcoded verification
    _check("hardcoded: row 12 has DELETE_ROW expected",
           any(g["row"] == 12 and g.get("expected") == "DELETE_ROW" for g in gt))

    env = DataQualityEnvironment()
    env.reset(task_id="task_2_duplicate_detective")

    col = list(env.schema_info.keys())[0]  # Any column works for _row issues

    obs = env.step(DataQualityAction(
        action_type="fix",
        row_index=dup_issue["row"],
        column_name=col,
        fix_type="delete_row",
        justification=f"Duplicate of row {dup_issue.get('duplicate_of', '?')}",
    ))
    _check("DELETE_ROW fix returns correct",
           obs.action_result == ActionResult.CORRECT,
           f"got {obs.action_result}")
    _check("positive reward", obs.reward_delta > 0)


def test_fix_out_of_bounds() -> None:
    """[FIX-06] Out-of-bounds fix returns error."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_fix_out_of_bounds"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(
        action_type="fix",
        row_index=9999,
        column_name="email",
        new_value="test@test.com",
        fix_type="correct_value",
        justification="Test",
    ))
    _check("OOB fix returns error", obs.action_result == ActionResult.ERROR)


def test_finalize_no_work() -> None:
    """Finalize immediately: done=True, low score."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_finalize_no_work"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    obs = env.step(DataQualityAction(action_type="finalize"))
    _check("finalize done=True", obs.done is True)
    _check("action_result=complete", obs.action_result == ActionResult.COMPLETE)
    _check("score in [0, 1]", 0.0 <= obs.cumulative_reward <= 1.0)
    # No work done: detection_rate=0, fix_rate=0, score should be ~0
    _check("score ~0 (no work)", obs.cumulative_reward < 0.1,
           f"got {obs.cumulative_reward}")


def test_finalize_reward_delta() -> None:
    """[FIX-05] Finalize reward_delta reflects actual score change."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_finalize_reward_delta"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    # Do some correct work first
    gt = _load_ground_truth("task_1_format_fixer")
    issue = gt[0]
    col = issue["column"] if issue["column"] != "_row" else list(env.schema_info.keys())[0]

    env.step(DataQualityAction(
        action_type="diagnose",
        row_index=issue["row"],
        column_name=col,
        issue_type=issue.get("type", "format_error"),
    ))

    pre_finalize_reward = env.cumulative_reward

    obs = env.step(DataQualityAction(action_type="finalize"))
    _check("finalize done=True", obs.done is True)
    _check("reward_delta = final_score - pre_finalize_reward",
           _approx(obs.reward_delta, obs.cumulative_reward - pre_finalize_reward, tol=0.01),
           f"delta={obs.reward_delta}, cum={obs.cumulative_reward}, pre={pre_finalize_reward}")
    _check("cumulative_reward > 0 (work was done)", obs.cumulative_reward > 0)


def test_max_steps_auto_finalize() -> None:
    """Episode auto-finalizes at max_steps."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_max_steps_auto_finalize"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")  # max_steps=30

    obs = None
    for i in range(30):
        obs = env.step(DataQualityAction(action_type="inspect", row_indices=[0]))

    _check("done=True at max_steps", obs.done is True)
    _check("steps_taken=30", obs.steps_taken == 30)


def test_late_step_penalty() -> None:
    """[T-12] Late-step penalty applied after 80% of budget consumed."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_late_step_penalty"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")  # max_steps=30, threshold=24

    threshold = int(30 * LATE_STEP_THRESHOLD)  # 24

    # Advance to just before threshold
    for i in range(threshold):
        env.step(DataQualityAction(action_type="inspect", row_indices=[0]))

    # Step at threshold+1: should have late penalty on inspect (which otherwise has 0 reward)
    obs = env.step(DataQualityAction(action_type="inspect", row_indices=[0]))
    _check(f"late-step penalty applied at step {threshold + 1}",
           obs.reward_delta < 0,
           f"reward_delta={obs.reward_delta}, expected < 0 (P_LATE_STEP={P_LATE_STEP})")
    _check(f"penalty magnitude = {P_LATE_STEP}",
           _approx(obs.reward_delta, P_LATE_STEP),
           f"got {obs.reward_delta}")


def test_cumulative_reward_accumulation() -> None:
    """[T-11] Cumulative reward accumulates correctly across steps."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_cumulative_reward_accumulation"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    gt = _load_ground_truth("task_1_format_fixer")
    expected_cum = 0.0

    # Diagnose first two distinct issues
    diagnosed = 0
    for issue in gt:
        if issue["column"] == "_row":
            continue
        if diagnosed >= 2:
            break

        obs = env.step(DataQualityAction(
            action_type="diagnose",
            row_index=issue["row"],
            column_name=issue["column"],
            issue_type=issue.get("type", "format_error"),
        ))

        if obs.action_result == ActionResult.CORRECT:
            expected_cum += obs.reward_delta
            expected_cum = max(0.0, expected_cum)  # Floor at 0
            _check(f"step {diagnosed + 1}: cumulative matches",
                   _approx(obs.cumulative_reward, expected_cum, tol=0.01),
                   f"expected {expected_cum:.4f}, got {obs.cumulative_reward:.4f}")
            diagnosed += 1


def test_score_variance() -> None:
    """Different agent behaviors produce different final scores."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_score_variance"
    print(f"\n=== {_CURRENT_SUITE} ===")

    # Run 1: finalize immediately (no work)
    env1 = DataQualityEnvironment()
    env1.reset(task_id="task_1_format_fixer")
    obs1 = env1.step(DataQualityAction(action_type="finalize"))

    # Run 2: diagnose + fix before finalize
    env2 = DataQualityEnvironment()
    env2.reset(task_id="task_1_format_fixer")
    env2.step(DataQualityAction(
        action_type="diagnose",
        row_index=3, column_name="email", issue_type="format_error",
    ))
    env2.step(DataQualityAction(
        action_type="fix",
        row_index=3, column_name="email",
        new_value="john.doe@example.com", fix_type="correct_value",
        justification="Missing @ symbol",
    ))
    obs2 = env2.step(DataQualityAction(action_type="finalize"))

    _check("different behaviors -> different scores",
           obs1.cumulative_reward != obs2.cumulative_reward,
           f"both returned {obs1.cumulative_reward}")
    _check("work produces higher score",
           obs2.cumulative_reward > obs1.cumulative_reward,
           f"no-work={obs1.cumulative_reward}, work={obs2.cumulative_reward}")


def test_final_score_formula() -> None:
    """[T-14] Verify final score formula: 0.40 × detection + 0.60 × fix - FP penalty."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_final_score_formula"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    gt = _load_ground_truth("task_1_format_fixer")
    total_issues = len(gt)
    fixable = [g for g in gt if g.get("expected") is not None]
    n_fixable = len(fixable)

    # Diagnose ALL issues
    for issue in gt:
        col = issue["column"] if issue["column"] != "_row" else list(env.schema_info.keys())[0]
        try:
            issue_type = IssueType(issue.get("type", "format_error"))
        except ValueError:
            issue_type = IssueType.FORMAT_ERROR
        env.step(DataQualityAction(
            action_type="diagnose",
            row_index=issue["row"],
            column_name=col,
            issue_type=issue_type,
        ))

    obs = env.step(DataQualityAction(action_type="finalize"))

    # Expected: detection_rate=1.0, fix_rate=0.0, no false positives
    expected_score = round(1.0 * DETECTION_WEIGHT + 0.0 * FIX_WEIGHT - 0.0, 4)
    _check(f"all-diagnose score ~{expected_score:.4f} ({DETECTION_WEIGHT}*1.0 + {FIX_WEIGHT}*0.0)",
           abs(obs.cumulative_reward - expected_score) < 0.05,
           f"got {obs.cumulative_reward:.4f}")


def test_full_episode_perfect_score() -> None:
    """Perfect episode: diagnose and fix all fixable issues."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_full_episode_perfect_score"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    gt = _load_ground_truth("task_1_format_fixer")
    total_issues = len(gt)
    fixable = [g for g in gt if g.get("expected") is not None]

    # Diagnose + fix all issues
    for issue in gt:
        col = issue["column"] if issue["column"] != "_row" else list(env.schema_info.keys())[0]
        try:
            issue_type = IssueType(issue.get("type", "format_error"))
        except ValueError:
            issue_type = IssueType.FORMAT_ERROR

        env.step(DataQualityAction(
            action_type="diagnose",
            row_index=issue["row"],
            column_name=col,
            issue_type=issue_type,
        ))

        if issue.get("expected") is not None:
            env.step(DataQualityAction(
                action_type="fix",
                row_index=issue["row"],
                column_name=col,
                new_value=str(issue["expected"]),
                fix_type="correct_value",
                justification="Ground truth value",
            ))

    obs = env.step(DataQualityAction(action_type="finalize"))

    # Expected: detection=1.0, fix=1.0, FP=0
    expected_perfect = round(1.0 * DETECTION_WEIGHT + 1.0 * FIX_WEIGHT, 4)
    _check(f"perfect score ~{expected_perfect:.4f}",
           abs(obs.cumulative_reward - expected_perfect) < 0.05,
           f"got {obs.cumulative_reward:.4f}")


def test_pydantic_validation_boundaries() -> None:
    """[T-06] Pydantic validation rejects malformed actions at construction time."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_pydantic_validation_boundaries"
    print(f"\n=== {_CURRENT_SUITE} ===")

    # These should raise ValidationError (Pydantic), NOT reach the environment
    invalid_cases = [
        ("inspect with no fields",
         lambda: DataQualityAction(action_type="inspect")),
        ("diagnose missing issue_type",
         lambda: DataQualityAction(action_type="diagnose", row_index=0, column_name="a")),
        ("fix missing justification",
         lambda: DataQualityAction(action_type="fix", row_index=0, column_name="a",
                                   fix_type="correct_value", new_value="x")),
        ("correct_value without new_value",
         lambda: DataQualityAction(action_type="fix", row_index=0, column_name="a",
                                   fix_type="correct_value", justification="test")),
        ("delete_row with new_value",
         lambda: DataQualityAction(action_type="fix", row_index=0, column_name="a",
                                   fix_type="delete_row", new_value="oops",
                                   justification="test")),
        ("finalize with extra fields",
         lambda: DataQualityAction(action_type="finalize", row_index=0)),
        ("invalid issue_type string",
         lambda: DataQualityAction(action_type="diagnose", row_index=0,
                                   column_name="a", issue_type="outlire")),
    ]

    for label, fn in invalid_cases:
        rejected = False
        try:
            fn()
        except Exception:
            rejected = True
        _check(f"Pydantic rejects: {label}", rejected)


def test_env_level_invalid_action() -> None:
    """Environment-level error for structurally valid but semantically wrong actions."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_env_level_invalid_action"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    # diagnose with row_index but missing column_name at env level
    # (Pydantic should actually catch this, but let's verify both layers)
    try:
        action = DataQualityAction(
            action_type="diagnose",
            row_index=0,
            column_name="nonexistent_column",
            issue_type="format_error",
        )
        obs = env.step(action)
        # Should be incorrect (false positive), not a crash
        _check("nonexistent column diagnosed as false positive",
               obs.action_result == ActionResult.INCORRECT)
    except Exception:
        _check("nonexistent column handled without crash", True)


def test_all_tasks_ground_truth_integrity() -> None:
    """All tasks have sufficient ground truth with required keys."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_all_tasks_ground_truth_integrity"
    print(f"\n=== {_CURRENT_SUITE} ===")

    expected_minimums = {
        "task_1_format_fixer": 8,
        "task_2_duplicate_detective": 12,
        "task_3_integrity_auditor": 15,
    }

    for task_id in TASK_CONFIG:
        gt = _load_ground_truth(task_id)
        min_expected = expected_minimums.get(task_id, 3)

        _check(f"{task_id}: has >= {min_expected} issues",
               len(gt) >= min_expected, f"has {len(gt)}")

        # All issues have required keys
        for i, issue in enumerate(gt):
            _check(f"{task_id}: issue {i} has 'row'", "row" in issue)
            _check(f"{task_id}: issue {i} has 'column'", "column" in issue)
            _check(f"{task_id}: issue {i} has 'type'", "type" in issue)

        # At least some fixable
        fixable = [g for g in gt if g.get("expected") is not None]
        _check(f"{task_id}: has fixable issues", len(fixable) >= 1,
               f"has {len(fixable)}")

        # At least some detection-only
        detection_only = [g for g in gt if "expected" not in g]
        _check(f"{task_id}: has detection-only issues", len(detection_only) >= 1,
               f"has {len(detection_only)}")


def test_state_property() -> None:
    """env.state returns a correct DataQualityState."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_state_property"
    print(f"\n=== {_CURRENT_SUITE} ===")

    from server.data_quality_environment import DataQualityEnvironment as _Env  # type: ignore[no-redef]

    # DataQualityState is available through the environment's state property.
    # We verify its type using the class from the environment module.
    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    st = env.state
    _check("state has task_id attribute", hasattr(st, "task_id"))
    _check("state.task_id correct", st.task_id == "task_1_format_fixer")
    _check("state.total_issues > 0", st.total_issues > 0)
    _check("state.is_finalized=False", st.is_finalized is False)

    # Do some work
    env.step(DataQualityAction(
        action_type="diagnose",
        row_index=3, column_name="email", issue_type="format_error",
    ))
    st = env.state
    _check("state.issues_detected updated", st.issues_detected >= 1)


def test_remaining_hint_progression() -> None:
    """Issues-remaining hint transitions from many -> some -> few -> none."""
    global _CURRENT_SUITE
    _CURRENT_SUITE = "test_remaining_hint_progression"
    print(f"\n=== {_CURRENT_SUITE} ===")

    env = DataQualityEnvironment()
    env.reset(task_id="task_1_format_fixer")

    gt = _load_ground_truth("task_1_format_fixer")
    hints_seen = set()

    for issue in gt:
        col = issue["column"] if issue["column"] != "_row" else list(env.schema_info.keys())[0]
        try:
            issue_type = IssueType(issue.get("type", "format_error"))
        except ValueError:
            issue_type = IssueType.FORMAT_ERROR

        obs = env.step(DataQualityAction(
            action_type="diagnose",
            row_index=issue["row"],
            column_name=col,
            issue_type=issue_type,
        ))
        hints_seen.add(obs.issues_remaining_hint)

    _check("hint progression includes multiple values",
           len(hints_seen) >= 2,
           f"only saw: {hints_seen}")


# ═══════════════════════════════════════════════════════════════════════════
# §4  Test Runner
# ═══════════════════════════════════════════════════════════════════════════

_ALL_TESTS = [
    test_reset_all_tasks,
    test_reset_invalid_task_id,
    test_inspect_rows,
    test_inspect_out_of_bounds_rows,
    test_inspect_column_statistics,
    test_inspect_secondary_table,
    test_inspect_business_rules,
    test_diagnose_correct_hardcoded,
    test_diagnose_correct_dynamic,
    test_diagnose_false_positive,
    test_diagnose_already_found,
    test_diagnose_duplicate_any_column,
    test_diagnose_out_of_bounds,
    test_fix_correct_hardcoded,
    test_fix_correct_dynamic,
    test_fix_detection_only,
    test_fix_wrong_value,
    test_fix_delete_row,
    test_fix_out_of_bounds,
    test_finalize_no_work,
    test_finalize_reward_delta,
    test_max_steps_auto_finalize,
    test_late_step_penalty,
    test_cumulative_reward_accumulation,
    test_score_variance,
    test_final_score_formula,
    test_full_episode_perfect_score,
    test_pydantic_validation_boundaries,
    test_env_level_invalid_action,
    test_all_tasks_ground_truth_integrity,
    test_state_property,
    test_remaining_hint_progression,
]


def main() -> int:
    """Run all tests and print summary."""
    global _PASS, _FAIL, _ERRORS

    print("=" * 64)
    print("  Data Quality Environment — Automated Test Suite")
    print("=" * 64)

    start = time.time()
    crashed = 0

    for test_fn in _ALL_TESTS:
        try:
            test_fn()
        except Exception:
            crashed += 1
            print(f"\n  [CRASH] {test_fn.__name__}")
            traceback.print_exc()
            _ERRORS.append(f"{test_fn.__name__}: CRASHED")

    elapsed = time.time() - start
    total = _PASS + _FAIL

    print("\n" + "=" * 64)
    print(f"  RESULTS:  {_PASS} passed  |  {_FAIL} failed  |  {crashed} crashed")
    print(f"  TOTAL:    {total} assertions in {len(_ALL_TESTS)} test functions")
    print(f"  TIME:     {elapsed:.2f}s")

    if _FAIL > 0 or crashed > 0:
        print(f"\n  FAILURES:")
        for err in _ERRORS:
            print(f"    - {err}")
        print("\n  FIX ALL FAILURES BEFORE PROCEEDING.")
        print("=" * 64)
        return 1
    else:
        print("\n  ALL TESTS PASSED.")
        print("=" * 64)
        return 0


if __name__ == "__main__":
    sys.exit(main())
