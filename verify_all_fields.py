#!/usr/bin/env python3
"""Exhaustive verification: check that ALL score fields (reward, cumulative_reward,
reward_delta) are STRICTLY in (0, 1) for ALL observations across ALL tasks,
including both Python objects and serialized JSON output."""

import sys
import os
import json
sys.path.insert(0, os.path.dirname(__file__))

from models import DataQualityAction, DataQualityObservation, IssueType, FixType
from server.data_quality_environment import DataQualityEnvironment

TASKS = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]

SCORE_FIELDS = ["reward", "cumulative_reward", "reward_delta"]
fail_count = 0


def check_strict(value, label):
    """Return True if value is strictly in (0, 1)."""
    global fail_count
    if not isinstance(value, (int, float)):
        print(f"    FAIL: {label}: not a number: {value}")
        fail_count += 1
        return False
    if value <= 0.0 or value >= 1.0:
        print(f"    FAIL: {label}: {value} (must be strictly in (0, 1))")
        fail_count += 1
        return False
    return True


def check_obs(obs, step_label):
    """Check all score fields on both the Python object and its serialized JSON."""
    all_ok = True
    
    # Check Python object fields
    for field in SCORE_FIELDS:
        val = getattr(obs, field, None)
        if val is not None:
            if not check_strict(val, f"{step_label}.{field}={val}"):
                all_ok = False
    
    # Check serialized JSON (model_dump)
    data = obs.model_dump(mode="json")
    for field in SCORE_FIELDS:
        if field in data and data[field] is not None:
            if not check_strict(data[field], f"{step_label}.json.{field}={data[field]}"):
                all_ok = False
    
    return all_ok


def test_scenario(task_id, scenario_name, actions):
    """Run a sequence of actions and check ALL score fields on ALL observations."""
    env = DataQualityEnvironment()
    obs = env.reset(task_id=task_id)
    all_ok = check_obs(obs, f"{scenario_name}.reset")

    for i, action_data in enumerate(actions):
        action = DataQualityAction(**action_data)
        obs = env.step(action)
        step_label = f"{scenario_name}.step_{i+1}_{action_data.get('action_type')}"
        if not check_obs(obs, step_label):
            all_ok = False
        if obs.done:
            break

    return all_ok


def main():
    global fail_count
    print("=" * 70)
    print("  EXHAUSTIVE FIELD VERIFICATION")
    print("  Checking reward, cumulative_reward, AND reward_delta")
    print("  on both Python objects AND serialized JSON")
    print("=" * 70)

    all_pass = True

    # Scenario 1: Zero-work finalize
    print("\n--- SCENARIO 1: Zero-work finalize ---")
    for task_id in TASKS:
        ok = test_scenario(task_id, "zero-work", [
            {"action_type": "finalize"},
        ])
        print(f"  [{'PASS' if ok else 'FAIL'}] {task_id}")
        if not ok: all_pass = False

    # Scenario 2: Inspect + finalize
    print("\n--- SCENARIO 2: Inspect + finalize ---")
    for task_id in TASKS:
        ok = test_scenario(task_id, "inspect-finalize", [
            {"action_type": "inspect", "row_indices": [0, 1, 2]},
            {"action_type": "inspect", "row_indices": [3, 4, 5]},
            {"action_type": "finalize"},
        ])
        print(f"  [{'PASS' if ok else 'FAIL'}] {task_id}")
        if not ok: all_pass = False

    # Scenario 3: Multiple false positives (drives reward towards 0)
    print("\n--- SCENARIO 3: Many false positives ---")
    for task_id in TASKS:
        actions = []
        for i in range(10):
            actions.append({"action_type": "diagnose", "row_index": 0, 
                          "column_name": f"fake_{i}", "issue_type": "format_error"})
        actions.append({"action_type": "finalize"})
        ok = test_scenario(task_id, "false-pos", actions)
        print(f"  [{'PASS' if ok else 'FAIL'}] {task_id}")
        if not ok: all_pass = False

    # Scenario 4: Perfect detection (high score close to 1.0)
    print("\n--- SCENARIO 4: Perfect detection + fix (high score) ---")
    for task_id in TASKS:
        env = DataQualityEnvironment()
        obs = env.reset(task_id=task_id)
        ok = check_obs(obs, "perfect.reset")
        
        for gt in env.ground_truth:
            col = gt["column"] if gt["column"] != "_row" else "email"
            issue_type_str = gt.get("type", "format_error")
            try:
                issue_type = IssueType(issue_type_str)
            except ValueError:
                issue_type = IssueType.FORMAT_ERROR
            
            action = DataQualityAction.diagnose(
                row_index=gt["row"], column_name=col, issue_type=issue_type
            )
            obs = env.step(action)
            if not check_obs(obs, f"perfect.diagnose.row{gt['row']}"):
                ok = False
            
            expected = gt.get("expected")
            if expected is not None and expected != "DELETE_ROW":
                action = DataQualityAction.fix(
                    row_index=gt["row"], column_name=col,
                    fix_type=FixType.CORRECT_VALUE, new_value=str(expected),
                    justification="Ground truth"
                )
                obs = env.step(action)
                if not check_obs(obs, f"perfect.fix.row{gt['row']}"):
                    ok = False
            elif expected == "DELETE_ROW":
                action = DataQualityAction.fix(
                    row_index=gt["row"], column_name=col,
                    fix_type=FixType.DELETE_ROW,
                    justification="Duplicate"
                )
                obs = env.step(action)
                if not check_obs(obs, f"perfect.delete.row{gt['row']}"):
                    ok = False
            
            if obs.done:
                break
        
        if not obs.done:
            obs = env.step(DataQualityAction.finalize())
            if not check_obs(obs, "perfect.finalize"):
                ok = False
        
        print(f"  [{'PASS' if ok else 'FAIL'}] {task_id} (score={obs.cumulative_reward:.4f})")
        if not ok: all_pass = False

    # Scenario 5: Max-steps auto-finalize
    print("\n--- SCENARIO 5: Max-steps auto-finalize ---")
    for task_id in TASKS:
        env = DataQualityEnvironment()
        obs = env.reset(task_id=task_id)
        ok = check_obs(obs, "maxsteps.reset")

        for i in range(100):
            action = DataQualityAction(action_type="inspect", row_indices=[i % 50])
            obs = env.step(action)
            if not check_obs(obs, f"maxsteps.step_{i+1}"):
                ok = False
            if obs.done:
                break

        print(f"  [{'PASS' if ok else 'FAIL'}] {task_id} (step={i+1})")
        if not ok: all_pass = False

    # Final result
    print("\n" + "=" * 70)
    if all_pass:
        print(f"  ALL FIELDS STRICTLY IN (0, 1) — READY TO SUBMIT (0 failures)")
    else:
        print(f"  {fail_count} FIELD(S) OUT OF RANGE — FIX NEEDED")
    print("=" * 70)

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
