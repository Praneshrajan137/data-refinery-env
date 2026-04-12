#!/usr/bin/env python3
"""Nuclear verification: check that ALL reward values from ALL steps
and ALL tasks are STRICTLY in (0, 1)."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from models import DataQualityAction
from server.data_quality_environment import DataQualityEnvironment

TASKS = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]

def check_strict(value, label):
    """Return True if value is strictly in (0, 1)."""
    if not isinstance(value, (int, float)):
        print("    FAIL: {}: not a number: {}".format(label, value))
        return False
    if value <= 0.0 or value >= 1.0:
        print("    FAIL: {}: {} (must be strictly in (0, 1))".format(label, value))
        return False
    return True


def test_scenario(task_id, scenario_name, actions):
    """Run a sequence of actions and check ALL rewards."""
    env = DataQualityEnvironment()
    obs = env.reset(task_id=task_id)
    all_ok = True

    # Check reset observation
    if not check_strict(obs.reward, "reset.reward"):
        all_ok = False
    if not check_strict(obs.cumulative_reward, "reset.cumulative_reward"):
        all_ok = False
    if not check_strict(obs.reward_delta, "reset.reward_delta"):
        all_ok = False

    # Run actions
    for i, action_data in enumerate(actions):
        action = DataQualityAction(**action_data)
        obs = env.step(action)

        step_label = "step_{}_{}".format(i+1, action_data.get("action_type"))
        if not check_strict(obs.reward, "{}.reward".format(step_label)):
            all_ok = False
        if not check_strict(obs.cumulative_reward, "{}.cumulative_reward".format(step_label)):
            all_ok = False
        if not check_strict(obs.reward_delta, "{}.reward_delta".format(step_label)):
            all_ok = False

        if obs.done:
            break

    return all_ok


def main():
    print("=" * 70)
    print("  NUCLEAR SCORE VERIFICATION")
    print("  Checking ALL reward values across ALL steps and ALL tasks")
    print("=" * 70)

    all_pass = True

    # Scenario 1: Zero-work finalize (produces minimum score)
    print("\n--- SCENARIO 1: Zero-work finalize (minimum score) ---")
    for task_id in TASKS:
        ok = test_scenario(task_id, "zero-work", [
            {"action_type": "finalize"},
        ])
        status = "PASS" if ok else "FAIL"
        print("  [{}] {}".format(status, task_id))
        if not ok:
            all_pass = False

    # Scenario 2: Inspect-only, then finalize
    print("\n--- SCENARIO 2: Inspect + finalize ---")
    for task_id in TASKS:
        ok = test_scenario(task_id, "inspect-finalize", [
            {"action_type": "inspect", "row_indices": [0, 1, 2]},
            {"action_type": "inspect", "row_indices": [3, 4, 5]},
            {"action_type": "finalize"},
        ])
        status = "PASS" if ok else "FAIL"
        print("  [{}] {}".format(status, task_id))
        if not ok:
            all_pass = False

    # Scenario 3: Max-steps auto-finalize
    print("\n--- SCENARIO 3: Max-steps auto-finalize ---")
    for task_id in TASKS:
        env = DataQualityEnvironment()
        obs = env.reset(task_id=task_id)
        ok = True

        if not check_strict(obs.reward, "reset.reward"):
            ok = False

        for i in range(100):
            action = DataQualityAction(
                action_type="inspect",
                row_indices=[i % 50],
            )
            obs = env.step(action)

            if not check_strict(obs.reward, "step_{}.reward".format(i+1)):
                ok = False
            if not check_strict(obs.cumulative_reward, "step_{}.cumulative_reward".format(i+1)):
                ok = False

            if obs.done:
                break

        status = "PASS" if ok else "FAIL"
        print("  [{}] {} (stopped at step {}, done={})".format(
            status, task_id, i+1, obs.done
        ))
        if not ok:
            all_pass = False

    # Scenario 4: Wrong diagnose + finalize (negative rewards)
    print("\n--- SCENARIO 4: Wrong diagnose + finalize ---")
    for task_id in TASKS:
        ok = test_scenario(task_id, "wrong-diagnose", [
            {"action_type": "diagnose", "row_index": 0, "column_name": "fake_col", "issue_type": "format_error"},
            {"action_type": "diagnose", "row_index": 0, "column_name": "fake_col2", "issue_type": "format_error"},
            {"action_type": "finalize"},
        ])
        status = "PASS" if ok else "FAIL"
        print("  [{}] {}".format(status, task_id))
        if not ok:
            all_pass = False

    # Final result
    print("\n" + "=" * 70)
    if all_pass:
        print("  ALL REWARDS STRICTLY IN (0, 1) — READY TO SUBMIT")
    else:
        print("  SOME REWARDS OUT OF RANGE — FIX NEEDED")
    print("=" * 70)

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
