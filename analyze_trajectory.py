#!/usr/bin/env python3
"""Trajectory analysis tool for the Data Quality RL environment.

Replays a recorded trajectory and produces detailed analytics:
- Per-step reward breakdown
- Action-type distribution
- Issue detection timeline
- Wasted-step analysis
- Grader diagnostics summary

Usage::

    # Analyze from a JSON trajectory file
    python analyze_trajectory.py trajectory.json

    # Run a sample episode and analyze
    python analyze_trajectory.py --demo task_1_format_fixer

    # Analyze with verbose per-step output
    python analyze_trajectory.py trajectory.json --verbose
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Ensure package imports work
sys.path.insert(0, str(Path(__file__).parent))

from models import DataQualityAction, IssueType, FixType
from server.data_quality_environment import DataQualityEnvironment


def replay_trajectory(
    task_id: str,
    actions: list[dict[str, Any]],
    seed: int | None = None,
    noisy: bool = False,
) -> dict[str, Any]:
    """Replay a list of actions and collect analytics.

    Args:
        task_id: Task to run.
        actions: List of action dicts (as they would be sent to the env).
        seed: Optional seed for procedural generation.
        noisy: Whether to enable stochastic observation mode.

    Returns:
        Analytics dict with per-step data, summary, and grader diagnostics.
    """
    env = DataQualityEnvironment()
    obs = env.reset(task_id=task_id, seed=seed, noisy=noisy)

    steps: list[dict[str, Any]] = []
    action_counts: dict[str, int] = {}
    cumulative = 0.0
    wasted_steps = 0

    for i, action_dict in enumerate(actions):
        action = DataQualityAction(**action_dict)
        obs = env.step(action)

        action_type = action.action_type
        action_counts[action_type] = action_counts.get(action_type, 0) + 1

        delta = obs.reward_delta
        cumulative = obs.cumulative_reward
        result = obs.action_result.value if hasattr(obs.action_result, "value") else str(obs.action_result)

        is_wasted = delta <= 0 and action_type != "finalize"
        if is_wasted:
            wasted_steps += 1

        steps.append({
            "step": i + 1,
            "action_type": action_type,
            "action_result": result,
            "reward_delta": round(delta, 4),
            "cumulative_reward": round(cumulative, 4),
            "issues_found": obs.issues_found,
            "wasted": is_wasted,
        })

        if obs.done:
            break

    diagnostics = None
    if obs.grader_diagnostics is not None:
        diagnostics = obs.grader_diagnostics
    elif hasattr(obs, "grader_diagnostics"):
        diagnostics = obs.grader_diagnostics

    return {
        "task_id": task_id,
        "total_steps": len(steps),
        "final_score": round(obs.cumulative_reward, 4),
        "action_distribution": action_counts,
        "wasted_steps": wasted_steps,
        "efficiency": round(1 - wasted_steps / max(len(steps), 1), 4),
        "grader_diagnostics": diagnostics,
        "steps": steps,
    }


def run_demo(task_id: str) -> dict[str, Any]:
    """Run a simple heuristic demo and return analytics."""
    env = DataQualityEnvironment()
    obs = env.reset(task_id=task_id)

    actions: list[dict[str, Any]] = []

    # Strategy: inspect rows in batches, diagnose ground truth issues, fix fixable ones
    gt = env.ground_truth
    total_rows = obs.total_rows

    # Phase 1: Inspect all rows in batches of 10
    for start in range(0, min(total_rows, 100), 10):
        actions.append({
            "action_type": "inspect",
            "row_indices": list(range(start, min(start + 10, total_rows))),
        })

    # Phase 2: Diagnose each ground truth issue
    for issue in gt:
        col = issue["column"]
        if col == "_row":
            col = "id" if "id" in obs.schema_info else list(obs.schema_info.keys())[0]
        actions.append({
            "action_type": "diagnose",
            "row_index": issue["row"],
            "column_name": col,
            "issue_type": issue.get("type", "format_error"),
        })

    # Phase 3: Fix fixable issues
    for issue in gt:
        if "expected" not in issue:
            continue
        col = issue["column"]
        if col == "_row":
            col = "id" if "id" in obs.schema_info else list(obs.schema_info.keys())[0]

        fix_action: dict[str, Any] = {
            "action_type": "fix",
            "row_index": issue["row"],
            "column_name": col,
        }
        if issue["expected"] == "DELETE_ROW":
            fix_action["fix_type"] = "delete_row"
            fix_action["justification"] = "Duplicate row removal"
        else:
            fix_action["fix_type"] = "correct_value"
            fix_action["new_value"] = issue["expected"]
            fix_action["justification"] = "Ground truth fix"

        actions.append(fix_action)

    # Phase 4: Finalize
    actions.append({"action_type": "finalize"})

    return replay_trajectory(task_id, actions)


def print_report(analytics: dict[str, Any], verbose: bool = False) -> None:
    """Print a human-readable analysis report."""
    SEP = "=" * 64

    print(f"\n{SEP}")
    print(f"  Trajectory Analysis: {analytics['task_id']}")
    print(SEP)

    print(f"\n  Final Score:       {analytics['final_score']:.4f}")
    print(f"  Total Steps:       {analytics['total_steps']}")
    print(f"  Wasted Steps:      {analytics['wasted_steps']}")
    print(f"  Efficiency:        {analytics['efficiency']:.1%}")
    print(f"\n  Action Distribution:")
    for action, count in sorted(analytics["action_distribution"].items()):
        print(f"    {action:12s}  {count}")

    diag = analytics.get("grader_diagnostics")
    if diag:
        print(f"\n  Grader Diagnostics:")
        formula = diag.get("formula", {})
        counts = diag.get("counts", {})
        print(f"    Detection Rate:  {formula.get('detection_rate', 0):.4f} (× {formula.get('detection_weight', 0.4)})")
        print(f"    Fix Rate:        {formula.get('fix_rate', 0):.4f} (× {formula.get('fix_weight', 0.6)})")
        print(f"    FP Penalty:      {formula.get('fp_penalty_total', 0):.4f} ({counts.get('false_positives', 0)} FPs)")
        print(f"    Raw Score:       {formula.get('raw_score', 0):.4f}")

        per_issue = diag.get("per_issue", [])
        if per_issue:
            missed = [p for p in per_issue if not p["detected"]]
            unfixed = [p for p in per_issue if p["fixable"] and not p.get("fixed")]
            if missed:
                print(f"\n  Missed Issues ({len(missed)}):")
                for m in missed:
                    print(f"    row={m['row']:3d} col={m['column']:20s} type={m['type']}")
            if unfixed:
                print(f"\n  Unfixed Issues ({len(unfixed)}):")
                for u in unfixed:
                    print(f"    row={u['row']:3d} col={u['column']:20s} type={u['type']}")

    if verbose:
        print(f"\n  Per-Step Detail:")
        print(f"  {'Step':>4s}  {'Action':12s}  {'Result':16s}  {'Delta':>8s}  {'Cumul':>8s}  {'Found':>5s}  {'W':>1s}")
        print(f"  {'-'*4}  {'-'*12}  {'-'*16}  {'-'*8}  {'-'*8}  {'-'*5}  {'-'*1}")
        for s in analytics["steps"]:
            w = "*" if s["wasted"] else ""
            print(
                f"  {s['step']:4d}  {s['action_type']:12s}  {s['action_result']:16s}  "
                f"{s['reward_delta']:+8.4f}  {s['cumulative_reward']:8.4f}  "
                f"{s['issues_found']:5d}  {w}"
            )

    print(f"\n{SEP}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Data Quality RL trajectories")
    parser.add_argument("trajectory", nargs="?", help="Path to trajectory JSON file")
    parser.add_argument("--demo", metavar="TASK_ID", help="Run a demo episode for the given task")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show per-step detail")
    args = parser.parse_args()

    if args.demo:
        analytics = run_demo(args.demo)
        print_report(analytics, verbose=args.verbose)
    elif args.trajectory:
        path = Path(args.trajectory)
        if not path.exists():
            print(f"Error: {path} not found", file=sys.stderr)
            sys.exit(1)
        data = json.loads(path.read_text())
        task_id = data.get("task_id", "task_1_format_fixer")
        actions = data.get("actions", [])
        seed = data.get("seed")
        analytics = replay_trajectory(task_id, actions, seed=seed)
        print_report(analytics, verbose=args.verbose)
    else:
        # Default: run demo on all tasks
        for task in ["task_1_format_fixer", "task_2_duplicate_detective", "task_3_integrity_auditor"]:
            analytics = run_demo(task)
            print_report(analytics, verbose=args.verbose)


if __name__ == "__main__":
    main()
