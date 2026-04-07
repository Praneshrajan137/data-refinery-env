#!/usr/bin/env python3
"""Deterministic baseline scorer — no API key required.

Runs a ground-truth-aware agent that systematically inspects rows,
diagnoses known issues, and applies known fixes. This produces the
maximum achievable baseline score for each task.

Usage:
    python run_baseline.py                          # local Docker
    python run_baseline.py --url https://...hf.space  # remote HF Space
"""

from __future__ import annotations

import json
import sys
import os
import time

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# ── Import models and client ──────────────────────────────────────────────
try:
    from models import DataQualityAction, IssueType, FixType
    from client import DataQualityEnv
except ImportError:
    print("ERROR: Cannot import models/client. Run from project root.")
    sys.exit(2)

# ── Configuration ─────────────────────────────────────────────────────────
ENV_URL = sys.argv[2] if len(sys.argv) > 2 and sys.argv[1] == "--url" else os.environ.get("ENV_URL", "http://localhost:7860")

TASKS = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]

GROUND_TRUTH_FILES = {
    "task_1_format_fixer": "datasets/task1_ground_truth.json",
    "task_2_duplicate_detective": "datasets/task2_ground_truth.json",
    "task_3_integrity_auditor": "datasets/task3_ground_truth.json",
}


def load_ground_truth(task_id: str) -> list[dict]:
    """Load ground truth issues for a task."""
    gt_path = PROJECT_ROOT / GROUND_TRUTH_FILES[task_id]
    with open(gt_path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("issues", data)


def run_task(task_id: str) -> float:
    """Run a single task using ground truth knowledge."""
    issues = load_ground_truth(task_id)
    total_reward = 0.0

    print(f"\n  {task_id}")
    print(f"  Ground truth: {len(issues)} issues")

    try:
        env = DataQualityEnv(base_url=ENV_URL)
        ctx = env.sync() if hasattr(env, "sync") else env

        with ctx as e:
            result = e.reset(task_id=task_id)
            obs = getattr(result, "observation", result)

            # Step 1: Inspect all rows in batches
            total_rows = getattr(obs, "total_rows", 50)
            for start in range(0, total_rows, 10):
                indices = list(range(start, min(start + 10, total_rows)))
                action = DataQualityAction(
                    action_type="inspect",
                    row_indices=indices,
                )
                result = e.step(action)
                obs = getattr(result, "observation", result)
                if getattr(obs, "done", False):
                    break

            # Step 2: Diagnose each issue
            for issue in issues:
                if getattr(obs, "done", False):
                    break

                row = issue["row"]
                column = issue["column"]
                issue_type = issue["type"]

                # Diagnose
                action = DataQualityAction(
                    action_type="diagnose",
                    row_index=row,
                    column_name=column,
                    issue_type=issue_type,
                )
                result = e.step(action)
                obs = getattr(result, "observation", result)

                # Fix if fixable
                expected = issue.get("expected")
                if expected is not None and not getattr(obs, "done", False):
                    # Determine fix type
                    if issue_type == "duplicate":
                        fix_type = "delete_row"
                        new_value = None
                    else:
                        fix_type = "correct_value"
                        new_value = str(expected)

                    action = DataQualityAction(
                        action_type="fix",
                        row_index=row,
                        column_name=column,
                        fix_type=fix_type,
                        new_value=new_value,
                        justification=issue.get("description", "Ground truth fix"),
                    )
                    result = e.step(action)
                    obs = getattr(result, "observation", result)

            # Finalize
            if not getattr(obs, "done", False):
                action = DataQualityAction(action_type="finalize")
                result = e.step(action)
                obs = getattr(result, "observation", result)

            total_reward = float(getattr(obs, "cumulative_reward", 0.0))

    except Exception as exc:
        print(f"  ERROR: {exc}")
        import traceback
        traceback.print_exc()

    return total_reward


def main() -> int:
    """Run all tasks and print results."""
    print(f"\n{'=' * 56}")
    print(f"  BASELINE INFERENCE SCORES")
    print(f"  Server: {ENV_URL}")
    print(f"  Method: Ground-truth deterministic agent")
    print(f"{'=' * 56}")

    scores: dict[str, float] = {}
    start = time.time()

    for task_id in TASKS:
        scores[task_id] = run_task(task_id)

    elapsed = time.time() - start

    print(f"\n{'-' * 56}")
    print(f"  RESULTS")
    print(f"{'-' * 56}")

    for tid, score in scores.items():
        bar = "█" * int(score * 40) + "░" * (40 - int(score * 40))
        print(f"  {tid:30s}  {score:.4f}  {bar}")

    avg = sum(scores.values()) / len(scores) if scores else 0.0
    bar = "█" * int(avg * 40) + "░" * (40 - int(avg * 40))
    print(f"{'-' * 56}")
    print(f"  {'AVERAGE':30s}  {avg:.4f}  {bar}")
    print(f"  Elapsed: {elapsed:.1f}s")
    print(f"{'=' * 56}")

    # Machine-readable summary
    summary = {"scores": scores, "average": round(avg, 4), "elapsed": round(elapsed, 1)}
    print(f"\n[SUMMARY] {json.dumps(summary)}")

    return 0 if avg > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
