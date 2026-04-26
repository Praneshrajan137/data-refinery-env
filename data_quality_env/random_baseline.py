#!/usr/bin/env python3
# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Random-action baseline agent for the Data Quality environment.

Takes uniformly random valid actions at each step.  Expected score: ~0.0.
Useful as a lower bound in benchmarking — any non-trivial agent should
significantly outperform this.

Usage::

    python random_baseline.py                   # Single run, all tasks
    python random_baseline.py --seeds 10        # 10 seeds per task
    python random_baseline.py --task task_1_format_fixer
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from typing import Any

from .models import DataQualityAction, IssueType
from .server.data_quality_environment import DataQualityEnvironment


TASKS = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]

ISSUE_TYPES = [e.value for e in IssueType]


def random_action(obs: Any, env: DataQualityEnvironment, rng: random.Random) -> DataQualityAction:
    """Generate a uniformly random valid action."""
    total_rows = obs.total_rows or 50
    schema_cols = list(obs.schema_info.keys()) if obs.schema_info else ["id"]

    # 40% inspect, 30% diagnose, 20% fix, 10% finalize
    roll = rng.random()

    if roll < 0.40:
        n = rng.randint(1, min(10, total_rows))
        indices = rng.sample(range(total_rows), n)
        return DataQualityAction(action_type="inspect", row_indices=indices)

    elif roll < 0.70:
        return DataQualityAction(
            action_type="diagnose",
            row_index=rng.randint(0, total_rows - 1),
            column_name=rng.choice(schema_cols),
            issue_type=rng.choice(ISSUE_TYPES),
        )

    elif roll < 0.90:
        col = rng.choice(schema_cols)
        return DataQualityAction(
            action_type="fix",
            row_index=rng.randint(0, total_rows - 1),
            column_name=col,
            fix_type="correct_value",
            new_value=str(rng.randint(0, 100)),
            justification="random fix",
        )

    else:
        return DataQualityAction(action_type="finalize")


def run_episode(
    task_id: str,
    seed: int | None = None,
    env_seed: int | None = None,
) -> dict[str, Any]:
    """Run one episode with random actions.  Returns result dict."""
    rng = random.Random(seed)
    env = DataQualityEnvironment()
    obs = env.reset(task_id=task_id, seed=env_seed)

    steps = 0
    while not obs.done:
        action = random_action(obs, env, rng)
        obs = env.step(action)
        steps += 1

    return {
        "task_id": task_id,
        "score": round(float(obs.cumulative_reward), 4),
        "steps": steps,
        "seed": seed,
        "env_seed": env_seed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Random baseline agent")
    parser.add_argument("--seeds", type=int, default=1, help="Number of seeds to run per task")
    parser.add_argument("--task", type=str, default=None, help="Run only this task")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    tasks = [args.task] if args.task else TASKS
    all_results: list[dict] = []
    start = time.time()

    for task_id in tasks:
        for s in range(args.seeds):
            result = run_episode(task_id, seed=s, env_seed=s)
            all_results.append(result)
            if not args.json:
                print(
                    f"  {task_id:35s} seed={s:3d}  score={result['score']:.4f}  steps={result['steps']}"
                )

    elapsed = time.time() - start

    if args.json:
        print(json.dumps(all_results, indent=2))
    else:
        # Summary per task
        print(f"\n{'=' * 60}")
        for task_id in tasks:
            task_results = [r for r in all_results if r["task_id"] == task_id]
            scores = [r["score"] for r in task_results]
            mean = sum(scores) / len(scores) if scores else 0
            print(f"  {task_id:35s}  mean={mean:.4f}  n={len(scores)}")
        print(f"  Elapsed: {elapsed:.1f}s")
        print(f"{'=' * 60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
