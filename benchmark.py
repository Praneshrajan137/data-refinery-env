#!/usr/bin/env python3
# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Multi-seed benchmark runner for the Data Quality environment.

Runs random and heuristic baselines across N procedural seeds, computing
mean +/- std for each (agent, task) pair.  Outputs results as Markdown
table (for README), LaTeX table (for papers), and raw JSON.

Usage::

    python benchmark.py                 # Default: 10 seeds
    python benchmark.py --seeds 50      # 50 seeds for tighter CIs
    python benchmark.py --json          # Raw JSON output only

Output files (written to ./benchmark_results/)::

    results.md      — Markdown table
    results.tex     — LaTeX table
    results.json    — Raw data
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from typing import Any

# Import the baseline runners
try:
    from random_baseline import run_episode as random_episode
    from heuristic_baseline import run_episode as heuristic_episode
except ImportError:
    from data_quality_env.random_baseline import run_episode as random_episode  # type: ignore
    from data_quality_env.heuristic_baseline import run_episode as heuristic_episode  # type: ignore


TASKS = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]

TASK_SHORT = {
    "task_1_format_fixer": "Task 1 (Format)",
    "task_2_duplicate_detective": "Task 2 (Duplicate)",
    "task_3_integrity_auditor": "Task 3 (Integrity)",
}

AGENTS = {
    "random": random_episode,
    "heuristic": heuristic_episode,
}

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "benchmark_results")


def stats(scores: list[float]) -> dict[str, float]:
    """Compute mean, std, min, max, median."""
    n = len(scores)
    if n == 0:
        return {"mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0, "median": 0.0, "n": 0}
    mean = sum(scores) / n
    var = sum((s - mean) ** 2 for s in scores) / max(n - 1, 1)
    std = math.sqrt(var)
    sorted_s = sorted(scores)
    median = sorted_s[n // 2] if n % 2 else (sorted_s[n // 2 - 1] + sorted_s[n // 2]) / 2
    return {
        "mean": round(mean, 4),
        "std": round(std, 4),
        "min": round(min(scores), 4),
        "max": round(max(scores), 4),
        "median": round(median, 4),
        "n": n,
    }


def run_benchmark(num_seeds: int) -> dict[str, Any]:
    """Run all agents across all tasks with num_seeds procedural seeds."""
    results: dict[str, dict[str, Any]] = {}

    for agent_name, run_fn in AGENTS.items():
        results[agent_name] = {}
        for task_id in TASKS:
            scores = []
            for seed in range(num_seeds):
                try:
                    if agent_name == "random":
                        r = run_fn(task_id, seed=seed, env_seed=seed)
                    else:
                        r = run_fn(task_id, env_seed=seed)
                    scores.append(r["score"])
                except Exception as exc:
                    print(f"  [ERROR] {agent_name}/{task_id}/seed={seed}: {exc}")
                    scores.append(0.0)

            results[agent_name][task_id] = {
                "scores": scores,
                **stats(scores),
            }

    return results


def format_markdown(results: dict[str, Any], num_seeds: int) -> str:
    """Generate Markdown results table."""
    lines = [
        f"### Benchmark Results (n={num_seeds} seeds per cell)",
        "",
        "| Agent | Task 1 (Format) | Task 2 (Duplicate) | Task 3 (Integrity) | Average |",
        "|-------|----------------:|-------------------:|-------------------:|--------:|",
    ]

    for agent_name in AGENTS:
        cells = []
        all_means = []
        for task_id in TASKS:
            s = results[agent_name][task_id]
            cells.append(f"{s['mean']:.3f} +/- {s['std']:.3f}")
            all_means.append(s["mean"])
        avg = sum(all_means) / len(all_means)
        lines.append(
            f"| {agent_name.capitalize():13s} | "
            + " | ".join(f"{c:>18s}" for c in cells)
            + f" | {avg:>7.3f} |"
        )

    lines.append("")
    return "\n".join(lines)


def format_latex(results: dict[str, Any], num_seeds: int) -> str:
    """Generate LaTeX results table."""
    lines = [
        r"\begin{table}[h]",
        r"\centering",
        f"\\caption{{Baseline scores (mean $\\pm$ std, $n={num_seeds}$ seeds)}}",
        r"\label{tab:baselines}",
        r"\begin{tabular}{lccc|c}",
        r"\toprule",
        r"Agent & Task 1 & Task 2 & Task 3 & Average \\",
        r"\midrule",
    ]

    for agent_name in AGENTS:
        cells = []
        all_means = []
        for task_id in TASKS:
            s = results[agent_name][task_id]
            cells.append(f"${s['mean']:.3f} \\pm {s['std']:.3f}$")
            all_means.append(s["mean"])
        avg = sum(all_means) / len(all_means)
        lines.append(
            f"{agent_name.capitalize()} & "
            + " & ".join(cells)
            + f" & ${avg:.3f}$ \\\\"
        )

    lines.extend([
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Multi-seed benchmark runner")
    parser.add_argument("--seeds", type=int, default=10, help="Seeds per (agent, task) cell")
    parser.add_argument("--json", action="store_true", help="Output only raw JSON")
    args = parser.parse_args()

    print(f"{'=' * 64}")
    print(f"  Data Quality Environment -- Benchmark Suite")
    print(f"  Seeds: {args.seeds} per (agent, task)")
    print(f"{'=' * 64}")

    start = time.time()
    results = run_benchmark(args.seeds)
    elapsed = time.time() - start

    # Write outputs
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # JSON
    json_path = os.path.join(OUTPUT_DIR, "results.json")
    # Strip raw scores for compact JSON
    compact = {}
    for agent, tasks in results.items():
        compact[agent] = {}
        for task_id, data in tasks.items():
            compact[agent][task_id] = {k: v for k, v in data.items() if k != "scores"}
    compact["_meta"] = {"seeds": args.seeds, "elapsed_s": round(elapsed, 1)}
    with open(json_path, "w") as f:
        json.dump(compact, f, indent=2)

    if args.json:
        print(json.dumps(compact, indent=2))
        return 0

    # Markdown
    md = format_markdown(results, args.seeds)
    md_path = os.path.join(OUTPUT_DIR, "results.md")
    with open(md_path, "w") as f:
        f.write(md)

    # LaTeX
    tex = format_latex(results, args.seeds)
    tex_path = os.path.join(OUTPUT_DIR, "results.tex")
    with open(tex_path, "w") as f:
        f.write(tex)

    # Console output
    print(f"\n{md}")
    print(f"  Elapsed: {elapsed:.1f}s")
    print(f"  Outputs: {OUTPUT_DIR}/")
    print(f"    results.json  results.md  results.tex")
    print(f"{'=' * 64}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
