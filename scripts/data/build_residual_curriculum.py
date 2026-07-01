"""Build a residual-focused SFT curriculum for the verified agent.

The verified agent only needs the LLM where the deterministic floor gives up.
This generator targets training data at exactly that surface: it runs the
deterministic floor on a dirty/clean dataset pair, finds the *residual* cells
(detected but not auto-repaired), and emits one SFT trajectory per residual cell
whose clean-oracle value is known. Each trajectory is a single
``observation -> correct FIX`` turn in the verified agent's own chat format, so a
fine-tuned model learns to add value precisely where the rules cannot.

This is the parallel, non-blocking model-improvement track. It changes no
defaults: the verified agent already bounds the downside via the deterministic
floor and the SMT + constitution gates. A model trained on this curriculum may
only become the default policy after passing
``dataforge.bench.agent_promotion_verdict``.

Usage::

    python -m scripts.data.build_residual_curriculum \\
        --dirty dataforge/datasets/embedded/hospital/dirty.csv \\
        --clean dataforge/datasets/embedded/hospital/clean.csv \\
        --dataset hospital \\
        --out data/sft_traj/residual_curriculum.jsonl
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dataforge.agent.policy import (
    AgentObservation,
    ResidualIssue,
    _format_observation,
    build_system_prompt,
)
from dataforge.cli.common import load_schema
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema
from dataforge.engine.repair import propose_repairs
from dataforge.repairers.base import RepairAttempt
from dataforge.table import (
    cell_value,
    column_names,
    copy_table,
    read_csv,
    row_count,
    set_cell_value,
)


def _residual_issues(issues: list[Issue], groups: list[list[RepairAttempt]]) -> list[Issue]:
    """Return issues the deterministic floor did not accept a fix for."""
    residual: list[Issue] = []
    for issue, attempts in zip(issues, groups, strict=False):
        if not attempts or attempts[-1].status != "accepted":
            residual.append(issue)
    return residual


def _sample_rows(df: object, row: int, columns: list[str]) -> tuple[dict[str, str], ...]:
    """Window of rows around a focus row for the observation."""
    total = row_count(df)  # type: ignore[arg-type]
    start, end = max(0, row - 1), min(total, row + 2)
    return tuple(
        {c: cell_value(df, i, c) for c in columns}  # type: ignore[arg-type]
        for i in range(start, end)
    )


def build_records(
    dirty_path: Path,
    clean_path: Path,
    dataset: str,
    schema: Schema | None,
) -> list[dict[str, Any]]:
    """Build residual SFT trajectories from a dirty/clean dataset pair."""
    dirty = read_csv(dirty_path)
    clean = read_csv(clean_path)
    columns = column_names(dirty)
    clean_columns = set(column_names(clean))

    issues = run_all_detectors(dirty, schema)
    floor_fixes, groups = propose_repairs(
        issues,
        dirty_path,
        copy_table(dirty),
        schema,
        allow_llm=False,
        model="gemini-2.0-flash",
        allow_pii=False,
        confirm_pii=False,
        confirm_escalations=False,
        interactive=False,
    )

    working = copy_table(dirty)
    for fix in floor_fixes:
        set_cell_value(working, fix.fix.row, fix.fix.column, fix.fix.new_value)

    system = build_system_prompt()
    records: list[dict[str, Any]] = []
    for issue in _residual_issues(issues, groups):
        if issue.column not in clean_columns or issue.row >= row_count(clean):
            continue
        oracle = cell_value(clean, issue.row, issue.column)
        current = cell_value(working, issue.row, issue.column)
        if oracle == "" or oracle == current:
            continue  # detection-only or nothing to teach

        observation = AgentObservation(
            columns=tuple(columns),
            row_count=row_count(dirty),
            residual_issues=(
                ResidualIssue(
                    row=issue.row,
                    column=issue.column,
                    issue_type=issue.issue_type,
                    severity=issue.severity.value,
                    expected=issue.expected,
                    actual=issue.actual,
                    reason=issue.reason,
                ),
            ),
            sample_rows=_sample_rows(dirty, issue.row, columns),
            scratchpad_summary="Hypotheses: 0 (0 pending). Confirmed: 0. Dead ends: 0.",
            last_result="",
            steps_taken=0,
            max_steps=30,
            staged_fix_count=len(floor_fixes),
        )
        assistant = {
            "action_type": "FIX",
            "row": issue.row,
            "column": issue.column,
            "new_value": oracle,
            "justification": (
                f"Clean-oracle value for {issue.issue_type} at row {issue.row}, "
                f"column {issue.column!r}."
            ),
        }
        records.append(
            {
                "dataset": dataset,
                "row": issue.row,
                "column": issue.column,
                "issue_type": issue.issue_type,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": _format_observation(observation)},
                    {"role": "assistant", "content": json.dumps(assistant)},
                ],
            }
        )
    return records


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the residual curriculum generator."""
    parser = argparse.ArgumentParser(description="Build a residual-focused SFT curriculum.")
    parser.add_argument("--dirty", type=Path, required=True, help="Dirty CSV path.")
    parser.add_argument("--clean", type=Path, required=True, help="Clean oracle CSV path.")
    parser.add_argument("--dataset", required=True, help="Dataset name label.")
    parser.add_argument("--schema", type=Path, default=None, help="Optional schema YAML.")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/sft_traj/residual_curriculum.jsonl"),
        help="Output JSONL path.",
    )
    args = parser.parse_args(argv)

    schema = load_schema(args.schema) if args.schema else None
    records = build_records(args.dirty, args.clean, args.dataset, schema)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    print(f"Wrote {len(records)} residual trajectory record(s) to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
