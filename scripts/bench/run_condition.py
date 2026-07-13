"""Bounded, cost-estimating run harness for the Phase B teacher/benchmark runs.

Prior live runs were ad-hoc and got interrupted at turn boundaries. This harness
makes each condition a single bounded batch with an explicit USD estimate printed
*before* anything billable happens, and a hard guard that refuses to run when the
estimate exceeds the cap. In Phase A it is dry-run only (no keys, no spend): it
prints the plan and exits. Live execution is a Phase B concern.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RunCondition:
    """One bounded benchmark/teacher condition to estimate and (later) run."""

    label: str
    n_issues: int
    samples: int = 3
    avg_input_tokens: int = 1200
    avg_output_tokens: int = 120


def estimate_condition(
    condition: RunCondition,
    *,
    usd_per_1k_input: float,
    usd_per_1k_output: float,
) -> dict[str, float | int | str]:
    """Estimate calls, tokens, and USD for one condition (conservative)."""
    calls = condition.n_issues * condition.samples
    input_tokens = calls * condition.avg_input_tokens
    output_tokens = calls * condition.avg_output_tokens
    usd = (input_tokens / 1000.0) * usd_per_1k_input + (output_tokens / 1000.0) * usd_per_1k_output
    return {
        "label": condition.label,
        "calls": calls,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "estimated_usd": round(usd, 4),
    }


def plan_run(
    conditions: Sequence[RunCondition],
    *,
    usd_per_1k_input: float,
    usd_per_1k_output: float,
    max_usd: float,
) -> dict[str, object]:
    """Return the full estimate and whether it fits under the USD guard."""
    per_condition = [
        estimate_condition(
            condition,
            usd_per_1k_input=usd_per_1k_input,
            usd_per_1k_output=usd_per_1k_output,
        )
        for condition in conditions
    ]
    total = round(sum(float(item["estimated_usd"]) for item in per_condition), 4)
    return {
        "conditions": per_condition,
        "total_estimated_usd": total,
        "max_usd": max_usd,
        "within_guard": total <= max_usd,
    }


def _parse_condition(spec: str) -> RunCondition:
    """Parse ``label:n_issues[:samples]`` into a ``RunCondition``."""
    parts = spec.split(":")
    if len(parts) < 2:
        raise ValueError(f"condition must be label:n_issues[:samples], got {spec!r}")
    label = parts[0]
    n_issues = int(parts[1])
    samples = int(parts[2]) if len(parts) > 2 else 3
    return RunCondition(label=label, n_issues=n_issues, samples=samples)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--condition",
        action="append",
        default=[],
        help="label:n_issues[:samples]; repeatable",
    )
    parser.add_argument("--usd-per-1k-input", type=float, default=0.003)
    parser.add_argument("--usd-per-1k-output", type=float, default=0.015)
    parser.add_argument("--max-usd", type=float, default=15.0)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Phase B only: live execution requires provider keys and is not wired in Phase A.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    conditions = [_parse_condition(spec) for spec in args.condition]
    plan = plan_run(
        conditions,
        usd_per_1k_input=args.usd_per_1k_input,
        usd_per_1k_output=args.usd_per_1k_output,
        max_usd=args.max_usd,
    )
    print(json.dumps(plan, indent=2, sort_keys=True))
    if not plan["within_guard"]:
        print(f"REFUSING TO RUN: estimate exceeds ${args.max_usd} guard.")
        return 1
    if args.execute:
        print("Live execution is a Phase B step (needs provider keys); not run in Phase A.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
