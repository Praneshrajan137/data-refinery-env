"""Thin wrapper for running DataForge agent comparisons on real-world datasets."""

from __future__ import annotations

import argparse
from pathlib import Path

from dataforge.bench.runner import run_agent_comparison


def _parse_csv_list(raw_value: str) -> list[str]:
    """Parse a comma-separated command-line string into a list."""
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def main() -> int:
    """Run the selected benchmark methods and write JSON output."""
    parser = argparse.ArgumentParser(description="Run DataForge agent comparisons.")
    parser.add_argument("--methods", default="heuristic,llm_zeroshot")
    parser.add_argument("--datasets", default="hospital")
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--really-run-big-bench", action="store_true")
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("eval/results/agent_comparison.json"),
    )
    args = parser.parse_args()

    run_agent_comparison(
        methods=_parse_csv_list(args.methods),
        datasets=_parse_csv_list(args.datasets),
        seeds=args.seeds,
        output_json=args.output_json,
        really_run_big_bench=args.really_run_big_bench,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
