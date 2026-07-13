"""Thin wrapper for running DataForge agent comparisons on real-world datasets."""

from __future__ import annotations

import argparse
from pathlib import Path

from dataforge.bench.runner import run_agent_comparison


def _parse_csv_list(raw_value: str) -> list[str]:
    """Parse a comma-separated command-line string into a list."""
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def _parse_seed_list(raw_value: str | None) -> list[int] | None:
    """Parse an optional comma-separated seed list."""
    if raw_value is None:
        return None
    return [int(value.strip()) for value in raw_value.split(",") if value.strip()]


def main() -> int:
    """Run the selected benchmark methods and write JSON output."""
    # Load .env at the script entry point (the library runner is hermetic).
    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser(description="Run DataForge agent comparisons.")
    parser.add_argument("--methods", default="heuristic,llm_zeroshot")
    parser.add_argument("--datasets", default="hospital")
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--seed-list", default=None)
    parser.add_argument("--really-run-big-bench", action="store_true")
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("eval/results/agent_comparison.json"),
    )
    args = parser.parse_args()
    methods = _parse_csv_list(args.methods)
    datasets = _parse_csv_list(args.datasets)
    seed_list = _parse_seed_list(args.seed_list)
    reproduction_command = (
        "python scripts\\bench\\run_agent_comparison.py "
        f"--methods {','.join(methods)} "
        f"--datasets {','.join(datasets)} "
        f"--seeds {len(seed_list) if seed_list is not None else args.seeds} "
    )
    if seed_list is not None:
        reproduction_command += f"--seed-list {','.join(str(seed) for seed in seed_list)} "
    reproduction_command += f"--output-json {args.output_json}"

    run_agent_comparison(
        methods=methods,
        datasets=datasets,
        seeds=args.seeds,
        seed_list=seed_list,
        output_json=args.output_json,
        really_run_big_bench=args.really_run_big_bench,
        reproduction_command=reproduction_command,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
