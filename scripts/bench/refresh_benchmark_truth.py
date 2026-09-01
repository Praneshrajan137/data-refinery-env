"""Regenerate benchmark truth artifacts from pinned data and fixed seeds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from run_sota_comparison import build_sota_payload

from dataforge.bench.report import write_benchmark_outputs
from dataforge.bench.runner import run_agent_comparison


def _parse_csv_list(raw_value: str) -> list[str]:
    """Parse a comma-separated command-line string into a list."""
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def _parse_seed_list(raw_value: str) -> list[int]:
    """Parse an explicit comma-separated seed list."""
    return [int(value.strip()) for value in raw_value.split(",") if value.strip()]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--methods", default="random,heuristic")
    # `beers` was removed from the dataset registry per the DATASET SCOPE RULE, and this
    # default was not updated with it, so this script raised
    # `ValueError: Unknown benchmark datasets: ['beers']` and could not run at all.
    # That matters more than a stale literal usually would: `scripts/ci/benchmark_truth.py`
    # names THIS script in its failure message ("run scripts/bench/refresh_benchmark_truth.py
    # and commit outputs"), so the documented remedy for a stale benchmark report was itself
    # broken. Found 2026-09-01 by following that instruction.
    parser.add_argument("--datasets", default="hospital,flights")
    parser.add_argument("--seed-list", default="0,1,2")
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=Path(".benchmarks") / "truth-cache",
        help="Benchmark dataset cache root. Defaults to an ignored clean project cache.",
    )
    parser.add_argument(
        "--agent-json",
        type=Path,
        default=Path("eval/results/agent_comparison.json"),
    )
    parser.add_argument(
        "--sota-json",
        type=Path,
        default=Path("eval/results/sota_comparison.json"),
    )
    parser.add_argument("--report", type=Path, default=Path("BENCHMARK_REPORT.md"))
    parser.add_argument("--readme", type=Path, default=Path("README.md"))
    parser.add_argument("--homepage", type=Path, default=Path("docs/docs/index.md"))
    parser.add_argument("--really-run-big-bench", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run local benchmarks and regenerate every public benchmark surface."""
    args = _build_parser().parse_args(argv)
    methods = _parse_csv_list(args.methods)
    datasets = _parse_csv_list(args.datasets)
    seed_list = _parse_seed_list(args.seed_list)

    run_agent_comparison(
        methods=methods,
        datasets=datasets,
        seeds=len(seed_list),
        seed_list=seed_list,
        output_json=args.agent_json,
        really_run_big_bench=args.really_run_big_bench,
        cache_root=args.cache_root,
    )
    sota_payload = build_sota_payload()
    args.sota_json.parent.mkdir(parents=True, exist_ok=True)
    args.sota_json.write_text(json.dumps(sota_payload, indent=2), encoding="utf-8")
    write_benchmark_outputs(
        agent_json_path=args.agent_json,
        sota_json_path=args.sota_json,
        report_path=args.report,
        readme_path=args.readme,
        homepage_path=args.homepage,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
