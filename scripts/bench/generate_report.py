"""Generate BENCHMARK_REPORT.md and update the README benchmark block."""

from __future__ import annotations

import argparse
from pathlib import Path

from dataforge.bench.report import write_benchmark_outputs


def main() -> int:
    """Generate the benchmark report and update the README benchmark block."""
    parser = argparse.ArgumentParser(description="Generate benchmark markdown outputs.")
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
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path("BENCHMARK_REPORT.md"),
    )
    parser.add_argument(
        "--readme-path",
        type=Path,
        default=Path("README.md"),
    )
    args = parser.parse_args()

    write_benchmark_outputs(
        agent_json_path=args.agent_json,
        sota_json_path=args.sota_json,
        report_path=args.report_path,
        readme_path=args.readme_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
