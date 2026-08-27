"""Refresh benchmark tables with Week 4 agents plus gated trained-model rows."""

from __future__ import annotations

import argparse
from pathlib import Path

from dataforge.bench.core import BenchmarkRunOutput, write_run_output
from dataforge.bench.report import write_benchmark_outputs
from dataforge.bench.runner import run_agent_comparison
from dataforge.datasets.registry import DATASET_REGISTRY

#: Derived from the registry, never hardcoded. A literal default cannot be de-registered,
#: so it becomes a crash rather than a corpus: `beers` was de-registered on 2026-07-12 and
#: `get_dataset_metadata("beers")` now raises KeyError, which this script's default
#: invocation walked straight into. Same defect scripts/data/audit_real_world_sources.py
#: records having shipped for six weeks.
DEFAULT_DATASETS = ",".join(sorted(DATASET_REGISTRY))


def load_grpo_release_evidence(path: Path) -> dict[str, object]:
    """Load GRPO release evidence before public trained-model benchmark refresh."""
    if not path.exists():
        raise FileNotFoundError(
            f"Missing GRPO release evidence: {path}. "
            "Run scripts/model/verify_grpo_release.py before refreshing README claims."
        )
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("GRPO release evidence must be a JSON object.")
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict) or metrics.get("acceptance_gate_passed") is not True:
        raise ValueError("GRPO release evidence does not show acceptance_gate_passed=true.")
    return payload


def load_trained_model_output(path: Path) -> BenchmarkRunOutput:
    """Load trained model benchmark rows, failing loudly if evidence is absent."""
    if not path.exists():
        raise FileNotFoundError(
            f"Missing trained model benchmark evidence: {path}. "
            "Run the GRPO eval gate before refreshing public benchmark tables."
        )
    return BenchmarkRunOutput.model_validate_json(path.read_text(encoding="utf-8"))


def merge_benchmark_outputs(
    *,
    agent_output: BenchmarkRunOutput,
    trained_output: BenchmarkRunOutput,
) -> BenchmarkRunOutput:
    """Return one benchmark output containing reproduced agents and trained models."""
    agent_methods = _metadata_list(agent_output.metadata.get("methods"))
    trained_methods = _metadata_list(trained_output.metadata.get("methods"))
    datasets = sorted(
        {
            str(dataset)
            for dataset in [
                *_metadata_list(agent_output.metadata.get("datasets")),
                *_metadata_list(trained_output.metadata.get("datasets")),
            ]
        }
    )
    metadata = {
        **agent_output.metadata,
        "methods": [*agent_methods, *trained_methods],
        "datasets": datasets,
        "trained_model_source": trained_output.metadata.get("reproduction_command", ""),
        "compute_cost_unit": "gpu_hours",
    }
    return BenchmarkRunOutput(
        metadata=metadata,
        records=[*agent_output.records, *trained_output.records],
        aggregates=[*agent_output.aggregates, *trained_output.aggregates],
    )


def _metadata_list(value: object) -> list[object]:
    """Return a metadata field as a list without trusting JSON shape."""
    return list(value) if isinstance(value, list) else []


def _parse_csv_list(raw_value: str) -> list[str]:
    """Parse a comma-separated CLI string into values."""
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--methods",
        default="random,heuristic,llm_zeroshot,llm_react",
        help="Week 4 agent methods to rerun.",
    )
    parser.add_argument("--datasets", default=DEFAULT_DATASETS)
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--really-run-big-bench", action="store_true")
    parser.add_argument(
        "--trained-model-json",
        type=Path,
        default=Path("eval/results/grpo_model_comparison.json"),
    )
    parser.add_argument(
        "--grpo-evidence-json",
        type=Path,
        default=Path("eval/results/grpo_release_evidence.json"),
    )
    parser.add_argument(
        "--agent-json",
        type=Path,
        default=Path("eval/results/agent_comparison_agents.json"),
    )
    parser.add_argument(
        "--merged-json",
        type=Path,
        default=Path("eval/results/agent_comparison.json"),
    )
    parser.add_argument("--sota-json", type=Path, default=Path("eval/results/sota_comparison.json"))
    parser.add_argument("--report-path", type=Path, default=Path("BENCHMARK_REPORT.md"))
    parser.add_argument("--readme-path", type=Path, default=Path("README.md"))
    parser.add_argument(
        "--skip-agent-run",
        action="store_true",
        help="Use --agent-json as an existing agent output instead of rerunning agents.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run or load benchmarks, merge trained rows, and refresh markdown outputs."""
    args = _build_parser().parse_args(argv)
    if args.skip_agent_run:
        if not args.agent_json.exists():
            raise FileNotFoundError(f"Missing agent benchmark evidence: {args.agent_json}")
        agent_output = BenchmarkRunOutput.model_validate_json(
            args.agent_json.read_text(encoding="utf-8")
        )
    else:
        agent_output = run_agent_comparison(
            methods=_parse_csv_list(args.methods),
            datasets=_parse_csv_list(args.datasets),
            seeds=args.seeds,
            output_json=args.agent_json,
            really_run_big_bench=args.really_run_big_bench,
        )
    load_grpo_release_evidence(args.grpo_evidence_json)
    trained_output = load_trained_model_output(args.trained_model_json)
    merged = merge_benchmark_outputs(agent_output=agent_output, trained_output=trained_output)
    write_run_output(merged, args.merged_json)
    write_benchmark_outputs(
        agent_json_path=args.merged_json,
        sota_json_path=args.sota_json,
        report_path=args.report_path,
        readme_path=args.readme_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
