"""Top-level benchmark orchestration for agent comparison runs."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from dataforge.bench.core import (
    AggregateBenchmarkResult,
    BenchmarkRunOutput,
    SeedBenchmarkResult,
    aggregate_seed_results,
    estimate_llm_calls,
    validate_estimated_calls,
    write_run_output,
)
from dataforge.bench.groq_client import GroqBenchClient
from dataforge.bench.methods import (
    run_heuristic_episode,
    run_llm_react_episode,
    run_llm_zeroshot_episode,
    run_random_episode,
)
from dataforge.datasets.real_world import load_real_world_dataset
from dataforge.datasets.registry import DATASET_REGISTRY

_SUPPORTED_METHODS = frozenset({"random", "heuristic", "llm_zeroshot", "llm_react"})


def _validate_inputs(methods: list[str], datasets: list[str], seeds: int) -> None:
    """Validate user-selected methods and datasets."""
    unknown_methods = sorted(set(methods) - _SUPPORTED_METHODS)
    unknown_datasets = sorted(set(datasets) - set(DATASET_REGISTRY))
    if unknown_methods:
        raise ValueError(f"Unknown benchmark methods: {unknown_methods}")
    if unknown_datasets:
        raise ValueError(f"Unknown benchmark datasets: {unknown_datasets}")
    if seeds <= 0:
        raise ValueError("Benchmark seeds must be >= 1.")


def _reproduction_command(methods: list[str], datasets: list[str], seeds: int) -> str:
    """Build the canonical command for reproducing a benchmark run."""
    return (
        "dataforge bench "
        f"--methods {','.join(methods)} "
        f"--datasets {','.join(datasets)} "
        f"--seeds {seeds}"
    )


def _llm_skip_reason() -> str | None:
    """Return a skip reason when LLM methods cannot run."""
    provider = os.environ.get("DATAFORGE_LLM_PROVIDER", "").strip().lower()
    if provider != "groq":
        return "DATAFORGE_LLM_PROVIDER must be set to groq."
    if not os.environ.get("GROQ_API_KEY"):
        return "GROQ_API_KEY is not set."
    return None


def _skipped_result(
    *,
    method: str,
    dataset: str,
    seed: int,
    reason: str,
    reproduction_command: str,
) -> SeedBenchmarkResult:
    """Build a skipped seed result with a clear reason."""
    return SeedBenchmarkResult(
        method=method,
        dataset=dataset,
        seed=seed,
        status="skipped",
        skip_reason=reason,
        llm_calls=0,
        prompt_tokens=0,
        completion_tokens=0,
        quota_units=0.0,
        runtime_s=0.0,
        provider=None,
        model=None,
        warnings=["provider_unset"],
        reproduction_command=reproduction_command,
    )


def run_agent_comparison(
    *,
    methods: list[str],
    datasets: list[str],
    seeds: int,
    output_json: Path,
    really_run_big_bench: bool,
    cache_root: Path | None = None,
) -> BenchmarkRunOutput:
    """Run the selected benchmark methods across real-world datasets."""
    load_dotenv()
    _validate_inputs(methods, datasets, seeds)

    estimated_calls = estimate_llm_calls(methods=methods, datasets=datasets, seeds=seeds)
    validate_estimated_calls(
        estimated_calls=estimated_calls,
        really_run_big_bench=really_run_big_bench,
    )

    reproduction_command = _reproduction_command(methods, datasets, seeds)
    records: list[SeedBenchmarkResult] = []
    loaded_datasets = {
        dataset_name: load_real_world_dataset(dataset_name, cache_root=cache_root)
        for dataset_name in datasets
    }

    llm_methods_requested = any(method.startswith("llm_") for method in methods)
    skip_reason = _llm_skip_reason() if llm_methods_requested else None
    client = (
        GroqBenchClient(api_key=os.environ["GROQ_API_KEY"])
        if llm_methods_requested and skip_reason is None
        else None
    )

    for dataset_name in datasets:
        dataset = loaded_datasets[dataset_name]
        for method in methods:
            for seed in range(seeds):
                if method == "random":
                    result = run_random_episode(dataset, seed=seed)
                elif method == "heuristic":
                    result = run_heuristic_episode(dataset, seed=seed)
                elif method == "llm_zeroshot":
                    if client is None or skip_reason is not None:
                        result = _skipped_result(
                            method=method,
                            dataset=dataset_name,
                            seed=seed,
                            reason=skip_reason or "LLM client unavailable.",
                            reproduction_command=reproduction_command,
                        )
                    else:
                        result = run_llm_zeroshot_episode(dataset, seed=seed, client=client)
                else:
                    if client is None or skip_reason is not None:
                        result = _skipped_result(
                            method=method,
                            dataset=dataset_name,
                            seed=seed,
                            reason=skip_reason or "LLM client unavailable.",
                            reproduction_command=reproduction_command,
                        )
                    else:
                        result = run_llm_react_episode(dataset, seed=seed, client=client)
                if result.reproduction_command != reproduction_command:
                    result = result.model_copy(
                        update={"reproduction_command": reproduction_command}
                    )
                if method == "heuristic":
                    result = result.model_copy(update={"seed": seed})
                records.append(result)

    aggregates: list[AggregateBenchmarkResult] = aggregate_seed_results(
        records, seeds_requested=seeds
    )
    output = BenchmarkRunOutput(
        metadata={
            "methods": methods,
            "datasets": datasets,
            "seeds": seeds,
            "reproduction_command": reproduction_command,
        },
        records=records,
        aggregates=aggregates,
    )
    write_run_output(output, output_json)
    return output
