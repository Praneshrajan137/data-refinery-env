"""Top-level benchmark orchestration for agent comparison runs."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dataforge.bench.core import (
    AggregateBenchmarkResult,
    BenchmarkRunOutput,
    SeedBenchmarkResult,
    aggregate_seed_results,
    build_benchmark_metadata,
    build_seed_list,
    dataset_evidence_from_loaded,
    estimate_llm_calls,
    validate_estimated_calls,
    write_run_output,
)
from dataforge.bench.groq_client import (
    AzureBenchClient,
    BedrockBenchClient,
    GeminiBenchClient,
    GrokBenchClient,
    GroqBenchClient,
)
from dataforge.bench.methods import (
    run_heuristic_episode,
    run_llm_corrector_episode,
    run_llm_react_episode,
    run_llm_zeroshot_episode,
    run_random_episode,
)
from dataforge.datasets.real_world import load_real_world_dataset
from dataforge.datasets.registry import DATASET_REGISTRY

_SUPPORTED_METHODS = frozenset(
    {"random", "heuristic", "llm_zeroshot", "llm_react", "llm_corrector"}
)


def _validate_inputs(methods: list[str], datasets: list[str]) -> None:
    """Validate user-selected methods and datasets."""
    unknown_methods = sorted(set(methods) - _SUPPORTED_METHODS)
    unknown_datasets = sorted(set(datasets) - set(DATASET_REGISTRY))
    if unknown_methods:
        raise ValueError(f"Unknown benchmark methods: {unknown_methods}")
    if unknown_datasets:
        raise ValueError(f"Unknown benchmark datasets: {unknown_datasets}")


def _reproduction_command(
    methods: list[str],
    datasets: list[str],
    *,
    seed_count: int,
    seed_list: list[int] | None,
) -> str:
    """Build the canonical command for reproducing a benchmark run."""
    command = (
        "dataforge bench "
        f"--methods {','.join(methods)} "
        f"--datasets {','.join(datasets)} "
        f"--seeds {seed_count}"
    )
    if seed_list is not None:
        command += f" --seed-list {','.join(str(seed) for seed in seed_list)}"
    return command


def _llm_skip_reason() -> str | None:
    """Return a skip reason when LLM methods cannot run."""
    provider = os.environ.get("DATAFORGE_LLM_PROVIDER", "").strip().lower()
    if provider == "groq":
        if not os.environ.get("GROQ_API_KEY"):
            return "GROQ_API_KEY is not set."
        return None
    if provider == "bedrock":
        if not os.environ.get("AWS_BEARER_TOKEN_BEDROCK"):
            return "AWS_BEARER_TOKEN_BEDROCK is not set."
        if not os.environ.get("DATAFORGE_BEDROCK_MODEL"):
            return "DATAFORGE_BEDROCK_MODEL is not set."
        return None
    if provider == "gemini":
        if not os.environ.get("GEMINI_API_KEY"):
            return "GEMINI_API_KEY is not set."
        return None
    if provider == "azure":
        if not os.environ.get("AZURE_API_KEY"):
            return "AZURE_API_KEY is not set."
        if not os.environ.get("AZURE_OPENAI_ENDPOINT"):
            return "AZURE_OPENAI_ENDPOINT is not set."
        if not os.environ.get("DATAFORGE_AZURE_MODEL"):
            return "DATAFORGE_AZURE_MODEL (deployment name) is not set."
        return None
    if provider == "grok":
        if not os.environ.get("XAI_API_KEY"):
            return "XAI_API_KEY is not set."
        return None
    return "DATAFORGE_LLM_PROVIDER must be set to groq, grok, bedrock, gemini, or azure."


def _env_float(name: str, default: float) -> float:
    """Read a float env var, falling back to a default on parse failure."""
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    """Read an int env var, falling back to a default on parse failure."""
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _corrector_max_issues() -> int | None:
    """Read the optional corrector issue cap; None means no cap (all issues)."""
    raw = os.environ.get("DATAFORGE_CORRECTOR_MAX_ISSUES", "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _corrector_cache_dir() -> Path | None:
    """Read the optional corrector response-cache dir (for reproducible replay).

    When set, the LLM corrector reads/writes per-call response samples under this
    directory. A committed cache lets the benchmark replay byte-identically OFFLINE
    (no provider/API key needed) -- the basis for reproducible calibration artifacts.
    """
    raw = os.environ.get("DATAFORGE_CORRECTOR_CACHE_DIR", "").strip()
    return Path(raw) if raw else None


def _bench_allow_embedded() -> bool:
    """Whether dataset loads may fall back to the committed embedded CSVs.

    Enables fully offline, network-free reproduction: with this set, an offline
    replay uses the repo's hash-pinned embedded dataset instead of the user cache.
    """
    return os.environ.get("DATAFORGE_BENCH_ALLOW_EMBEDDED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _build_groq_client() -> GroqBenchClient:
    """Construct a Groq benchmark client from env-driven knobs."""
    return GroqBenchClient(
        api_key=os.environ["GROQ_API_KEY"],
        model=os.environ.get("DATAFORGE_GROQ_MODEL", "llama-3.3-70b-versatile"),
        min_interval_s=_env_float("DATAFORGE_GROQ_MIN_INTERVAL_S", 1.0),
        max_tokens=_env_int("DATAFORGE_GROQ_MAX_TOKENS", 256),
        max_retries=_env_int("DATAFORGE_GROQ_MAX_RETRIES", 3),
        timeout_s=_env_float("DATAFORGE_GROQ_TIMEOUT_S", 30.0),
    )


def _build_grok_client() -> GrokBenchClient:
    """Construct an xAI Grok benchmark client from env-driven knobs.

    Grok is metered, so the USD guard is on by default (DATAFORGE_GROK_MAX_USD).
    """
    raw_cap = os.environ.get("DATAFORGE_GROK_MAX_USD", "").strip()
    max_usd: float | None = 15.0
    if raw_cap:
        try:
            parsed = float(raw_cap)
        except ValueError:
            parsed = 0.0
        max_usd = parsed if parsed > 0 else None
    return GrokBenchClient(
        api_key=os.environ["XAI_API_KEY"],
        model=os.environ.get("DATAFORGE_GROK_MODEL", "grok-4.5"),
        min_interval_s=_env_float("DATAFORGE_GROK_MIN_INTERVAL_S", 0.5),
        max_tokens=_env_int("DATAFORGE_GROK_MAX_TOKENS", 256),
        max_retries=_env_int("DATAFORGE_GROK_MAX_RETRIES", 5),
        timeout_s=_env_float("DATAFORGE_GROK_TIMEOUT_S", 60.0),
        usd_per_1k_input=_env_float("DATAFORGE_GROK_USD_PER_1K_INPUT", 0.002),
        usd_per_1k_output=_env_float("DATAFORGE_GROK_USD_PER_1K_OUTPUT", 0.006),
        max_usd=max_usd,
    )


def _build_bedrock_client() -> BedrockBenchClient:
    """Construct a Bedrock benchmark client from env-driven knobs."""
    raw_cap = os.environ.get("DATAFORGE_BEDROCK_MAX_USD", "").strip()
    max_usd: float | None = None
    if raw_cap:
        try:
            parsed = float(raw_cap)
        except ValueError:
            parsed = 0.0
        if parsed > 0:
            max_usd = parsed
    return BedrockBenchClient(
        api_key=os.environ["AWS_BEARER_TOKEN_BEDROCK"],
        model=os.environ["DATAFORGE_BEDROCK_MODEL"],
        region=os.environ.get("AWS_REGION", "us-east-1"),
        min_interval_s=_env_float("DATAFORGE_BEDROCK_MIN_INTERVAL_S", 1.0),
        max_tokens=_env_int("DATAFORGE_BEDROCK_MAX_TOKENS", 256),
        max_retries=_env_int("DATAFORGE_BEDROCK_MAX_RETRIES", 3),
        timeout_s=_env_float("DATAFORGE_BEDROCK_TIMEOUT_S", 30.0),
        max_usd=max_usd,
        usd_per_1k_input=_env_float("DATAFORGE_BEDROCK_USD_PER_1K_INPUT", 0.003),
        usd_per_1k_output=_env_float("DATAFORGE_BEDROCK_USD_PER_1K_OUTPUT", 0.015),
        temperature=_env_float("DATAFORGE_BEDROCK_TEMPERATURE", 0.0),
    )


def _build_gemini_client() -> GeminiBenchClient:
    """Construct a Gemini benchmark client from env-driven knobs."""
    return GeminiBenchClient(
        api_key=os.environ["GEMINI_API_KEY"],
        model=os.environ.get("DATAFORGE_GEMINI_MODEL", "gemini-3.1-pro-preview"),
        min_interval_s=_env_float("DATAFORGE_GEMINI_MIN_INTERVAL_S", 1.0),
        max_tokens=_env_int("DATAFORGE_GEMINI_MAX_TOKENS", 256),
        max_retries=_env_int("DATAFORGE_GEMINI_MAX_RETRIES", 5),
        timeout_s=_env_float("DATAFORGE_GEMINI_TIMEOUT_S", 60.0),
        temperature=_env_float("DATAFORGE_GEMINI_TEMPERATURE", 0.0),
    )


def _build_azure_client() -> AzureBenchClient:
    """Construct an Azure OpenAI benchmark client from env-driven knobs."""
    raw_cap = os.environ.get("DATAFORGE_AZURE_MAX_USD", "").strip()
    max_usd: float | None = None
    if raw_cap:
        try:
            parsed = float(raw_cap)
        except ValueError:
            parsed = 0.0
        if parsed > 0:
            max_usd = parsed
    return AzureBenchClient(
        api_key=os.environ["AZURE_API_KEY"],
        model=os.environ["DATAFORGE_AZURE_MODEL"],
        endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2025-04-01-preview"),
        min_interval_s=_env_float("DATAFORGE_AZURE_MIN_INTERVAL_S", 1.0),
        max_tokens=_env_int("DATAFORGE_AZURE_MAX_TOKENS", 256),
        max_retries=_env_int("DATAFORGE_AZURE_MAX_RETRIES", 5),
        timeout_s=_env_float("DATAFORGE_AZURE_TIMEOUT_S", 60.0),
        max_usd=max_usd,
        usd_per_1k_input=_env_float("DATAFORGE_AZURE_USD_PER_1K_INPUT", 0.005),
        usd_per_1k_output=_env_float("DATAFORGE_AZURE_USD_PER_1K_OUTPUT", 0.015),
        send_temperature=os.environ.get("DATAFORGE_AZURE_SEND_TEMPERATURE", "").strip().lower()
        in {"1", "true", "yes", "on"},
        temperature=_env_float("DATAFORGE_AZURE_TEMPERATURE", 0.0),
        reasoning_effort=os.environ.get("DATAFORGE_AZURE_REASONING_EFFORT", "").strip().lower()
        or None,
    )


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
    reproduction_command: str | None = None,
    seed_list: list[int] | None = None,
    verify_dataset_hashes: bool = True,
) -> BenchmarkRunOutput:
    """Run the selected benchmark methods across real-world datasets.

    Note: this library function never loads ``.env`` -- environment loading is an
    application-boundary concern owned by the entry points (the ``dataforge bench``
    CLI and the ``scripts/bench`` runners). Keeping it out here makes the function
    hermetic and order-independent under test, so the proof suite cannot be
    polluted by a ``.env`` file loaded mid-run.
    """
    _validate_inputs(methods, datasets)
    resolved_seed_list = build_seed_list(seeds=seeds, seed_list=seed_list)

    corrector_max_issues = _corrector_max_issues()
    corrector_cache_dir = _corrector_cache_dir()
    allow_embedded = _bench_allow_embedded()
    estimated_calls = estimate_llm_calls(
        methods=methods,
        datasets=datasets,
        seeds=len(resolved_seed_list),
        corrector_max_issues=corrector_max_issues,
    )
    # Validate call budget before any client instantiation or dataset loads that could
    # trigger network access in tests with environment variables set.
    validate_estimated_calls(
        estimated_calls=estimated_calls,
        really_run_big_bench=really_run_big_bench,
    )

    reproduction_command = reproduction_command or _reproduction_command(
        methods,
        datasets,
        seed_count=len(resolved_seed_list),
        seed_list=seed_list,
    )
    records: list[SeedBenchmarkResult] = []
    loaded_datasets = {
        dataset_name: load_real_world_dataset(
            dataset_name,
            cache_root=cache_root,
            verify_hashes=verify_dataset_hashes,
            allow_embedded_fallback=allow_embedded,
        )
        for dataset_name in datasets
    }

    llm_methods_requested = any(method.startswith("llm_") for method in methods)
    skip_reason = _llm_skip_reason() if llm_methods_requested else None
    client: (
        GroqBenchClient
        | GrokBenchClient
        | BedrockBenchClient
        | GeminiBenchClient
        | AzureBenchClient
        | None
    ) = None
    if llm_methods_requested and skip_reason is None:
        provider = os.environ.get("DATAFORGE_LLM_PROVIDER", "").strip().lower()
        if provider == "bedrock":
            client = _build_bedrock_client()
        elif provider == "gemini":
            client = _build_gemini_client()
        elif provider == "azure":
            client = _build_azure_client()
        elif provider == "grok":
            client = _build_grok_client()
        else:
            client = _build_groq_client()

    for dataset_name in datasets:
        dataset = loaded_datasets[dataset_name]
        for method in methods:
            for seed in resolved_seed_list:
                if os.environ.get("DATAFORGE_BENCH_VERBOSE"):
                    print(
                        f"[dataforge bench] start method={method} dataset={dataset_name} seed={seed}",
                        file=sys.stderr,
                        flush=True,
                    )
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
                elif method == "llm_corrector":
                    if client is None or skip_reason is not None:
                        result = _skipped_result(
                            method=method,
                            dataset=dataset_name,
                            seed=seed,
                            reason=skip_reason or "LLM client unavailable.",
                            reproduction_command=reproduction_command,
                        )
                    else:
                        result = run_llm_corrector_episode(
                            dataset,
                            seed=seed,
                            client=client,
                            max_issues=corrector_max_issues,
                            cache_dir=corrector_cache_dir,
                        )
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
                if os.environ.get("DATAFORGE_BENCH_VERBOSE"):
                    print(
                        f"[dataforge bench] done  method={method} dataset={dataset_name} seed={seed} status={result.status}",
                        file=sys.stderr,
                        flush=True,
                    )

    aggregates: list[AggregateBenchmarkResult] = aggregate_seed_results(
        records, seeds_requested=len(resolved_seed_list)
    )
    dataset_evidence = [
        dataset_evidence_from_loaded(loaded_datasets[dataset_name]) for dataset_name in datasets
    ]
    metadata = build_benchmark_metadata(
        methods=methods,
        datasets=datasets,
        seed_list=resolved_seed_list,
        reproduction_command=reproduction_command,
        dataset_evidence=dataset_evidence,
    )
    output = BenchmarkRunOutput(
        metadata=metadata.model_dump(mode="json"),
        records=records,
        aggregates=aggregates,
    )
    write_run_output(output, output_json)
    return output
