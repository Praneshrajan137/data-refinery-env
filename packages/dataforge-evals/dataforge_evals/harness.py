from __future__ import annotations

import contextlib
import multiprocessing as mp
import queue
import subprocess
import time
from collections import Counter, OrderedDict
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Literal

from pydantic import BaseModel, Field

from dataforge_evals.agents.base import (
    Agent,
    AgentRunResult,
    AgentTask,
    Fix,
    InferabilityLabel,
    Task,
    Usage,
)
from dataforge_evals.grader import Grade, grade_fixes, normalize_fixes
from dataforge_evals.repair_contract import repair_failure_taxonomy
from dataforge_evals.tasks import load_task

FailureKind = Literal[
    "timeout",
    "safety_refuse",
    "smt_reject",
    "parse_failure",
    "truncated_json",
    "wrong_cell",
    "wrong_value",
    "overrepair",
    "missed_repair",
    "schema_case_error",
    "duplicate_cell",
    "unsupported_inference",
    "exception",
]
RunStatus = Literal["ok", "failed"]


class TrialResult(BaseModel):
    """Serializable result for one agent/dataset/trial run."""

    agent: str = Field(min_length=1)
    dataset: str = Field(min_length=1)
    seed: int = Field(ge=0)
    inferability: InferabilityLabel | None = None
    status: RunStatus
    failure_kind: FailureKind | None = None
    failure_message: str | None = None
    precision: float | None = None
    recall: float | None = None
    f1: float | None = None
    tp: int | None = None
    fp: int | None = None
    fn: int | None = None
    steps: int = Field(default=0, ge=0)
    fixes: list[Fix] = Field(default_factory=list)
    usage: Usage = Field(default_factory=Usage)
    model: str | None = None
    runtime_s: float = Field(default=0.0, ge=0.0)
    failure_taxonomy: dict[str, int] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class AggregateResult(BaseModel):
    """Aggregate metrics for an agent/dataset pair."""

    agent: str = Field(min_length=1)
    dataset: str = Field(min_length=1)
    inferability: InferabilityLabel | None = None
    trials_requested: int = Field(ge=0)
    trials_completed: int = Field(ge=0)
    precision_mean: float | None = None
    precision_std: float | None = None
    recall_mean: float | None = None
    recall_std: float | None = None
    f1_mean: float | None = None
    f1_std: float | None = None
    avg_steps_mean: float | None = None
    avg_steps_std: float | None = None
    quota_units_mean: float | None = None
    quota_units_std: float | None = None
    quota_units_total: float = Field(default=0.0, ge=0.0)
    runtime_s_mean: float | None = None
    runtime_s_std: float | None = None
    failure_taxonomy: dict[str, int] = Field(default_factory=dict)
    model: str | None = None


class Reproducibility(BaseModel):
    """Metadata required to interpret and reproduce a run."""

    dataforge_evals_commit: str
    dataforge_commit: str | None
    seeds: list[int]
    provider_models: dict[str, str]
    run_date_utc: str
    dependency_versions: dict[str, str]
    nondeterminism_note: str


class HarnessRun(BaseModel):
    """Complete serializable harness output."""

    records: list[TrialResult]
    aggregates: list[AggregateResult]
    reproducibility: Reproducibility


@dataclass(frozen=True, kw_only=True)
class HarnessConfig:
    """Configuration for one harness execution."""

    agents: tuple[Agent, ...]
    datasets: tuple[str, ...]
    trials: int
    seeds: tuple[int, ...]
    timeout_s: float = 120.0
    output: Path = Path("dataforge-evals-report.md")
    cache_root: Path | None = None
    dirty_csv: Path | None = None
    clean_csv: Path | None = None


def _git_commit(path: Path) -> str:
    """Return the current git commit hash for a repository path."""
    with contextlib.suppress(Exception):
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=path,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip()
    return "unknown"


def _dataforge_commit() -> str | None:
    """Return the installed DataForge source commit when available."""
    with contextlib.suppress(Exception):
        import dataforge  # type: ignore[import-not-found]

        package_root = Path(dataforge.__file__).resolve().parents[1]
        return _git_commit(package_root)
    return None


def _run_date_utc() -> str:
    """Return an ISO-8601 UTC timestamp for a run."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _dependency_versions() -> dict[str, str]:
    """Return dependency versions needed to interpret a report."""
    versions: dict[str, str] = {}
    for package in ("pandas", "pydantic", "httpx", "typer"):
        try:
            versions[package] = version(package)
        except PackageNotFoundError:
            versions[package] = "not installed"
    return versions


def _mean_std(values: list[float]) -> tuple[float, float]:
    """Return rounded mean and sample standard deviation."""
    if len(values) == 1:
        return round(values[0], 4), 0.0
    return round(mean(values), 4), round(stdev(values), 4)


def _failure_kind(exc: BaseException) -> FailureKind:
    """Classify an exception into the public failure taxonomy."""
    text = str(exc).lower()
    if isinstance(exc, TimeoutError) or "timed out" in text:
        return "timeout"
    if "safety" in text or "refuse" in text:
        return "safety_refuse"
    if "smt" in text or "verifier" in text:
        return "smt_reject"
    if "truncated" in text or "unterminated" in text:
        return "truncated_json"
    if "diagn" in text or "schema" in text or "parse" in text or "json" in text:
        return "parse_failure"
    return "exception"


def _repair_failure_taxonomy(task: Task, fixes: list[Fix]) -> dict[str, int]:
    """Classify exact-match misses for a successful agent run."""
    return repair_failure_taxonomy(
        ground_truth=task.ground_truth,
        fixes=fixes,
        allowed_columns=task.canonical_columns,
        valid_rows=range(len(task.dirty_df.index)),
    )


def _coerce_result(raw_result: list[Fix] | AgentRunResult) -> AgentRunResult:
    """Normalize protocol-compatible return values into AgentRunResult."""
    if isinstance(raw_result, AgentRunResult):
        return raw_result
    return AgentRunResult(fixes=raw_result, steps=len(raw_result))


def _agent_task_view(task: Task) -> AgentTask:
    """Return the label-hidden view passed to non-oracle agents."""
    return AgentTask(
        name=task.name,
        dirty_df=task.dirty_df,
        canonical_columns=task.canonical_columns,
        metadata=task.metadata,
        inferability=task.inferability,
    )


def _task_for_agent(agent: Agent, task: Task) -> AgentTask | Task:
    """Return the safe task view for an agent.

    Only oracle/test adapters marked with ``uses_ground_truth = True`` receive
    labels. Normal adapters get a task object without a ``ground_truth`` field.
    """
    if getattr(agent, "uses_ground_truth", False) is True:
        return task
    return _agent_task_view(task)


def _agent_worker(
    result_queue: Any, agent: Agent, task: AgentTask | Task
) -> None:  # pragma: no cover - exercised through parent process tests
    """Run an agent in a child process and return a pickle-safe result envelope."""
    try:
        result_queue.put(("ok", agent.run(task)))
    except Exception as exc:
        result_queue.put(("error", exc.__class__.__name__, str(exc)))


def _run_one(agent: Agent, task: Task, *, seed: int, timeout_s: float) -> TrialResult:
    """Run and grade one agent/task/trial episode."""
    start = time.perf_counter()
    try:
        if timeout_s <= 0:
            raise TimeoutError("timeout must be positive")
        context = mp.get_context("spawn")
        result_queue: Any = context.Queue(maxsize=1)
        agent_task = _task_for_agent(agent, task)
        process = context.Process(target=_agent_worker, args=(result_queue, agent, agent_task))
        process.start()
        process.join(timeout_s)
        if process.is_alive():
            process.terminate()
            process.join(timeout=1)
            raise TimeoutError(f"agent exceeded timeout_s={timeout_s}")
        if process.exitcode not in (0, None) and result_queue.empty():
            raise RuntimeError(f"agent worker exited with code {process.exitcode}")
        try:
            payload = result_queue.get_nowait()
        except queue.Empty as exc:
            raise RuntimeError("agent worker returned no result") from exc
        if payload[0] == "error":
            raise RuntimeError(f"{payload[1]}: {payload[2]}")
        raw = payload[1]
        elapsed = time.perf_counter() - start
        result = _coerce_result(raw)
        normalized_fixes = normalize_fixes(result.fixes)
        grade: Grade = grade_fixes(task.ground_truth, normalized_fixes)
        return TrialResult(
            agent=agent.name,
            dataset=task.name,
            seed=seed,
            inferability=task.inferability,
            status="ok",
            precision=grade.precision,
            recall=grade.recall,
            f1=grade.f1,
            tp=grade.tp,
            fp=grade.fp,
            fn=grade.fn,
            steps=result.steps,
            fixes=normalized_fixes,
            usage=result.usage,
            model=result.model,
            runtime_s=round(elapsed, 4),
            failure_taxonomy=_repair_failure_taxonomy(task, normalized_fixes),
            warnings=result.warnings,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - start
        failure_kind = _failure_kind(exc)
        return TrialResult(
            agent=agent.name,
            dataset=task.name,
            seed=seed,
            inferability=task.inferability,
            status="failed",
            failure_kind=failure_kind,
            failure_message=str(exc),
            runtime_s=round(elapsed, 4),
            failure_taxonomy={failure_kind: 1},
        )


def aggregate_results(
    records: list[TrialResult], *, trials_requested: int
) -> list[AggregateResult]:
    """Aggregate trial results by agent and dataset."""
    grouped: OrderedDict[tuple[str, str], list[TrialResult]] = OrderedDict()
    for record in records:
        grouped.setdefault((record.agent, record.dataset), []).append(record)
    aggregates: list[AggregateResult] = []
    for (agent, dataset), rows in grouped.items():
        ok_rows = [row for row in rows if row.status == "ok"]
        failures: Counter[str] = Counter()
        for row in rows:
            failures.update(row.failure_taxonomy)
        if not ok_rows:
            aggregates.append(
                AggregateResult(
                    agent=agent,
                    dataset=dataset,
                    inferability=rows[0].inferability,
                    trials_requested=trials_requested,
                    trials_completed=0,
                    quota_units_total=round(sum(row.usage.quota_units for row in rows), 4),
                    failure_taxonomy={str(kind): count for kind, count in failures.items()},
                )
            )
            continue
        precision_mean, precision_std = _mean_std([row.precision or 0.0 for row in ok_rows])
        recall_mean, recall_std = _mean_std([row.recall or 0.0 for row in ok_rows])
        f1_mean, f1_std = _mean_std([row.f1 or 0.0 for row in ok_rows])
        steps_mean, steps_std = _mean_std([float(row.steps) for row in ok_rows])
        quota_mean, quota_std = _mean_std([row.usage.quota_units for row in ok_rows])
        runtime_mean, runtime_std = _mean_std([row.runtime_s for row in ok_rows])
        aggregates.append(
            AggregateResult(
                agent=agent,
                dataset=dataset,
                inferability=rows[0].inferability,
                trials_requested=trials_requested,
                trials_completed=len(ok_rows),
                precision_mean=precision_mean,
                precision_std=precision_std,
                recall_mean=recall_mean,
                recall_std=recall_std,
                f1_mean=f1_mean,
                f1_std=f1_std,
                avg_steps_mean=steps_mean,
                avg_steps_std=steps_std,
                quota_units_mean=quota_mean,
                quota_units_std=quota_std,
                quota_units_total=round(sum(row.usage.quota_units for row in rows), 4),
                runtime_s_mean=runtime_mean,
                runtime_s_std=runtime_std,
                failure_taxonomy={str(kind): count for kind, count in failures.items()},
                model=ok_rows[0].model,
            )
        )
    return aggregates


def run_harness(config: HarnessConfig) -> HarnessRun:
    """Run all configured agents across all datasets and trials."""
    if config.trials <= 0:
        raise ValueError("trials must be >= 1")
    if len(config.seeds) < config.trials:
        raise ValueError("seeds must contain at least one seed per requested trial")
    tasks = [
        load_task(
            dataset,
            dirty_csv=config.dirty_csv,
            clean_csv=config.clean_csv,
            cache_root=config.cache_root,
        )
        for dataset in config.datasets
    ]
    records: list[TrialResult] = []
    provider_models: dict[str, str] = {}
    for task in tasks:
        for agent in config.agents:
            for seed in config.seeds[: config.trials]:
                record = _run_one(agent, task, seed=seed, timeout_s=config.timeout_s)
                records.append(record)
                if record.model is not None:
                    provider_models[agent.name] = record.model
    return HarnessRun(
        records=records,
        aggregates=aggregate_results(records, trials_requested=config.trials),
        reproducibility=Reproducibility(
            dataforge_evals_commit=_git_commit(Path(__file__).resolve().parents[1]),
            dataforge_commit=_dataforge_commit(),
            seeds=list(config.seeds[: config.trials]),
            provider_models=provider_models,
            run_date_utc=_run_date_utc(),
            dependency_versions=_dependency_versions(),
            nondeterminism_note="Deterministic adapters and mock agents are reproducible from the recorded seeds. Hosted LLM providers may still change outputs because providers can update model weights, routing, safety systems, or tokenization without notice.",
        ),
    )
