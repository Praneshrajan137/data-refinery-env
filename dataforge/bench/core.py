"""Shared benchmark types, metrics, and quota helpers."""

from __future__ import annotations

from collections import OrderedDict
from math import ceil
from pathlib import Path
from statistics import mean, stdev
from typing import Literal

from pydantic import BaseModel, Field

from dataforge.datasets.real_world import GroundTruthCell
from dataforge.datasets.registry import DATASET_REGISTRY

BenchmarkStatus = Literal["ok", "skipped"]


class BenchmarkRepair(BaseModel):
    """One benchmark repair prediction."""

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    new_value: str
    reason: str = Field(min_length=1)

    model_config = {"frozen": True}


class RepairScore(BaseModel):
    """Exact-match cell repair metrics for one episode."""

    tp: int = Field(ge=0)
    fp: int = Field(ge=0)
    fn: int = Field(ge=0)
    precision: float = Field(ge=0.0, le=1.0)
    recall: float = Field(ge=0.0, le=1.0)
    f1: float = Field(ge=0.0, le=1.0)

    model_config = {"frozen": True}


class SeedBenchmarkResult(BaseModel):
    """Benchmark result for one dataset/method/seed run."""

    method: str = Field(min_length=1)
    dataset: str = Field(min_length=1)
    seed: int = Field(ge=0)
    status: BenchmarkStatus
    skip_reason: str | None = None
    precision: float | None = None
    recall: float | None = None
    f1: float | None = None
    tp: int | None = None
    fp: int | None = None
    fn: int | None = None
    avg_steps: float | None = None
    llm_calls: int = Field(ge=0, default=0)
    prompt_tokens: int = Field(ge=0, default=0)
    completion_tokens: int = Field(ge=0, default=0)
    quota_units: float = Field(ge=0.0, default=0.0)
    runtime_s: float = Field(ge=0.0, default=0.0)
    provider: str | None = None
    model: str | None = None
    warnings: list[str] = Field(default_factory=list)
    reproduction_command: str = Field(min_length=1)


class AggregateBenchmarkResult(BaseModel):
    """Aggregated benchmark result across seeds for one method/dataset pair."""

    method: str = Field(min_length=1)
    dataset: str = Field(min_length=1)
    status: BenchmarkStatus
    skip_reason: str | None = None
    seeds_requested: int = Field(ge=0)
    seeds_completed: int = Field(ge=0)
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
    runtime_s_mean: float | None = None
    runtime_s_std: float | None = None
    provider: str | None = None
    model: str | None = None
    reproduction_command: str = Field(min_length=1)


class BenchmarkRunOutput(BaseModel):
    """Serializable benchmark run output."""

    metadata: dict[str, object]
    records: list[SeedBenchmarkResult]
    aggregates: list[AggregateBenchmarkResult]


def chunk_row_indices(n_rows: int) -> tuple[tuple[int, ...], ...]:
    """Split rows into contiguous chunks with a target of twenty chunks."""
    if n_rows <= 0:
        return ()
    chunk_size = ceil(n_rows / 20)
    chunks: list[tuple[int, ...]] = []
    for start in range(0, n_rows, chunk_size):
        stop = min(start + chunk_size, n_rows)
        chunks.append(tuple(range(start, stop)))
    return tuple(chunks)


def normalize_repairs(repairs: list[BenchmarkRepair]) -> list[BenchmarkRepair]:
    """Collapse repairs to one final prediction per cell using last-write-wins."""
    by_cell: OrderedDict[tuple[int, str], BenchmarkRepair] = OrderedDict()
    for repair in repairs:
        key = (repair.row, repair.column)
        if key in by_cell:
            del by_cell[key]
        by_cell[key] = repair
    return list(by_cell.values())


def score_repairs(
    ground_truth: tuple[GroundTruthCell, ...] | list[GroundTruthCell],
    repairs: list[BenchmarkRepair],
) -> RepairScore:
    """Score repaired cells against exact dirty-to-clean ground truth."""
    normalized = normalize_repairs(repairs)
    ground_truth_map = {(cell.row, cell.column): cell.clean_value for cell in ground_truth}

    matched: set[tuple[int, str]] = set()
    tp = 0
    fp = 0
    for repair in normalized:
        key = (repair.row, repair.column)
        clean_value = ground_truth_map.get(key)
        if clean_value is not None and repair.new_value == clean_value:
            tp += 1
            matched.add(key)
        else:
            fp += 1

    fn = len(ground_truth_map) - len(matched)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return RepairScore(
        tp=tp,
        fp=fp,
        fn=fn,
        precision=round(precision, 4),
        recall=round(recall, 4),
        f1=round(f1, 4),
    )


def quota_units(*, llm_calls: int, prompt_tokens: int, completion_tokens: int) -> float:
    """Compute free-tier quota units consumed by one episode."""
    request_fraction = llm_calls / 1000 if llm_calls else 0.0
    token_fraction = (
        (prompt_tokens + completion_tokens) / 100000
        if (prompt_tokens or completion_tokens)
        else 0.0
    )
    return round(max(request_fraction, token_fraction), 4)


def estimate_llm_calls(*, methods: list[str], datasets: list[str], seeds: int) -> int:
    """Estimate total LLM calls for the selected run configuration."""
    estimated = 0
    for dataset_name in datasets:
        chunks = len(chunk_row_indices(DATASET_REGISTRY[dataset_name].n_rows))
        for method in methods:
            if method == "llm_zeroshot":
                estimated += chunks * seeds
            elif method == "llm_react":
                estimated += chunks * 2 * seeds
    return estimated


def validate_estimated_calls(*, estimated_calls: int, really_run_big_bench: bool) -> None:
    """Enforce the free-tier call budget."""
    if estimated_calls > 500 and not really_run_big_bench:
        raise ValueError(
            "Estimated benchmark size exceeds 500 free-tier LLM calls. "
            "Pass --really-run-big-bench to continue."
        )


def aggregate_seed_results(
    records: list[SeedBenchmarkResult],
    *,
    seeds_requested: int,
) -> list[AggregateBenchmarkResult]:
    """Aggregate seed-level results into method/dataset summaries."""
    grouped: OrderedDict[tuple[str, str], list[SeedBenchmarkResult]] = OrderedDict()
    for record in records:
        grouped.setdefault((record.method, record.dataset), []).append(record)

    def _mean_std(values: list[float]) -> tuple[float, float]:
        if len(values) == 1:
            return round(values[0], 4), 0.0
        return round(mean(values), 4), round(stdev(values), 4)

    aggregates: list[AggregateBenchmarkResult] = []
    for (method, dataset), rows in grouped.items():
        ok_rows = [row for row in rows if row.status == "ok"]
        if not ok_rows:
            aggregates.append(
                AggregateBenchmarkResult(
                    method=method,
                    dataset=dataset,
                    status="skipped",
                    skip_reason=rows[0].skip_reason,
                    seeds_requested=seeds_requested,
                    seeds_completed=0,
                    provider=rows[0].provider,
                    model=rows[0].model,
                    reproduction_command=rows[0].reproduction_command,
                )
            )
            continue

        precision_mean, precision_std = _mean_std([row.precision or 0.0 for row in ok_rows])
        recall_mean, recall_std = _mean_std([row.recall or 0.0 for row in ok_rows])
        f1_mean, f1_std = _mean_std([row.f1 or 0.0 for row in ok_rows])
        avg_steps_mean, avg_steps_std = _mean_std([row.avg_steps or 0.0 for row in ok_rows])
        quota_mean, quota_std = _mean_std([row.quota_units for row in ok_rows])
        runtime_mean, runtime_std = _mean_std([row.runtime_s for row in ok_rows])
        aggregates.append(
            AggregateBenchmarkResult(
                method=method,
                dataset=dataset,
                status="ok",
                skip_reason=None,
                seeds_requested=seeds_requested,
                seeds_completed=len(ok_rows),
                precision_mean=precision_mean,
                precision_std=precision_std,
                recall_mean=recall_mean,
                recall_std=recall_std,
                f1_mean=f1_mean,
                f1_std=f1_std,
                avg_steps_mean=avg_steps_mean,
                avg_steps_std=avg_steps_std,
                quota_units_mean=quota_mean,
                quota_units_std=quota_std,
                runtime_s_mean=runtime_mean,
                runtime_s_std=runtime_std,
                provider=ok_rows[0].provider,
                model=ok_rows[0].model,
                reproduction_command=ok_rows[0].reproduction_command,
            )
        )
    return aggregates


def write_run_output(output: BenchmarkRunOutput, path: Path) -> None:
    """Serialize benchmark run output to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output.model_dump_json(indent=2), encoding="utf-8")
