"""Shared benchmark types, metrics, and quota helpers."""

from __future__ import annotations

import importlib.metadata as package_metadata
import platform
import subprocess
import sys
from collections import OrderedDict
from datetime import UTC, datetime
from math import ceil
from pathlib import Path
from statistics import mean, stdev
from typing import Literal

from pydantic import BaseModel, Field

from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset
from dataforge.datasets.registry import DATASET_REGISTRY

BenchmarkStatus = Literal["ok", "skipped"]
BENCHMARK_SCHEMA_VERSION = "dataforge_benchmark_run_v2"

# Default self-consistency sample count used to size the corrector's call budget.
# Kept in sync with LLMCorrectorRepairer's default; duplicated here to avoid a
# circular import between core and the bench methods module.
_CORRECTOR_ESTIMATE_SAMPLES = 3


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


class ClassScore(BaseModel):
    """Per-error-class detection/correction metrics for one episode.

    Detection and correction are scored separately (the Raha-vs-Baran split):
    ``detection_recall`` credits flagging a class's cells regardless of whether
    a correct value was produced, while ``recall``/``precision_on_class`` credit
    only correct repairs. A detector can therefore honestly improve coverage
    (detection) even when the correct value is not derivable (correction).

    ``precision_on_class`` is the fraction of repairs that landed on a
    ground-truth cell of this class and were correct; spurious repairs on
    non-error cells are not class-attributable and are reported in the
    aggregate :class:`RepairScore`.
    """

    error_class: str = Field(min_length=1)
    support: int = Field(ge=0, description="Ground-truth cells of this class")
    detected: int = Field(default=0, ge=0, description="Class cells flagged by a detector")
    detection_recall: float = Field(default=0.0, ge=0.0, le=1.0)
    tp: int = Field(ge=0)
    fn: int = Field(ge=0)
    recall: float = Field(ge=0.0, le=1.0)
    predicted_on_class: int = Field(ge=0)
    precision_on_class: float = Field(ge=0.0, le=1.0)

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
    gpu_hours: float = Field(ge=0.0, default=0.0)
    runtime_s: float = Field(ge=0.0, default=0.0)
    provider: str | None = None
    model: str | None = None
    warnings: list[str] = Field(default_factory=list)
    reproduction_command: str = Field(min_length=1)
    by_class: dict[str, ClassScore] | None = Field(
        default=None,
        description="Per-error-class metrics; None for methods that do not compute them.",
    )
    ece: float | None = Field(
        default=None,
        description="Expected calibration error of correction confidences; None if not computed.",
    )
    precision_at_auto_apply: float | None = Field(
        default=None,
        description="Correction precision among proposals at/above the auto-apply confidence.",
    )
    auto_apply_count: int | None = Field(
        default=None,
        description="How many proposals cleared the auto-apply confidence threshold.",
    )
    calibration_samples_by_class: dict[str, list[tuple[float, bool]]] | None = Field(
        default=None,
        description=(
            "Raw per-error-class (confidence, was_correct) pairs, persisted so a "
            "distribution-free certified-coverage report can be computed from the "
            "committed artifact (never an in-sample number). None if not computed."
        ),
    )
    calibration_samples_by_type: dict[str, list[tuple[float, bool]]] | None = Field(
        default=None,
        description=(
            "Raw per-detector-issue-type (confidence, was_correct) pairs. Keyed by the "
            "issue_type the repairer stamps on each fix (CellFix.detector_id), which is "
            "the key the auto-apply policy uses at inference. This is the correct key to "
            "fit calibration maps and certify conformal thresholds against; the by_class "
            "variant (ground-truth error class) is for the human-readable ECE report only. "
            "None if not computed."
        ),
    )


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
    gpu_hours_mean: float | None = None
    gpu_hours_std: float | None = None
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


class BenchmarkDatasetEvidence(BaseModel):
    """Pinned source and loaded artifact evidence for one benchmark dataset."""

    name: str = Field(min_length=1)
    source_urls: tuple[str, str]
    source_revision: str = Field(min_length=7)
    dirty_sha256: str = Field(min_length=64, max_length=64)
    clean_sha256: str = Field(min_length=64, max_length=64)
    n_rows: int = Field(ge=0)
    n_columns: int = Field(ge=1)


class BenchmarkEvidenceMetadata(BaseModel):
    """Typed provenance block written into benchmark JSON artifacts."""

    schema_version: str = BENCHMARK_SCHEMA_VERSION
    methods: list[str]
    datasets: list[str]
    seeds: int = Field(ge=1)
    seed_list: list[int]
    git_commit: str | None
    git_dirty: bool | None
    generated_at_utc: str
    python_version: str
    platform: str
    dependency_versions: dict[str, str]
    generator_command: str
    reproduction_command: str
    dataset_evidence: list[BenchmarkDatasetEvidence]
    artifact_sha256s: dict[str, str]


def build_seed_list(*, seeds: int, seed_list: list[int] | None = None) -> list[int]:
    """Resolve either a seed count or explicit seed list into concrete seeds."""
    if seed_list is not None:
        if not seed_list:
            raise ValueError("Benchmark seed list must contain at least one seed.")
        if any(seed < 0 for seed in seed_list):
            raise ValueError("Benchmark seeds must be >= 0.")
        if len(set(seed_list)) != len(seed_list):
            raise ValueError("Benchmark seed list must not contain duplicates.")
        return list(seed_list)
    if seeds <= 0:
        raise ValueError("Benchmark seeds must be >= 1.")
    return list(range(seeds))


def _package_version(name: str) -> str:
    """Return an installed package version or a stable missing marker."""
    try:
        return package_metadata.version(name)
    except package_metadata.PackageNotFoundError:
        return "not-installed"


def benchmark_dependency_versions() -> dict[str, str]:
    """Return versions of dependencies that influence benchmark behavior."""
    return {
        "dataforge": _package_version("dataforge"),
        "httpx": _package_version("httpx"),
        "pandas": _package_version("pandas"),
        "pydantic": _package_version("pydantic"),
        "python-dotenv": _package_version("python-dotenv"),
        "typer": _package_version("typer"),
    }


def _project_root() -> Path:
    """Return the source checkout root when running from this repository."""
    return Path(__file__).resolve().parents[2]


def _git_command(args: list[str]) -> str | None:
    """Run a read-only git command and return stdout when available."""
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=_project_root(),
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def current_git_commit() -> str | None:
    """Return the current source commit, if this checkout is under git."""
    return _git_command(["rev-parse", "HEAD"])


def git_worktree_dirty() -> bool | None:
    """Return whether the checkout has tracked or untracked changes."""
    status = _git_command(["status", "--porcelain"])
    if status is None:
        return None
    return bool(status)


def dataset_evidence_from_loaded(dataset: RealWorldDataset) -> BenchmarkDatasetEvidence:
    """Build source and loaded-byte evidence for one dataset."""
    return BenchmarkDatasetEvidence(
        name=dataset.metadata.name,
        source_urls=dataset.metadata.source_urls,
        source_revision=dataset.metadata.source_revision,
        dirty_sha256=dataset.dirty_sha256,
        clean_sha256=dataset.clean_sha256,
        n_rows=len(dataset.clean_df.index),
        n_columns=len(dataset.clean_df.columns),
    )


def build_benchmark_metadata(
    *,
    methods: list[str],
    datasets: list[str],
    seed_list: list[int],
    reproduction_command: str,
    dataset_evidence: list[BenchmarkDatasetEvidence],
) -> BenchmarkEvidenceMetadata:
    """Build the typed provenance metadata stored in benchmark JSON."""
    artifact_sha256s: dict[str, str] = {}
    for evidence in dataset_evidence:
        artifact_sha256s[f"dataset:{evidence.name}:dirty.csv"] = evidence.dirty_sha256
        artifact_sha256s[f"dataset:{evidence.name}:clean.csv"] = evidence.clean_sha256

    return BenchmarkEvidenceMetadata(
        methods=methods,
        datasets=datasets,
        seeds=len(seed_list),
        seed_list=seed_list,
        git_commit=current_git_commit(),
        git_dirty=git_worktree_dirty(),
        generated_at_utc=datetime.now(UTC).replace(microsecond=0).isoformat(),
        python_version=sys.version.split()[0],
        platform=platform.platform(),
        dependency_versions=benchmark_dependency_versions(),
        generator_command=reproduction_command,
        reproduction_command=reproduction_command,
        dataset_evidence=dataset_evidence,
        artifact_sha256s=artifact_sha256s,
    )


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


def estimate_llm_calls(
    *,
    methods: list[str],
    datasets: list[str],
    seeds: int,
    corrector_max_issues: int | None = None,
) -> int:
    """Estimate total LLM calls for the selected run configuration."""
    estimated = 0
    for dataset_name in datasets:
        n_rows = DATASET_REGISTRY[dataset_name].n_rows
        chunks = len(chunk_row_indices(n_rows))
        for method in methods:
            if method == "llm_zeroshot":
                estimated += chunks * seeds
            elif method == "llm_react":
                estimated += chunks * 2 * seeds
            elif method == "llm_corrector":
                # Conservative upper bound: at most one detected issue per row,
                # each resolved with the default self-consistency sample count.
                # A configured issue cap bounds this directly.
                issue_bound = n_rows if corrector_max_issues is None else corrector_max_issues
                estimated += issue_bound * _CORRECTOR_ESTIMATE_SAMPLES * seeds
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
        gpu_hours_mean, gpu_hours_std = _mean_std([row.gpu_hours for row in ok_rows])
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
                gpu_hours_mean=gpu_hours_mean,
                gpu_hours_std=gpu_hours_std,
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
