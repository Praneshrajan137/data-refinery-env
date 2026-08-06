"""Collect Week 9 chunk-level SFT trajectories from a ReAct teacher."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from rich.console import Console

from dataforge.bench.core import BenchmarkRepair, RepairScore, score_repairs
from dataforge.bench.groq_client import (
    AzureBenchClient,
    CerebrasBenchClient,
    GeminiBenchClient,
    GrokBenchClient,
    GroqBenchClient,
    GroqCompletion,
    ProviderRequestError,
)
from dataforge.bench.methods import (
    _chunk_records,
    _column_stats,
    _extract_json_object,
    _repairs_from_payload,
    chunk_row_indices,
)
from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset, load_real_world_dataset
from dataforge.evaluation_contract import InferabilityLabel
from dataforge.spend import cap_from_env

Difficulty = Literal["easy", "medium"]
Preset = Literal["smoke", "full"]
TeacherProvider = Literal["groq", "grok", "cerebras", "gemini", "azure"]
FlightsRepairMode = Literal["strict", "verified"]
NormalizationCandidate = dict[str, str | int]

SCHEMA_VERSION = "expert_v1"
DEFAULT_DATASETS: tuple[str, ...] = ("hospital", "flights", "beers")
DEFAULT_DIFFICULTIES: tuple[Difficulty, ...] = ("easy", "medium")
DEFAULT_OUTPUT = Path("data/sft_traj/expert_v1.jsonl")
DEFAULT_DATASET_REPO_NAME = "dataforge-sft-trajectories"
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
DEFAULT_GROK_MODEL = "grok-4.5"
DEFAULT_CEREBRAS_MODEL = "llama3.1-8b"
DEFAULT_GEMINI_MODEL = "gemini-3.1-pro-preview"
LIGHT_WINDOW_SIZES: dict[str, int] = {"easy": 48, "medium": 96}


@dataclass(frozen=True, slots=True)
class PresetDefaults:
    """Resolved defaults for one collection preset."""

    datasets: tuple[str, ...]
    difficulties: tuple[Difficulty, ...]
    seeds: int
    max_trajectories: int
    max_total_requests: int
    daily_request_budget: int
    min_interval_s: float
    groq_timeout_s: float
    groq_max_retries: int
    max_runtime_min: float | None
    progress_every_chunks: int
    ready_min_records: int


@dataclass(frozen=True, slots=True)
class CollectionSettings:
    """Collector settings after applying preset defaults and explicit CLI overrides."""

    preset: Preset
    datasets: list[str]
    difficulties: list[Difficulty]
    seeds: int
    max_trajectories: int
    min_episode_f1: float
    max_total_requests: int
    daily_request_budget: int
    teacher_provider: TeacherProvider
    teacher_model: str
    teacher_max_tokens: int
    min_interval_s: float
    teacher_timeout_s: float
    teacher_max_retries: int
    teacher_max_retry_after_s: float
    context_window_rows: int | None
    flights_repair_mode: FlightsRepairMode
    flights_verifier_model: str | None
    max_runtime_min: float | None
    progress_every_chunks: int
    ready_min_records: int


SMOKE_DEFAULTS = PresetDefaults(
    datasets=("hospital",),
    difficulties=("easy",),
    seeds=8,
    max_trajectories=32,
    max_total_requests=256,
    daily_request_budget=256,
    min_interval_s=1.0,
    groq_timeout_s=30.0,
    groq_max_retries=2,
    max_runtime_min=30.0,
    progress_every_chunks=1,
    ready_min_records=32,
)

FULL_DEFAULTS = PresetDefaults(
    datasets=DEFAULT_DATASETS,
    difficulties=DEFAULT_DIFFICULTIES,
    seeds=24,
    max_trajectories=2000,
    max_total_requests=2800,
    daily_request_budget=900,
    min_interval_s=2.0,
    groq_timeout_s=60.0,
    groq_max_retries=5,
    max_runtime_min=None,
    progress_every_chunks=20,
    ready_min_records=32,
)


class CompletionClient(Protocol):
    """Minimal protocol shared by benchmark teacher clients and tests."""

    @property
    def provider(self) -> str:
        """Return the teacher provider identifier."""

    @property
    def model(self) -> str:
        """Return the teacher model identifier."""

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        """Return a chat completion for the supplied messages."""


@dataclass(frozen=True, slots=True)
class TrajectoryKey:
    """Idempotency key for one chunk-level SFT example."""

    task_id: str
    seed: int
    chunk_index: int


@dataclass(slots=True)
class BudgetGuard:
    """Small request counter that fails before exceeding the configured budget."""

    max_total_requests: int
    daily_request_budget: int | None = None
    used_requests: int = 0

    def consume(self, requests: int = 1) -> None:
        """Consume request budget or raise before an over-budget API call."""
        if requests < 1:
            raise ValueError("requests must be positive")
        if self.used_requests + requests > self.max_total_requests:
            raise RuntimeError(
                "Teacher request budget exhausted before collection completed "
                f"({self.used_requests + requests}>{self.max_total_requests})."
            )
        if (
            self.daily_request_budget is not None
            and self.used_requests + requests > self.daily_request_budget
        ):
            raise RuntimeError(
                "Teacher daily request budget exhausted before collection completed "
                f"({self.used_requests + requests}>{self.daily_request_budget})."
            )
        self.used_requests += requests


@dataclass(frozen=True, slots=True)
class RuntimeDeadline:
    """Wall-clock guard for laptop-safe collection runs."""

    started_at: float
    max_runtime_s: float | None

    def elapsed_s(self) -> float:
        """Return elapsed wall-clock seconds."""
        return time.monotonic() - self.started_at

    def raise_if_expired(self) -> None:
        """Raise when the configured runtime deadline has been reached."""
        if self.max_runtime_s is None:
            return
        if self.elapsed_s() >= self.max_runtime_s:
            minutes = self.max_runtime_s / 60
            raise RuntimeError(f"Teacher runtime deadline reached after {minutes:.1f} minutes.")


@dataclass(slots=True)
class ProgressReporter:
    """Small console progress reporter for long-running teacher collection."""

    console: Console
    deadline: RuntimeDeadline
    every_chunks: int
    accepted_records: int = 0
    chunks_seen: int = 0
    _log_current_chunk: bool = False

    def update_accepted(self, accepted_records: int) -> None:
        """Update the accepted trajectory count shown in later progress lines."""
        self.accepted_records = accepted_records

    def _enabled(self) -> bool:
        return self.every_chunks > 0

    def _elapsed(self) -> str:
        return f"{self.deadline.elapsed_s():.1f}s"

    def chunk_start(
        self,
        *,
        dataset: str,
        difficulty: Difficulty,
        seed: int,
        chunk_index: int,
        total_chunks: int,
        budget: BudgetGuard,
    ) -> None:
        """Print the start of a chunk when progress output is enabled for it."""
        self.chunks_seen += 1
        self._log_current_chunk = self._enabled() and (
            self.chunks_seen == 1 or self.chunks_seen % self.every_chunks == 0
        )
        if not self._log_current_chunk:
            return
        self.console.print(
            "[sft] chunk start "
            f"dataset={dataset} difficulty={difficulty} seed={seed} "
            f"chunk={chunk_index + 1}/{total_chunks} "
            f"requests={budget.used_requests}/{budget.max_total_requests} "
            f"accepted={self.accepted_records} elapsed={self._elapsed()}"
        )

    def api_start(
        self,
        *,
        label: str,
        dataset: str,
        difficulty: Difficulty,
        seed: int,
        chunk_index: int,
        budget: BudgetGuard,
    ) -> None:
        """Print before one teacher request."""
        if not self._log_current_chunk:
            return
        self.console.print(
            "[sft] api start   "
            f"phase={label} dataset={dataset} difficulty={difficulty} seed={seed} "
            f"chunk={chunk_index + 1} next_request={budget.used_requests + 1} "
            f"accepted={self.accepted_records} elapsed={self._elapsed()}"
        )

    def api_done(
        self,
        *,
        label: str,
        dataset: str,
        difficulty: Difficulty,
        seed: int,
        chunk_index: int,
        budget: BudgetGuard,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> None:
        """Print after one teacher request."""
        if not self._log_current_chunk:
            return
        self.console.print(
            "[sft] api done    "
            f"phase={label} dataset={dataset} difficulty={difficulty} seed={seed} "
            f"chunk={chunk_index + 1} requests={budget.used_requests}/{budget.max_total_requests} "
            f"tokens={prompt_tokens}+{completion_tokens} "
            f"accepted={self.accepted_records} elapsed={self._elapsed()}"
        )

    def chunk_done(
        self,
        *,
        dataset: str,
        difficulty: Difficulty,
        seed: int,
        chunk_index: int,
        repairs: int,
        llm_calls: int,
    ) -> None:
        """Print the end of a chunk when progress output is enabled for it."""
        if not self._log_current_chunk:
            return
        self.console.print(
            "[sft] chunk done  "
            f"dataset={dataset} difficulty={difficulty} seed={seed} "
            f"chunk={chunk_index + 1} repairs={repairs} llm_calls={llm_calls} "
            f"accepted={self.accepted_records} elapsed={self._elapsed()}"
        )

    def episode_score(
        self,
        *,
        dataset: str,
        difficulty: Difficulty,
        seed: int,
        score: RepairScore,
        min_episode_f1: float,
    ) -> None:
        """Print the episode-level score used by the acceptance gate."""
        if not self._enabled():
            return
        self.console.print(
            "[sft] episode score "
            f"dataset={dataset} difficulty={difficulty} seed={seed} "
            f"precision={score.precision:.3f} recall={score.recall:.3f} "
            f"f1={score.f1:.3f} threshold={min_episode_f1:.3f} "
            f"elapsed={self._elapsed()}"
        )


class TrajectoryRecord(BaseModel):
    """Validated on-disk JSONL schema for one SFT trajectory chunk."""

    schema_version: Literal["expert_v1", "expert_v2", "expert_v3", "expert_v4"]
    trajectory_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    dataset: str = Field(min_length=1)
    difficulty: Difficulty
    seed: int = Field(ge=0)
    chunk_index: int = Field(ge=0)
    state: dict[str, Any]
    tool_calls: list[dict[str, Any]]
    diagnosis: list[Any]
    fix: list[dict[str, Any]]
    messages: list[dict[str, str]]
    teacher: dict[str, Any]
    metrics: dict[str, Any]
    provenance: dict[str, Any]
    prompt_contract_version: str | None = None
    inferability: InferabilityLabel | None = None
    target_confidence: float | None = None
    should_abstain: bool | None = None


@dataclass(frozen=True, slots=True)
class ChunkCollection:
    """Internal result for one ReAct chunk before episode-level filtering."""

    record: dict[str, Any]
    repairs: list[BenchmarkRepair]


def validate_trajectory_record(record: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a trajectory record for JSONL serialization."""
    return TrajectoryRecord.model_validate(record).model_dump(mode="json", exclude_none=True)


def existing_trajectory_keys(path: Path) -> set[TrajectoryKey]:
    """Load chunk-level idempotency keys from an existing JSONL file."""
    if not path.exists():
        return set()

    keys: set[TrajectoryKey] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = validate_trajectory_record(cast(dict[str, Any], json.loads(line)))
        keys.add(
            TrajectoryKey(
                task_id=str(record["task_id"]),
                seed=int(record["seed"]),
                chunk_index=int(record["chunk_index"]),
            )
        )
    return keys


def write_jsonl_records(path: Path, records: list[dict[str, Any]]) -> None:
    """Append validated trajectory records to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        return
    needs_leading_newline = (
        path.exists()
        and path.stat().st_size > 0
        and not path.read_text(encoding="utf-8").endswith("\n")
    )
    with path.open("a", encoding="utf-8") as handle:
        if needs_leading_newline:
            handle.write("\n")
        for record in records:
            validated = validate_trajectory_record(record)
            handle.write(json.dumps(validated, sort_keys=True) + "\n")


def _stable_offset(*, dataset_name: str, difficulty: Difficulty, seed: int, limit: int) -> int:
    """Return a deterministic offset in `[0, limit)` without Python hash randomization."""
    digest = hashlib.sha256(f"{dataset_name}:{difficulty}:{seed}".encode()).hexdigest()
    return int(digest[:12], 16) % limit


def build_light_dataset(
    dataset: RealWorldDataset,
    *,
    difficulty: Difficulty,
    seed: int,
) -> RealWorldDataset:
    """Build a deterministic DataForge-Bench-light window for one dataset/seed."""
    window_size = min(LIGHT_WINDOW_SIZES[difficulty], len(dataset.dirty_df.index))
    if window_size == len(dataset.dirty_df.index):
        return dataset

    error_rows = sorted({cell.row for cell in dataset.ground_truth})
    if error_rows:
        anchor = error_rows[
            _stable_offset(
                dataset_name=dataset.metadata.name,
                difficulty=difficulty,
                seed=seed,
                limit=len(error_rows),
            )
        ]
    else:
        anchor = _stable_offset(
            dataset_name=dataset.metadata.name,
            difficulty=difficulty,
            seed=seed,
            limit=len(dataset.dirty_df.index),
        )
    max_start = len(dataset.dirty_df.index) - window_size
    start = min(max(anchor - window_size // 2, 0), max_start)
    stop = start + window_size

    dirty_df = dataset.dirty_df.iloc[start:stop].reset_index(drop=True)
    clean_df = dataset.clean_df.iloc[start:stop].reset_index(drop=True)
    ground_truth: list[GroundTruthCell] = []
    for cell in dataset.ground_truth:
        if start <= cell.row < stop:
            ground_truth.append(
                GroundTruthCell(
                    row=cell.row - start,
                    column=cell.column,
                    dirty_value=cell.dirty_value,
                    clean_value=cell.clean_value,
                )
            )

    metadata = dataset.metadata.model_copy(update={"n_rows": len(dirty_df.index)})
    return RealWorldDataset(
        metadata=metadata,
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=dataset.canonical_columns,
        ground_truth=tuple(ground_truth),
        dirty_sha256=dataset.dirty_sha256,
        clean_sha256=dataset.clean_sha256,
    )


def _score_for_rows(
    ground_truth: tuple[GroundTruthCell, ...],
    repairs: list[BenchmarkRepair],
    row_indices: tuple[int, ...],
) -> RepairScore:
    """Score repairs against only the cells in one chunk."""
    chunk_truth = [cell for cell in ground_truth if cell.row in row_indices]
    return score_repairs(chunk_truth, repairs)


def _repairs_for_rows(
    repairs: list[BenchmarkRepair],
    row_indices: tuple[int, ...],
    columns: tuple[str, ...] | None = None,
) -> list[BenchmarkRepair]:
    """Keep only repairs that target the active chunk rows."""
    allowed_rows = set(row_indices)
    allowed_columns = set(columns) if columns is not None else None
    return [
        repair
        for repair in repairs
        if repair.row in allowed_rows
        and (allowed_columns is None or repair.column in allowed_columns)
    ]


def _context_row_indices(
    total_rows: int,
    row_indices: tuple[int, ...],
    context_window_rows: int | None,
) -> tuple[int, ...]:
    """Return context rows for a chunk, optionally limited to a local window."""
    if context_window_rows is None:
        return tuple(range(total_rows))
    if not row_indices:
        return ()
    start = max(0, min(row_indices) - context_window_rows)
    end = min(total_rows, max(row_indices) + context_window_rows + 1)
    return tuple(range(start, end))


_NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")
_TIME_RE = re.compile(r"\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.)", re.IGNORECASE)
_FLIGHT_TIME_ONLY_RE = re.compile(r"^\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.)$", re.IGNORECASE)
_FLIGHT_STATUS_RE = re.compile(
    r"\s+(?:on\s+time|delayed|cancelled|canceled|arrived|departed|early|late)\b.*$",
    re.IGNORECASE,
)
_FLIGHT_DATE_TOKEN_RE = re.compile(
    r"\b(?:mon|tue|wed|thu|fri|sat|sun|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b"
    r"|\d{1,2}[-/][A-Za-z]{3}[-/]\d{2,4}"
    r"|[A-Za-z]{3}\s+\d{1,2},?\s+\d{2,4}",
    re.IGNORECASE,
)
_FLIGHT_SCHEDULE_COLUMNS = {"sched_dep_time", "sched_arr_time"}
_FLIGHT_TIME_COLUMNS = _FLIGHT_SCHEDULE_COLUMNS | {"act_dep_time", "act_arr_time"}
_FLIGHT_SCHEDULE_FOR_ACTUAL = {
    "act_dep_time": "sched_dep_time",
    "act_arr_time": "sched_arr_time",
}
_FLIGHT_ACTUAL_FOR_SCHEDULE = {
    "sched_dep_time": "act_dep_time",
    "sched_arr_time": "act_arr_time",
}


def _normalize_number_text(value: str) -> str:
    """Return a compact decimal string without a redundant `.0` suffix."""
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return str(number).rstrip("0").rstrip(".")


def _candidate(
    *,
    row: int,
    column: str,
    current_value: str,
    suggested_value: str,
    reason: str,
) -> NormalizationCandidate:
    """Build a dirty-data-only normalization candidate."""
    return {
        "row": row,
        "column": column,
        "current_value": current_value,
        "suggested_value": suggested_value,
        "reason": reason,
    }


def _candidate_with_tier(
    *,
    row: int,
    column: str,
    current_value: str,
    suggested_value: str,
    reason: str,
    tier: str,
) -> NormalizationCandidate:
    """Build a normalization candidate with explicit provenance."""
    candidate = _candidate(
        row=row,
        column=column,
        current_value=current_value,
        suggested_value=suggested_value,
        reason=reason,
    )
    candidate["tier"] = tier
    return candidate


def _beers_candidate(row: int, column: str, raw_value: str) -> NormalizationCandidate | None:
    """Return a high-confidence Beers normalization candidate."""
    value = raw_value.strip()
    if column == "ounces":
        match = _NUMBER_RE.search(value)
        if match and re.search(r"\boz\.?\b|\bounce\b", value, re.IGNORECASE):
            normalized = _normalize_number_text(match.group(0))
            if normalized != raw_value:
                return _candidate(
                    row=row,
                    column=column,
                    current_value=raw_value,
                    suggested_value=normalized,
                    reason="strip beer volume unit text",
                )
    if column == "abv" and value.endswith("%"):
        normalized = value.rstrip("%").strip()
        if normalized != raw_value:
            return _candidate(
                row=row,
                column=column,
                current_value=raw_value,
                suggested_value=normalized,
                reason="strip percent sign from ABV",
            )
    if column in {"ibu", "abv"} and value.upper() in {"N/A", "NA", "NULL", "NONE"}:
        return _candidate(
            row=row,
            column=column,
            current_value=raw_value,
            suggested_value="",
            reason="convert numeric placeholder to empty string",
        )
    return None


def _clean_flight_time(value: str) -> str | None:
    """Return the canonical time inside a Flights dirty time value."""
    stripped = value.strip()
    if not stripped:
        return None
    if "(" in stripped or ")" in stripped:
        return None
    without_status = _FLIGHT_STATUS_RE.sub("", stripped).strip()
    if without_status != stripped and _TIME_RE.fullmatch(without_status):
        return without_status
    if _FLIGHT_DATE_TOKEN_RE.search(stripped):
        time_matches = _TIME_RE.findall(stripped)
        if len(time_matches) == 1:
            candidate = str(time_matches[0]).strip()
            if candidate != stripped:
                return candidate
    return None


def _flight_same_key(row: dict[str, str]) -> tuple[str, str]:
    """Return a stable dirty-data key for matching nearby flight rows."""
    return (row.get("src", ""), row.get("flight", ""))


def _canonical_dirty_flight_time(value: str) -> str | None:
    """Return a canonical dirty Flights time without inventing a correction."""
    stripped = value.strip()
    cleaned = _clean_flight_time(stripped)
    if cleaned is not None:
        return cleaned
    if _FLIGHT_TIME_ONLY_RE.fullmatch(stripped):
        return stripped
    return None


def _flight_time_minutes(value: str) -> int | None:
    """Convert a canonical Flights time into minutes after midnight."""
    stripped = value.strip().lower()
    match = re.fullmatch(r"(\d{1,2}):(\d{2})\s*(a\.m\.|p\.m\.)", stripped)
    if match is None:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    meridiem = match.group(3)
    if hour == 12:
        hour = 0
    if meridiem == "p.m.":
        hour += 12
    return hour * 60 + minute


def _flight_time_distance_minutes(left: str, right: str) -> int | None:
    """Return the shortest minute distance between two canonical Flights times."""
    left_minutes = _flight_time_minutes(left)
    right_minutes = _flight_time_minutes(right)
    if left_minutes is None or right_minutes is None:
        return None
    distance = abs(left_minutes - right_minutes)
    return min(distance, 24 * 60 - distance)


def _flight_reference_value(
    *,
    rows_by_index: dict[int, dict[str, str]],
    target_row: dict[str, str],
    column: str,
) -> str | None:
    """Infer a blank flight time only when dirty references are unambiguous."""
    target_key = _flight_same_key(target_row)
    candidates: set[str] = set()
    for row in rows_by_index.values():
        if row is target_row or _flight_same_key(row) != target_key:
            continue
        cleaned = _canonical_dirty_flight_time(row.get(column, ""))
        if cleaned:
            candidates.add(cleaned)
    if len(candidates) != 1:
        return None
    return str(next(iter(candidates)))


def _flight_candidate(
    row_index: int,
    column: str,
    raw_value: str,
    *,
    rows_by_index: dict[int, dict[str, str]],
) -> NormalizationCandidate | None:
    """Return a high-confidence Flights normalization candidate."""
    if column not in _FLIGHT_TIME_COLUMNS:
        return None
    cleaned = _clean_flight_time(raw_value)
    if cleaned is not None:
        return _candidate_with_tier(
            row=row_index,
            column=column,
            current_value=raw_value,
            suggested_value=cleaned,
            reason="extract canonical flight time",
            tier="normalization",
        )
    target_row = rows_by_index[row_index]
    paired_schedule_column = _FLIGHT_SCHEDULE_FOR_ACTUAL.get(column)
    if paired_schedule_column is not None:
        current_time = _canonical_dirty_flight_time(raw_value)
        paired_schedule = _canonical_dirty_flight_time(target_row.get(paired_schedule_column, ""))
        departure_distance = (
            _flight_time_distance_minutes(current_time, paired_schedule)
            if current_time is not None and paired_schedule is not None
            else None
        )
        if (
            column == "act_dep_time"
            and current_time is not None
            and paired_schedule is not None
            and current_time != paired_schedule
            and departure_distance is not None
            and departure_distance <= 2
        ):
            return _candidate_with_tier(
                row=row_index,
                column=column,
                current_value=raw_value,
                suggested_value=paired_schedule,
                reason="align near-identical actual departure with scheduled departure",
                tier="intra_row_consistency",
            )
        if column == "act_arr_time" and raw_value.strip() == "" and paired_schedule is not None:
            return _candidate_with_tier(
                row=row_index,
                column=column,
                current_value=raw_value,
                suggested_value=paired_schedule,
                reason="fill blank actual arrival from scheduled arrival",
                tier="intra_row_consistency",
            )
    if raw_value.strip() == "":
        inferred = _flight_reference_value(
            rows_by_index=rows_by_index,
            target_row=target_row,
            column=column,
        )
        if inferred is not None:
            return _candidate_with_tier(
                row=row_index,
                column=column,
                current_value=raw_value,
                suggested_value=inferred,
                reason="fill blank from identical dirty flight reference",
                tier="dirty_reference",
            )
    return None


def _normalization_candidates(
    dataset: RealWorldDataset,
    *,
    row_indices: tuple[int, ...],
    context_indices: tuple[int, ...],
) -> list[NormalizationCandidate]:
    """Return dirty-data-only candidate repairs for high-confidence patterns."""
    reference_indices = (
        tuple(range(len(dataset.dirty_df.index)))
        if dataset.metadata.name == "flights"
        else context_indices
    )
    rows_by_index = {int(row["_row"]): row for row in _chunk_records(dataset, reference_indices)}
    candidates: list[NormalizationCandidate] = []
    for row_index in row_indices:
        row = rows_by_index.get(row_index)
        if row is None:
            continue
        for column in dataset.canonical_columns:
            raw_value = row.get(column, "")
            candidate: NormalizationCandidate | None = None
            if dataset.metadata.name == "beers":
                candidate = _beers_candidate(row_index, column, raw_value)
            elif dataset.metadata.name == "flights":
                candidate = _flight_candidate(
                    row_index,
                    column,
                    raw_value,
                    rows_by_index=rows_by_index,
                )
            if candidate is not None:
                candidates.append(candidate)
    return candidates


def _diagnosis_from_payloads(payloads: list[dict[str, Any]]) -> list[Any]:
    """Extract lightweight diagnostic text from ReAct payloads."""
    diagnosis: list[Any] = []
    for payload in payloads:
        raw = payload.get("diagnosis")
        if isinstance(raw, list):
            diagnosis.extend(raw)
        elif isinstance(raw, str):
            diagnosis.append(raw)
        for repair in (
            payload.get("repairs", []) if isinstance(payload.get("repairs"), list) else []
        ):
            if isinstance(repair, dict) and isinstance(repair.get("reason"), str):
                diagnosis.append(repair["reason"])
    return diagnosis


def _completion_payload(completion: GroqCompletion) -> dict[str, Any] | None:
    """Parse a teacher completion as a JSON object."""
    parsed = _extract_json_object(completion.text)
    if parsed is None:
        return None
    return cast(dict[str, Any], parsed)


def _repair_payload_issues(
    payload: dict[str, Any],
    *,
    row_indices: tuple[int, ...],
    columns: tuple[str, ...],
    dataset_name: str,
    normalization_candidates: list[NormalizationCandidate],
    flights_repair_mode: FlightsRepairMode,
    rows_by_index: dict[int, dict[str, str]] | None = None,
) -> list[str]:
    """Return compact validation issues for a submit_repairs payload."""
    raw_repairs = payload.get("repairs")
    if not isinstance(raw_repairs, list):
        return ["submit_repairs must include a repairs list"]
    allowed_rows = set(row_indices)
    allowed_columns = set(columns)
    allowed_candidate_repairs = _candidate_repair_keys(normalization_candidates)
    issues: list[str] = []
    for index, raw_repair in enumerate(raw_repairs):
        if not isinstance(raw_repair, dict):
            issues.append(f"repair {index} is not an object")
            continue
        row = raw_repair.get("row")
        column = raw_repair.get("column")
        new_value = raw_repair.get("new_value")
        if not isinstance(row, int) or row not in allowed_rows:
            issues.append(f"repair {index} row must be one of {sorted(allowed_rows)}")
        if not isinstance(column, str) or column not in allowed_columns:
            issues.append(f"repair {index} column must be an exact schema column")
        if not isinstance(new_value, str):
            issues.append(f"repair {index} new_value must be a string")
        if dataset_name == "flights" and isinstance(row, int) and isinstance(column, str):
            if isinstance(new_value, str) and (row, column, new_value) in allowed_candidate_repairs:
                continue
            if flights_repair_mode == "strict":
                issues.append(f"repair {index} must match a Flights normalization candidate")
            else:
                issues.extend(
                    _teacher_proposed_flights_issues(
                        raw_repair,
                        repair_index=index,
                        rows_by_index=rows_by_index or {},
                    )
                )
    return issues


def _candidate_repair_keys(
    normalization_candidates: list[NormalizationCandidate],
) -> set[tuple[int, str, str]]:
    """Return candidate keys that can be compared against submitted repairs."""
    keys: set[tuple[int, str, str]] = set()
    for candidate in normalization_candidates:
        row = candidate.get("row")
        column = candidate.get("column")
        suggested_value = candidate.get("suggested_value")
        if isinstance(row, int) and isinstance(column, str) and isinstance(suggested_value, str):
            keys.add((row, column, suggested_value))
    return keys


def _raw_repair_key(raw_repair: dict[str, Any]) -> tuple[int, str, str] | None:
    """Return a comparable repair key from a raw repair payload."""
    row = raw_repair.get("row")
    column = raw_repair.get("column")
    new_value = raw_repair.get("new_value")
    if isinstance(row, int) and isinstance(column, str) and isinstance(new_value, str):
        return (row, column, new_value)
    return None


def _repair_key(repair: BenchmarkRepair) -> tuple[int, str, str]:
    """Return a comparable repair key from a validated repair."""
    return (repair.row, repair.column, repair.new_value)


def _teacher_proposed_flights_issues(
    raw_repair: dict[str, Any],
    *,
    repair_index: int,
    rows_by_index: dict[int, dict[str, str]],
) -> list[str]:
    """Validate the shape and dirty-data plausibility of a teacher-only Flights repair."""
    key = _raw_repair_key(raw_repair)
    if key is None:
        return []
    row, column, new_value = key
    row_payload = rows_by_index.get(row)
    issues: list[str] = []
    reason = raw_repair.get("reason")
    evidence = raw_repair.get("evidence")
    confidence = raw_repair.get("confidence")
    if not isinstance(reason, str) or not reason.strip():
        issues.append(f"repair {repair_index} reason must be a non-empty string")
    if not (
        isinstance(evidence, str)
        and evidence.strip()
        or isinstance(evidence, list)
        and all(isinstance(item, str) and item.strip() for item in evidence)
    ):
        issues.append(f"repair {repair_index} evidence must be a string or list of strings")
    if not isinstance(confidence, int | float) or not 0.0 <= float(confidence) <= 1.0:
        issues.append(f"repair {repair_index} confidence must be a number between 0 and 1")
    if column not in _FLIGHT_SCHEDULE_COLUMNS:
        issues.append(f"repair {repair_index} teacher proposal must target a blank schedule column")
        return issues
    if row_payload is None:
        return issues
    if row_payload.get(column, "").strip() != "":
        issues.append(f"repair {repair_index} teacher proposal must target a blank Flights cell")
    if _canonical_dirty_flight_time(new_value) != new_value.strip():
        issues.append(f"repair {repair_index} new_value must be a canonical Flights time")
    paired_actual_column = _FLIGHT_ACTUAL_FOR_SCHEDULE[column]
    paired_actual = _canonical_dirty_flight_time(row_payload.get(paired_actual_column, ""))
    distance = (
        _flight_time_distance_minutes(new_value, paired_actual)
        if paired_actual is not None
        else None
    )
    if distance is None or distance > 90:
        issues.append(
            f"repair {repair_index} proposed schedule time must be within 90 minutes "
            "of the paired dirty actual time"
        )
    return issues


def _filter_flights_candidate_repairs(
    repairs: list[BenchmarkRepair],
    *,
    dataset_name: str,
    normalization_candidates: list[NormalizationCandidate],
) -> tuple[list[BenchmarkRepair], int]:
    """Keep only candidate-backed repairs for Flights."""
    if dataset_name != "flights":
        return repairs, 0
    allowed_candidate_repairs = _candidate_repair_keys(normalization_candidates)
    filtered = [
        repair
        for repair in repairs
        if (repair.row, repair.column, repair.new_value) in allowed_candidate_repairs
    ]
    return filtered, len(repairs) - len(filtered)


def _verification_payload_approvals(payload: dict[str, Any] | None) -> set[tuple[int, str, str]]:
    """Parse approved repair keys from a verifier response."""
    if payload is None:
        return set()
    raw_repairs = payload.get("repairs")
    if raw_repairs is None:
        raw_repairs = payload.get("approved_repairs")
    if not isinstance(raw_repairs, list):
        return set()
    approvals: set[tuple[int, str, str]] = set()
    for raw_repair in raw_repairs:
        if not isinstance(raw_repair, dict):
            continue
        approved = raw_repair.get("approved", True)
        if approved is False:
            continue
        key = _raw_repair_key(raw_repair)
        if key is not None:
            approvals.add(key)
    return approvals


def _verify_flights_repairs(
    *,
    repairs: list[BenchmarkRepair],
    raw_repairs: list[dict[str, Any]],
    dataset: RealWorldDataset,
    row_indices: tuple[int, ...],
    context_payload: list[dict[str, str]],
    normalization_candidates: list[NormalizationCandidate],
    verifier_client: CompletionClient | None,
    budget: BudgetGuard,
    deadline: RuntimeDeadline | None,
    progress: ProgressReporter | None,
    difficulty: Difficulty,
    seed: int,
    chunk_index: int,
    messages: list[dict[str, str]],
    warnings: list[str],
) -> tuple[list[BenchmarkRepair], int, int, int, int, int, int]:
    """Apply strict evidence and optional verifier approval to Flights repairs."""
    if dataset.metadata.name != "flights":
        return repairs, 0, 0, 0, 0, 0, 0
    allowed_candidate_repairs = _candidate_repair_keys(normalization_candidates)
    proposed_count = len(repairs)
    candidate_repairs = [
        repair for repair in repairs if _repair_key(repair) in allowed_candidate_repairs
    ]
    teacher_repairs = [
        repair for repair in repairs if _repair_key(repair) not in allowed_candidate_repairs
    ]
    if not teacher_repairs or verifier_client is None:
        dropped = proposed_count - len(candidate_repairs)
        if dropped:
            warnings.append("unsupported_flights_repairs_dropped")
        return (
            candidate_repairs,
            proposed_count,
            len(teacher_repairs),
            len(candidate_repairs),
            dropped,
            0,
            0,
        )

    teacher_keys = {_repair_key(repair) for repair in teacher_repairs}
    raw_teacher_repairs = [
        raw_repair
        for raw_repair in raw_repairs
        if (key := _raw_repair_key(raw_repair)) is not None and key in teacher_keys
    ]
    if deadline is not None:
        deadline.raise_if_expired()
    if progress is not None:
        progress.api_start(
            label="flights_verifier",
            dataset=dataset.metadata.name,
            difficulty=difficulty,
            seed=seed,
            chunk_index=chunk_index,
            budget=budget,
        )
    budget.consume()
    verifier_messages = [
        {
            "role": "system",
            "content": (
                "You are a strict Flights data-repair verifier. Return exactly one JSON object. "
                "Approve only teacher_proposed repairs that are supported by dirty target/context "
                "rows, target blank scheduled time cells, canonical time strings, and aviation "
                "time consistency. Reject guesses, non-target rows, operational-note corrections, "
                "and unsupported actual-time edits. Use "
                '{"action":"verify_repairs","repairs":[{"row":0,"column":"sched_dep_time",'
                '"new_value":"7:00 p.m.","approved":true,"reason":"why"}]}.'
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "target_row_indices": list(row_indices),
                    "columns": list(dataset.canonical_columns),
                    "context_rows": context_payload,
                    "normalization_candidates": normalization_candidates,
                    "teacher_proposed_repairs": raw_teacher_repairs,
                },
                sort_keys=True,
            ),
        },
    ]
    completion = verifier_client.complete(verifier_messages)
    prompt_tokens = completion.prompt_tokens
    completion_tokens = completion.completion_tokens
    warnings.extend(completion.warnings)
    if progress is not None:
        progress.api_done(
            label="flights_verifier",
            dataset=dataset.metadata.name,
            difficulty=difficulty,
            seed=seed,
            chunk_index=chunk_index,
            budget=budget,
            prompt_tokens=completion.prompt_tokens,
            completion_tokens=completion.completion_tokens,
        )
    messages.append(
        {
            "role": "user",
            "content": json.dumps({"flights_verifier": verifier_messages[1]}, sort_keys=True),
        }
    )
    messages.append({"role": "assistant", "content": completion.text})
    approvals = _verification_payload_approvals(_completion_payload(completion))
    verified_teacher_repairs = [
        repair for repair in teacher_repairs if _repair_key(repair) in approvals
    ]
    verified_repairs = candidate_repairs + verified_teacher_repairs
    dropped = proposed_count - len(verified_repairs)
    if dropped:
        warnings.append("unsupported_flights_repairs_dropped")
    return (
        verified_repairs,
        proposed_count,
        len(teacher_repairs),
        len(verified_repairs),
        dropped,
        1,
        prompt_tokens + completion_tokens,
    )


def _validation_feedback(
    *,
    payload: dict[str, Any] | None,
    normalization_candidates: list[NormalizationCandidate],
    row_indices: tuple[int, ...],
    columns: tuple[str, ...],
    dataset_name: str,
    flights_repair_mode: FlightsRepairMode,
    rows_by_index: dict[int, dict[str, str]],
) -> str | None:
    """Return one repair-focused feedback message when the teacher response is unusable."""
    if payload is None:
        return (
            "Your previous response was not valid JSON. Return exactly one JSON object. "
            "If target rows contain suspicious values, use submit_repairs."
        )
    action = payload.get("action")
    if action == "finish" and normalization_candidates:
        return (
            "You returned finish, but normalization_candidates lists suspicious target cells. "
            "Return submit_repairs for every candidate you agree with, using exact row ids, "
            "exact column names, and string new_value fields."
        )
    if action == "submit_repairs":
        issues = _repair_payload_issues(
            payload,
            row_indices=row_indices,
            columns=columns,
            dataset_name=dataset_name,
            normalization_candidates=normalization_candidates,
            flights_repair_mode=flights_repair_mode,
            rows_by_index=rows_by_index,
        )
        if issues:
            candidate_instruction = (
                " For Flights, only submit repairs that exactly match normalization_candidates; "
                "do not guess missing or changed actual times."
                if dataset_name == "flights" and flights_repair_mode == "strict"
                else (
                    " For Flights verified mode, non-candidate repairs may only target blank "
                    "scheduled time cells and must include evidence, confidence, and reason."
                    if dataset_name == "flights"
                    else ""
                )
            )
            return (
                "Your submit_repairs payload has validation issues: "
                + "; ".join(issues[:5])
                + ". Return a corrected submit_repairs JSON object."
                + candidate_instruction
            )
    return None


def _collect_chunk(
    dataset: RealWorldDataset,
    *,
    difficulty: Difficulty,
    seed: int,
    chunk_index: int,
    row_indices: tuple[int, ...],
    client: CompletionClient,
    budget: BudgetGuard,
    context_window_rows: int | None,
    flights_repair_mode: FlightsRepairMode = "strict",
    verifier_client: CompletionClient | None = None,
    deadline: RuntimeDeadline | None = None,
    progress: ProgressReporter | None = None,
) -> ChunkCollection:
    """Run the constrained ReAct loop for one chunk and return an auditable record."""
    task_id = f"{dataset.metadata.name}:{difficulty}"
    chunk_payload = _chunk_records(dataset, row_indices)
    context_indices = _context_row_indices(
        len(dataset.dirty_df.index),
        row_indices,
        context_window_rows,
    )
    context_payload = _chunk_records(dataset, context_indices)
    target_rows_by_index = {int(row["_row"]): row for row in chunk_payload}
    normalization_candidates = _normalization_candidates(
        dataset,
        row_indices=row_indices,
        context_indices=context_indices,
    )
    schema_summary = {
        "dataset": dataset.metadata.name,
        "columns": list(dataset.canonical_columns),
        "chunk_rows": len(row_indices),
        "target_row_indices": list(row_indices),
        "context_row_indices": list(context_indices),
        "difficulty": difficulty,
        "seed": seed,
    }
    flights_instruction = (
        "For Flights, submit only repairs that exactly match normalization_candidates; do not "
        "guess changed actual times or operational-note values such as runway notes."
        if flights_repair_mode == "strict"
        else (
            "For Flights verified mode, submit all normalization_candidates you agree with. "
            "You may additionally propose repairs only for blank scheduled departure/arrival "
            "time cells when dirty target/context rows support the exact canonical time. "
            "Every non-candidate Flights repair must include non-empty evidence, numeric "
            "confidence from 0 to 1, and reason. Do not propose operational-note corrections "
            "or unsupported actual-time edits."
        )
    )
    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert tabular data-cleaning teacher. Return exactly one JSON "
                "action object with no prose, markdown, or comments. Allowed actions are "
                "submit_repairs, inspect_rows, column_stats, and finish. Prefer submit_repairs "
                "whenever target rows contain suspicious values. Never repair context-only rows. "
                "Use exact target row ids, exact schema column names, and string new_value fields. "
                "For Beers: strip units from ounces (for example 12.0 oz. -> 12), strip a trailing "
                "% from abv, and convert N/A placeholders in numeric columns such as ibu to an "
                "empty string. For Flights: remove date/day prefixes from time fields, remove "
                "status suffixes such as Delayed or On Time, and fill blank scheduled times only "
                "when dirty references make the exact value unambiguous. "
                + flights_instruction
                + " Treat normalization_candidates "
                "as high-confidence dirty-data hints; submit every candidate you agree with. "
                "submit_repairs must use "
                '{"action":"submit_repairs","repairs":[{"row":0,"column":"Column",'
                '"new_value":"value","reason":"why"}]}.'
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "schema_summary": schema_summary,
                    "target_rows": chunk_payload,
                    "context_rows": context_payload,
                    "normalization_candidates": normalization_candidates,
                    "flights_repair_mode": flights_repair_mode
                    if dataset.metadata.name == "flights"
                    else None,
                },
                sort_keys=True,
            ),
        },
    ]

    llm_calls = 0
    prompt_tokens = 0
    completion_tokens = 0
    warnings: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    payloads: list[dict[str, Any]] = []
    repairs: list[BenchmarkRepair] = []

    def call_teacher(label: str) -> dict[str, Any] | None:
        """Call the teacher once and append the assistant response."""
        nonlocal llm_calls, prompt_tokens, completion_tokens
        if deadline is not None:
            deadline.raise_if_expired()
        if progress is not None:
            progress.api_start(
                label=label,
                dataset=dataset.metadata.name,
                difficulty=difficulty,
                seed=seed,
                chunk_index=chunk_index,
                budget=budget,
            )
        budget.consume()
        completion = client.complete(messages)
        llm_calls += 1
        prompt_tokens += completion.prompt_tokens
        completion_tokens += completion.completion_tokens
        warnings.extend(completion.warnings)
        if progress is not None:
            progress.api_done(
                label=label,
                dataset=dataset.metadata.name,
                difficulty=difficulty,
                seed=seed,
                chunk_index=chunk_index,
                budget=budget,
                prompt_tokens=completion.prompt_tokens,
                completion_tokens=completion.completion_tokens,
            )
        messages.append({"role": "assistant", "content": completion.text})
        payload = _completion_payload(completion)
        if payload is not None:
            payloads.append(payload)
        return payload

    def execute_tool(payload: dict[str, Any]) -> bool:
        """Execute one supported tool action and append its result."""
        action = payload.get("action")
        tool_result: dict[str, Any] | None = None
        if action == "inspect_rows":
            requested_rows = payload.get("row_indices", [])
            safe_rows = (
                [
                    row
                    for row in requested_rows
                    if isinstance(row, int) and 0 <= row < len(dataset.dirty_df.index)
                ]
                if isinstance(requested_rows, list)
                else []
            )
            tool_result = {"rows": _chunk_records(dataset, tuple(safe_rows))}
            tool_calls.append(
                {
                    "name": "inspect_rows",
                    "arguments": {"row_indices": safe_rows},
                    "result": tool_result,
                }
            )
        elif action == "column_stats":
            requested_columns = payload.get("columns", [])
            safe_columns = (
                [
                    column
                    for column in requested_columns
                    if isinstance(column, str) and column in dataset.canonical_columns
                ]
                if isinstance(requested_columns, list)
                else []
            )
            tool_result = {"column_stats": _column_stats(dataset, safe_columns)}
            tool_calls.append(
                {
                    "name": "column_stats",
                    "arguments": {"columns": safe_columns},
                    "result": tool_result,
                }
            )
        if tool_result is None:
            return False
        messages.append({"role": "user", "content": json.dumps(tool_result, sort_keys=True)})
        return True

    current_payload = call_teacher("first")
    if current_payload is not None and execute_tool(current_payload):
        current_payload = call_teacher("second")

    feedback = _validation_feedback(
        payload=current_payload,
        normalization_candidates=normalization_candidates,
        row_indices=row_indices,
        columns=dataset.canonical_columns,
        dataset_name=dataset.metadata.name,
        flights_repair_mode=flights_repair_mode,
        rows_by_index=target_rows_by_index,
    )
    if feedback is not None:
        warnings.append("validation_retry")
        messages.append(
            {
                "role": "user",
                "content": json.dumps({"validation_feedback": feedback}, sort_keys=True),
            }
        )
        current_payload = call_teacher("validation")

    raw_repairs = (
        current_payload.get("repairs", [])
        if current_payload is not None
        and current_payload.get("action") == "submit_repairs"
        and isinstance(current_payload.get("repairs"), list)
        else []
    )
    raw_repair_payloads = [raw_repair for raw_repair in raw_repairs if isinstance(raw_repair, dict)]
    if current_payload is not None and current_payload.get("action") == "submit_repairs":
        repairs.extend(_repairs_from_payload(current_payload))

    repairs = _repairs_for_rows(repairs, row_indices, dataset.canonical_columns)
    verifier_llm_calls = 0
    verifier_tokens = 0
    if dataset.metadata.name == "flights" and flights_repair_mode == "verified":
        (
            repairs,
            proposed_repairs,
            teacher_proposed_repairs,
            verified_repairs,
            dropped_repairs,
            verifier_llm_calls,
            verifier_tokens,
        ) = _verify_flights_repairs(
            repairs=repairs,
            raw_repairs=raw_repair_payloads,
            dataset=dataset,
            row_indices=row_indices,
            context_payload=context_payload,
            normalization_candidates=normalization_candidates,
            verifier_client=verifier_client,
            budget=budget,
            deadline=deadline,
            progress=progress,
            difficulty=difficulty,
            seed=seed,
            chunk_index=chunk_index,
            messages=messages,
            warnings=warnings,
        )
    else:
        repairs, dropped_repairs = _filter_flights_candidate_repairs(
            repairs,
            dataset_name=dataset.metadata.name,
            normalization_candidates=normalization_candidates,
        )
        proposed_repairs = len(raw_repair_payloads) if dataset.metadata.name == "flights" else 0
        teacher_proposed_repairs = 0
        verified_repairs = len(repairs) if dataset.metadata.name == "flights" else 0
        if dropped_repairs:
            warnings.append("unsupported_flights_repairs_dropped")
    chunk_score = _score_for_rows(dataset.ground_truth, repairs, row_indices)
    record = {
        "schema_version": SCHEMA_VERSION,
        "trajectory_id": f"{task_id}:{seed}:{chunk_index}",
        "task_id": task_id,
        "dataset": dataset.metadata.name,
        "difficulty": difficulty,
        "seed": seed,
        "chunk_index": chunk_index,
        "state": {
            "schema_summary": schema_summary,
            "target_rows": chunk_payload,
            "context_rows": context_payload,
            "normalization_candidates": normalization_candidates,
            "flights_repair_mode": flights_repair_mode
            if dataset.metadata.name == "flights"
            else None,
        },
        "tool_calls": tool_calls,
        "diagnosis": _diagnosis_from_payloads(payloads),
        "fix": [repair.model_dump(mode="json") for repair in repairs],
        "messages": messages,
        "teacher": {"provider": client.provider, "model": client.model},
        "metrics": {
            "chunk_precision": chunk_score.precision,
            "chunk_recall": chunk_score.recall,
            "chunk_f1": chunk_score.f1,
            "llm_calls": llm_calls,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "warnings": warnings,
            "unsupported_flights_repairs_dropped": dropped_repairs,
            "flights_proposed_repairs": proposed_repairs,
            "flights_teacher_proposed_repairs": teacher_proposed_repairs,
            "flights_verified_repairs": verified_repairs,
            "flights_dropped_repairs": dropped_repairs,
            "flights_repair_mode": flights_repair_mode
            if dataset.metadata.name == "flights"
            else None,
            "flights_verifier_llm_calls": verifier_llm_calls,
            "flights_verifier_tokens": verifier_tokens,
        },
        "provenance": {
            "citation": dataset.metadata.citation,
            "source_urls": list(dataset.metadata.source_urls),
            "collection_method": "llm_react_chunk",
        },
    }
    if progress is not None:
        progress.chunk_done(
            dataset=dataset.metadata.name,
            difficulty=difficulty,
            seed=seed,
            chunk_index=chunk_index,
            repairs=len(repairs),
            llm_calls=llm_calls,
        )
    return ChunkCollection(record=record, repairs=repairs)


def collect_episode_trajectories(
    dataset: RealWorldDataset,
    *,
    difficulty: Difficulty,
    seed: int,
    client: CompletionClient,
    existing_keys: set[TrajectoryKey],
    budget: BudgetGuard,
    min_episode_f1: float,
    context_window_rows: int | None = None,
    flights_repair_mode: FlightsRepairMode = "strict",
    verifier_client: CompletionClient | None = None,
    deadline: RuntimeDeadline | None = None,
    progress: ProgressReporter | None = None,
) -> list[dict[str, Any]]:
    """Collect chunk-level records for one episode, keeping only passing episodes."""
    task_id = f"{dataset.metadata.name}:{difficulty}"
    chunks = chunk_row_indices(len(dataset.dirty_df.index))
    collections: list[ChunkCollection] = []
    all_repairs: list[BenchmarkRepair] = []

    for chunk_index, row_indices in enumerate(chunks):
        if TrajectoryKey(task_id, seed, chunk_index) in existing_keys:
            continue
        if deadline is not None:
            deadline.raise_if_expired()
        if progress is not None:
            progress.chunk_start(
                dataset=dataset.metadata.name,
                difficulty=difficulty,
                seed=seed,
                chunk_index=chunk_index,
                total_chunks=len(chunks),
                budget=budget,
            )
        collection = _collect_chunk(
            dataset,
            difficulty=difficulty,
            seed=seed,
            chunk_index=chunk_index,
            row_indices=row_indices,
            client=client,
            budget=budget,
            context_window_rows=context_window_rows,
            flights_repair_mode=flights_repair_mode,
            verifier_client=verifier_client,
            deadline=deadline,
            progress=progress,
        )
        collections.append(collection)
        all_repairs.extend(collection.repairs)

    if not collections:
        return []

    episode_score = score_repairs(dataset.ground_truth, all_repairs)
    if progress is not None:
        progress.episode_score(
            dataset=dataset.metadata.name,
            difficulty=difficulty,
            seed=seed,
            score=episode_score,
            min_episode_f1=min_episode_f1,
        )
    if episode_score.f1 < min_episode_f1:
        return []

    records: list[dict[str, Any]] = []
    for collection in collections:
        metrics = dict(collection.record["metrics"])
        metrics.update(
            {
                "episode_precision": episode_score.precision,
                "episode_recall": episode_score.recall,
                "episode_f1": episode_score.f1,
                "episode_tp": episode_score.tp,
                "episode_fp": episode_score.fp,
                "episode_fn": episode_score.fn,
            }
        )
        collection.record["metrics"] = metrics
        records.append(validate_trajectory_record(collection.record))
    return records


def _parse_csv(value: str) -> list[str]:
    """Parse a comma-separated CLI value."""
    return [item.strip() for item in value.split(",") if item.strip()]


def _preset_defaults(preset: Preset) -> PresetDefaults:
    """Return defaults for one named collection preset."""
    return SMOKE_DEFAULTS if preset == "smoke" else FULL_DEFAULTS


def _default_teacher_model(provider: TeacherProvider) -> str:
    """Return the default model for one teacher provider."""
    if provider == "gemini":
        return DEFAULT_GEMINI_MODEL
    if provider == "azure":
        # Azure has no built-in default: the deployment name is account-specific.
        return os.environ.get("DATAFORGE_AZURE_MODEL", "")
    if provider == "grok":
        return DEFAULT_GROK_MODEL
    return DEFAULT_CEREBRAS_MODEL if provider == "cerebras" else DEFAULT_GROQ_MODEL


def _resolve_collection_settings(args: argparse.Namespace) -> CollectionSettings:
    """Apply preset defaults while allowing explicit CLI flags to override them."""
    preset = cast(Preset, args.preset)
    defaults = _preset_defaults(preset)
    datasets = (
        _parse_csv(cast(str, args.datasets))
        if args.datasets is not None
        else list(defaults.datasets)
    )
    raw_difficulties = (
        _parse_csv(cast(str, args.difficulties))
        if args.difficulties is not None
        else list(defaults.difficulties)
    )
    difficulties = cast(list[Difficulty], raw_difficulties)
    teacher_provider = cast(
        TeacherProvider,
        (args.teacher_provider or os.environ.get("DATAFORGE_LLM_PROVIDER") or "groq"),
    )
    teacher_model_arg = args.teacher_model if args.teacher_model is not None else args.groq_model
    teacher_max_tokens_arg = (
        args.teacher_max_tokens if args.teacher_max_tokens is not None else args.groq_max_tokens
    )
    teacher_timeout_arg = (
        args.teacher_timeout_s if args.teacher_timeout_s is not None else args.groq_timeout_s
    )
    teacher_retries_arg = (
        args.teacher_max_retries if args.teacher_max_retries is not None else args.groq_max_retries
    )
    default_min_interval_s = defaults.min_interval_s
    if teacher_provider == "cerebras":
        default_min_interval_s = max(default_min_interval_s, 2.1)
    settings = CollectionSettings(
        preset=preset,
        datasets=datasets,
        difficulties=difficulties,
        seeds=defaults.seeds if args.seeds is None else cast(int, args.seeds),
        max_trajectories=defaults.max_trajectories
        if args.max_trajectories is None
        else cast(int, args.max_trajectories),
        min_episode_f1=0.6 if args.min_episode_f1 is None else cast(float, args.min_episode_f1),
        max_total_requests=defaults.max_total_requests
        if args.max_total_requests is None
        else cast(int, args.max_total_requests),
        daily_request_budget=defaults.daily_request_budget
        if args.daily_request_budget is None
        else cast(int, args.daily_request_budget),
        teacher_provider=teacher_provider,
        teacher_model=_default_teacher_model(teacher_provider)
        if teacher_model_arg is None
        else cast(str, teacher_model_arg),
        teacher_max_tokens=512
        if teacher_max_tokens_arg is None
        else cast(int, teacher_max_tokens_arg),
        min_interval_s=default_min_interval_s
        if args.min_interval_s is None
        else cast(float, args.min_interval_s),
        teacher_timeout_s=defaults.groq_timeout_s
        if teacher_timeout_arg is None
        else cast(float, teacher_timeout_arg),
        teacher_max_retries=defaults.groq_max_retries
        if teacher_retries_arg is None
        else cast(int, teacher_retries_arg),
        teacher_max_retry_after_s=120.0
        if args.teacher_max_retry_after_s is None
        else cast(float, args.teacher_max_retry_after_s),
        context_window_rows=args.context_window_rows,
        flights_repair_mode=cast(FlightsRepairMode, args.flights_repair_mode),
        flights_verifier_model=args.flights_verifier_model,
        max_runtime_min=defaults.max_runtime_min
        if args.max_runtime_min is None
        else cast(float, args.max_runtime_min),
        progress_every_chunks=defaults.progress_every_chunks
        if args.progress_every_chunks is None
        else cast(int, args.progress_every_chunks),
        ready_min_records=defaults.ready_min_records
        if args.ready_min_records is None
        else cast(int, args.ready_min_records),
    )
    _validate_collection_settings(settings)
    return settings


def _validate_collection_settings(settings: CollectionSettings) -> None:
    """Validate resolved collection settings before any API work starts."""
    if not settings.datasets:
        raise ValueError("At least one dataset must be selected.")
    unknown_difficulties = sorted(set(settings.difficulties) - set(DEFAULT_DIFFICULTIES))
    if unknown_difficulties:
        raise ValueError(f"Unknown difficulties: {unknown_difficulties}")
    if settings.seeds < 1:
        raise ValueError("--seeds must be >= 1.")
    if settings.max_trajectories < 1:
        raise ValueError("--max-trajectories must be >= 1.")
    if settings.max_total_requests < 1:
        raise ValueError("--max-total-requests must be >= 1.")
    if settings.daily_request_budget < 1:
        raise ValueError("--daily-request-budget must be >= 1.")
    if settings.teacher_provider not in {"groq", "cerebras", "gemini", "azure"}:
        raise ValueError("--teacher-provider must be groq, grok, cerebras, gemini, or azure.")
    if settings.teacher_max_tokens < 1:
        raise ValueError("--teacher-max-tokens must be >= 1.")
    if settings.teacher_timeout_s <= 0:
        raise ValueError("--teacher-timeout-s must be > 0.")
    if settings.teacher_max_retries < 1:
        raise ValueError("--teacher-max-retries must be >= 1.")
    if settings.teacher_max_retry_after_s <= 0:
        raise ValueError("--teacher-max-retry-after-s must be > 0.")
    if settings.context_window_rows is not None and settings.context_window_rows < 0:
        raise ValueError("--context-window-rows must be >= 0 when provided.")
    if settings.flights_repair_mode not in {"strict", "verified"}:
        raise ValueError("--flights-repair-mode must be strict or verified.")
    if settings.flights_verifier_model is not None and not settings.flights_verifier_model.strip():
        raise ValueError("--flights-verifier-model must be non-empty when provided.")
    if settings.min_interval_s < 0:
        raise ValueError("--min-interval-s must be >= 0.")
    if settings.max_runtime_min is not None and settings.max_runtime_min <= 0:
        raise ValueError("--max-runtime-min must be > 0 when provided.")
    if settings.progress_every_chunks < 0:
        raise ValueError("--progress-every-chunks must be >= 0.")
    if settings.ready_min_records < 2:
        raise ValueError("--ready-min-records must be >= 2.")


def _teacher_api_key_env(provider: TeacherProvider) -> str:
    """Return the API-key environment variable for a teacher provider."""
    if provider == "gemini":
        return "GEMINI_API_KEY"
    if provider == "azure":
        return "AZURE_API_KEY"
    if provider == "grok":
        return "XAI_API_KEY"
    return "CEREBRAS_API_KEY" if provider == "cerebras" else "GROQ_API_KEY"


def _build_teacher_client(settings: CollectionSettings, *, api_key: str) -> CompletionClient:
    """Build the configured teacher client."""
    return _build_provider_client(
        provider=settings.teacher_provider,
        model=settings.teacher_model,
        api_key=api_key,
        min_interval_s=settings.min_interval_s,
        max_tokens=settings.teacher_max_tokens,
        max_retries=settings.teacher_max_retries,
        max_retry_after_s=settings.teacher_max_retry_after_s,
        timeout_s=settings.teacher_timeout_s,
    )


def _build_provider_client(
    *,
    provider: TeacherProvider,
    model: str,
    api_key: str,
    min_interval_s: float,
    max_tokens: int,
    max_retries: int,
    max_retry_after_s: float,
    timeout_s: float,
) -> CompletionClient:
    """Build one OpenAI-compatible provider client."""
    if provider == "azure":
        endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "").strip()
        if not endpoint:
            raise ValueError(
                "AZURE_OPENAI_ENDPOINT must be set to use the azure teacher provider "
                "(e.g. https://<resource>.openai.azure.com). See docs/azure-teacher-setup.md."
            )
        # Cap resolution is shared with the bench runner via dataforge.spend, so
        # the teacher path also honours the global DATAFORGE_MAX_USD fallback
        # (it previously read only the Azure-specific variable).
        return AzureBenchClient(
            api_key=api_key,
            model=model,
            endpoint=endpoint,
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2025-04-01-preview"),
            min_interval_s=min_interval_s,
            max_tokens=max_tokens,
            max_retries=max_retries,
            max_retry_after_s=max_retry_after_s,
            timeout_s=timeout_s,
            max_usd=cap_from_env("azure"),
            usd_per_1k_input=float(os.environ.get("DATAFORGE_AZURE_USD_PER_1K_INPUT", "0.005")),
            usd_per_1k_output=float(os.environ.get("DATAFORGE_AZURE_USD_PER_1K_OUTPUT", "0.015")),
            send_temperature=os.environ.get("DATAFORGE_AZURE_SEND_TEMPERATURE", "").strip().lower()
            in {"1", "true", "yes", "on"},
            reasoning_effort=os.environ.get("DATAFORGE_AZURE_REASONING_EFFORT", "").strip().lower()
            or None,
        )
    client_cls: (
        type[CerebrasBenchClient]
        | type[GeminiBenchClient]
        | type[GrokBenchClient]
        | type[GroqBenchClient]
    )
    if provider == "gemini":
        client_cls = GeminiBenchClient
    elif provider == "cerebras":
        client_cls = CerebrasBenchClient
    elif provider == "grok":
        client_cls = GrokBenchClient
    else:
        client_cls = GroqBenchClient
    return client_cls(
        api_key=api_key,
        model=model,
        min_interval_s=min_interval_s,
        max_tokens=max_tokens,
        max_retries=max_retries,
        max_retry_after_s=max_retry_after_s,
        timeout_s=timeout_s,
    )


def _resolve_hf_user(*, api: Any, token: str | None) -> str:
    """Resolve the authenticated Hugging Face username."""
    whoami = api.whoami(token=token)
    if not isinstance(whoami, dict) or not isinstance(whoami.get("name"), str):
        raise RuntimeError("Could not resolve Hugging Face username from HF_TOKEN.")
    return str(whoami["name"])


def resolve_dataset_repo_id(repo_id: str, *, api: Any, token: str | None) -> str:
    """Resolve `auto` to the current user's Week 9 trajectory dataset repo."""
    if repo_id != "auto":
        return repo_id
    return f"{_resolve_hf_user(api=api, token=token)}/{DEFAULT_DATASET_REPO_NAME}"


def push_trajectory_dataset(
    *,
    output: Path,
    repo_id: str,
    token: str | None,
    api: Any | None = None,
    split_manifest: Path = Path("data/sft_traj/split_manifest.json"),
) -> str:
    """Push generated trajectories and release metadata to an HF dataset repo."""
    if api is None:
        from huggingface_hub import HfApi

        api = HfApi(token=token)

    resolved_repo = resolve_dataset_repo_id(repo_id, api=api, token=token)
    api.create_repo(repo_id=resolved_repo, repo_type="dataset", exist_ok=True, token=token)
    api.upload_file(
        path_or_fileobj=str(output),
        path_in_repo=output.name,
        repo_id=resolved_repo,
        repo_type="dataset",
        token=token,
        commit_message="Upload Week 9 expert trajectories",
    )
    config_filename_by_trajectory = {
        "expert_v2.jsonl": "sft_05b_v2.yaml",
        "expert_v3.jsonl": "sft_05b_v3.yaml",
        "expert_v4.jsonl": "sft_05b_v4.yaml",
    }
    config_filename = config_filename_by_trajectory.get(output.name, "sft_05b.yaml")
    for path, path_in_repo in (
        (Path("training/DATASET_README.md"), "README.md"),
        (Path("training/configs") / config_filename, config_filename),
        (Path("training/MODEL_CARD_TEMPLATE.md"), "MODEL_CARD_TEMPLATE.md"),
        (split_manifest, split_manifest.name),
    ):
        if path.exists():
            api.upload_file(
                path_or_fileobj=str(path),
                path_in_repo=path_in_repo,
                repo_id=resolved_repo,
                repo_type="dataset",
                token=token,
                commit_message=f"Upload {path_in_repo}",
            )
    return resolved_repo


def _resolve_hf_token() -> str | None:
    """Resolve a Hugging Face token from env or the local token store."""
    token = (os.environ.get("HF_TOKEN") or "").strip()
    if token:
        return token
    try:
        from huggingface_hub import get_token
    except ImportError:
        return None
    return get_token()


def ensure_ready_for_push(
    *,
    output: Path,
    ready_min_records: int,
    split_manifest: Path = Path("data/sft_traj/split_manifest.json"),
) -> None:
    """Refuse to push partial or invalid trajectory datasets to Hugging Face."""
    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from scripts.data.validate_sft_readiness import validate_sft_readiness

    config_name_by_trajectory = {
        "expert_v2.jsonl": "sft_05b_v2.yaml",
        "expert_v3.jsonl": "sft_05b_v3.yaml",
        "expert_v4.jsonl": "sft_05b_v4.yaml",
    }
    config_name = config_name_by_trajectory.get(output.name, "sft_05b.yaml")
    validate_sft_readiness(
        jsonl=output,
        config_path=Path("training/configs") / config_name,
        split_manifest=split_manifest,
        min_records=ready_min_records,
    )


def _build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--preset",
        choices=("smoke", "full"),
        default="smoke",
        help="Collection preset. 'smoke' is laptop-safe; 'full' restores the original large run.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--datasets", default=None)
    parser.add_argument("--difficulties", default=None)
    parser.add_argument("--seeds", type=int, default=None)
    parser.add_argument("--max-trajectories", type=int, default=None)
    parser.add_argument("--min-episode-f1", type=float, default=None)
    parser.add_argument("--max-total-requests", type=int, default=None)
    parser.add_argument("--daily-request-budget", type=int, default=None)
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--push-to-hub", action="store_true")
    parser.add_argument("--hf-dataset-repo", default="auto")
    parser.add_argument(
        "--teacher-provider", choices=("groq", "grok", "cerebras", "gemini", "azure"), default=None
    )
    parser.add_argument("--teacher-model", default=None)
    parser.add_argument("--teacher-max-tokens", type=int, default=None)
    parser.add_argument("--teacher-timeout-s", type=float, default=None)
    parser.add_argument("--teacher-max-retries", type=int, default=None)
    parser.add_argument("--teacher-max-retry-after-s", type=float, default=None)
    parser.add_argument(
        "--context-window-rows",
        type=int,
        default=None,
        help="Limit context rows to N rows before/after the active chunk. Default: full table.",
    )
    parser.add_argument(
        "--flights-repair-mode",
        choices=("strict", "verified"),
        default="strict",
        help="Flights repair policy. strict accepts only candidates; verified adds verifier-approved proposals.",
    )
    parser.add_argument(
        "--flights-verifier-model",
        default=None,
        help="Optional verifier model for Flights verified mode. Defaults to the teacher model.",
    )
    parser.add_argument("--groq-model", default=None)
    parser.add_argument("--groq-max-tokens", type=int, default=None)
    parser.add_argument("--min-interval-s", type=float, default=None)
    parser.add_argument("--groq-timeout-s", type=float, default=None)
    parser.add_argument("--groq-max-retries", type=int, default=None)
    parser.add_argument("--max-runtime-min", type=float, default=None)
    parser.add_argument(
        "--progress-every-chunks",
        type=int,
        default=None,
        help="Print chunk/API progress every N chunks. Use 0 to disable.",
    )
    parser.add_argument("--ready-min-records", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the Week 9 trajectory collection CLI."""
    load_dotenv()
    args = _build_parser().parse_args(argv)
    settings = _resolve_collection_settings(args)
    console = Console()

    api_key_env = _teacher_api_key_env(settings.teacher_provider)
    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise RuntimeError(f"{api_key_env} is required for trajectory collection.")

    existing_keys = existing_trajectory_keys(args.output)
    budget = BudgetGuard(
        max_total_requests=settings.max_total_requests,
        daily_request_budget=settings.daily_request_budget,
    )
    client = _build_teacher_client(settings, api_key=api_key)
    verifier_client = (
        _build_provider_client(
            provider=settings.teacher_provider,
            model=settings.flights_verifier_model or settings.teacher_model,
            api_key=api_key,
            min_interval_s=settings.min_interval_s,
            max_tokens=settings.teacher_max_tokens,
            max_retries=settings.teacher_max_retries,
            max_retry_after_s=settings.teacher_max_retry_after_s,
            timeout_s=settings.teacher_timeout_s,
        )
        if settings.flights_repair_mode == "verified"
        else None
    )
    deadline = RuntimeDeadline(
        started_at=time.monotonic(),
        max_runtime_s=(
            settings.max_runtime_min * 60 if settings.max_runtime_min is not None else None
        ),
    )
    progress = ProgressReporter(
        console=console,
        deadline=deadline,
        every_chunks=settings.progress_every_chunks,
        accepted_records=len(existing_keys),
    )

    existing_count = len(existing_keys)
    target_new = max(settings.max_trajectories - existing_count, 0)
    collected = 0
    stop_collection = target_new == 0
    console.print(
        "[sft] starting collection "
        f"preset={settings.preset} datasets={','.join(settings.datasets)} "
        f"difficulties={','.join(settings.difficulties)} seeds={settings.seeds} "
        f"teacher={settings.teacher_provider}:{settings.teacher_model} "
        f"flights_repair_mode={settings.flights_repair_mode} "
        f"target={settings.max_trajectories} existing={existing_count} "
        f"request_budget={settings.max_total_requests} timeout_s={settings.teacher_timeout_s} "
        f"deadline_min={settings.max_runtime_min}"
    )
    for seed in range(settings.seeds):
        if stop_collection:
            break
        for dataset_name in settings.datasets:
            if stop_collection:
                break
            full_dataset = load_real_world_dataset(dataset_name, cache_root=args.cache_root)
            for difficulty in settings.difficulties:
                if stop_collection:
                    break
                light_dataset = build_light_dataset(full_dataset, difficulty=difficulty, seed=seed)
                try:
                    deadline.raise_if_expired()
                    records = collect_episode_trajectories(
                        light_dataset,
                        difficulty=difficulty,
                        seed=seed,
                        client=client,
                        existing_keys=existing_keys,
                        budget=budget,
                        min_episode_f1=settings.min_episode_f1,
                        context_window_rows=settings.context_window_rows,
                        flights_repair_mode=settings.flights_repair_mode,
                        verifier_client=verifier_client,
                        deadline=deadline,
                        progress=progress,
                    )
                except ProviderRequestError as exc:
                    console.print(
                        "[sft] episode skipped "
                        f"dataset={dataset_name} difficulty={difficulty} seed={seed} "
                        f"reason={exc}"
                    )
                    continue
                except RuntimeError as exc:
                    message = str(exc)
                    if "request budget" not in message and "runtime deadline" not in message:
                        raise
                    console.print(f"[sft] stopping cleanly: {exc}")
                    stop_collection = True
                    break
                if not records:
                    console.print(
                        "[sft] episode rejected "
                        f"dataset={dataset_name} difficulty={difficulty} seed={seed} "
                        f"accepted={existing_count + collected} elapsed={deadline.elapsed_s():.1f}s"
                    )
                    continue
                remaining = target_new - collected
                if remaining <= 0:
                    stop_collection = True
                    break
                records_to_write = records[:remaining]
                write_jsonl_records(args.output, records_to_write)
                for record in records_to_write:
                    existing_keys.add(
                        TrajectoryKey(
                            task_id=str(record["task_id"]),
                            seed=int(record["seed"]),
                            chunk_index=int(record["chunk_index"]),
                        )
                    )
                collected += len(records_to_write)
                progress.update_accepted(existing_count + collected)
                console.print(
                    "[sft] episode accepted "
                    f"dataset={dataset_name} difficulty={difficulty} seed={seed} "
                    f"wrote={len(records_to_write)} accepted={existing_count + collected} "
                    f"elapsed={deadline.elapsed_s():.1f}s"
                )
                if len(records_to_write) < len(records) or collected >= target_new:
                    stop_collection = True
                    break

    console.print(
        f"Collected {collected} new trajectories to {args.output} "
        f"({existing_count + collected}/{settings.max_trajectories} total cap) "
        f"using {budget.used_requests}/{budget.max_total_requests} teacher requests "
        f"(daily planning budget: {settings.daily_request_budget})."
    )
    if args.push_to_hub:
        ensure_ready_for_push(output=args.output, ready_min_records=settings.ready_min_records)
        token = _resolve_hf_token()
        repo_id = push_trajectory_dataset(
            output=args.output,
            repo_id=args.hf_dataset_repo,
            token=token,
        )
        console.print(f"Pushed trajectory dataset to {repo_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
