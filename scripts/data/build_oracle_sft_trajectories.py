"""Build split-safe SFT trajectories from dirty/clean CSV diffs."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from rich.console import Console

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.bench.core import BenchmarkRepair, score_repairs  # noqa: E402
from dataforge.bench.methods import _chunk_records  # noqa: E402
from dataforge.datasets.real_world import (  # noqa: E402
    GroundTruthCell,
    RealWorldDataset,
    load_real_world_dataset,
)
from dataforge.evaluation_contract import (  # noqa: E402
    ABSTENTION_SLICES,
    AUXILIARY_SLICES,
    PROMOTION_SLICE,
    InferabilityLabel,
)
from dataforge.repair_contract import (  # noqa: E402
    CONTRACT_VERSION,
    CONTRACT_VERSION_V1,
    CONTRACT_VERSION_V2,
    render_repair_messages,
)
from scripts.data.collect_sft_trajectories import (  # noqa: E402
    DEFAULT_DATASET_REPO_NAME,
    DEFAULT_DATASETS,
    DEFAULT_OUTPUT,
    SCHEMA_VERSION,
    Difficulty,
    NormalizationCandidate,
    _normalization_candidates,
    ensure_ready_for_push,
    push_trajectory_dataset,
    validate_trajectory_record,
)

DEFAULT_CONFIG = Path("training/configs/sft_05b.yaml")
DEFAULT_SPLIT_MANIFEST = Path("data/sft_traj/split_manifest.json")
DEFAULT_SPLIT_SEED = 42
DEFAULT_EVAL_FRACTION = 0.1
DEFAULT_CHUNK_ROWS = 32
DEFAULT_CONTEXT_WINDOW_ROWS = 24
ORACLE_PROVIDER = "oracle"
ORACLE_MODEL = "clean-diff-v1"
COLLECTION_METHOD = "oracle_from_clean_diff"
SYNTHETIC_COLLECTION_METHOD = "synthetic_from_canonical_train_rows"
HOSPITAL_SYNTHETIC_DATASET = "hospital_synthetic_deterministic_v1"
HOSPITAL_SYNTHETIC_POLICY = "hospital_format_normalization_v1"
SPLIT_MANIFEST_SCHEMA = "split_manifest_v1"
INFERABLE_LABELS = {"deterministic_normalization", "context_derivable"}
INFERABILITY_LABELS = INFERABLE_LABELS | {
    "external_reference_required",
    "not_inferable_from_prompt",
}
INFERABILITY_CHOICES = INFERABILITY_LABELS | {"auto"}
EXPERT_V4_SCHEMA = "expert_v4"
InferabilitySetting = InferabilityLabel | Literal["auto"]


@dataclass(frozen=True, slots=True)
class OracleSettings:
    """Resolved settings for deterministic oracle trajectory generation."""

    datasets: tuple[str, ...]
    difficulties: tuple[Difficulty, ...]
    split_seed: int
    eval_fraction: float
    min_eval_rows: int
    chunk_rows: int
    context_window_rows: int
    include_noop_records: bool
    schema_version: str
    prompt_contract_version: str
    max_repairs_per_record: int | None
    min_noop_ratio: float
    ready_min_records: int
    output: Path
    manifest_output: Path
    overwrite: bool
    inferability: InferabilitySetting = "auto"
    train_only_inferable: bool = False
    abstain_noninferable: bool = False
    include_context_derivable: bool = True
    include_hospital_synthetic: bool = False
    hospital_synthetic_records_per_difficulty: int = 128


@dataclass(frozen=True, slots=True)
class RowSplit:
    """Deterministic row split for one dataset."""

    train_rows: tuple[int, ...]
    eval_rows: tuple[int, ...]


def _as_mapping(value: object, *, name: str) -> dict[str, Any]:
    """Return a loaded YAML object as a string-keyed mapping."""
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a mapping.")
    return cast(dict[str, Any], value)


def load_oracle_config(path: Path) -> dict[str, Any]:
    """Load the SFT YAML config used to own oracle split defaults."""
    if not path.exists():
        return {}
    return _as_mapping(yaml.safe_load(path.read_text(encoding="utf-8")), name=str(path))


def _csv_list(value: str | None, *, default: tuple[str, ...]) -> tuple[str, ...]:
    """Parse a comma-separated CLI list."""
    if value is None:
        return default
    parsed = tuple(item.strip() for item in value.split(",") if item.strip())
    if not parsed:
        raise ValueError("Comma-separated list must contain at least one value.")
    return parsed


def _difficulty_list(
    value: str | None, *, default: tuple[Difficulty, ...]
) -> tuple[Difficulty, ...]:
    """Parse and validate comma-separated difficulty values."""
    raw = _csv_list(value, default=cast(tuple[str, ...], default))
    unknown = sorted(set(raw) - {"easy", "medium"})
    if unknown:
        raise ValueError(f"Unknown difficulties: {unknown}")
    return cast(tuple[Difficulty, ...], raw)


def _collection_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return the collection section from the SFT config."""
    raw = config.get("collection", {})
    return _as_mapping(raw, name="collection") if raw else {}


def _oracle_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return the oracle section from the SFT config."""
    collection = _collection_config(config)
    raw = collection.get("oracle", {})
    return _as_mapping(raw, name="collection.oracle") if raw else {}


def _repos_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return the repos section from the SFT config."""
    raw = config.get("repos", {})
    return _as_mapping(raw, name="repos") if raw else {}


def resolve_settings(args: argparse.Namespace) -> OracleSettings:
    """Resolve CLI options and YAML defaults into builder settings."""
    config = load_oracle_config(cast(Path, args.config))
    collection = _collection_config(config)
    oracle = _oracle_config(config)
    datasets_default = (
        tuple(
            str(item)
            for item in collection.get("datasets", DEFAULT_DATASETS)
            if isinstance(item, str)
        )
        or DEFAULT_DATASETS
    )
    difficulties_default = _difficulty_list(
        None,
        default=tuple(
            cast(Difficulty, str(item))
            for item in collection.get("difficulties", ("easy", "medium"))
            if isinstance(item, str)
        )
        or ("easy", "medium"),
    )
    repos = _repos_config(config)
    output = cast(Path, args.output)
    if output == DEFAULT_OUTPUT and isinstance(repos.get("trajectory_filename"), str):
        output = DEFAULT_OUTPUT.parent / str(repos["trajectory_filename"])
    manifest_output = cast(Path, args.manifest_output)
    if manifest_output == DEFAULT_SPLIT_MANIFEST and isinstance(
        repos.get("split_manifest_filename"), str
    ):
        manifest_output = DEFAULT_SPLIT_MANIFEST.parent / str(repos["split_manifest_filename"])
    raw_max_repairs = oracle.get("max_repairs_per_record")
    if args.max_repairs_per_record is not None:
        max_repairs_per_record = cast(int, args.max_repairs_per_record)
    elif raw_max_repairs is None:
        max_repairs_per_record = None
    else:
        max_repairs_per_record = int(str(raw_max_repairs))
    settings = OracleSettings(
        datasets=_csv_list(args.datasets, default=datasets_default),
        difficulties=_difficulty_list(args.difficulties, default=difficulties_default),
        split_seed=(
            int(oracle.get("split_seed", DEFAULT_SPLIT_SEED))
            if args.split_seed is None
            else cast(int, args.split_seed)
        ),
        eval_fraction=(
            float(oracle.get("eval_fraction", DEFAULT_EVAL_FRACTION))
            if args.eval_fraction is None
            else cast(float, args.eval_fraction)
        ),
        min_eval_rows=(
            int(oracle.get("min_eval_rows", 1))
            if args.min_eval_rows is None
            else cast(int, args.min_eval_rows)
        ),
        chunk_rows=(
            int(oracle.get("chunk_rows", DEFAULT_CHUNK_ROWS))
            if args.chunk_rows is None
            else cast(int, args.chunk_rows)
        ),
        context_window_rows=(
            int(oracle.get("context_window_rows", DEFAULT_CONTEXT_WINDOW_ROWS))
            if args.context_window_rows is None
            else cast(int, args.context_window_rows)
        ),
        include_noop_records=bool(oracle.get("include_noop_records", False))
        and not cast(bool, args.skip_noop_records),
        schema_version=str(
            _collection_config(config).get("schema_version", SCHEMA_VERSION)
            if args.schema_version is None
            else cast(str, args.schema_version)
        ),
        prompt_contract_version=str(
            _collection_config(config).get("prompt_contract_version", CONTRACT_VERSION)
        ),
        inferability=cast(
            InferabilitySetting,
            str(
                oracle.get(
                    "inferability",
                    "auto",
                )
                if args.inferability is None
                else cast(str, args.inferability)
            ),
        ),
        train_only_inferable=bool(
            oracle.get("train_only_inferable", False)
            if args.train_only_inferable is None
            else cast(bool, args.train_only_inferable)
        ),
        abstain_noninferable=bool(oracle.get("abstain_noninferable", False)),
        include_context_derivable=bool(oracle.get("include_context_derivable", True)),
        include_hospital_synthetic=bool(
            oracle.get("include_hospital_synthetic", False)
            if args.include_hospital_synthetic is None
            else cast(bool, args.include_hospital_synthetic)
        ),
        hospital_synthetic_records_per_difficulty=(
            int(oracle.get("hospital_synthetic_records_per_difficulty", 128))
            if args.hospital_synthetic_records_per_difficulty is None
            else cast(int, args.hospital_synthetic_records_per_difficulty)
        ),
        max_repairs_per_record=max_repairs_per_record,
        min_noop_ratio=(
            float(oracle.get("min_noop_ratio", 0.0))
            if args.min_noop_ratio is None
            else cast(float, args.min_noop_ratio)
        ),
        ready_min_records=(
            int(oracle.get("ready_min_records", 32))
            if args.ready_min_records is None
            else cast(int, args.ready_min_records)
        ),
        output=output,
        manifest_output=manifest_output,
        overwrite=not cast(bool, args.append),
    )
    validate_settings(settings)
    return settings


def validate_settings(settings: OracleSettings) -> None:
    """Validate oracle builder settings before reading datasets."""
    if not settings.datasets:
        raise ValueError("At least one dataset is required.")
    if not settings.difficulties:
        raise ValueError("At least one difficulty is required.")
    if settings.eval_fraction <= 0.0 or settings.eval_fraction >= 0.5:
        raise ValueError("--eval-fraction must be > 0 and < 0.5.")
    if settings.min_eval_rows < 1:
        raise ValueError("--min-eval-rows must be >= 1.")
    if settings.chunk_rows < 1:
        raise ValueError("--chunk-rows must be >= 1.")
    if settings.context_window_rows < 0:
        raise ValueError("--context-window-rows must be >= 0.")
    if settings.schema_version not in {"expert_v1", "expert_v2", "expert_v3", EXPERT_V4_SCHEMA}:
        raise ValueError("--schema-version must be expert_v1, expert_v2, expert_v3, or expert_v4.")
    if settings.inferability not in INFERABILITY_CHOICES:
        raise ValueError(
            "--inferability must be one of: " + ", ".join(sorted(INFERABILITY_CHOICES))
        )
    if settings.train_only_inferable and settings.inferability not in INFERABLE_LABELS | {"auto"}:
        raise ValueError(
            "--train-only-inferable requires inferability deterministic_normalization "
            "or context_derivable, or --inferability auto."
        )
    if settings.max_repairs_per_record is not None and settings.max_repairs_per_record < 1:
        raise ValueError("--max-repairs-per-record must be >= 1.")
    if settings.min_noop_ratio < 0.0 or settings.min_noop_ratio >= 1.0:
        raise ValueError("--min-noop-ratio must be >= 0 and < 1.")
    if settings.ready_min_records < 2:
        raise ValueError("--ready-min-records must be >= 2.")
    if settings.hospital_synthetic_records_per_difficulty < 1:
        raise ValueError("--hospital-synthetic-records-per-difficulty must be >= 1.")


def deterministic_row_split(
    *,
    dataset_name: str,
    n_rows: int,
    split_seed: int,
    eval_fraction: float,
    min_eval_rows: int = 1,
) -> RowSplit:
    """Split rows deterministically by stable hash, independent of source order quirks."""
    if n_rows < 2:
        raise ValueError("Need at least two rows for a non-empty train/eval split.")
    eval_rows_count = min(n_rows - 1, max(min_eval_rows, round(n_rows * eval_fraction)))
    ranked_rows = sorted(
        range(n_rows),
        key=lambda row: hashlib.sha256(f"{dataset_name}:{split_seed}:{row}".encode()).hexdigest(),
    )
    eval_rows = frozenset(ranked_rows[:eval_rows_count])
    train_rows = tuple(row for row in range(n_rows) if row not in eval_rows)
    return RowSplit(train_rows=train_rows, eval_rows=tuple(sorted(eval_rows)))


def _dirty_row_hash(dataset: RealWorldDataset, row: int) -> str:
    """Return a stable hash of one dirty source row without exposing clean labels."""
    values = {
        str(column): str(dataset.dirty_df.iloc[row][column]) for column in dataset.canonical_columns
    }
    payload = {
        "dataset": dataset.metadata.name,
        "row": row,
        "dirty_values": values,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _manifest_rows(dataset: RealWorldDataset, rows: tuple[int, ...]) -> list[dict[str, Any]]:
    """Return manifest row references containing only row ids and dirty-row hashes."""
    return [
        {
            "row": row,
            "dirty_row_sha256": _dirty_row_hash(dataset, row),
        }
        for row in rows
    ]


def chunk_train_rows(
    train_rows: tuple[int, ...], *, chunk_rows: int
) -> tuple[tuple[int, ...], ...]:
    """Chunk already-split train rows without reintroducing held-out rows."""
    return tuple(
        train_rows[start : start + chunk_rows] for start in range(0, len(train_rows), chunk_rows)
    )


def context_rows_for_chunk(
    train_rows: tuple[int, ...],
    row_indices: tuple[int, ...],
    *,
    context_window_rows: int,
) -> tuple[int, ...]:
    """Return context rows drawn only from the training split."""
    if not row_indices:
        return ()
    start = min(row_indices) - context_window_rows
    stop = max(row_indices) + context_window_rows
    return tuple(row for row in train_rows if start <= row <= stop)


def _truth_by_row(
    ground_truth: tuple[GroundTruthCell, ...],
    *,
    allowed_rows: set[int],
) -> dict[int, list[GroundTruthCell]]:
    """Group ground-truth cells by row for train-only repair construction."""
    grouped: dict[int, list[GroundTruthCell]] = {}
    for cell in ground_truth:
        if cell.row in allowed_rows:
            grouped.setdefault(cell.row, []).append(cell)
    return grouped


def _repairs_from_truth(cells: list[GroundTruthCell]) -> list[BenchmarkRepair]:
    """Convert dirty/clean diff cells into exact supervised repairs."""
    return [
        BenchmarkRepair(
            row=cell.row,
            column=cell.column,
            new_value=cell.clean_value,
            reason=f"oracle clean diff: replace dirty {cell.column!r} value",
        )
        for cell in sorted(cells, key=lambda item: (item.row, item.column))
    ]


def _hospital_synthetic_dirty_value(column: str, clean_value: str) -> str | None:
    """Return a deterministic Hospital formatting corruption, when one is defensible."""
    value = str(clean_value).strip()
    digits = "".join(char for char in value if char.isdigit())
    if column == "PhoneNumber" and len(digits) == 10 and value == digits:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if column == "ProviderNumber" and value.isdigit():
        return f"provider-{value}"
    if column == "ZipCode" and len(digits) == 5 and value == digits:
        return f"{digits}-0000"
    if column == "State" and len(value) == 2 and value.islower():
        return value.upper()
    if column == "EmergencyService" and value in {"yes", "no"}:
        return value.upper()
    return None


def _hospital_synthetic_candidates(
    dataset: RealWorldDataset,
    *,
    train_rows: tuple[int, ...],
) -> list[tuple[int, str, str, str]]:
    """Return stable synthetic Hospital repair candidates from canonical train rows."""
    if dataset.metadata.name != "hospital":
        return []
    candidates: list[tuple[int, str, str, str]] = []
    candidate_columns = (
        "PhoneNumber",
        "ProviderNumber",
        "ZipCode",
        "State",
        "EmergencyService",
    )
    for row in train_rows:
        clean_row = dataset.clean_df.iloc[row]
        for column in candidate_columns:
            if column not in dataset.clean_df.columns:
                continue
            clean_value = str(clean_row[column])
            dirty_value = _hospital_synthetic_dirty_value(column, clean_value)
            if dirty_value is None or dirty_value == clean_value:
                continue
            candidates.append((row, column, dirty_value, clean_value))
    return sorted(
        candidates,
        key=lambda item: hashlib.sha256(
            f"{HOSPITAL_SYNTHETIC_POLICY}:{dataset.metadata.source_revision}:"
            f"{item[0]}:{item[1]}:{item[2]}".encode()
        ).hexdigest(),
    )


def _clean_row_with_synthetic_dirty_cell(
    dataset: RealWorldDataset,
    *,
    row: int,
    column: str,
    dirty_value: str,
) -> dict[str, str]:
    """Return one canonical Hospital row with a single synthetic dirty cell."""
    clean_row = dataset.clean_df.iloc[row]
    rendered = {str(name): str(clean_row[name]) for name in dataset.canonical_columns}
    rendered[column] = dirty_value
    rendered["_row"] = str(row)
    return rendered


def build_hospital_synthetic_records(
    dataset: RealWorldDataset,
    *,
    difficulty: Difficulty,
    split_seed: int,
    eval_fraction: float,
    min_eval_rows: int,
    records_per_difficulty: int,
    schema_version: str = EXPERT_V4_SCHEMA,
    prompt_contract_version: str = CONTRACT_VERSION_V2,
) -> list[dict[str, Any]]:
    """Build provenance-marked Hospital repair examples from train rows only."""
    if dataset.metadata.name != "hospital" or records_per_difficulty <= 0:
        return []
    split = deterministic_row_split(
        dataset_name=dataset.metadata.name,
        n_rows=len(dataset.dirty_df.index),
        split_seed=split_seed,
        eval_fraction=eval_fraction,
        min_eval_rows=min_eval_rows,
    )
    candidates = _hospital_synthetic_candidates(dataset, train_rows=split.train_rows)
    records: list[dict[str, Any]] = []
    for chunk_index, (row, column, dirty_value, clean_value) in enumerate(
        candidates[:records_per_difficulty]
    ):
        if row in split.eval_rows:
            raise RuntimeError("Synthetic Hospital construction attempted to include eval rows.")
        target_rows = [
            _clean_row_with_synthetic_dirty_cell(
                dataset,
                row=row,
                column=column,
                dirty_value=dirty_value,
            )
        ]
        schema_summary = {
            "dataset": HOSPITAL_SYNTHETIC_DATASET,
            "base_dataset": dataset.metadata.name,
            "columns": list(dataset.canonical_columns),
            "chunk_rows": 1,
            "target_row_indices": [row],
            "context_row_indices": [],
            "difficulty": difficulty,
            "seed": split_seed,
            "split": "train",
            "synthetic_policy": HOSPITAL_SYNTHETIC_POLICY,
        }
        repair = BenchmarkRepair(
            row=row,
            column=column,
            new_value=clean_value,
            reason=(
                "synthetic deterministic Hospital normalization: "
                f"canonicalize {column!r} formatting"
            ),
        )
        task_id = f"{HOSPITAL_SYNTHETIC_DATASET}:{difficulty}"
        record = {
            "schema_version": schema_version,
            "trajectory_id": f"{task_id}:{split_seed}:{chunk_index}",
            "task_id": task_id,
            "dataset": HOSPITAL_SYNTHETIC_DATASET,
            "difficulty": difficulty,
            "seed": split_seed,
            "chunk_index": chunk_index,
            "state": {
                "schema_summary": schema_summary,
                "target_rows": target_rows,
                "context_rows": [],
                "normalization_candidates": [],
                "split": "train",
                "heldout_policy": {
                    "split_seed": split_seed,
                    "eval_fraction": eval_fraction,
                    "min_eval_rows": min_eval_rows,
                },
            },
            "tool_calls": [],
            "diagnosis": [repair.reason],
            "fix": [repair.model_dump(mode="json")],
            "messages": render_repair_messages(
                schema_summary=schema_summary,
                target_rows=target_rows,
                context_rows=[],
                allowed_columns=dataset.canonical_columns,
                valid_rows=[row],
                label_source=SYNTHETIC_COLLECTION_METHOD,
                dataset_note=(
                    "Synthetic Hospital formatting-normalization examples derived only "
                    "from canonical training rows; they are auxiliary learning signal, "
                    "not real-world benchmark evidence."
                ),
                metadata={
                    "base_dataset": dataset.metadata.name,
                    "synthetic_policy": HOSPITAL_SYNTHETIC_POLICY,
                    "source_revision": dataset.metadata.source_revision,
                },
                repairs=[repair],
                contract_version=prompt_contract_version,
            ),
            "teacher": {"provider": ORACLE_PROVIDER, "model": ORACLE_MODEL},
            "metrics": {
                "chunk_precision": 1.0,
                "chunk_recall": 1.0,
                "chunk_f1": 1.0,
                "episode_precision": 1.0,
                "episode_recall": 1.0,
                "episode_f1": 1.0,
                "episode_tp": 1,
                "episode_fp": 0,
                "episode_fn": 0,
                "llm_calls": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "warnings": [],
            },
            "provenance": {
                "citation": dataset.metadata.citation,
                "source_urls": list(dataset.metadata.source_urls),
                "collection_method": SYNTHETIC_COLLECTION_METHOD,
                "base_collection_method": COLLECTION_METHOD,
                "label_source": "synthetic_dirty_from_clean_train_row",
                "synthetic_policy": HOSPITAL_SYNTHETIC_POLICY,
                "base_dataset": dataset.metadata.name,
                "split": "train",
                "split_seed": split_seed,
                "eval_fraction": eval_fraction,
                "eval_rows": list(split.eval_rows),
                "prompt_contract_version": prompt_contract_version,
                "inferability": PROMOTION_SLICE,
                "source_revision": dataset.metadata.source_revision,
                "dirty_sha256": dataset.dirty_sha256,
                "clean_sha256": dataset.clean_sha256,
            },
            "prompt_contract_version": prompt_contract_version,
            "inferability": PROMOTION_SLICE,
        }
        records.append(validate_trajectory_record(record))
    return records


def _candidate_keys(
    candidates: list[NormalizationCandidate],
    *,
    dataset_name: str,
    deterministic_only: bool = False,
) -> set[tuple[int, str, str]]:
    """Return exact deterministic candidate keys."""
    keys: set[tuple[int, str, str]] = set()
    for candidate in candidates:
        if (
            deterministic_only
            and dataset_name == "flights"
            and candidate.get("tier") != "normalization"
        ):
            continue
        row = candidate.get("row")
        column = candidate.get("column")
        suggested_value = candidate.get("suggested_value")
        if isinstance(row, int) and isinstance(column, str) and isinstance(suggested_value, str):
            keys.add((row, column, suggested_value))
    return keys


def inferability_for_record(
    *,
    dataset: RealWorldDataset,
    row_indices: tuple[int, ...],
    context_indices: tuple[int, ...],
    repairs: list[BenchmarkRepair],
    configured: InferabilitySetting,
    strict_deterministic: bool = False,
) -> InferabilityLabel:
    """Classify whether the clean labels are inferable from the prompt alone."""
    if configured != "auto":
        return configured
    if not repairs:
        return "not_inferable_from_prompt" if strict_deterministic else PROMOTION_SLICE
    candidates = _normalization_candidates(
        dataset,
        row_indices=row_indices,
        context_indices=context_indices,
    )
    candidate_keys = _candidate_keys(candidates, dataset_name=dataset.metadata.name)
    deterministic_keys = _candidate_keys(
        candidates,
        dataset_name=dataset.metadata.name,
        deterministic_only=strict_deterministic,
    )
    repair_keys = {(repair.row, repair.column, repair.new_value) for repair in repairs}
    if repair_keys and repair_keys.issubset(deterministic_keys):
        return PROMOTION_SLICE
    if strict_deterministic and repair_keys and repair_keys.issubset(candidate_keys):
        return "context_derivable"
    return "external_reference_required"


def _v4_output_repairs(
    repairs: list[BenchmarkRepair],
    *,
    inferability: InferabilityLabel,
    abstain_noninferable: bool,
) -> list[BenchmarkRepair]:
    """Return the supervised assistant repairs for the v4 contract."""
    if not abstain_noninferable:
        return repairs
    if inferability in ABSTENTION_SLICES or inferability in AUXILIARY_SLICES:
        return []
    return repairs


def _diagnosis_for_record(
    *,
    output_repairs: list[BenchmarkRepair],
    original_repairs: list[BenchmarkRepair],
    inferability: InferabilityLabel,
) -> list[str]:
    """Return short audit text for repair or abstention records."""
    if output_repairs:
        return [repair.reason for repair in output_repairs]
    if original_repairs and inferability == "external_reference_required":
        return ["abstain: exact clean value requires an external reference"]
    if original_repairs and inferability == "context_derivable":
        return ["abstain: context-derived repair is reported as auxiliary only"]
    return ["finish: no deterministic repair is justified by the prompt"]


def _messages_for_record(
    *,
    dataset: RealWorldDataset,
    schema_summary: dict[str, Any],
    target_rows: list[dict[str, str]],
    context_rows: list[dict[str, str]],
    repairs: list[BenchmarkRepair],
    prompt_contract_version: str = CONTRACT_VERSION,
) -> list[dict[str, str]]:
    """Build chat messages for SFT from already-known oracle labels."""
    if prompt_contract_version not in {CONTRACT_VERSION_V1, CONTRACT_VERSION_V2}:
        raise ValueError(f"Unsupported repair prompt contract: {prompt_contract_version}")
    return render_repair_messages(
        schema_summary=schema_summary,
        target_rows=target_rows,
        context_rows=context_rows,
        allowed_columns=dataset.canonical_columns,
        valid_rows=[int(row["_row"]) for row in target_rows],
        label_source=COLLECTION_METHOD,
        dataset_note=(
            "Flights schedule and actual-time values are supervised from dirty/clean labels; "
            "do not infer schedules from incomplete context."
            if dataset.metadata.name == "flights"
            else "Repairs are supervised from dirty/clean labels."
        ),
        repairs=repairs,
        contract_version=prompt_contract_version,
    )


def build_dataset_records(
    dataset: RealWorldDataset,
    *,
    difficulty: Difficulty,
    split_seed: int,
    eval_fraction: float,
    min_eval_rows: int,
    chunk_rows: int,
    context_window_rows: int,
    include_noop_records: bool = False,
    schema_version: str = SCHEMA_VERSION,
    prompt_contract_version: str = CONTRACT_VERSION,
    inferability: InferabilitySetting = "auto",
    train_only_inferable: bool = False,
    abstain_noninferable: bool = False,
    include_context_derivable: bool = True,
    max_repairs_per_record: int | None = None,
) -> list[dict[str, Any]]:
    """Build train-only oracle records for one dataset/difficulty pair."""
    if train_only_inferable and inferability != "auto" and inferability not in INFERABLE_LABELS:
        return []
    strict_deterministic = schema_version == EXPERT_V4_SCHEMA
    split = deterministic_row_split(
        dataset_name=dataset.metadata.name,
        n_rows=len(dataset.dirty_df.index),
        split_seed=split_seed,
        eval_fraction=eval_fraction,
        min_eval_rows=min_eval_rows,
    )
    train_set = set(split.train_rows)
    eval_set = set(split.eval_rows)
    train_truth = _truth_by_row(dataset.ground_truth, allowed_rows=train_set)
    all_train_repairs = [
        repair
        for row in split.train_rows
        for repair in _repairs_from_truth(train_truth.get(row, []))
    ]
    episode_score = score_repairs(
        [cell for cell in dataset.ground_truth if cell.row in train_set],
        all_train_repairs,
    )
    records: list[dict[str, Any]] = []
    task_id = f"{dataset.metadata.name}:{difficulty}"
    chunk_index = 0
    for row_indices in chunk_train_rows(split.train_rows, chunk_rows=chunk_rows):
        row_set = set(row_indices)
        repairs = [
            repair
            for row in row_indices
            for repair in _repairs_from_truth(train_truth.get(row, []))
        ]
        if not repairs and not include_noop_records:
            continue
        context_indices = context_rows_for_chunk(
            split.train_rows,
            row_indices,
            context_window_rows=context_window_rows,
        )
        record_inferability = inferability_for_record(
            dataset=dataset,
            row_indices=row_indices,
            context_indices=context_indices,
            repairs=repairs,
            configured=inferability,
            strict_deterministic=strict_deterministic,
        )
        if train_only_inferable and record_inferability not in INFERABLE_LABELS:
            continue
        if (
            strict_deterministic
            and record_inferability in AUXILIARY_SLICES
            and not include_context_derivable
        ):
            continue
        output_repairs = _v4_output_repairs(
            repairs,
            inferability=record_inferability,
            abstain_noninferable=strict_deterministic and abstain_noninferable,
        )
        if max_repairs_per_record is not None and len(output_repairs) > max_repairs_per_record:
            continue
        if not output_repairs and not include_noop_records:
            continue
        if eval_set.intersection(row_indices) or eval_set.intersection(context_indices):
            raise RuntimeError("Oracle trajectory construction attempted to include eval rows.")
        chunk_truth = [cell for cell in dataset.ground_truth if cell.row in row_set]
        chunk_score = score_repairs(chunk_truth, output_repairs)
        if not chunk_truth and not output_repairs:
            chunk_metrics = {
                "chunk_precision": 1.0,
                "chunk_recall": 1.0,
                "chunk_f1": 1.0,
            }
        else:
            chunk_metrics = {
                "chunk_precision": chunk_score.precision,
                "chunk_recall": chunk_score.recall,
                "chunk_f1": chunk_score.f1,
            }
        target_rows = _chunk_records(dataset, row_indices)
        context_rows = _chunk_records(dataset, context_indices)
        schema_summary = {
            "dataset": dataset.metadata.name,
            "columns": list(dataset.canonical_columns),
            "chunk_rows": len(row_indices),
            "target_row_indices": list(row_indices),
            "context_row_indices": list(context_indices),
            "difficulty": difficulty,
            "seed": split_seed,
            "split": "train",
        }
        record = {
            "schema_version": schema_version,
            "trajectory_id": f"{task_id}:{split_seed}:{chunk_index}",
            "task_id": task_id,
            "dataset": dataset.metadata.name,
            "difficulty": difficulty,
            "seed": split_seed,
            "chunk_index": chunk_index,
            "state": {
                "schema_summary": schema_summary,
                "target_rows": target_rows,
                "context_rows": context_rows,
                "normalization_candidates": [],
                "split": "train",
                "heldout_policy": {
                    "split_seed": split_seed,
                    "eval_fraction": eval_fraction,
                    "min_eval_rows": min_eval_rows,
                },
            },
            "tool_calls": [],
            "diagnosis": _diagnosis_for_record(
                output_repairs=output_repairs,
                original_repairs=repairs,
                inferability=record_inferability,
            ),
            "fix": [repair.model_dump(mode="json") for repair in output_repairs],
            "messages": _messages_for_record(
                dataset=dataset,
                schema_summary=schema_summary,
                target_rows=target_rows,
                context_rows=context_rows,
                repairs=output_repairs,
                prompt_contract_version=prompt_contract_version,
            ),
            "teacher": {"provider": ORACLE_PROVIDER, "model": ORACLE_MODEL},
            "metrics": {
                **chunk_metrics,
                "episode_precision": episode_score.precision,
                "episode_recall": episode_score.recall,
                "episode_f1": episode_score.f1,
                "episode_tp": episode_score.tp,
                "episode_fp": episode_score.fp,
                "episode_fn": episode_score.fn,
                "llm_calls": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "warnings": [],
            },
            "provenance": {
                "citation": dataset.metadata.citation,
                "source_urls": list(dataset.metadata.source_urls),
                "collection_method": COLLECTION_METHOD,
                "label_source": "dirty_clean_csv_diff",
                "split": "train",
                "split_seed": split_seed,
                "eval_fraction": eval_fraction,
                "eval_rows": list(split.eval_rows),
                "prompt_contract_version": prompt_contract_version,
                "inferability": record_inferability,
            },
            "prompt_contract_version": prompt_contract_version,
            "inferability": record_inferability,
        }
        records.append(validate_trajectory_record(record))
        chunk_index += 1
    return records


def write_records(path: Path, records: list[dict[str, Any]], *, overwrite: bool) -> None:
    """Write validated records as JSONL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if overwrite else "a"
    with path.open(mode, encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(validate_trajectory_record(record), sort_keys=True) + "\n")


def build_split_manifest(
    settings: OracleSettings,
    *,
    cache_root: Path | None = None,
) -> dict[str, Any]:
    """Build a deterministic split manifest with row hashes and no clean labels."""
    datasets: list[dict[str, Any]] = []
    for dataset_name in settings.datasets:
        dataset = load_real_world_dataset(dataset_name, cache_root=cache_root)
        split = deterministic_row_split(
            dataset_name=dataset.metadata.name,
            n_rows=len(dataset.dirty_df.index),
            split_seed=settings.split_seed,
            eval_fraction=settings.eval_fraction,
            min_eval_rows=settings.min_eval_rows,
        )
        datasets.append(
            {
                "dataset": dataset.metadata.name,
                "domain": dataset.metadata.domain,
                "source_revision": dataset.metadata.source_revision,
                "dirty_sha256": dataset.dirty_sha256,
                "clean_sha256": dataset.clean_sha256,
                "n_rows": len(dataset.dirty_df.index),
                "n_columns": len(dataset.canonical_columns),
                "ground_truth_cells": len(dataset.ground_truth),
                "train_rows": len(split.train_rows),
                "eval_rows": len(split.eval_rows),
                "train": _manifest_rows(dataset, split.train_rows),
                "eval": _manifest_rows(dataset, split.eval_rows),
            }
        )
        if (
            dataset.metadata.name == "hospital"
            and settings.include_hospital_synthetic
            and settings.schema_version == EXPERT_V4_SCHEMA
        ):
            datasets.append(
                {
                    "dataset": HOSPITAL_SYNTHETIC_DATASET,
                    "base_dataset": dataset.metadata.name,
                    "domain": dataset.metadata.domain,
                    "source_revision": dataset.metadata.source_revision,
                    "dirty_sha256": dataset.dirty_sha256,
                    "clean_sha256": dataset.clean_sha256,
                    "n_rows": len(dataset.dirty_df.index),
                    "n_columns": len(dataset.canonical_columns),
                    "ground_truth_cells": len(dataset.ground_truth),
                    "train_rows": len(split.train_rows),
                    "eval_rows": len(split.eval_rows),
                    "train": _manifest_rows(dataset, split.train_rows),
                    "eval": _manifest_rows(dataset, split.eval_rows),
                    "collection_method": SYNTHETIC_COLLECTION_METHOD,
                    "synthetic_policy": HOSPITAL_SYNTHETIC_POLICY,
                    "synthetic_records_per_difficulty": (
                        settings.hospital_synthetic_records_per_difficulty
                    ),
                }
            )
    return {
        "schema_version": SPLIT_MANIFEST_SCHEMA,
        "collection_method": COLLECTION_METHOD,
        "label_visibility": (
            "No clean values, ground-truth cells, suggested values, or repair labels are "
            "stored in this manifest."
        ),
        "inferability": settings.inferability,
        "train_only_inferable": settings.train_only_inferable,
        "abstain_noninferable": settings.abstain_noninferable,
        "include_context_derivable": settings.include_context_derivable,
        "include_hospital_synthetic": settings.include_hospital_synthetic,
        "hospital_synthetic_dataset": HOSPITAL_SYNTHETIC_DATASET
        if settings.include_hospital_synthetic
        else None,
        "hospital_synthetic_policy": HOSPITAL_SYNTHETIC_POLICY
        if settings.include_hospital_synthetic
        else None,
        "promotion_slice": PROMOTION_SLICE,
        "split_seed": settings.split_seed,
        "eval_fraction": settings.eval_fraction,
        "min_eval_rows": settings.min_eval_rows,
        "datasets": datasets,
    }


def write_split_manifest(path: Path, manifest: dict[str, Any]) -> None:
    """Write the split manifest as stable, human-auditable JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_oracle_trajectories(
    settings: OracleSettings,
    *,
    cache_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Build all configured oracle records."""
    records: list[dict[str, Any]] = []
    for dataset_name in settings.datasets:
        dataset = load_real_world_dataset(dataset_name, cache_root=cache_root)
        for difficulty in settings.difficulties:
            records.extend(
                build_dataset_records(
                    dataset,
                    difficulty=difficulty,
                    split_seed=settings.split_seed,
                    eval_fraction=settings.eval_fraction,
                    min_eval_rows=settings.min_eval_rows,
                    chunk_rows=settings.chunk_rows,
                    context_window_rows=settings.context_window_rows,
                    include_noop_records=settings.include_noop_records,
                    schema_version=settings.schema_version,
                    prompt_contract_version=settings.prompt_contract_version,
                    inferability=settings.inferability,
                    train_only_inferable=settings.train_only_inferable,
                    abstain_noninferable=settings.abstain_noninferable,
                    include_context_derivable=settings.include_context_derivable,
                    max_repairs_per_record=settings.max_repairs_per_record,
                )
            )
            if (
                dataset.metadata.name == "hospital"
                and settings.include_hospital_synthetic
                and settings.schema_version == EXPERT_V4_SCHEMA
            ):
                records.extend(
                    build_hospital_synthetic_records(
                        dataset,
                        difficulty=difficulty,
                        split_seed=settings.split_seed,
                        eval_fraction=settings.eval_fraction,
                        min_eval_rows=settings.min_eval_rows,
                        records_per_difficulty=(
                            settings.hospital_synthetic_records_per_difficulty
                        ),
                        schema_version=settings.schema_version,
                        prompt_contract_version=settings.prompt_contract_version,
                    )
                )
    if records and settings.min_noop_ratio > 0.0:
        noop_records = sum(1 for record in records if not record["fix"])
        noop_ratio = noop_records / len(records)
        if noop_ratio < settings.min_noop_ratio:
            raise RuntimeError(
                "Oracle trajectory set does not meet the configured no-op ratio "
                f"({noop_ratio:.3f} < {settings.min_noop_ratio:.3f})."
            )
    return records


def _build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--manifest-output", type=Path, default=DEFAULT_SPLIT_MANIFEST)
    parser.add_argument("--datasets", default=None)
    parser.add_argument("--difficulties", default=None)
    parser.add_argument("--split-seed", type=int, default=None)
    parser.add_argument("--eval-fraction", type=float, default=None)
    parser.add_argument("--min-eval-rows", type=int, default=None)
    parser.add_argument("--chunk-rows", type=int, default=None)
    parser.add_argument("--context-window-rows", type=int, default=None)
    parser.add_argument("--schema-version", default=None)
    parser.add_argument(
        "--inferability",
        choices=tuple(sorted(INFERABILITY_CHOICES)),
        default=None,
        help="Inferability label applied to generated oracle records.",
    )
    parser.add_argument(
        "--train-only-inferable",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Emit records only when the selected inferability label is trainable.",
    )
    parser.add_argument(
        "--include-hospital-synthetic",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Append provenance-marked deterministic Hospital normalization examples.",
    )
    parser.add_argument("--hospital-synthetic-records-per-difficulty", type=int, default=None)
    parser.add_argument("--max-repairs-per-record", type=int, default=None)
    parser.add_argument("--min-noop-ratio", type=float, default=None)
    parser.add_argument("--skip-noop-records", action="store_true")
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--append", action="store_true")
    parser.add_argument("--push-to-hub", action="store_true")
    parser.add_argument("--hf-dataset-repo", default="auto")
    parser.add_argument("--ready-min-records", type=int, default=None)
    return parser


def _resolve_hf_token() -> str | None:
    """Resolve a Hugging Face token from env or the local token store."""
    import os

    token = (os.environ.get("HF_TOKEN") or "").strip()
    if token:
        return token
    try:
        from huggingface_hub import get_token
    except ImportError:
        return None
    return get_token()


def main(argv: list[str] | None = None) -> int:
    """Run oracle trajectory generation."""
    args = _build_parser().parse_args(argv)
    settings = resolve_settings(args)
    records = build_oracle_trajectories(settings, cache_root=args.cache_root)
    write_records(settings.output, records, overwrite=settings.overwrite)
    manifest = build_split_manifest(settings, cache_root=args.cache_root)
    write_split_manifest(settings.manifest_output, manifest)
    datasets = ", ".join(settings.datasets)
    Console().print(
        f"Wrote {len(records)} {COLLECTION_METHOD} records for {datasets} to {settings.output}."
    )
    Console().print(f"Wrote split manifest to {settings.manifest_output}.")
    if args.push_to_hub:
        ensure_ready_for_push(
            output=settings.output,
            ready_min_records=settings.ready_min_records,
            split_manifest=settings.manifest_output,
        )
        repo_id = push_trajectory_dataset(
            output=settings.output,
            repo_id=cast(str, args.hf_dataset_repo),
            token=_resolve_hf_token(),
            split_manifest=settings.manifest_output,
        )
        Console().print(f"Pushed trajectory dataset to {repo_id or DEFAULT_DATASET_REPO_NAME}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
