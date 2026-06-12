from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from importlib import import_module
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd
from pydantic import BaseModel, Field

from dataforge_evals.agents.base import GroundTruthCell, InferabilityLabel, Task

_CANONICAL_DATASETS = frozenset({"hospital", "flights", "beers"})


class TaskLoadError(RuntimeError):
    """Raised when an evaluation task cannot be loaded."""


class EvaluationTaskV2(BaseModel):
    """Serializable grading task with hidden labels excluded from JSON output."""

    schema_version: Literal["evaluation_task_v2"] = "evaluation_task_v2"
    task_id: str = Field(min_length=1)
    prompt_hash: str = Field(min_length=64, max_length=64)
    dataset_sha: str = Field(min_length=64, max_length=64)
    split_id: str = Field(min_length=1)
    inferability: InferabilityLabel
    prompt: dict[str, Any]
    allowed_columns: list[str] = Field(min_length=1)
    valid_rows: list[int] = Field(min_length=1)
    provenance: dict[str, Any]
    hidden_ground_truth: list[dict[str, Any]] = Field(default_factory=list, exclude=True)

    model_config = {"frozen": True}


def _prompt_hash(prompt: dict[str, Any]) -> str:
    encoded = json.dumps(prompt, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _dataset_sha(dirty_df: pd.DataFrame, canonical_columns: tuple[str, ...]) -> str:
    payload = {
        "columns": list(canonical_columns),
        "rows": dirty_df.astype(str).to_dict(orient="records"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def evaluation_task_v2(task: Task, *, split_id: str = "eval") -> EvaluationTaskV2:
    """Convert a runtime task into the v2 model-grading interface."""
    target_rows: list[dict[str, str]] = []
    for row_index, (_index, row) in enumerate(task.dirty_df.iterrows()):
        target_rows.append(
            {
                "_row": str(row_index),
                **{str(column): str(row[column]) for column in task.canonical_columns},
            }
        )
    prompt = {
        "schema_summary": {
            "dataset": task.name,
            "columns": list(task.canonical_columns),
            "rows": len(task.dirty_df.index),
            "split": split_id,
        },
        "allowed_columns": list(task.canonical_columns),
        "valid_rows": list(range(len(task.dirty_df.index))),
        "target_rows": target_rows,
        "context_rows": [],
    }
    return EvaluationTaskV2(
        task_id=task.name,
        prompt_hash=_prompt_hash(prompt),
        dataset_sha=_dataset_sha(task.dirty_df, task.canonical_columns),
        split_id=split_id,
        inferability=task.inferability,
        prompt=prompt,
        allowed_columns=list(task.canonical_columns),
        valid_rows=list(range(len(task.dirty_df.index))),
        provenance=dict(task.metadata),
        hidden_ground_truth=[cell.model_dump(mode="json") for cell in task.ground_truth],
    )


def _ground_truth_from_dataframes(
    dirty_df: pd.DataFrame,
    clean_df: pd.DataFrame,
) -> tuple[GroundTruthCell, ...]:
    """Compute exact cell corrections across aligned dirty and clean frames."""
    corrections: list[GroundTruthCell] = []
    for row_index, (dirty_row, clean_row) in enumerate(
        zip(
            dirty_df.itertuples(index=False, name=None),
            clean_df.itertuples(index=False, name=None),
            strict=True,
        )
    ):
        for column, dirty_value, clean_value in zip(
            clean_df.columns,
            dirty_row,
            clean_row,
            strict=True,
        ):
            dirty_text = str(dirty_value)
            clean_text = str(clean_value)
            if dirty_text != clean_text:
                corrections.append(
                    GroundTruthCell(
                        row=row_index,
                        column=str(column),
                        dirty_value=dirty_text,
                        clean_value=clean_text,
                    )
                )
    return tuple(corrections)


def _read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV using string-preserving defaults."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)


def load_csv_pair_task(
    *,
    name: str,
    dirty_csv: Path,
    clean_csv: Path,
) -> Task:
    """Load a custom task from aligned dirty and clean CSV files."""
    dirty_df = _read_csv(dirty_csv)
    clean_df = _read_csv(clean_csv)
    if len(dirty_df.index) != len(clean_df.index):
        raise TaskLoadError(f"Task '{name}' dirty and clean CSV row counts do not match.")
    if len(dirty_df.columns) != len(clean_df.columns):
        raise TaskLoadError(f"Task '{name}' dirty and clean CSV column counts do not match.")
    clean_columns = tuple(str(column) for column in clean_df.columns)
    dirty_df.columns = list(clean_columns)
    clean_df.columns = list(clean_columns)
    return Task(
        name=name,
        dirty_df=dirty_df,
        canonical_columns=clean_columns,
        ground_truth=_ground_truth_from_dataframes(dirty_df, clean_df),
        metadata={"source": "csv-pair", "rows": len(clean_df.index), "columns": len(clean_columns)},
        inferability="external_reference_required",
    )


def load_synthetic_task() -> Task:
    """Return a small deterministic no-network task for smoke tests and examples."""
    dirty_df = pd.DataFrame(
        [
            {"HospitalName": "Mercy Hosp", "Phone": "217-555-0100", "Score": "45"},
            {"HospitalName": "General Hospital", "Phone": "not available", "Score": "4.0"},
        ]
    )
    ground_truth = (
        GroundTruthCell(
            row=0, column="HospitalName", dirty_value="Mercy Hosp", clean_value="Mercy Hospital"
        ),
        GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),
        GroundTruthCell(
            row=1, column="Phone", dirty_value="not available", clean_value="217-555-0101"
        ),
    )
    return Task(
        name="synthetic",
        dirty_df=dirty_df,
        canonical_columns=tuple(str(column) for column in dirty_df.columns),
        ground_truth=ground_truth,
        metadata={"source": "built-in synthetic", "rows": 2, "columns": 3},
        inferability="deterministic_normalization",
    )


def load_canonical_dataforge_task(name: str, *, cache_root: Path | None = None) -> Task:
    """Load a canonical Hospital, Flights, or Beers task through optional DataForge."""
    if name not in _CANONICAL_DATASETS:
        raise TaskLoadError(
            f"Unknown canonical dataset '{name}'. Expected one of: {sorted(_CANONICAL_DATASETS)}."
        )
    try:
        real_world = import_module("dataforge.datasets.real_world")
    except ImportError as exc:
        raise TaskLoadError(
            "Canonical datasets require the optional 'dataforge' package. Install DataForge or use '--dataset synthetic' / '--dirty-csv' with '--clean-csv'."
        ) from exc
    load_real_world_dataset = cast(Any, real_world).load_real_world_dataset
    dataset = load_real_world_dataset(name, cache_root=cache_root)
    return Task(
        name=dataset.metadata.name,
        dirty_df=dataset.dirty_df,
        canonical_columns=dataset.canonical_columns,
        ground_truth=tuple(
            GroundTruthCell(
                row=cell.row,
                column=cell.column,
                dirty_value=cell.dirty_value,
                clean_value=cell.clean_value,
            )
            for cell in dataset.ground_truth
        ),
        metadata={
            "source": "dataforge",
            "domain": dataset.metadata.domain,
            "rows": len(dataset.clean_df.index),
            "columns": len(dataset.canonical_columns),
            "error_types": dataset.metadata.error_types,
            "citation": dataset.metadata.citation,
        },
        inferability="external_reference_required",
    )


def load_task(
    dataset: str,
    *,
    dirty_csv: Path | None = None,
    clean_csv: Path | None = None,
    cache_root: Path | None = None,
) -> Task:
    """Load a named evaluation task from built-in, DataForge, or CSV-pair sources."""
    if dirty_csv is not None or clean_csv is not None:
        if dirty_csv is None or clean_csv is None:
            raise TaskLoadError(
                "Both --dirty-csv and --clean-csv are required for a custom CSV task."
            )
        return load_csv_pair_task(name=dataset, dirty_csv=dirty_csv, clean_csv=clean_csv)
    if dataset == "synthetic":
        return load_synthetic_task()
    return load_canonical_dataforge_task(dataset, cache_root=cache_root)


def available_datasets() -> tuple[str, ...]:
    """Return the stable list of built-in dataset identifiers."""
    return ("synthetic", "hospital", "flights", "beers")


def validate_dataset_names(names: Iterable[str]) -> None:
    """Validate dataset identifiers before a harness run starts."""
    unknown = sorted(set(names) - set(available_datasets()))
    if unknown:
        raise TaskLoadError(
            f"Unknown datasets: {unknown}. Expected one of: {list(available_datasets())}."
        )
