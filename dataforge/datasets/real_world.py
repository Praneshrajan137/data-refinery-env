"""Download, cache, and align real-world benchmark datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import httpx
import pandas as pd
from pydantic import BaseModel, Field

from dataforge.datasets.registry import DatasetMetadata, HeaderMismatch, get_dataset_metadata


class DatasetDownloadError(RuntimeError):
    """Raised when a real-world dataset cannot be downloaded or loaded from cache."""


class GroundTruthCell(BaseModel):
    """Single cell-level dirty-to-clean correction used for benchmark scoring."""

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    dirty_value: str
    clean_value: str

    model_config = {"frozen": True}


@dataclass(frozen=True, kw_only=True)
class RealWorldDataset:
    """Loaded real-world dataset with aligned dirty/clean DataFrames."""

    metadata: DatasetMetadata
    dirty_df: pd.DataFrame
    clean_df: pd.DataFrame
    canonical_columns: tuple[str, ...]
    ground_truth: tuple[GroundTruthCell, ...]


def _resolve_cache_root(cache_root: Path | None) -> Path:
    """Resolve the root benchmark cache directory."""
    if cache_root is not None:
        return cache_root
    return Path.home() / ".dataforge" / "cache"


def _dataset_cache_dir(dataset_name: str, *, cache_root: Path | None) -> Path:
    """Return the cache directory for one dataset."""
    return _resolve_cache_root(cache_root) / "real_world" / dataset_name


def _read_cached_csv(path: Path) -> pd.DataFrame:
    """Read a cached CSV using string-preserving defaults."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)


def _download_bytes(url: str) -> bytes:
    """Download raw CSV bytes from an upstream source URL."""
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
    return response.content


def _download_to_cache(metadata: DatasetMetadata, dataset_dir: Path) -> None:
    """Download dirty/clean CSV files into the dataset cache directory."""
    dataset_dir.mkdir(parents=True, exist_ok=True)
    dirty_url, clean_url = metadata.source_urls
    (dataset_dir / "dirty.csv").write_bytes(_download_bytes(dirty_url))
    (dataset_dir / "clean.csv").write_bytes(_download_bytes(clean_url))


def _manual_download_message(metadata: DatasetMetadata, dataset_dir: Path, cause: Exception) -> str:
    """Build a user-facing manual download error message."""
    dirty_url, clean_url = metadata.source_urls
    return (
        f"Could not download dataset '{metadata.name}' and no cached copy was found.\n\n"
        f"Cause: {cause}\n"
        f"Cache target: {dataset_dir}\n"
        f"Dirty URL: {dirty_url}\n"
        f"Clean URL: {clean_url}\n\n"
        "How to download manually:\n"
        f"1. Download both CSV files from the URLs above into '{dataset_dir}'.\n"
        "2. Save them exactly as 'dirty.csv' and 'clean.csv', then rerun the benchmark."
    )


def _header_mismatches(
    dirty_columns: list[str], clean_columns: list[str]
) -> tuple[HeaderMismatch, ...]:
    """Collect header-name mismatches across aligned dirty/clean columns."""
    mismatches: list[HeaderMismatch] = []
    for dirty_name, clean_name in zip(dirty_columns, clean_columns, strict=True):
        if dirty_name != clean_name:
            mismatches.append(HeaderMismatch(dirty_name=dirty_name, clean_name=clean_name))
    return tuple(mismatches)


def _compute_ground_truth(
    dirty_df: pd.DataFrame,
    clean_df: pd.DataFrame,
) -> tuple[GroundTruthCell, ...]:
    """Compute cell-level dirty-to-clean diffs across aligned DataFrames."""
    ground_truth: list[GroundTruthCell] = []
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
                ground_truth.append(
                    GroundTruthCell(
                        row=row_index,
                        column=str(column),
                        dirty_value=dirty_text,
                        clean_value=clean_text,
                    )
                )
    return tuple(ground_truth)


def load_real_world_dataset(
    name: str,
    *,
    cache_root: Path | None = None,
) -> RealWorldDataset:
    """Load a real-world benchmark dataset from cache or upstream.

    Args:
        name: Canonical dataset name.
        cache_root: Optional cache root override, mainly for tests.

    Returns:
        The aligned dirty/clean dataset bundle.

    Raises:
        DatasetDownloadError: If the dataset is not cached and download fails.
        ValueError: If dirty/clean files disagree on row or column count.
    """
    metadata = get_dataset_metadata(name)
    dataset_dir = _dataset_cache_dir(name, cache_root=cache_root)
    dirty_path = dataset_dir / "dirty.csv"
    clean_path = dataset_dir / "clean.csv"

    if not dirty_path.exists() or not clean_path.exists():
        try:
            _download_to_cache(metadata, dataset_dir)
        except Exception as exc:  # pragma: no cover - exercised through tests via monkeypatch
            raise DatasetDownloadError(
                _manual_download_message(metadata, dataset_dir, exc)
            ) from exc

    dirty_df = _read_cached_csv(dirty_path)
    clean_df = _read_cached_csv(clean_path)

    if len(dirty_df.index) != len(clean_df.index):
        raise ValueError(f"Dataset '{name}' dirty/clean row counts do not match.")
    if len(dirty_df.columns) != len(clean_df.columns):
        raise ValueError(f"Dataset '{name}' dirty/clean column counts do not match.")

    clean_columns = [str(column) for column in clean_df.columns]
    mismatches = _header_mismatches(
        [str(column) for column in dirty_df.columns],
        clean_columns,
    )
    dirty_df.columns = clean_columns
    clean_df.columns = clean_columns

    loaded_metadata = metadata.model_copy(
        update={
            "n_rows": len(clean_df.index),
            "n_columns": len(clean_columns),
            "header_mismatches": mismatches,
        }
    )
    return RealWorldDataset(
        metadata=loaded_metadata,
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=tuple(clean_columns),
        ground_truth=_compute_ground_truth(dirty_df, clean_df),
    )
