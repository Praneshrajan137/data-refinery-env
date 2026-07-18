"""Download, cache, and align real-world benchmark datasets."""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, replace
from pathlib import Path

import httpx
import pandas as pd
from pydantic import BaseModel, Field

from dataforge.datasets.registry import DatasetMetadata, HeaderMismatch, get_dataset_metadata


class DatasetDownloadError(RuntimeError):
    """Raised when a real-world dataset cannot be downloaded or loaded from cache."""


_LOGGER = logging.getLogger("dataforge.datasets.real_world")


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
    dirty_sha256: str
    clean_sha256: str


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


def _sha256_file(path: Path) -> str:
    """Return the SHA-256 digest for a cached artifact."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_bytes(url: str) -> bytes:
    """Download raw CSV bytes from an upstream source URL."""
    try:
        timeout = float(os.environ.get("DATAFORGE_DOWNLOAD_TIMEOUT_S", "5"))
    except ValueError:
        timeout = 5.0
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
    return response.content


def _download_to_cache(metadata: DatasetMetadata, dataset_dir: Path) -> None:
    """Download dirty/clean CSV files into the dataset cache directory."""
    dataset_dir.mkdir(parents=True, exist_ok=True)
    dirty_url, clean_url = metadata.source_urls
    _LOGGER.info("dataset_download_start name=%s dir=%s", metadata.name, dataset_dir)
    (dataset_dir / "dirty.csv").write_bytes(_download_bytes(dirty_url))
    (dataset_dir / "clean.csv").write_bytes(_download_bytes(clean_url))
    _LOGGER.info("dataset_download_complete name=%s dir=%s", metadata.name, dataset_dir)


def _validate_cached_hashes(
    *,
    metadata: DatasetMetadata,
    dirty_path: Path,
    clean_path: Path,
) -> tuple[str, str]:
    """Verify cached bytes match the pinned upstream source metadata."""
    dirty_sha256 = _sha256_file(dirty_path)
    clean_sha256 = _sha256_file(clean_path)
    mismatches: list[str] = []
    if dirty_sha256 != metadata.dirty_sha256:
        mismatches.append(
            f"dirty.csv sha256 mismatch: expected {metadata.dirty_sha256}, got {dirty_sha256}"
        )
    if clean_sha256 != metadata.clean_sha256:
        mismatches.append(
            f"clean.csv sha256 mismatch: expected {metadata.clean_sha256}, got {clean_sha256}"
        )
    if mismatches:
        raise DatasetDownloadError(
            f"Cached dataset '{metadata.name}' does not match pinned Raha source "
            f"{metadata.source_revision}: "
            + "; ".join(mismatches)
            + f". Remove '{dirty_path.parent}' or rerun with a clean --cache-root."
        )
    return dirty_sha256, clean_sha256


def _load_embedded_dataset(name: str) -> tuple[pd.DataFrame, pd.DataFrame, str, str] | None:
    root = Path(__file__).parent / "embedded" / name
    dirty_path = root / "dirty.csv"
    clean_path = root / "clean.csv"
    if not dirty_path.exists() or not clean_path.exists():
        return None
    return (
        _read_cached_csv(dirty_path),
        _read_cached_csv(clean_path),
        _sha256_file(dirty_path),
        _sha256_file(clean_path),
    )


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
    verify_hashes: bool = True,
    allow_embedded_fallback: bool = False,
) -> RealWorldDataset:
    """Load a real-world benchmark dataset from cache or upstream.

    Args:
        name: Canonical dataset name.
        cache_root: Optional cache root override, mainly for tests.
        verify_hashes: Verify cached/downloaded bytes against pinned upstream hashes.
        allow_embedded_fallback: Allow tiny bundled fixture data when the canonical
            upstream dataset cannot be downloaded. This is intended for local tests only.

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

    dirty_df: pd.DataFrame | None = None
    clean_df: pd.DataFrame | None = None
    dirty_sha256: str | None = None
    clean_sha256: str | None = None

    if not dirty_path.exists() or not clean_path.exists():
        _LOGGER.info("dataset_cache_miss name=%s dir=%s", name, dataset_dir)
        try:
            _download_to_cache(metadata, dataset_dir)
        except Exception as exc:  # pragma: no cover - exercised through tests via monkeypatch
            fallback = _load_embedded_dataset(name) if allow_embedded_fallback else None
            if fallback is None:
                raise DatasetDownloadError(
                    _manual_download_message(metadata, dataset_dir, exc)
                ) from exc
            dirty_df, clean_df, dirty_sha256, clean_sha256 = fallback
    else:
        _LOGGER.info("dataset_cache_hit name=%s dir=%s", name, dataset_dir)

    if dirty_df is None or clean_df is None:
        if verify_hashes:
            dirty_sha256, clean_sha256 = _validate_cached_hashes(
                metadata=metadata,
                dirty_path=dirty_path,
                clean_path=clean_path,
            )
        else:
            dirty_sha256 = _sha256_file(dirty_path)
            clean_sha256 = _sha256_file(clean_path)
        dirty_df = _read_cached_csv(dirty_path)
        clean_df = _read_cached_csv(clean_path)
    elif dirty_sha256 is None or clean_sha256 is None:
        raise DatasetDownloadError(f"Dataset '{name}' loaded without artifact hashes.")

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
    if dirty_sha256 is None or clean_sha256 is None:
        raise DatasetDownloadError(f"Dataset '{name}' loaded without artifact hashes.")
    return RealWorldDataset(
        metadata=loaded_metadata,
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=tuple(clean_columns),
        ground_truth=_compute_ground_truth(dirty_df, clean_df),
        dirty_sha256=dirty_sha256,
        clean_sha256=clean_sha256,
    )


def _sha256_frame(frame: pd.DataFrame) -> str:
    """SHA-256 of a frame's canonical CSV serialization (for sampled datasets)."""
    return hashlib.sha256(frame.to_csv(index=False).encode("utf-8")).hexdigest()


def sample_dataset_rows(dataset: RealWorldDataset, max_rows: int) -> RealWorldDataset:
    """Deterministic head-sample of a dataset for scale-aware measurement.

    Keeps the first ``max_rows`` rows of the row-aligned dirty/clean frames and
    only the ground-truth cells within that window (row indices are preserved by
    a head slice, so no re-indexing is needed and scoring stays aligned). This
    unblocks measuring datasets whose full size is impractical for the
    deterministic bench (e.g. ``tax`` at 200k rows, where schema inference is
    super-linear) WITHOUT fabricating results.

    Honesty boundary: the returned dataset is an explicit SAMPLE. Its recomputed
    ``dirty_sha256``/``clean_sha256`` describe the sampled bytes, NOT the pinned
    upstream source, and ``metadata.n_rows`` is updated to the sample size. Never
    use a sampled dataset for pinned-hash verification or for committed
    ``benchmark_truth`` artifacts -- it is a measurement instrument only, and any
    result derived from it must be reported as sampled (n = ``max_rows``), never
    as the full-dataset number.
    """
    if max_rows < 0:
        raise ValueError("max_rows must be non-negative")
    total = len(dataset.dirty_df.index)
    if max_rows >= total:
        return dataset
    dirty = dataset.dirty_df.head(max_rows).reset_index(drop=True)
    clean = dataset.clean_df.head(max_rows).reset_index(drop=True)
    ground_truth = tuple(cell for cell in dataset.ground_truth if cell.row < max_rows)
    sampled_metadata = dataset.metadata.model_copy(update={"n_rows": max_rows})
    return replace(
        dataset,
        metadata=sampled_metadata,
        dirty_df=dirty,
        clean_df=clean,
        ground_truth=ground_truth,
        dirty_sha256=_sha256_frame(dirty),
        clean_sha256=_sha256_frame(clean),
    )
