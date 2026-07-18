"""Unit tests for scale-aware dataset row sampling (measure-first for tax)."""

from __future__ import annotations

import pandas as pd

from dataforge.datasets.real_world import (
    RealWorldDataset,
    _compute_ground_truth,
    sample_dataset_rows,
)
from dataforge.datasets.registry import get_dataset_metadata


def _synthetic_dataset(n: int) -> RealWorldDataset:
    """A row-aligned dirty/clean dataset where every 3rd row has one error."""
    dirty = pd.DataFrame(
        {
            "id": [str(i) for i in range(n)],
            "v": [("bad" if i % 3 == 0 else str(i)) for i in range(n)],
        }
    )
    clean = pd.DataFrame({"id": [str(i) for i in range(n)], "v": [str(i) for i in range(n)]})
    metadata = get_dataset_metadata("hospital").model_copy(update={"n_rows": n, "n_columns": 2})
    return RealWorldDataset(
        metadata=metadata,
        dirty_df=dirty,
        clean_df=clean,
        canonical_columns=("id", "v"),
        ground_truth=_compute_ground_truth(dirty, clean),
        dirty_sha256="0" * 64,
        clean_sha256="0" * 64,
    )


def test_head_sample_keeps_first_rows_and_aligned_ground_truth() -> None:
    dataset = _synthetic_dataset(30)
    sampled = sample_dataset_rows(dataset, 9)

    assert len(sampled.dirty_df.index) == 9
    assert len(sampled.clean_df.index) == 9
    # Ground truth is filtered to the window and row indices stay valid.
    assert all(cell.row < 9 for cell in sampled.ground_truth)
    assert {cell.row for cell in sampled.ground_truth} == {0, 3, 6}
    # Rows still align: each GT row's dirty value matches the sampled dirty frame.
    for cell in sampled.ground_truth:
        assert sampled.dirty_df.iloc[cell.row][cell.column] == cell.dirty_value


def test_sample_updates_metadata_and_recomputes_hashes() -> None:
    dataset = _synthetic_dataset(30)
    sampled = sample_dataset_rows(dataset, 10)

    assert sampled.metadata.n_rows == 10
    # Hashes describe the SAMPLE, not the (placeholder) original bytes.
    assert sampled.dirty_sha256 != dataset.dirty_sha256
    assert sampled.clean_sha256 != dataset.clean_sha256


def test_sample_at_or_above_total_returns_same_object() -> None:
    dataset = _synthetic_dataset(12)
    assert sample_dataset_rows(dataset, 12) is dataset
    assert sample_dataset_rows(dataset, 100) is dataset


def test_negative_max_rows_rejected() -> None:
    dataset = _synthetic_dataset(5)
    try:
        sample_dataset_rows(dataset, -1)
    except ValueError:
        return
    raise AssertionError("negative max_rows must raise ValueError")
