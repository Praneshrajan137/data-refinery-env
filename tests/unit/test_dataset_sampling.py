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
    """The legacy strategy, now explicitly requested rather than defaulted.

    Kept reachable so the one committed artifact derived from it
    (``eval/results/heuristic_tax_sampled.json``) stays reproducible.
    """
    dataset = _synthetic_dataset(30)
    sampled = sample_dataset_rows(dataset, 9, strategy="head")

    assert len(sampled.dirty_df.index) == 9
    assert len(sampled.clean_df.index) == 9
    # Ground truth is filtered to the window and row indices stay valid.
    assert all(cell.row < 9 for cell in sampled.ground_truth)
    assert {cell.row for cell in sampled.ground_truth} == {0, 3, 6}
    # Rows still align: each GT row's dirty value matches the sampled dirty frame.
    for cell in sampled.ground_truth:
        assert sampled.dirty_df.iloc[cell.row][cell.column] == cell.dirty_value


class TestRandomSampling:
    """The default strategy since 2026-08-23. `head` of a sorted table is not a sample."""

    def test_random_is_the_default(self) -> None:
        dataset = _synthetic_dataset(300)
        default = sample_dataset_rows(dataset, 30)
        explicit = sample_dataset_rows(dataset, 30, strategy="random")
        assert list(default.dirty_df["id"]) == list(explicit.dirty_df["id"])

    def test_random_does_not_reduce_to_the_leading_slice(self) -> None:
        """The whole point: `tax` is sorted, so a head slice is a biased stratum."""
        dataset = _synthetic_dataset(300)
        sampled = sample_dataset_rows(dataset, 30)
        head = sample_dataset_rows(dataset, 30, strategy="head")
        assert list(sampled.dirty_df["id"]) != list(head.dirty_df["id"])

    def test_ground_truth_follows_the_rows_it_describes(self) -> None:
        """Re-indexing is load-bearing under `random`; under `head` it was the identity.

        Omitting the remap would attach every label to the wrong row while keeping the
        counts plausible, which is the failure mode that produces a confidently wrong
        precision.
        """
        dataset = _synthetic_dataset(300)
        sampled = sample_dataset_rows(dataset, 30)

        assert sampled.ground_truth, "precondition: the sample must contain errors"
        for cell in sampled.ground_truth:
            assert 0 <= cell.row < 30
            assert sampled.dirty_df.iloc[cell.row][cell.column] == cell.dirty_value
            assert sampled.clean_df.iloc[cell.row][cell.column] == cell.clean_value

    def test_same_seed_is_reproducible_and_different_seeds_differ(self) -> None:
        dataset = _synthetic_dataset(300)
        first = sample_dataset_rows(dataset, 30, seed=7)
        again = sample_dataset_rows(dataset, 30, seed=7)
        other = sample_dataset_rows(dataset, 30, seed=8)

        assert list(first.dirty_df["id"]) == list(again.dirty_df["id"])
        assert list(first.dirty_df["id"]) != list(other.dirty_df["id"])

    def test_unknown_strategy_rejected(self) -> None:
        dataset = _synthetic_dataset(30)
        try:
            sample_dataset_rows(dataset, 5, strategy="first_n")
        except ValueError as exc:
            assert "unknown sampling strategy" in str(exc)
            return
        raise AssertionError("an unrecognised strategy must raise rather than fall back")


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
