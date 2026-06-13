"""Tests for canonical real-world source audit reports."""

from __future__ import annotations

import pandas as pd

from dataforge.datasets.real_world import DatasetDownloadError, RealWorldDataset
from dataforge.datasets.registry import DATASET_REGISTRY
from scripts.data.audit_real_world_sources import audit_real_world_sources


def _fake_dataset(name: str) -> RealWorldDataset:
    expected = DATASET_REGISTRY[name]
    columns = [f"c{i}" for i in range(expected.n_columns)]
    dirty_df = pd.DataFrame([[""] * expected.n_columns] * expected.n_rows, columns=columns)
    clean_df = dirty_df.copy()
    return RealWorldDataset(
        metadata=expected,
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=tuple(columns),
        ground_truth=(),
        dirty_sha256=expected.dirty_sha256,
        clean_sha256=expected.clean_sha256,
    )


def test_source_audit_passes_for_canonical_loaded_dataset(monkeypatch) -> None:
    def fake_loader(name: str, **kwargs: object) -> RealWorldDataset:
        assert kwargs["verify_hashes"] is True
        assert kwargs["allow_embedded_fallback"] is False
        return _fake_dataset(name)

    monkeypatch.setattr(
        "scripts.data.audit_real_world_sources.load_real_world_dataset", fake_loader
    )

    report = audit_real_world_sources(datasets=("hospital",))

    assert report["ok"] is True
    assert report["datasets"][0]["dataset"] == "hospital"
    assert report["datasets"][0]["source_revision"] == DATASET_REGISTRY["hospital"].source_revision
    assert report["datasets"][0]["ground_truth_cells"] == 0


def test_source_audit_blocks_hash_mismatch_or_stale_cache(monkeypatch) -> None:
    def fake_loader(name: str, **kwargs: object) -> RealWorldDataset:
        raise DatasetDownloadError("sha256 mismatch")

    monkeypatch.setattr(
        "scripts.data.audit_real_world_sources.load_real_world_dataset", fake_loader
    )

    report = audit_real_world_sources(datasets=("hospital",))

    assert report["ok"] is False
    assert report["status"] == "block"
    assert report["blockers"] == ["hospital"]
    assert "sha256 mismatch" in report["datasets"][0]["error"]
