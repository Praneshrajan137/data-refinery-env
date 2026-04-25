"""Unit tests for real-world benchmark dataset loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.datasets.real_world import DatasetDownloadError, load_real_world_dataset
from dataforge.datasets.registry import DATASET_REGISTRY

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "bench"


def _populate_cache(cache_root: Path, dataset: str, dirty_fixture: str, clean_fixture: str) -> None:
    dataset_dir = cache_root / "real_world" / dataset
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "dirty.csv").write_text(
        (_FIXTURES / dirty_fixture).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (dataset_dir / "clean.csv").write_text(
        (_FIXTURES / clean_fixture).read_text(encoding="utf-8"),
        encoding="utf-8",
    )


class TestDatasetRegistry:
    """Registry metadata should remain explicit and canonical."""

    def test_registry_contains_expected_metadata(self) -> None:
        assert DATASET_REGISTRY["hospital"].domain == "healthcare"
        assert DATASET_REGISTRY["hospital"].n_rows == 1000
        assert DATASET_REGISTRY["flights"].n_columns == 7
        assert DATASET_REGISTRY["beers"].n_rows == 2410


class TestRealWorldLoader:
    """Dataset loading behavior around cache and header alignment."""

    def test_load_aligns_headers_by_position_and_excludes_header_only_diffs(
        self,
        tmp_path: Path,
    ) -> None:
        cache_root = tmp_path / "cache"
        _populate_cache(cache_root, "hospital", "hospital_dirty.csv", "hospital_clean.csv")

        dataset = load_real_world_dataset("hospital", cache_root=cache_root)

        assert dataset.canonical_columns == (
            "index",
            "ProviderNumber",
            "HospitalName",
            "Phone",
            "Score",
        )
        assert len(dataset.ground_truth) == 3
        assert {cell.column for cell in dataset.ground_truth} == {
            "HospitalName",
            "Phone",
            "Score",
        }
        assert len(dataset.metadata.header_mismatches) == 4

    def test_cache_hit_does_not_attempt_download(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache_root = tmp_path / "cache"
        _populate_cache(cache_root, "beers", "beers_dirty.csv", "beers_clean.csv")

        def _unexpected_download(*args: object, **kwargs: object) -> None:
            raise AssertionError("download should not be called when cache is populated")

        monkeypatch.setattr(
            "dataforge.datasets.real_world._download_to_cache", _unexpected_download
        )

        dataset = load_real_world_dataset("beers", cache_root=cache_root)

        assert dataset.metadata.name == "beers"
        assert len(dataset.ground_truth) == 2

    def test_cache_miss_raises_manual_download_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cache_root = tmp_path / "cache"

        def _fail_download(*args: object, **kwargs: object) -> None:
            raise RuntimeError("network blocked")

        monkeypatch.setattr("dataforge.datasets.real_world._download_to_cache", _fail_download)

        with pytest.raises(DatasetDownloadError) as exc_info:
            load_real_world_dataset("hospital", cache_root=cache_root)

        message = str(exc_info.value)
        assert "hospital" in message
        assert "dirty.csv" in message
        assert "clean.csv" in message
        assert str(cache_root / "real_world" / "hospital") in message
        assert "1." in message and "2." in message
