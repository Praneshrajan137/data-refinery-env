"""Unit tests for real-world benchmark dataset loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.datasets.real_world import DatasetDownloadError, load_real_world_dataset
from dataforge.datasets.registry import DATASET_REGISTRY, RAHA_GIT_REVISION

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
        assert DATASET_REGISTRY["hospital"].source_revision == RAHA_GIT_REVISION
        assert "refs/heads/master" not in DATASET_REGISTRY["hospital"].source_urls[0]
        assert len(DATASET_REGISTRY["hospital"].dirty_sha256) == 64
        assert len(DATASET_REGISTRY["hospital"].clean_sha256) == 64


class TestRealWorldLoader:
    """Dataset loading behavior around cache and header alignment."""

    def test_load_aligns_headers_by_position_and_excludes_header_only_diffs(
        self,
        tmp_path: Path,
    ) -> None:
        cache_root = tmp_path / "cache"
        _populate_cache(cache_root, "hospital", "hospital_dirty.csv", "hospital_clean.csv")

        dataset = load_real_world_dataset(
            "hospital",
            cache_root=cache_root,
            verify_hashes=False,
        )

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
        _populate_cache(cache_root, "flights", "flights_dirty.csv", "flights_clean.csv")

        def _unexpected_download(*args: object, **kwargs: object) -> None:
            raise AssertionError("download should not be called when cache is populated")

        monkeypatch.setattr(
            "dataforge.datasets.real_world._download_to_cache", _unexpected_download
        )

        dataset = load_real_world_dataset(
            "flights",
            cache_root=cache_root,
            verify_hashes=False,
        )

        assert dataset.metadata.name == "flights"
        assert len(dataset.ground_truth) == 1

    def test_cache_miss_uses_embedded_fallback_when_available(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cache_root = tmp_path / "cache"

        def _fail_download(*args: object, **kwargs: object) -> None:
            raise RuntimeError("network blocked")

        monkeypatch.setattr("dataforge.datasets.real_world._download_to_cache", _fail_download)

        dataset = load_real_world_dataset(
            "hospital",
            cache_root=cache_root,
            verify_hashes=False,
            allow_embedded_fallback=True,
        )

        assert dataset.metadata.name == "hospital"
        assert len(dataset.ground_truth) == 2
        assert not (cache_root / "real_world" / "hospital" / "dirty.csv").exists()

    def test_cache_miss_without_embedded_fallback_raises_manual_download_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cache_root = tmp_path / "cache"

        def _fail_download(*args: object, **kwargs: object) -> None:
            raise RuntimeError("network blocked")

        monkeypatch.setattr("dataforge.datasets.real_world._download_to_cache", _fail_download)

        with pytest.raises(DatasetDownloadError) as exc_info:
            load_real_world_dataset("flights", cache_root=cache_root)

        message = str(exc_info.value)
        assert "flights" in message
        assert "dirty.csv" in message
        assert "clean.csv" in message
        assert str(cache_root / "real_world" / "flights") in message
        assert "1." in message and "2." in message

    def test_cache_hash_mismatch_fails_by_default(self, tmp_path: Path) -> None:
        """Fixture-sized cache bytes must not be mistaken for canonical Raha data."""
        cache_root = tmp_path / "cache"
        _populate_cache(cache_root, "hospital", "hospital_dirty.csv", "hospital_clean.csv")

        with pytest.raises(DatasetDownloadError, match="sha256 mismatch"):
            load_real_world_dataset("hospital", cache_root=cache_root)
