"""Loader tests for column-level detection benchmarks.

Offline by construction, following the convention in ``test_bench_real_world.py``: a
cache is pre-populated and the network is monkeypatched to fail, so a passing suite is
never evidence that a download worked.

Each trap asserted here was found by measuring the real pinned corpora, not imagined.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from dataforge.datasets.column_corpus import (
    ColumnBenchmarkError,
    load_column_benchmark,
)
from dataforge.datasets.registry import (
    AUTOTEST_GIT_REVISION,
    COLUMN_BENCHMARK_REGISTRY,
)

_HEADER = "index,header,ground_truth,ground_truth_debatable,dist_val_count,dist_val\r\n"


def _row(index: int, header: str, truth: str, debatable: str, count: int, values: str) -> str:
    """Render one CSV row with the upstream quoting convention."""
    return f'{index},{header},"{truth}","{debatable}",{count},"{values}"\r\n'


def _write_cache(
    cache_root: Path,
    name: str,
    body: str,
    *,
    bom: bool = True,
) -> str:
    """Write a synthetic benchmark into the cache and return its sha256."""
    path = cache_root / "column_benchmarks" / f"{name}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    text = ("\ufeff" if bom else "") + _HEADER + body
    raw = text.encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make any download attempt a hard failure."""

    def _fail(url: str) -> bytes:
        raise AssertionError(f"test attempted a network download: {url}")

    monkeypatch.setattr("dataforge.datasets.column_corpus._download", _fail)


class TestRegistryPinning:
    """The registry must pin bytes, not a moving reference."""

    def test_both_corpora_are_registered(self) -> None:
        assert set(COLUMN_BENCHMARK_REGISTRY) == {"rt_bench", "st_bench"}

    def test_revision_is_a_commit_sha_not_a_branch(self) -> None:
        assert len(AUTOTEST_GIT_REVISION) == 40
        for metadata in COLUMN_BENCHMARK_REGISTRY.values():
            assert "refs/heads/" not in metadata.source_url
            assert AUTOTEST_GIT_REVISION in metadata.source_url
            assert len(metadata.sha256) == 64

    def test_axis_is_detection_only(self) -> None:
        """No clean values ship, so a correction claim from here would be fabricated."""
        for metadata in COLUMN_BENCHMARK_REGISTRY.values():
            assert metadata.axis == "detection"
            assert metadata.error_provenance == "natural"

    def test_absent_licence_is_recorded_as_none_not_assumed(self) -> None:
        """A licence is recorded when present and null when absent, never assumed.

        Asserted as "no entry claims a licence it cannot evidence" rather than "every
        entry is unlicensed", so a genuinely-licensed corpus can be registered later
        without deleting the check. Both Auto-Test corpora are unlicensed today.
        """
        unlicensed = [
            name
            for name, metadata in COLUMN_BENCHMARK_REGISTRY.items()
            if metadata.license_spdx is None
        ]
        assert set(unlicensed) == {"rt_bench", "st_bench"}, (
            "the Auto-Test corpora publish no LICENSE upstream. If a licence has since "
            "appeared, record the SPDX id and move the corpus out of this set "
            "deliberately."
        )

    def test_unlicensed_corpora_are_not_vendored_into_the_repository(self) -> None:
        """A licence-less corpus must exist only in a user's cache.

        Derived from the registry rather than a hardcoded name tuple: the tuple form
        silently left a newly registered licence-less corpus unprotected, which is the
        wrong direction for a check whose whole purpose is redistribution risk.
        """
        repo_root = Path(__file__).resolve().parents[2]
        unlicensed = [
            name
            for name, metadata in COLUMN_BENCHMARK_REGISTRY.items()
            if metadata.license_spdx is None
        ]
        assert unlicensed, (
            "precondition: at least one licence-less corpus must be registered, or this "
            "test passes without checking anything"
        )
        for name in unlicensed:
            assert not list(repo_root.glob(f"**/{name}.csv")), (
                f"{name}.csv must not be committed: upstream grants no redistribution right"
            )


class TestParsing:
    """The happy path, and the four upstream traps."""

    def test_parses_labels_values_and_the_neutral_zone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _block_network(monkeypatch)
        body = _row(1, "month", "['febuary']", "['total']", 3, "['january', 'febuary', 'total']")
        digest = _write_cache(tmp_path, "rt_bench", body)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "rt_bench",
            COLUMN_BENCHMARK_REGISTRY["rt_bench"].model_copy(
                update={"sha256": digest, "declared_columns": 1}
            ),
        )
        benchmark = load_column_benchmark("rt_bench", cache_root=tmp_path)

        assert benchmark.n_columns == 1
        column = benchmark.columns[0]
        assert column.header == "month"
        assert column.ground_truth == frozenset({"febuary"})
        assert column.debatable == frozenset({"total"})
        assert column.distinct_values == ("january", "febuary", "total")

    def test_utf8_bom_does_not_corrupt_the_first_field(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Trap 4: the real files carry a BOM, so utf-8 would name the field '\\ufeffindex'."""
        _block_network(monkeypatch)
        body = _row(1, "h", "[]", "[]", 1, "['a']")
        digest = _write_cache(tmp_path, "st_bench", body, bom=True)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "st_bench",
            COLUMN_BENCHMARK_REGISTRY["st_bench"].model_copy(
                update={"sha256": digest, "declared_columns": 1}
            ),
        )
        benchmark = load_column_benchmark("st_bench", cache_root=tmp_path)
        assert benchmark.n_columns == 1, "a BOM must not hide the header row"

    def test_misspelled_documented_field_name_is_rejected_loudly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Trap 2: reading the documented spelling would silently empty the neutral zone."""
        _block_network(monkeypatch)
        path = tmp_path / "column_benchmarks" / "rt_bench.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        text = (
            "\ufeffindex,header,ground_truth,ground_truth_debateable,dist_val_count,dist_val\r\n"
            + _row(1, "h", "[]", "[]", 1, "['a']")
        )
        raw = text.encode("utf-8")
        path.write_bytes(raw)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "rt_bench",
            COLUMN_BENCHMARK_REGISTRY["rt_bench"].model_copy(
                update={"sha256": hashlib.sha256(raw).hexdigest(), "declared_columns": 1}
            ),
        )
        with pytest.raises(ColumnBenchmarkError, match="ground_truth_debatable"):
            load_column_benchmark("rt_bench", cache_root=tmp_path)

    def test_blank_padded_tail_is_discarded_silently(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Trap 1: rt_bench is an Excel export padded to 1,048,575 rows."""
        _block_network(monkeypatch)
        body = _row(1, "h", "['bx']", "[]", 2, "['a', 'bx']") + ",,,,,\r\n" * 5
        digest = _write_cache(tmp_path, "rt_bench", body)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "rt_bench",
            COLUMN_BENCHMARK_REGISTRY["rt_bench"].model_copy(
                update={"sha256": digest, "declared_columns": 1}
            ),
        )
        benchmark = load_column_benchmark("rt_bench", cache_root=tmp_path)
        assert benchmark.n_columns == 1
        assert benchmark.n_ground_truth_values == 1, "precondition: real labels survived"

    def test_nonblank_row_past_the_declared_count_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Truncation is safe only while the tail is empty. If upstream grows, fail loudly."""
        _block_network(monkeypatch)
        body = _row(1, "h", "[]", "[]", 1, "['a']") + _row(2, "h2", "['zx']", "[]", 1, "['zx']")
        digest = _write_cache(tmp_path, "rt_bench", body)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "rt_bench",
            COLUMN_BENCHMARK_REGISTRY["rt_bench"].model_copy(
                update={"sha256": digest, "declared_columns": 1}
            ),
        )
        with pytest.raises(ColumnBenchmarkError, match="not blank"):
            load_column_benchmark("rt_bench", cache_root=tmp_path)

    def test_unparseable_row_is_quarantined_and_counted_not_dropped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Trap 3: st_bench row 1133 holds a leaked spreadsheet formula reference."""
        _block_network(monkeypatch)
        body = _row(1, "ok", "['bx']", "[]", 2, "['a', 'bx']") + _row(
            2, "leaked", "['refridgerator'+C1187]", "[]", 1, "['refridgerator']"
        )
        digest = _write_cache(tmp_path, "st_bench", body)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "st_bench",
            COLUMN_BENCHMARK_REGISTRY["st_bench"].model_copy(
                update={"sha256": digest, "declared_columns": 2}
            ),
        )
        benchmark = load_column_benchmark("st_bench", cache_root=tmp_path)

        assert benchmark.n_columns == 1
        assert len(benchmark.quarantined) == 1, (
            "a dropped label row must be counted, or precision is inflated by omission"
        )
        assert benchmark.quarantined[0].index == 1

    def test_column_with_no_values_is_quarantined(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _block_network(monkeypatch)
        body = _row(1, "ok", "['bx']", "[]", 2, "['a', 'bx']") + _row(
            2, "empty", "[]", "[]", 0, "[]"
        )
        digest = _write_cache(tmp_path, "st_bench", body)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "st_bench",
            COLUMN_BENCHMARK_REGISTRY["st_bench"].model_copy(
                update={"sha256": digest, "declared_columns": 2}
            ),
        )
        benchmark = load_column_benchmark("st_bench", cache_root=tmp_path)
        assert benchmark.n_columns == 1
        assert [q.reason for q in benchmark.quarantined] == ["EmptyColumn"]


class TestCorpusInvariants:
    """Violations of the scoring rule's premises must fail closed, not degrade."""

    def _load_single(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, row: str) -> None:
        digest = _write_cache(tmp_path, "rt_bench", row)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "rt_bench",
            COLUMN_BENCHMARK_REGISTRY["rt_bench"].model_copy(
                update={"sha256": digest, "declared_columns": 1}
            ),
        )
        load_column_benchmark("rt_bench", cache_root=tmp_path)

    def test_overlapping_labels_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _block_network(monkeypatch)
        with pytest.raises(ColumnBenchmarkError, match="disjoint"):
            self._load_single(
                tmp_path, monkeypatch, _row(1, "h", "['bx']", "['bx']", 2, "['a', 'bx']")
            )

    def test_label_outside_its_own_value_set_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _block_network(monkeypatch)
        with pytest.raises(ColumnBenchmarkError, match="absent from its"):
            self._load_single(tmp_path, monkeypatch, _row(1, "h", "['zx']", "[]", 1, "['a']"))

    def test_zero_admissible_columns_raises_rather_than_scoring_zeros(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty corpus must not be indistinguishable from a measured result."""
        _block_network(monkeypatch)
        with pytest.raises(ColumnBenchmarkError, match="zero admissible columns"):
            self._load_single(tmp_path, monkeypatch, _row(1, "h", "[]", "[]", 0, "[]"))


class TestIntegrity:
    """Hash pinning, and the one integrity defect that is reported rather than fatal."""

    def test_digest_mismatch_raises_by_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _block_network(monkeypatch)
        _write_cache(tmp_path, "rt_bench", _row(1, "h", "[]", "[]", 1, "['a']"))
        with pytest.raises(ColumnBenchmarkError, match="pinned Auto-Test revision"):
            load_column_benchmark("rt_bench", cache_root=tmp_path)

    def test_declared_value_count_mismatch_is_reported_not_fatal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ten upstream rows disagree; discarding 2,397 good columns over it would be worse."""
        _block_network(monkeypatch)
        body = _row(1, "h", "['bx']", "[]", 99, "['a', 'bx']")
        digest = _write_cache(tmp_path, "rt_bench", body)
        monkeypatch.setitem(
            COLUMN_BENCHMARK_REGISTRY,
            "rt_bench",
            COLUMN_BENCHMARK_REGISTRY["rt_bench"].model_copy(
                update={"sha256": digest, "declared_columns": 1}
            ),
        )
        benchmark = load_column_benchmark("rt_bench", cache_root=tmp_path)
        assert benchmark.n_columns == 1
        assert benchmark.value_count_mismatches == 1
        assert not benchmark.columns[0].value_count_matches_declaration
