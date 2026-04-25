"""Unit tests for benchmark orchestration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.bench.runner import run_agent_comparison

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "bench"


def _populate_cache(cache_root: Path) -> None:
    dataset_dir = cache_root / "real_world" / "hospital"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "dirty.csv").write_text(
        (_FIXTURES / "hospital_dirty.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (dataset_dir / "clean.csv").write_text(
        (_FIXTURES / "hospital_clean.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )


class TestRunAgentComparison:
    """Benchmark orchestration should write JSON and skip unavailable LLM methods."""

    def test_runner_rejects_unknown_inputs(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unknown benchmark methods"):
            run_agent_comparison(
                methods=["unknown"],
                datasets=["hospital"],
                seeds=1,
                output_json=tmp_path / "out.json",
                really_run_big_bench=False,
                cache_root=tmp_path / "cache",
            )

    def test_runner_writes_json_and_skips_unconfigured_llm(self, tmp_path: Path) -> None:
        cache_root = tmp_path / "cache"
        output_json = tmp_path / "eval" / "results" / "agent_comparison.json"
        _populate_cache(cache_root)

        result = run_agent_comparison(
            methods=["heuristic", "llm_zeroshot"],
            datasets=["hospital"],
            seeds=1,
            output_json=output_json,
            really_run_big_bench=False,
            cache_root=cache_root,
        )

        assert output_json.exists()
        assert result.aggregates[0].dataset == "hospital"
        assert any(row.method == "heuristic" and row.status == "ok" for row in result.aggregates)
        assert any(
            row.method == "llm_zeroshot" and row.status == "skipped" for row in result.aggregates
        )

        payload = json.loads(output_json.read_text(encoding="utf-8"))
        assert payload["metadata"]["datasets"] == ["hospital"]
        assert payload["records"][0]["dataset"] == "hospital"

    def test_heuristic_runner_produces_nonzero_true_positives(self, tmp_path: Path) -> None:
        cache_root = tmp_path / "cache"
        output_json = tmp_path / "agent_comparison.json"
        _populate_cache(cache_root)

        result = run_agent_comparison(
            methods=["heuristic"],
            datasets=["hospital"],
            seeds=1,
            output_json=output_json,
            really_run_big_bench=False,
            cache_root=cache_root,
        )

        aggregate = result.aggregates[0]
        assert aggregate.status == "ok"
        assert aggregate.f1_mean is not None and aggregate.f1_mean > 0.0

    def test_runner_blocks_large_llm_bench_without_override(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "groq")
        monkeypatch.setenv("GROQ_API_KEY", "test-key")

        with pytest.raises(ValueError, match="really-run-big-bench"):
            run_agent_comparison(
                methods=["llm_zeroshot", "llm_react"],
                datasets=["hospital", "flights", "beers"],
                seeds=3,
                output_json=tmp_path / "out.json",
                really_run_big_bench=False,
                cache_root=tmp_path / "cache",
            )
