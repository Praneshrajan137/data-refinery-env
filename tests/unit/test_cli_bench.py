"""Unit tests for the ``dataforge bench`` CLI command."""

from __future__ import annotations

import importlib
import re
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from dataforge.bench.core import AggregateBenchmarkResult, BenchmarkRunOutput
from dataforge.cli import app

bench_module = importlib.import_module("dataforge.cli.bench")

runner = CliRunner()


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def _stub_output() -> BenchmarkRunOutput:
    return BenchmarkRunOutput(
        metadata={
            "methods": ["heuristic"],
            "datasets": ["hospital"],
            "seeds": 3,
            "reproduction_command": "dataforge bench --methods heuristic --datasets hospital --seeds 3",
        },
        records=[],
        aggregates=[
            AggregateBenchmarkResult(
                method="heuristic",
                dataset="hospital",
                status="ok",
                skip_reason=None,
                seeds_requested=3,
                seeds_completed=3,
                precision_mean=1.0,
                precision_std=0.0,
                recall_mean=0.5,
                recall_std=0.0,
                f1_mean=0.6667,
                f1_std=0.0,
                avg_steps_mean=3.0,
                avg_steps_std=0.0,
                quota_units_mean=0.0,
                quota_units_std=0.0,
                runtime_s_mean=0.1,
                runtime_s_std=0.0,
                provider="local",
                model="deterministic",
                reproduction_command="dataforge bench --methods heuristic --datasets hospital --seeds 3",
            )
        ],
    )


class TestBenchCommand:
    """CLI registration and argument plumbing."""

    def test_bench_help_registered(self) -> None:
        result = runner.invoke(app, ["bench", "--help"])
        output = _strip_ansi(result.output)

        assert result.exit_code == 0
        assert "Usage:" in output
        assert "root bench [OPTIONS]" in output
        assert "--methods" in output
        assert "benchmark methods" in output
        assert "--datasets" in output
        assert "benchmark datasets" in output
        assert "--cache-root" in output
        assert "--no-verify-datas" in output

    def test_bench_uses_expected_defaults(self, monkeypatch: Any) -> None:
        captured: dict[str, Any] = {}

        def _fake_run_agent_comparison(**kwargs: Any) -> BenchmarkRunOutput:
            captured.update(kwargs)
            return _stub_output()

        monkeypatch.setattr(bench_module, "run_agent_comparison", _fake_run_agent_comparison)

        result = runner.invoke(app, ["bench"])

        assert result.exit_code == 0
        assert captured["methods"] == ["heuristic", "llm_zeroshot"]
        assert captured["datasets"] == ["hospital"]
        assert captured["seeds"] == 3
        assert captured["cache_root"] is None
        assert captured["verify_dataset_hashes"] is True

    def test_bench_accepts_documented_long_options(self, monkeypatch: Any) -> None:
        captured: dict[str, Any] = {}

        def _fake_run_agent_comparison(**kwargs: Any) -> BenchmarkRunOutput:
            captured.update(kwargs)
            return _stub_output()

        monkeypatch.setattr(bench_module, "run_agent_comparison", _fake_run_agent_comparison)

        result = runner.invoke(
            app,
            [
                "bench",
                "--methods",
                "heuristic",
                "--datasets",
                "hospital",
                "--seeds",
                "3",
                "--cache-root",
                "cache-dir",
                "--no-verify-dataset-hashes",
            ],
        )

        assert result.exit_code == 0
        assert captured["methods"] == ["heuristic"]
        assert captured["datasets"] == ["hospital"]
        assert captured["seeds"] == 3
        assert captured["cache_root"] == Path("cache-dir")
        assert captured["verify_dataset_hashes"] is False

    def test_bench_json_output(self, monkeypatch: Any) -> None:
        def _fake_run_agent_comparison(**kwargs: Any) -> BenchmarkRunOutput:
            return _stub_output()

        monkeypatch.setattr(bench_module, "run_agent_comparison", _fake_run_agent_comparison)

        result = runner.invoke(app, ["bench", "--json"])

        assert result.exit_code == 0
        assert '"aggregates"' in result.output
        assert '"method": "heuristic"' in result.output
