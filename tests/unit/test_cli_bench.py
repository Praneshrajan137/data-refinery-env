"""Unit tests for the ``dataforge bench`` CLI command."""

from __future__ import annotations

import importlib
from typing import Any

from typer.testing import CliRunner

from dataforge.bench.core import AggregateBenchmarkResult, BenchmarkRunOutput
from dataforge.cli import app

bench_module = importlib.import_module("dataforge.cli.bench")

runner = CliRunner()


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

        assert result.exit_code == 0
        assert "--methods" in result.output
        assert "--datasets" in result.output

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
