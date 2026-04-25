"""Integration tests for benchmark workflow and report generation."""

from __future__ import annotations

import functools
import importlib
import subprocess
import sys
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dataforge.bench.runner import run_agent_comparison
from dataforge.cli import app

bench_module = importlib.import_module("dataforge.cli.bench")

_ROOT = Path(__file__).resolve().parents[2]
_FIXTURES = _ROOT / "tests" / "fixtures" / "bench"
runner = CliRunner()


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


class TestBenchWorkflow:
    """End-to-end benchmark paths that stay fully offline."""

    def test_cached_heuristic_bench_command_runs_under_thirty_seconds(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cache_root = tmp_path / "cache"
        output_json = tmp_path / "eval" / "results" / "agent_comparison.json"
        _populate_cache(cache_root)

        monkeypatch.setattr(
            bench_module,
            "run_agent_comparison",
            functools.partial(run_agent_comparison, cache_root=cache_root),
        )

        start = time.monotonic()
        result = runner.invoke(
            app,
            [
                "bench",
                "--methods",
                "heuristic",
                "--datasets",
                "hospital",
                "--seeds",
                "1",
                "--output-json",
                str(output_json),
            ],
        )
        elapsed = time.monotonic() - start

        assert result.exit_code == 0
        assert output_json.exists()
        assert elapsed < 30.0

    def test_generate_report_script_creates_report_and_updates_readme_block(
        self,
        tmp_path: Path,
    ) -> None:
        eval_dir = tmp_path / "eval" / "results"
        eval_dir.mkdir(parents=True, exist_ok=True)
        agent_json = eval_dir / "agent_comparison.json"
        sota_json = eval_dir / "sota_comparison.json"
        report_path = tmp_path / "BENCHMARK_REPORT.md"
        readme_path = tmp_path / "README.md"
        agent_json.write_text(
            (_FIXTURES / "agent_comparison.json").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        sota_json.write_text(
            (_FIXTURES / "sota_comparison.json").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        readme_path.write_text(
            "# DataForge\n\n## Benchmark Results\n\n<!-- BENCH:START -->old<!-- BENCH:END -->\n",
            encoding="utf-8",
        )

        result = subprocess.run(
            [
                sys.executable,
                str(_ROOT / "scripts" / "bench" / "generate_report.py"),
                "--agent-json",
                str(agent_json),
                "--sota-json",
                str(sota_json),
                "--report-path",
                str(report_path),
                "--readme-path",
                str(readme_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert report_path.exists()
        report_text = report_path.read_text(encoding="utf-8")
        readme_text = readme_path.read_text(encoding="utf-8")
        assert "Per-Dataset Local Results" in report_text
        assert "Citation-Only SOTA Reference" in report_text
        assert "BENCHMARK_REPORT.md" in readme_text
