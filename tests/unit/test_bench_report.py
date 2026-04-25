"""Unit tests for benchmark report helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.bench.report import (
    build_readme_benchmark_block,
    load_agent_output,
    load_sota_output,
    render_benchmark_report,
    replace_benchmark_block,
    write_benchmark_outputs,
)

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "bench"


class TestReportHelpers:
    """Report rendering and README block updates."""

    def test_replace_benchmark_block_requires_markers(self) -> None:
        with pytest.raises(ValueError, match="markers"):
            replace_benchmark_block("# DataForge", "new")

    def test_render_report_and_readme_block(self) -> None:
        agent_output = load_agent_output(_FIXTURES / "agent_comparison.json")
        sota_output = load_sota_output(_FIXTURES / "sota_comparison.json")

        report = render_benchmark_report(agent_output, sota_output)
        block = build_readme_benchmark_block(agent_output, Path("BENCHMARK_REPORT.md"))

        assert "Cross-Dataset Local Results" in report
        assert "Citation-Only SOTA Reference" in report
        assert "BENCHMARK_REPORT.md" in block

    def test_write_benchmark_outputs_is_idempotent(self, tmp_path: Path) -> None:
        agent_json = tmp_path / "agent.json"
        sota_json = tmp_path / "sota.json"
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
            "# DataForge\n\n<!-- BENCH:START -->old<!-- BENCH:END -->\n",
            encoding="utf-8",
        )

        write_benchmark_outputs(
            agent_json_path=agent_json,
            sota_json_path=sota_json,
            report_path=report_path,
            readme_path=readme_path,
        )
        first_readme = readme_path.read_text(encoding="utf-8")
        write_benchmark_outputs(
            agent_json_path=agent_json,
            sota_json_path=sota_json,
            report_path=report_path,
            readme_path=readme_path,
        )

        assert readme_path.read_text(encoding="utf-8") == first_readme
