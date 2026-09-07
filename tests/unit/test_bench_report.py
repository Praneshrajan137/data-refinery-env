"""Unit tests for benchmark report helpers."""

from __future__ import annotations

import json
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
from scripts.bench.run_sota_comparison import build_sota_payload

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "bench"
_ROOT = Path(__file__).resolve().parents[2]


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
        homepage_path = tmp_path / "docs" / "docs" / "index.md"
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
        homepage_path.parent.mkdir(parents=True, exist_ok=True)
        homepage_path.write_text(
            "# Home\n\n<!-- BENCH:START -->old<!-- BENCH:END -->\n",
            encoding="utf-8",
        )

        write_benchmark_outputs(
            agent_json_path=agent_json,
            sota_json_path=sota_json,
            report_path=report_path,
            readme_path=readme_path,
            homepage_path=homepage_path,
        )
        first_readme = readme_path.read_text(encoding="utf-8")
        first_homepage = homepage_path.read_text(encoding="utf-8")
        write_benchmark_outputs(
            agent_json_path=agent_json,
            sota_json_path=sota_json,
            report_path=report_path,
            readme_path=readme_path,
            homepage_path=homepage_path,
        )

        assert readme_path.read_text(encoding="utf-8") == first_readme
        assert homepage_path.read_text(encoding="utf-8") == first_homepage
        assert "Generated from `eval/results/agent_comparison.json`" in first_homepage

    def test_sota_payload_is_citation_evidence_not_reproduced_rows(self) -> None:
        payload = build_sota_payload()

        assert payload["schema_version"] == "dataforge_sota_citation_v1"
        source = payload["source"]
        assert isinstance(source, dict)
        assert source["title"] == "BClean: A Bayesian Data Cleaning System"
        assert source["url"] == "https://arxiv.org/abs/2311.06517"
        assert len(source["source_sha256"]) == 64

        # Every row must cite one of the DECLARED sources -- not necessarily the
        # primary one. This assertion previously read
        # `row["source_title"] == source["title"]`, which silently required the
        # artifact to be single-source and would have rejected citing a second
        # paper at all. The property that actually matters is that no row carries
        # unattributed provenance.
        declared = payload["sources"]
        assert isinstance(declared, list)
        titles = {str(entry["source_title"]) for entry in declared}
        hashes = {str(entry["source_sha256"]) for entry in declared}
        assert source["title"] in titles

        for row in payload["rows"]:
            assert row["evidence_kind"] == "citation_only"
            assert row["source_title"] in titles
            assert row["source_sha256"] in hashes
            assert len(str(row["source_sha256"])) == 64
            assert row["source_short"]
            assert row["source_table"]

    def test_generator_matches_the_committed_artifact(self) -> None:
        """The generator must be the single source of truth for the committed JSON.

        It was not. On 2026-09-01 four rows (BClean 0.976, BClean PI/PIP, PClean,
        GARF) were added to `eval/results/sota_comparison.json` by hand and never to
        `run_sota_comparison.py`, which kept emitting the original four. Running the
        documented regeneration command would therefore have deleted the correction
        and restored the two-weakest-rows mis-citation it existed to fix.

        The pre-existing test could not catch it: it asserted the schema version and
        per-row evidence kind, never the row POPULATION. Same defect class as the
        four gates in this repository found unable to fail -- good assertions about a
        check's logic, none about its effect.
        """
        committed = json.loads(
            (_ROOT / "eval" / "results" / "sota_comparison.json").read_text(encoding="utf-8")
        )

        assert build_sota_payload() == committed, (
            "eval/results/sota_comparison.json has diverged from its generator. "
            "Regenerate with `python scripts/bench/run_sota_comparison.py` and rerun "
            "`python scripts/bench/generate_report.py`; never hand-edit the artifact."
        )

    def test_every_system_above_us_on_hospital_is_cited(self) -> None:
        """Omitting a stronger row of a correctly-cited table is a misleading citation.

        This is the rule the 2026-09-01 audit extracted after the table was found to
        hold only the two weakest rows of BClean Table 4. Pinning the known-stronger
        systems means dropping one becomes a test failure rather than a quiet
        improvement in how the comparison reads.
        """
        payload = build_sota_payload()
        hospital = {
            str(row["method"]): float(row["f1"])
            for row in payload["rows"]
            if row["dataset"] == "hospital"
        }

        # Each of these is published ABOVE this repository's 0.7926 on hospital.
        for method, expected in {
            "BClean": 0.976,
            "BClean (PI/PIP)": 0.980,
            "PClean": 0.962,
            "Cocoon": 0.900,
        }.items():
            assert method in hospital, (
                f"{method} is published above us on hospital and must be cited"
            )
            assert hospital[method] == expected
