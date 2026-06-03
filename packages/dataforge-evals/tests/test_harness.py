"""Harness orchestration and reporting smoke tests.

Validates the full pipeline: task loading, agent execution, grading,
aggregation, and report generation without network access.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from dataforge_evals.agents.base import AgentRunResult, Fix, Task, Usage
from dataforge_evals.agents.mock import MockAgent
from dataforge_evals.agents.provider_base import ProviderError
from dataforge_evals.harness import HarnessConfig, run_harness
from dataforge_evals.report import render_markdown, write_report


class BrokenAgent:
    """Agent that raises for failure-taxonomy smoke coverage."""

    name = "broken"

    def run(self, task: Task) -> list[Fix]:
        """Raise a deterministic diagnostic failure."""
        raise ValueError(f"parse failed for {task.name}")


class BadJsonProviderAgent:
    """Agent that mimics a provider returning malformed JSON."""

    name = "bad-json"

    def run(self, task: Task) -> list[Fix]:
        """Raise the same error shape produced by provider parse failures."""
        raise ProviderError("Provider returned non-JSON fixes: not-json", provider="bad-json")


class TimeoutProviderAgent:
    """Agent that mimics a provider HTTP timeout."""

    name = "provider-timeout"

    def run(self, task: Task) -> list[Fix]:
        """Raise the same error shape produced by provider request timeouts."""
        raise ProviderError("request timed out after 3.0 seconds", provider="provider-timeout")


class SlowAgent:
    """Agent that sleeps longer than any reasonable timeout."""

    name = "slow"

    def run(self, task: Task) -> list[Fix]:
        """Block indefinitely to test timeout enforcement."""
        time.sleep(300)
        return []  # pragma: no cover â€” should be interrupted


class PartialAgent:
    """Agent that returns some correct and some incorrect fixes."""

    name = "partial"
    uses_ground_truth = True

    def run(self, task: Task) -> AgentRunResult:
        """Return a mix of correct and incorrect fixes for aggregation testing."""
        fixes: list[Fix] = []
        for i, cell in enumerate(task.ground_truth):
            if i % 2 == 0:
                fixes.append(
                    Fix(
                        row=cell.row,
                        column=cell.column,
                        new_value=cell.clean_value,
                        reason="correct",
                    )
                )
            else:
                fixes.append(
                    Fix(row=cell.row, column=cell.column, new_value="WRONG", reason="incorrect")
                )
        return AgentRunResult(
            fixes=fixes,
            usage=Usage(calls=1, prompt_tokens=100, completion_tokens=50, quota_units=0.001),
            steps=len(fixes),
            model="partial-test",
        )


class EmptyAgent:
    """Agent that misses every repair."""

    name = "empty"

    def run(self, task: Task) -> list[Fix]:
        """Return no fixes."""
        return []


class OverrepairAgent:
    """Agent that changes a valid but clean cell."""

    name = "overrepair"

    def run(self, task: Task) -> list[Fix]:
        """Return one spurious repair on an in-bounds cell."""
        return [Fix(row=0, column="Phone", new_value="SPURIOUS")]


class SchemaCaseAgent:
    """Agent that uses the right column letters with the wrong case."""

    name = "schema-case"
    uses_ground_truth = True

    def run(self, task: Task) -> list[Fix]:
        """Return one case-mismatched column name."""
        cell = task.ground_truth[0]
        return [Fix(row=cell.row, column=cell.column.lower(), new_value=cell.clean_value)]


class LabelLeakingAgent:
    """Agent that should fail if labels are hidden correctly."""

    name = "label-leak"

    def run(self, task: Task) -> list[Fix]:
        """Try to read hidden labels without declaring oracle/test behavior."""
        cell = task.ground_truth[0]
        return [Fix(row=cell.row, column=cell.column, new_value=cell.clean_value)]


class TestHarness:
    """Harness orchestration should run no-network smoke tests."""

    def test_mock_agent_scores_perfectly_on_synthetic_task(self) -> None:
        """Mock oracle agent must achieve F1=1.0 on synthetic task."""
        run = run_harness(
            HarnessConfig(
                agents=(MockAgent(),),
                datasets=("synthetic",),
                trials=3,
                seeds=(0, 1, 2),
            )
        )

        assert len(run.records) == 3
        aggregate = run.aggregates[0]
        assert aggregate.agent == "mock"
        assert aggregate.dataset == "synthetic"
        assert aggregate.trials_completed == 3
        assert aggregate.f1_mean == 1.0
        assert aggregate.quota_units_total == 0.0
        assert run.reproducibility.seeds == [0, 1, 2]
        assert run.records[0].fixes
        assert run.records[0].fixes[0].new_value == "Mercy Hospital"

    def test_failure_taxonomy_records_parse_failure(self) -> None:
        """BrokenAgent with 'parse' in message should classify as parse_failure."""
        run = run_harness(
            HarnessConfig(
                agents=(BrokenAgent(),),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
            )
        )

        aggregate = run.aggregates[0]
        assert aggregate.trials_completed == 0
        assert aggregate.failure_taxonomy == {"parse_failure": 1}

    def test_malformed_provider_json_classified_as_wrong_diag(self) -> None:
        """Malformed provider JSON should be diagnostic, not generic failure."""
        run = run_harness(
            HarnessConfig(
                agents=(BadJsonProviderAgent(),),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
            )
        )

        aggregate = run.aggregates[0]
        assert aggregate.trials_completed == 0
        assert aggregate.failure_taxonomy == {"parse_failure": 1}

    def test_provider_timeout_classified_as_timeout(self) -> None:
        """Provider request timeouts should be timeout failures."""
        run = run_harness(
            HarnessConfig(
                agents=(TimeoutProviderAgent(),),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
            )
        )

        aggregate = run.aggregates[0]
        assert aggregate.trials_completed == 0
        assert aggregate.failure_taxonomy == {"timeout": 1}

    def test_timeout_agent_classified_correctly(self) -> None:
        """SlowAgent should be classified as timeout failure."""
        run = run_harness(
            HarnessConfig(
                agents=(SlowAgent(),),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
                timeout_s=0.5,
            )
        )

        aggregate = run.aggregates[0]
        assert aggregate.trials_completed == 0
        assert "timeout" in aggregate.failure_taxonomy

    def test_multiple_agents_multiple_datasets_cross_product(self) -> None:
        """Two agents x one dataset x 2 trials = 4 trial records."""
        run = run_harness(
            HarnessConfig(
                agents=(MockAgent(), PartialAgent()),
                datasets=("synthetic",),
                trials=2,
                seeds=(0, 1),
            )
        )

        assert len(run.records) == 4
        assert len(run.aggregates) == 2
        mock_agg = next(a for a in run.aggregates if a.agent == "mock")
        partial_agg = next(a for a in run.aggregates if a.agent == "partial")
        assert mock_agg.f1_mean == 1.0
        assert partial_agg.f1_mean is not None
        assert partial_agg.f1_mean < 1.0
        assert partial_agg.failure_taxonomy["wrong_value"] > 0

    def test_reproducibility_block_captures_versions(self) -> None:
        """Reproducibility metadata must include dependency versions."""
        run = run_harness(
            HarnessConfig(
                agents=(MockAgent(),),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
            )
        )

        assert "pandas" in run.reproducibility.dependency_versions
        assert "pydantic" in run.reproducibility.dependency_versions
        assert run.reproducibility.run_date_utc  # non-empty

    def test_report_writes_markdown_and_json(self, tmp_path: Path) -> None:
        """Report generation must produce both markdown and JSON files."""
        run = run_harness(
            HarnessConfig(
                agents=(MockAgent(),),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
            )
        )
        markdown_path = tmp_path / "report.md"
        json_path = tmp_path / "report.json"

        write_report(run, markdown_path, json_path=json_path)

        assert markdown_path.exists()
        assert json_path.exists()
        content = render_markdown(run)
        assert "dataforge-evals commit" in content
        assert "Not a Leaderboard" in content
        assert "Result Type" in content
        assert "baseline" in content
        assert "Runtime (s)" in content
        assert "Mercy Hospital" not in content
        report_payload = json.loads(json_path.read_text(encoding="utf-8"))
        assert report_payload["records"][0]["fixes"][0]["new_value"] == "Mercy Hospital"

    def test_report_includes_dependency_versions(self) -> None:
        """Rendered markdown must contain dependency version information."""
        run = run_harness(
            HarnessConfig(
                agents=(MockAgent(),),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
            )
        )
        content = render_markdown(run)
        assert "Dependency versions:" in content

    def test_partial_agent_quota_units_aggregated(self) -> None:
        """PartialAgent with non-zero usage must aggregate quota units."""
        run = run_harness(
            HarnessConfig(
                agents=(PartialAgent(),),
                datasets=("synthetic",),
                trials=2,
                seeds=(0, 1),
            )
        )
        aggregate = run.aggregates[0]
        assert aggregate.quota_units_total > 0
        assert aggregate.model == "partial-test"

    def test_successful_runs_get_repair_failure_taxonomy(self) -> None:
        """Successful JSON with bad repairs should still explain why F1 is low."""
        run = run_harness(
            HarnessConfig(
                agents=(EmptyAgent(), OverrepairAgent(), SchemaCaseAgent()),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
            )
        )

        taxonomies = {aggregate.agent: aggregate.failure_taxonomy for aggregate in run.aggregates}
        assert taxonomies["empty"]["missed_repair"] > 0
        assert taxonomies["overrepair"]["overrepair"] == 1
        assert taxonomies["schema-case"]["schema_case_error"] == 1

    def test_non_oracle_agents_do_not_receive_ground_truth(self) -> None:
        """Normal agents get label-hidden tasks; only marked oracles receive labels."""
        run = run_harness(
            HarnessConfig(
                agents=(LabelLeakingAgent(),),
                datasets=("synthetic",),
                trials=1,
                seeds=(0,),
            )
        )

        record = run.records[0]
        assert record.status == "failed"
        assert "ground_truth" in (record.failure_message or "")
