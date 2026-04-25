"""Unit tests for benchmark method helpers and LLM baselines."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from dataforge.bench.groq_client import GroqCompletion
from dataforge.bench.methods import (
    _chunk_records,
    _column_stats,
    _extract_json_object,
    _repairs_from_payload,
    chunk_row_indices,
    run_llm_react_episode,
    run_llm_zeroshot_episode,
    run_random_episode,
)
from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset
from dataforge.datasets.registry import DatasetMetadata


def _dataset() -> RealWorldDataset:
    dirty_df = pd.DataFrame(
        {
            "Phone": ["2175550101", "not available"],
            "Score": ["5", "45"],
        }
    )
    clean_df = pd.DataFrame(
        {
            "Phone": ["2175550101", "2175550202"],
            "Score": ["5", "4.5"],
        }
    )
    metadata = DatasetMetadata(
        name="hospital",
        domain="healthcare",
        n_rows=2,
        n_columns=2,
        error_types=("missing_value", "formatting"),
        source_urls=("dirty", "clean"),
        citation="fixture",
    )
    return RealWorldDataset(
        metadata=metadata,
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=("Phone", "Score"),
        ground_truth=(
            GroundTruthCell(
                row=1,
                column="Phone",
                dirty_value="not available",
                clean_value="2175550202",
            ),
            GroundTruthCell(row=1, column="Score", dirty_value="45", clean_value="4.5"),
        ),
    )


@dataclass
class _StubClient:
    responses: list[GroqCompletion]
    model: str = "llama-3.3-70b-versatile"

    def __post_init__(self) -> None:
        self.calls: list[list[dict[str, str]]] = []

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        self.calls.append(messages)
        return self.responses.pop(0)


class TestMethodHelpers:
    """Low-level helper coverage for benchmark methods."""

    def test_chunk_records_and_column_stats(self) -> None:
        dataset = _dataset()
        records = _chunk_records(dataset, (0, 1))
        stats = _column_stats(dataset, ["Score"])

        assert records[1]["Phone"] == "not available"
        assert stats["Score"]["unique_count"] == 2
        assert stats["Score"]["median"] == 25.0

    def test_extract_json_object_and_repairs_from_payload(self) -> None:
        payload = _extract_json_object(
            '```json\n{"repairs":[{"row":1,"column":"Score","new_value":"4.5","reason":"fix"}]}\n```'
        )
        assert payload is not None

        repairs = _repairs_from_payload(payload)
        assert repairs[0].new_value == "4.5"
        assert chunk_row_indices(2) == ((0,), (1,))


class TestBenchmarkMethods:
    """Method-level behavior for random and LLM-backed baselines."""

    def test_random_episode_uses_bounded_budget(self) -> None:
        result = run_random_episode(_dataset(), seed=7)

        assert result.status == "ok"
        assert result.avg_steps == 25.0
        assert result.provider == "local"

    def test_llm_zeroshot_episode_scores_predicted_repairs(self) -> None:
        client = _StubClient(
            [
                GroqCompletion(
                    text='{"repairs":[]}',
                    prompt_tokens=6,
                    completion_tokens=3,
                    warnings=(),
                ),
                GroqCompletion(
                    text=(
                        '{"repairs":['
                        '{"row":1,"column":"Phone","new_value":"2175550202","reason":"phone"},'
                        '{"row":1,"column":"Score","new_value":"4.5","reason":"score"}]}'
                    ),
                    prompt_tokens=10,
                    completion_tokens=5,
                    warnings=(),
                ),
            ]
        )

        result = run_llm_zeroshot_episode(_dataset(), seed=0, client=client)

        assert result.status == "ok"
        assert result.f1 == 1.0
        assert result.llm_calls == 2
        assert result.quota_units == 0.002

    def test_llm_react_episode_uses_tool_then_submit_repairs(self) -> None:
        client = _StubClient(
            [
                GroqCompletion(
                    text='{"action":"finish"}',
                    prompt_tokens=6,
                    completion_tokens=3,
                    warnings=(),
                ),
                GroqCompletion(
                    text='{"action":"column_stats","columns":["Score"]}',
                    prompt_tokens=8,
                    completion_tokens=4,
                    warnings=(),
                ),
                GroqCompletion(
                    text=(
                        '{"action":"submit_repairs","repairs":['
                        '{"row":1,"column":"Score","new_value":"4.5","reason":"score"}]}'
                    ),
                    prompt_tokens=9,
                    completion_tokens=4,
                    warnings=("missing_usage_payload",),
                ),
            ]
        )

        result = run_llm_react_episode(_dataset(), seed=0, client=client)

        assert result.status == "ok"
        assert result.llm_calls == 3
        assert result.avg_steps == 4.0
        assert "missing_usage_payload" in result.warnings
