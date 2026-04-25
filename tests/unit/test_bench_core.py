"""Unit tests for benchmark helpers and metrics."""

from __future__ import annotations

import pytest

from dataforge.bench.core import (
    BenchmarkRepair,
    chunk_row_indices,
    estimate_llm_calls,
    normalize_repairs,
    quota_units,
    score_repairs,
    validate_estimated_calls,
)
from dataforge.datasets.real_world import GroundTruthCell


class TestChunking:
    """Chunk helpers should be deterministic and bounded."""

    def test_chunk_row_indices_cover_rows_in_order(self) -> None:
        chunks = chunk_row_indices(2376)

        assert len(chunks) == 20
        assert chunks[0][0] == 0
        assert chunks[-1][-1] == 2375
        flattened = [row for chunk in chunks for row in chunk]
        assert flattened == list(range(2376))


class TestRepairNormalization:
    """Normalization should apply last-write-wins per cell."""

    def test_normalize_repairs_last_write_wins(self) -> None:
        repairs = [
            BenchmarkRepair(row=2, column="Score", new_value="4.0", reason="first"),
            BenchmarkRepair(row=2, column="Score", new_value="4.5", reason="second"),
            BenchmarkRepair(row=1, column="Phone", new_value="2175550202", reason="phone"),
        ]

        normalized = normalize_repairs(repairs)

        assert normalized == [
            BenchmarkRepair(row=2, column="Score", new_value="4.5", reason="second"),
            BenchmarkRepair(row=1, column="Phone", new_value="2175550202", reason="phone"),
        ]


class TestScoring:
    """Repair scoring should use exact corrected values."""

    def test_wrong_value_on_right_cell_counts_as_fp_and_fn(self) -> None:
        ground_truth = [
            GroundTruthCell(row=2, column="Score", dirty_value="45", clean_value="4.5"),
            GroundTruthCell(
                row=2, column="Phone", dirty_value="not available", clean_value="2175550303"
            ),
        ]
        repairs = [BenchmarkRepair(row=2, column="Score", new_value="5.0", reason="wrong")]

        metrics = score_repairs(ground_truth, repairs)

        assert metrics.tp == 0
        assert metrics.fp == 1
        assert metrics.fn == 2
        assert metrics.precision == 0.0
        assert metrics.recall == 0.0
        assert metrics.f1 == 0.0


class TestQuotaDiscipline:
    """Quota helpers should protect the free-tier budget."""

    def test_estimate_llm_calls_matches_plan_budget(self) -> None:
        estimated = estimate_llm_calls(
            methods=["llm_zeroshot", "llm_react"],
            datasets=["hospital", "flights", "beers"],
            seeds=3,
        )

        assert estimated == 540

    def test_validate_estimated_calls_requires_override(self) -> None:
        with pytest.raises(ValueError, match="really-run-big-bench"):
            validate_estimated_calls(estimated_calls=540, really_run_big_bench=False)

        validate_estimated_calls(estimated_calls=540, really_run_big_bench=True)

    def test_quota_units_prefers_larger_fraction(self) -> None:
        assert quota_units(llm_calls=10, prompt_tokens=100, completion_tokens=100) == 0.01
        assert quota_units(llm_calls=1, prompt_tokens=30000, completion_tokens=30000) == 0.6
