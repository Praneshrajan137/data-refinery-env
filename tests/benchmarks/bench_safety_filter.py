"""Benchmark gate for the Week 3 safety filter."""

from __future__ import annotations

import math

from dataforge.detectors.base import AggregateDependency, Schema
from dataforge.repairers.base import ProposedFix
from dataforge.safety import SafetyContext, SafetyFilter
from dataforge.transactions.txn import CellFix


def _p95(samples: list[float]) -> float:
    sorted_samples = sorted(samples)
    index = max(0, math.ceil(0.95 * len(sorted_samples)) - 1)
    return sorted_samples[index]


def test_safety_filter_p95_under_one_millisecond(benchmark: object) -> None:
    filter_ = SafetyFilter()
    fix = ProposedFix(
        fix=CellFix(
            row=3,
            column="amount",
            old_value="1020",
            new_value="102",
            detector_id="decimal_shift",
        ),
        reason="candidate",
        confidence=0.9,
        provenance="deterministic",
    )
    schema = Schema(
        columns={"amount": "float"},
        aggregate_dependencies=[
            AggregateDependency(
                source_column="amount",
                aggregate="sum",
                target_column="total_amount",
            )
        ],
    )

    benchmark.pedantic(
        lambda: filter_.evaluate(fix, schema, SafetyContext(confirm_escalations=True)),
        rounds=60,
        iterations=1,
    )
    stats = benchmark.stats.stats
    assert _p95(list(stats.data)) < 0.001
