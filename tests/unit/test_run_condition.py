"""Tests for the bounded, cost-estimating Phase B run harness."""

from __future__ import annotations

from scripts.bench.run_condition import RunCondition, estimate_condition, plan_run


def test_estimate_condition_math() -> None:
    condition = RunCondition(
        label="minimal", n_issues=100, samples=3, avg_input_tokens=1000, avg_output_tokens=100
    )
    estimate = estimate_condition(condition, usd_per_1k_input=0.003, usd_per_1k_output=0.015)
    assert estimate["calls"] == 300
    assert estimate["input_tokens"] == 300_000
    assert estimate["output_tokens"] == 30_000
    # 300 * 0.003 + 30 * 0.015 = 0.9 + 0.45
    assert estimate["estimated_usd"] == 1.35


def test_plan_run_within_guard() -> None:
    plan = plan_run(
        [RunCondition(label="a", n_issues=10, samples=3)],
        usd_per_1k_input=0.003,
        usd_per_1k_output=0.015,
        max_usd=15.0,
    )
    assert plan["within_guard"] is True
    assert plan["total_estimated_usd"] == plan["conditions"][0]["estimated_usd"]


def test_plan_run_trips_guard_when_over_budget() -> None:
    plan = plan_run(
        [RunCondition(label="huge", n_issues=1_000_000, samples=3)],
        usd_per_1k_input=0.003,
        usd_per_1k_output=0.015,
        max_usd=15.0,
    )
    assert plan["within_guard"] is False
    assert plan["total_estimated_usd"] > 15.0
