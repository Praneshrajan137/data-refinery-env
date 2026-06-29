"""Additional edge coverage for the DataForge OpenEnv runtime."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from dataforge.agent.tool_actions import (
    Action,
    Diagnose,
    Fix,
    Hypothesis,
    InspectRows,
    PatternMatch,
    RootCause,
    SqlQuery,
    StatTest,
)
from dataforge.causal.dag import CausalDAG
from dataforge.env.environment import DataForgeEnv


def _env(max_steps: int = 5) -> DataForgeEnv:
    env = DataForgeEnv(max_steps=max_steps)
    env.reset(seed=7)
    return env


def test_raw_parse_errors_and_terminal_reentry_are_reported() -> None:
    """Malformed raw actions return error observations and terminal calls stay terminal."""
    env = _env(max_steps=1)

    result = env.step({"action_type": "NOPE"})
    repeated = env.step(InspectRows(action_type="INSPECT_ROWS", row_indices=[0]))

    assert result.done is True
    assert result.observation.latest_result is not None
    assert result.observation.latest_result.action_type == "ERROR"
    assert repeated.done is True
    assert repeated.observation.step_budget_remaining == 0


def test_inspect_sql_stat_and_pattern_error_branches() -> None:
    """Read-only tools surface validation failures without crashing the episode."""
    env = _env(max_steps=30)

    invalid_inspect = env.step(InspectRows(action_type="INSPECT_ROWS", row_indices=[999]))
    filtered_inspect = env.step(
        InspectRows(action_type="INSPECT_ROWS", row_indices=[0], column_names=["missing"])
    )
    parse_error = env.step(SqlQuery(action_type="SQL_QUERY", query="SELECT * FROM"))
    rejected_write = env.step(SqlQuery(action_type="SQL_QUERY", query="DELETE FROM data"))
    execution_error = env.step(SqlQuery(action_type="SQL_QUERY", query="SELECT missing FROM data"))

    env._df["text_only"] = ["alpha"] * len(env._df)
    non_numeric = env.step(
        StatTest(action_type="STAT_TEST", test_type="zscore", column="text_only")
    )
    iqr = env.step(StatTest(action_type="STAT_TEST", test_type="iqr", column="rating"))
    ks = env.step(StatTest(action_type="STAT_TEST", test_type="ks", column="rating"))
    invalid_stat = env.step(
        cast(
            Action,
            StatTest.model_construct(action_type="STAT_TEST", test_type="unknown", column="rating"),
        )
    )
    missing_pattern_column = env.step(
        PatternMatch(action_type="PATTERN_MATCH", column="missing", pattern="x")
    )
    invalid_pattern = env.step(
        PatternMatch(action_type="PATTERN_MATCH", column="rating", pattern="[")
    )
    non_matches = env.step(
        PatternMatch(
            action_type="PATTERN_MATCH",
            column="rating",
            pattern=r"^999$",
            expect_match=False,
        )
    )

    assert invalid_inspect.observation.latest_result.error is not None
    assert filtered_inspect.observation.latest_result.success is True
    assert parse_error.observation.latest_result.error["verdict"] == "error"
    assert rejected_write.observation.latest_result.error["verdict"] == "rejected"
    assert execution_error.observation.latest_result.error["verdict"] == "error"
    assert non_numeric.observation.latest_result.error["verdict"] == "error"
    assert iqr.observation.latest_result.data["test"] == "iqr"
    assert ks.observation.latest_result.data["test"] == "ks"
    assert invalid_stat.observation.latest_result.error["reason"].startswith("Unknown test type")
    assert missing_pattern_column.observation.latest_result.error["verdict"] == "error"
    assert invalid_pattern.observation.latest_result.error["reason"].startswith("Invalid regex")
    assert non_matches.observation.latest_result.data["total_matches"] == len(env._df)


def test_ks_stat_rejects_a_constant_column() -> None:
    """KS normality testing needs a finite, non-zero fitted scale."""
    env = _env()
    env._df["constant"] = [1.0] * len(env._df)

    result = env.step(StatTest(action_type="STAT_TEST", test_type="ks", column="constant"))

    assert result.observation.latest_result.success is False
    assert result.observation.latest_result.error == {
        "verdict": "error",
        "reason": "KS test requires at least two distinct numeric values",
    }


def test_hypothesis_root_cause_and_fix_scoring_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Diagnosis, root cause, and fix branches exercise success and failure paths."""
    env = _env(max_steps=20)

    hypothesis = env.step(
        Hypothesis(
            action_type="HYPOTHESIS",
            claim="rating row has a decimal shift",
            affected_rows=[5],
            affected_columns=["rating"],
            root_cause_type="decimal_shift",
        )
    )
    no_detected_roots = env.step(RootCause(action_type="ROOT_CAUSE", error_indices=[0]))
    env.step(Diagnose(action_type="DIAGNOSE", row=5, column="rating", issue_type="decimal_shift"))
    invalid_root_index = env.step(RootCause(action_type="ROOT_CAUSE", error_indices=[5]))
    monkeypatch.setattr(
        "dataforge.causal.pc.discover_causal_dag",
        lambda df: SimpleNamespace(dag=CausalDAG(["rating"])),
    )
    first_root = env.step(RootCause(action_type="ROOT_CAUSE", error_indices=[0]))
    cached_root = env.step(RootCause(action_type="ROOT_CAUSE", error_indices=[0]))

    monkeypatch.setattr(env, "_check_safety", lambda action: (True, "ok"))
    detection_only = env.step(
        Fix(
            action_type="FIX",
            row=4,
            column="phone_number",
            new_value="5550000000",
            justification="type mismatch",
        )
    )
    wrong_value = env.step(
        Fix(
            action_type="FIX",
            row=5,
            column="rating",
            new_value="7.0",
            justification="wrong candidate",
        )
    )
    partial_numeric = env.step(
        Fix(
            action_type="FIX",
            row=5,
            column="rating",
            new_value="4.51",
            justification="near expected value",
        )
    )
    already_fixed = env.step(
        Fix(
            action_type="FIX",
            row=5,
            column="rating",
            new_value="4.5",
            justification="already fixed",
        )
    )

    env2 = _env()
    monkeypatch.setattr(env2, "_check_safety", lambda action: (False, "blocked"))
    safety_block = env2.step(
        Fix(
            action_type="FIX",
            row=5,
            column="rating",
            new_value="4.5",
            justification="blocked",
        )
    )
    actual_safety_ok, actual_safety_msg = _env()._check_safety(
        Fix(
            action_type="FIX",
            row=5,
            column="rating",
            new_value="4.5",
            justification="decimal shift",
        )
    )

    assert hypothesis.reward > 0
    assert no_detected_roots.observation.latest_result.error["verdict"] == "error"
    assert invalid_root_index.observation.latest_result.error["verdict"] == "error"
    assert first_root.observation.latest_result.success is True
    assert cached_root.observation.latest_result.success is True
    assert detection_only.observation.latest_result.data["result"] == "detection_only"
    assert wrong_value.observation.latest_result.data["result"] == "wrong_value"
    assert partial_numeric.observation.latest_result.data["result"] == "partial_numeric"
    assert already_fixed.observation.latest_result.data["result"] == "already_fixed"
    assert safety_block.observation.latest_result.error["reason"] == "blocked"
    assert actual_safety_ok is True
    assert actual_safety_msg == "Passed safety and verification"


def test_noise_helper_is_deterministic_and_noops_without_rng() -> None:
    """Noise helper preserves rows when noise is disabled and returns copies when enabled."""
    env = _env()
    rows = [{"name": "Springfield General", "_row_index": 0}]

    assert env._inject_noise(rows) == rows

    noisy_env = _env()
    noisy_env.reset(seed=1, noisy=True)
    noisy = noisy_env._inject_noise(rows)

    assert noisy[0]["_row_index"] == 0
    assert rows[0]["name"] == "Springfield General"
