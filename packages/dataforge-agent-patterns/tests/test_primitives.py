"""Tests for the five public agent-pattern primitives."""

from __future__ import annotations

from collections.abc import Callable

from dataforge_agent_patterns import (
    CausalCascadeDetector,
    ConstitutionalFilter,
    ConstitutionalRule,
    DirectedEffect,
    ProgressiveToolDisclosure,
    ReversibleTransaction,
    SMTVerifiedAction,
)


def test_progressive_tool_disclosure() -> None:
    """Tools are hidden until the task is difficult enough."""
    disclosure = ProgressiveToolDisclosure({"inspect": 0, "shell": 3})
    assert disclosure.visible_tools(task_difficulty=1) == ("inspect",)
    assert disclosure.should_disclose("shell", task_difficulty=2) is False


def test_constitutional_filter_wraps_agent() -> None:
    """Rejected actions do not reach the wrapped agent."""
    rule = ConstitutionalRule(
        rule_id="no-delete",
        description="Reject delete actions.",
        predicate=lambda action: action != "delete",
    )
    guarded = ConstitutionalFilter([rule]).wrap(lambda action: f"ran:{action}")

    verdict, result = guarded("delete")

    assert verdict.allowed is False
    assert result is None


def test_reversible_transaction_rolls_back() -> None:
    """Rollback calls undo in reverse transaction order."""
    state: list[str] = []
    tx = ReversibleTransaction()

    @tx.wrap("append")
    def append_item(value: str) -> tuple[str, Callable[[], None]]:
        state.append(value)

        def undo() -> None:
            state.pop()

        return value, undo

    assert append_item("x") == "x"
    assert tx.records[0].name == "append"
    tx.rollback_last()
    assert state == []


def test_smt_verified_action() -> None:
    """Z3 constraints reject invalid structured actions."""
    checker = SMTVerifiedAction([lambda variables: variables["amount"] >= 0])
    assert checker.verify({"amount": 1}).satisfiable is True
    assert checker.verify({"amount": -1}).satisfiable is False


def test_causal_cascade_detector() -> None:
    """Selected actions are marked cascading when one reaches another."""
    detector = CausalCascadeDetector(
        [DirectedEffect(source="discount", target="total", confidence=0.9)]
    )
    verdict = detector.detect(["discount", "total"])
    assert verdict.cascading is True
    assert verdict.roots == ("discount",)
