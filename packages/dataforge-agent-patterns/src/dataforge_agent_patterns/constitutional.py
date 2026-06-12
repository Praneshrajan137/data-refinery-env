"""Constitutional verdict wrapper for arbitrary agents."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["ConstitutionalFilter", "ConstitutionalRule", "SafetyVerdict"]

R = TypeVar("R")


class SafetyVerdict(BaseModel):
    """Safety decision produced by ConstitutionalFilter.

    Args:
        allowed: Whether the action passed every rule.
        failed_rules: Rule ids that rejected the action.
    """

    allowed: bool
    failed_rules: tuple[str, ...] = ()

    model_config = {"frozen": True}


class ConstitutionalRule(BaseModel):
    """A named predicate over an action object.

    Args:
        rule_id: Stable rule identifier.
        description: Human-readable rule description.
        predicate: Callable returning True when the action is allowed.
    """

    rule_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    predicate: Callable[[object], bool] = Field(exclude=True)

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)


class ConstitutionalFilter:
    """Evaluate actions against a small constitution before agent execution.

    Args:
        rules: Ordered safety rules.

    Example:
        >>> rule = ConstitutionalRule(
        ...     rule_id="no-delete",
        ...     description="Reject delete actions.",
        ...     predicate=lambda action: action != "delete",
        ... )
        >>> ConstitutionalFilter([rule]).evaluate("delete").allowed
        False
    """

    def __init__(self, rules: list[ConstitutionalRule] | tuple[ConstitutionalRule, ...]) -> None:
        self._rules = tuple(rules)

    def evaluate(self, action: object) -> SafetyVerdict:
        """Evaluate an action against every rule.

        Args:
            action: Action object to evaluate.

        Returns:
            SafetyVerdict with failed rule ids.
        """
        failed = tuple(rule.rule_id for rule in self._rules if not rule.predicate(action))
        return SafetyVerdict(allowed=not failed, failed_rules=failed)

    def wrap(self, agent: Callable[..., R]) -> Callable[..., tuple[SafetyVerdict, R | None]]:
        """Wrap an agent callable with a first-argument safety check.

        Args:
            agent: Callable whose first positional argument is the action.

        Returns:
            Callable returning `(verdict, result)`; result is None when denied.
        """

        def guarded(action: object, *args: Any, **kwargs: Any) -> tuple[SafetyVerdict, R | None]:
            verdict = self.evaluate(action)
            if not verdict.allowed:
                return verdict, None
            return verdict, agent(action, *args, **kwargs)

        return guarded
