"""Lazy Z3 verification for structured actions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pydantic import BaseModel, Field

__all__ = ["SMTVerifiedAction", "SMTVerdict"]

Constraint = Callable[[Mapping[str, Any]], Any]


class SMTVerdict(BaseModel):
    """SMT verification result.

    Args:
        satisfiable: Whether the action satisfies all constraints.
        reason: Short explanation.
    """

    satisfiable: bool
    reason: str = Field(min_length=1)

    model_config = {"frozen": True}


class SMTVerifiedAction:
    """Verify integer action fields with lazy Z3 constraints.

    Args:
        constraints: Callables that accept Z3 variables and return Z3 formulas.

    Example:
        >>> checker = SMTVerifiedAction([lambda v: v["amount"] >= 0])
        >>> checker.verify({"amount": 3}).satisfiable
        True
    """

    def __init__(self, constraints: Sequence[Constraint]) -> None:
        self._constraints = tuple(constraints)

    def verify(self, action: Mapping[str, int | bool]) -> SMTVerdict:
        """Verify a structured action.

        Args:
            action: Mapping of field names to integer or boolean values.

        Returns:
            SMTVerdict.
        """
        try:
            from z3 import Bool, BoolVal, Int, Solver, sat  # type: ignore[import-untyped]
        except Exception as exc:
            return SMTVerdict(satisfiable=False, reason=f"z3 unavailable: {exc}")

        solver = Solver()
        variables: dict[str, Any] = {}
        for name, value in action.items():
            if isinstance(value, bool):
                variable = Bool(name)
                solver.add(variable == BoolVal(value))
            else:
                variable = Int(name)
                solver.add(variable == int(value))
            variables[name] = variable

        for constraint in self._constraints:
            solver.add(constraint(variables))

        if solver.check() == sat:
            return SMTVerdict(satisfiable=True, reason="satisfiable")
        return SMTVerdict(satisfiable=False, reason="unsatisfiable")
