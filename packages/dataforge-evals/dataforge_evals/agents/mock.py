"""Deterministic mock/oracle agent for smoke tests and CI.

Returns perfect ground-truth fixes for every task, enabling
deterministic evaluation without network access. This agent
intentionally reads ``task.ground_truth`` — it is an oracle,
not a realistic agent. Use it only for harness validation.
"""

from __future__ import annotations

from dataforge_evals.agents.base import AgentRunResult, Fix, Task, Usage


class MockAgent:
    """Deterministic no-network oracle agent for smoke tests and CI.

    Returns the exact ground-truth fixes for any task, always achieving
    perfect precision, recall, and F1. Designed for validating the harness,
    grader, and report pipeline without external dependencies.

    Attributes:
        name: CLI identifier ``"mock"``.
    """

    name = "mock"
    uses_ground_truth = True

    def run(self, task: Task) -> AgentRunResult:
        """Return the known ground-truth fixes for deterministic evaluation.

        Args:
            task: Any evaluation task with ground-truth corrections.

        Returns:
            AgentRunResult with perfect fixes and zero usage.
        """
        fixes = [
            Fix(row=cell.row, column=cell.column, new_value=cell.clean_value, reason="mock oracle")
            for cell in task.ground_truth
        ]
        return AgentRunResult(fixes=fixes, usage=Usage(), steps=len(fixes), model="mock-oracle")
