"""Demonstrate the dataforge-evals agent protocol with a custom rule-based agent.

Shows how to implement a minimal agent adapter that plugs into the
evaluation harness without any provider dependencies.

Usage:
    python examples/plug_in_your_own_agent.py
"""

from __future__ import annotations

from pathlib import Path

from dataforge_evals import AgentRunResult, Fix, Task, Usage
from dataforge_evals.harness import HarnessConfig, run_harness
from dataforge_evals.report import write_report


class RuleBasedExampleAgent:
    """Small custom adapter showing the dataforge-evals agent protocol.

    This agent applies a single heuristic rule: if a cell in the "Score"
    column contains "45", it proposes "4.5" (a decimal shift correction).
    It intentionally misses other ground-truth corrections to demonstrate
    partial scoring.
    """

    name = "rule-based-example"

    def run(self, task: Task) -> AgentRunResult:
        """Return a simple deterministic fix list for demonstration.

        Args:
            task: The evaluation task.

        Returns:
            AgentRunResult with rule-based fixes and zero usage.
        """
        fixes: list[Fix] = []
        for row_index, row in task.dirty_df.iterrows():
            if "Score" in task.canonical_columns and str(row["Score"]) == "45":
                fixes.append(
                    Fix(row=int(row_index), column="Score", new_value="4.5", reason="decimal shift")
                )
        return AgentRunResult(fixes=fixes, usage=Usage(), steps=1, model="local-rules")


def main() -> None:
    """Run a custom in-process agent on the synthetic task."""
    run = run_harness(
        HarnessConfig(
            agents=(RuleBasedExampleAgent(),),
            datasets=("synthetic",),
            trials=3,
            seeds=(0, 1, 2),
            output=Path("reports/custom-agent.md"),
        )
    )
    write_report(run, Path("reports/custom-agent.md"), json_path=Path("reports/custom-agent.json"))
    print("Report written to reports/custom-agent.md")


if __name__ == "__main__":
    main()
