"""Release gate for the verified autonomous agent.

A release must not ship an agent that regresses the deterministic baseline.
This module exposes a single check the release workflow can call: it runs the
offline non-regression comparison and returns a structured pass/fail verdict.
The F1 promotion bar (deciding whether the agent becomes the *default* path)
lives in :func:`dataforge.bench.agent_promotion_verdict`, fed by real eval runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dataforge.bench.agent_gate import AgentGateReport, compare_agent_vs_deterministic
from dataforge.detectors.base import Schema

__all__ = ["AgentReleaseGateResult", "check_agent_release_gate"]


@dataclass(frozen=True)
class AgentReleaseGateResult:
    """Outcome of the agent non-regression release gate."""

    passed: bool
    report: AgentGateReport
    reason: str


def check_agent_release_gate(
    fixture_paths: list[Path] | None = None,
    *,
    schema: Schema | None = None,
) -> AgentReleaseGateResult:
    """Verify the agent reproduces the deterministic floor on bundled fixtures.

    Args:
        fixture_paths: Optional fixtures; defaults to the bundled gate fixtures.
        schema: Optional schema applied to every fixture.

    Returns:
        An :class:`AgentReleaseGateResult`. ``passed`` is ``False`` if any
        fixture shows the agent diverging from the deterministic baseline.
    """
    report = compare_agent_vs_deterministic(fixture_paths, schema=schema)
    if not report.fixtures:
        return AgentReleaseGateResult(
            passed=False,
            report=report,
            reason="No gate fixtures were available to evaluate.",
        )
    if report.all_parity:
        return AgentReleaseGateResult(
            passed=True,
            report=report,
            reason=(
                f"Agent reproduced the deterministic floor on all "
                f"{len(report.fixtures)} fixture(s); no regression."
            ),
        )
    diverged = [item.fixture for item in report.fixtures if not item.parity]
    return AgentReleaseGateResult(
        passed=False,
        report=report,
        reason=f"Agent diverged from the deterministic floor on: {diverged}",
    )
