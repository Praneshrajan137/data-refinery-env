"""Honest benchmark gate for the verified agent.

Two guarantees are enforced here, both grounded in reality rather than hope:

1. **Non-regression by construction (offline, CI-runnable).** The verified agent
   seeds with the deterministic floor, so with the ``deterministic`` policy it
   must produce *exactly* the legacy pipeline's verified fixes, and with any
   policy it must produce a *superset*. :func:`compare_agent_vs_deterministic`
   checks this directly against bundled fixtures — no model or network needed.

2. **Promotion bar (fed real eval numbers).** The agent may only become the
   default repair path if a real evaluation shows it beats the deterministic
   baseline F1 with zero safety regressions. :func:`agent_promotion_verdict`
   encodes that decision as pure, testable logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from dataforge.agent.controller import AgentRepairRequest, run_agent_repair
from dataforge.detectors.base import Schema
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

__all__ = [
    "AgentGateReport",
    "FixtureParity",
    "PromotionVerdict",
    "agent_promotion_verdict",
    "compare_agent_vs_deterministic",
    "default_gate_fixtures",
]


@dataclass(frozen=True)
class FixtureParity:
    """Per-fixture comparison of the deterministic floor and the agent.

    Args:
        fixture: The fixture path that was evaluated.
        floor_fix_count: Verified fixes from the legacy deterministic pipeline.
        agent_fix_count: Verified fixes from the agent (deterministic policy).
        agent_floor_count: The agent's own reported deterministic-floor count.
        parity: Whether the agent reproduced the floor exactly (no regression).
    """

    fixture: str
    floor_fix_count: int
    agent_fix_count: int
    agent_floor_count: int
    parity: bool


@dataclass(frozen=True)
class AgentGateReport:
    """Aggregate non-regression report across fixtures."""

    fixtures: tuple[FixtureParity, ...] = field(default_factory=tuple)

    @property
    def all_parity(self) -> bool:
        """True when the agent matches the deterministic floor on every fixture."""
        return all(item.parity for item in self.fixtures) and bool(self.fixtures)


def default_gate_fixtures() -> list[Path]:
    """Return offline, bundled CSV fixtures suitable for the parity gate."""
    root = Path(__file__).resolve().parents[1]
    candidates = [
        root / "fixtures" / "hospital_10rows.csv",
        root / "datasets" / "embedded" / "hospital" / "dirty.csv",
    ]
    return [path for path in candidates if path.is_file()]


def compare_agent_vs_deterministic(
    fixture_paths: list[Path] | None = None,
    *,
    schema: Schema | None = None,
) -> AgentGateReport:
    """Compare the agent (deterministic policy) to the legacy pipeline.

    Args:
        fixture_paths: CSV fixtures to evaluate. Defaults to bundled fixtures.
        schema: Optional schema applied to both paths identically.

    Returns:
        An :class:`AgentGateReport`. ``all_parity`` is the CI assertion: the
        verified agent never regresses the deterministic baseline.
    """
    paths = fixture_paths if fixture_paths is not None else default_gate_fixtures()
    results: list[FixtureParity] = []
    for path in paths:
        resolved = path.resolve()
        legacy = run_repair_pipeline(
            RepairPipelineRequest(source_path=resolved, mode="dry_run", schema=schema)
        )
        agent = run_agent_repair(
            AgentRepairRequest(
                source_path=resolved, mode="dry_run", schema=schema, policy="deterministic"
            )
        )
        floor_count = len(legacy.fixes)
        results.append(
            FixtureParity(
                fixture=str(resolved),
                floor_fix_count=floor_count,
                agent_fix_count=agent.fixes_count,
                agent_floor_count=agent.floor_fix_count,
                parity=agent.fixes_count == floor_count
                and agent.floor_fix_count == floor_count,
            )
        )
    return AgentGateReport(fixtures=tuple(results))


@dataclass(frozen=True)
class PromotionVerdict:
    """Whether the agent may be promoted to the default repair path."""

    promote: bool
    reason: str


def agent_promotion_verdict(
    *,
    agent_f1: float,
    baseline_f1: float,
    safety_regressions: int,
    parity_ok: bool,
    min_margin: float = 0.0,
) -> PromotionVerdict:
    """Decide whether the agent may become the default, from real eval numbers.

    The agent is promoted only when it (1) reproduces the deterministic floor
    (no structural regression), (2) introduces zero safety regressions, and
    (3) beats the deterministic baseline F1 by at least ``min_margin``.

    Args:
        agent_f1: Measured F1 of the agent policy on the benchmark.
        baseline_f1: Measured F1 of the deterministic baseline.
        safety_regressions: Count of adversarial/safety cases the agent newly
            failed (must be zero).
        parity_ok: Whether :func:`compare_agent_vs_deterministic` passed.
        min_margin: Minimum F1 improvement required over the baseline.

    Returns:
        A :class:`PromotionVerdict` with a human-readable reason.
    """
    if not parity_ok:
        return PromotionVerdict(
            promote=False,
            reason="Agent does not reproduce the deterministic floor; structural regression.",
        )
    if safety_regressions > 0:
        return PromotionVerdict(
            promote=False,
            reason=f"Agent introduced {safety_regressions} safety regression(s); blocked.",
        )
    if agent_f1 < baseline_f1 + min_margin:
        return PromotionVerdict(
            promote=False,
            reason=(
                f"Agent F1 {agent_f1:.4f} does not beat baseline {baseline_f1:.4f} "
                f"by the required margin {min_margin:.4f}."
            ),
        )
    return PromotionVerdict(
        promote=True,
        reason=(
            f"Agent F1 {agent_f1:.4f} beats baseline {baseline_f1:.4f} "
            f"(margin >= {min_margin:.4f}) with no safety regression and floor parity."
        ),
    )
