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
        """True when the agent matches the deterministic floor on every fixture.

        Non-vacuous by construction: see :attr:`non_vacuous`. Parity alone is satisfied
        by three zeros, so this property requires BOTH that every fixture agrees and
        that at least one fixture actually exercised a write.
        """
        return (
            all(item.parity for item in self.fixtures) and bool(self.fixtures) and self.non_vacuous
        )

    @property
    def non_vacuous(self) -> bool:
        """True when at least one fixture produced a non-zero deterministic floor.

        Why this exists. ``parity`` is
        ``agent.fixes_count == floor_count and agent.floor_fix_count == floor_count``.
        When the floor is zero that is ``0 == 0 and 0 == 0`` -- a three-way equality
        between three zeros, which an agent that silently dropped EVERY fix would
        satisfy perfectly, and be certified as "reproducing the deterministic floor on
        all fixtures".

        Scope, stated honestly. This guard is PROPHYLACTIC, not a fix for a defect that
        was live. When ``decimal_shift`` left the auto-apply allowlist on 2026-08-22 it
        was predicted here that the bundled floor would drop to zero; measurement says
        otherwise. Actual floors after that change:

        * ``premised_fd_10rows.csv``  1  (``fd_violation``, declared premise)
        * ``hospital_10rows.csv``     1  (``type_mismatch`` on ``phone_number``)
        * ``dirty.csv``               2  (``type_mismatch`` on ``age``, twice)

        All three survive because ``type_mismatch`` and ``fd_violation`` are themselves
        allowlisted. The gate was therefore never vacuous in practice, and the earlier
        claim that ``decimal_shift`` was the only fix the hospital floor contained was
        simply wrong -- it was never measured before being asserted.

        The guard is kept because the failure mode is real, cheap to exclude, and silent:
        removing a detector from the allowlist is a one-line change that can empty a
        floor without touching gate code, and the resulting pass looks identical to a
        real one. Note the pre-existing ``and bool(self.fixtures)`` in :attr:`all_parity`
        -- the author anticipated an empty fixture LIST and missed empty results PER
        fixture. This closes that level. Compare the corruption oracle in
        ``docs/trust/deterministic-is-not-sound.md``, which generated clean columns so
        tightly clustered that no correct cell could be flagged, making the invariant it
        guarded unfalsifiable.
        """
        return any(item.floor_fix_count > 0 for item in self.fixtures)

    @property
    def vacuity_reason(self) -> str | None:
        """A human-readable reason when the report is vacuous, else ``None``."""
        if not self.fixtures:
            return "no fixtures were evaluated"
        if not self.non_vacuous:
            names = ", ".join(Path(item.fixture).name for item in self.fixtures)
            return (
                f"every fixture produced a ZERO deterministic floor ({names}), so the "
                "parity assertion compares zero against zero and would be satisfied by "
                "an agent that dropped every fix. Add a fixture whose repair the product "
                "stands behind -- one with a declared premise and a detector in "
                "CONSTRAINT_CHECKABLE_DETECTORS."
            )
        return None


def default_gate_fixtures() -> list[Path]:
    """Return offline, bundled CSV fixtures suitable for the parity gate.

    ``premised_fd_10rows.csv`` is the only one carrying a DECLARED premise (a sibling
    ``.schema.yaml`` declaring ``state -> city``). It was added on 2026-08-22 because the
    other two fixtures reach their floor via ``type_mismatch``, which needs no schema, so
    nothing in this gate exercised the schema-dependent ``fd_violation`` write path --
    the path that every gate change since has had to reason about blind.

    Measured floors: ``premised_fd_10rows`` 1, ``hospital_10rows`` 1, ``dirty`` 2.
    """
    root = Path(__file__).resolve().parents[1]
    candidates = [
        root / "fixtures" / "premised_fd_10rows.csv",
        root / "fixtures" / "hospital_10rows.csv",
        root / "datasets" / "embedded" / "hospital" / "dirty.csv",
    ]
    return [path for path in candidates if path.is_file()]


def _premise_for(fixture: Path, explicit: Schema | None) -> Schema | None:
    """Resolve the schema for one fixture: an explicit override, else its sibling file.

    A fixture is a table PLUS its premise, not a table alone. Before this, the gate took
    a single ``schema`` for every fixture, which made it impossible to add one premised
    fixture without asserting that premise over the unpremised ones too -- and a premise
    that does not hold is worse than none, because it manufactures violations.
    """
    if explicit is not None:
        return explicit
    sidecar = fixture.with_suffix(".schema.yaml")
    if not sidecar.is_file():
        return None
    from dataforge.cli.common import load_schema

    return load_schema(sidecar)


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
        premise = _premise_for(resolved, schema)
        legacy = run_repair_pipeline(
            RepairPipelineRequest(source_path=resolved, mode="dry_run", schema=premise)
        )
        agent = run_agent_repair(
            AgentRepairRequest(
                source_path=resolved, mode="dry_run", schema=premise, policy="deterministic"
            )
        )
        floor_count = len(legacy.fixes)
        results.append(
            FixtureParity(
                fixture=str(resolved),
                floor_fix_count=floor_count,
                agent_fix_count=agent.fixes_count,
                agent_floor_count=agent.floor_fix_count,
                parity=agent.fixes_count == floor_count and agent.floor_fix_count == floor_count,
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
