"""Week 3 constitutional safety gate for proposed repairs."""

from __future__ import annotations

import enum

from pydantic import BaseModel, Field

from dataforge.detectors.base import Schema
from dataforge.repairers.base import ProposedFix
from dataforge.safety.constitution import default_constitution_path, load_constitution


class SafetyVerdict(enum.Enum):
    """Possible outcomes of the safety gate."""

    ALLOW = "allow"
    ESCALATE = "escalate"
    DENY = "deny"


class SafetyResult(BaseModel):
    """Typed result for the safety gate."""

    verdict: SafetyVerdict
    reason: str = Field(min_length=1)
    rule_ids: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}


class SafetyContext(BaseModel):
    """Runtime context for safety evaluation."""

    allow_pii: bool = False
    confirm_pii: bool = False
    confirm_escalations: bool = False

    model_config = {"frozen": True}


class SafetyFilter:
    """Compiled constitutional safety gate for candidate repairs."""

    def __init__(self) -> None:
        self._constitution = load_constitution(default_constitution_path())

    def evaluate(
        self,
        proposed_fix: ProposedFix,
        schema: Schema | None,
        context: SafetyContext,
    ) -> SafetyResult:
        """Return whether a single proposed fix may continue to verification."""
        for rule in self._constitution.single_rules:
            if not rule.predicate(proposed_fix, schema, context):
                continue

            if rule.tier == "hard_never":
                if rule.override_flag and getattr(context, rule.override_flag, False):
                    if rule.confirm_flag and not getattr(context, rule.confirm_flag, False):
                        return SafetyResult(
                            verdict=SafetyVerdict.ESCALATE,
                            reason=f"{rule.rule_id}: {rule.description} Confirmation is required.",
                            rule_ids=(rule.rule_id,),
                        )
                    continue
                return SafetyResult(
                    verdict=SafetyVerdict.DENY,
                    reason=f"{rule.rule_id}: {rule.description}",
                    rule_ids=(rule.rule_id,),
                )

            if rule.tier == "soft_require_confirm":
                if rule.confirm_flag and getattr(context, rule.confirm_flag, False):
                    continue
                return SafetyResult(
                    verdict=SafetyVerdict.ESCALATE,
                    reason=f"{rule.rule_id}: {rule.description}",
                    rule_ids=(rule.rule_id,),
                )

        return SafetyResult(
            verdict=SafetyVerdict.ALLOW,
            reason="All proposed fixes passed the constitutional safety gate.",
        )

    def evaluate_batch(self, fixes: list[ProposedFix]) -> SafetyResult:
        """Return whether a batch of accepted fixes is internally consistent."""
        for rule in self._constitution.batch_rules:
            if rule.predicate(fixes):
                return SafetyResult(
                    verdict=SafetyVerdict.DENY,
                    reason=f"{rule.rule_id}: {rule.description}",
                    rule_ids=(rule.rule_id,),
                )
        return SafetyResult(
            verdict=SafetyVerdict.ALLOW,
            reason="Accepted fixes are batch-consistent.",
        )

    def choose_preferred(
        self,
        fixes: list[ProposedFix],
        schema: Schema | None,
        context: SafetyContext,
    ) -> ProposedFix:
        """Choose the preferred candidate using configured soft-prefer rules."""
        if not fixes:
            raise ValueError("choose_preferred requires at least one proposed fix")
        if len(fixes) == 1 or not self._constitution.preference_rules:
            return fixes[0]
        return min(
            fixes,
            key=lambda fix: tuple(
                rule.scorer(fix, schema, context) for rule in self._constitution.preference_rules
            ),
        )
