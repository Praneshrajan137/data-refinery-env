"""Week 3 constitutional safety gate for proposed repairs."""

from __future__ import annotations

import enum
from typing import Final

from pydantic import BaseModel, Field

from dataforge.detectors.base import Schema
from dataforge.repairers.base import ProposedFix
from dataforge.safety.constitution import (
    CompiledBatchRule,
    CompiledSingleRule,
    default_constitution_path,
    load_constitution,
)

#: Flags that the deprecated blanket ``confirm_escalations`` alias satisfies.
#:
#: Enumerated explicitly rather than defined as "every confirm flag", because the alias
#: must never reach ``confirm_pii``: that flag guards a ``hard_never`` rule behind a
#: separate ``allow_pii`` override, and letting ``--confirm-escalations`` satisfy it would
#: turn a compatibility shim into a widening of write authority.
#:
#: ``tests/unit/test_safety_filter.py`` derives the shipped constitution's
#: ``soft_require_confirm`` confirm flags and asserts this set equals them, so adding a
#: rule to that tier with a new flag fails CI rather than silently escaping the alias.
_LEGACY_ESCALATION_FLAGS: Final = frozenset(
    {
        "confirm_untrusted_write",
        "confirm_high_volume",
        "confirm_aggregate_break",
        "confirm_injection_text",
    }
)


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
    """Runtime context for safety evaluation.

    One confirmation flag per independently-motivated rule. Until 2026-08-29 a single
    ``confirm_escalations`` boolean gated all four ``soft_require_confirm`` rules, so
    clearing the untrusted-write guard on a fix's *origin label* also silently disabled
    the blast-radius guard, the aggregate-dependency guard, and the prompt-injection
    guard. Those rules share nothing but a tier: an operator who accepts one has stated
    nothing about the others.

    The coupling was sharper on the interactive path. ``engine/repair.py`` reassigns the
    context returned by ``escalation_resolver`` into its loop variable, so a single
    ``y`` at one prompt disabled all four rules for **every remaining issue in the run**.

    ``confirm_escalations`` is retained as a deprecated blanket alias so existing callers
    and artifacts keep their meaning exactly; see :data:`_LEGACY_ESCALATION_FLAGS` for
    what it covers and why that set is enumerated rather than derived.
    """

    allow_pii: bool = False
    confirm_pii: bool = False
    confirm_untrusted_write: bool = False
    confirm_high_volume: bool = False
    confirm_aggregate_break: bool = False
    confirm_injection_text: bool = False
    #: Deprecated. Satisfies every flag in :data:`_LEGACY_ESCALATION_FLAGS` at once.
    #: Prefer the specific flag for the rule you mean to confirm.
    confirm_escalations: bool = False

    model_config = {"frozen": True}

    def confirms(self, flag: str) -> bool:
        """Return whether ``flag`` is satisfied, honoring the deprecated blanket alias.

        Read through this rather than ``getattr`` so the alias cannot be honored on one
        code path and ignored on another.
        """
        if getattr(self, flag, False):
            return True
        return self.confirm_escalations and flag in _LEGACY_ESCALATION_FLAGS


class SafetyFilter:
    """Compiled constitutional safety gate for candidate repairs."""

    def __init__(self) -> None:
        self._constitution = load_constitution(default_constitution_path())

    def confirm_flags_for(self, rule_ids: tuple[str, ...] | list[str]) -> frozenset[str]:
        """Return the confirmation flags belonging to ``rule_ids``.

        Derived from the compiled constitution rather than restated as a literal map, so a
        rule whose flag changes cannot leave a caller confirming the wrong guard. A rule id
        with no confirm flag contributes nothing.
        """
        wanted = set(rule_ids)
        rules: list[CompiledSingleRule | CompiledBatchRule] = [
            *self._constitution.single_rules,
            *self._constitution.batch_rules,
        ]
        flags = {
            rule.confirm_flag for rule in rules if rule.rule_id in wanted and rule.confirm_flag
        }
        return frozenset(flags)

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
                    if rule.confirm_flag and not context.confirms(rule.confirm_flag):
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
                if rule.confirm_flag and context.confirms(rule.confirm_flag):
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

    def evaluate_batch(
        self,
        fixes: list[ProposedFix],
        context: SafetyContext | None = None,
    ) -> SafetyResult:
        """Return whether a batch of accepted fixes is internally consistent."""
        context = context or SafetyContext()
        for rule in self._constitution.batch_rules:
            if rule.predicate(fixes):
                if (
                    rule.tier == "soft_require_confirm"
                    and rule.confirm_flag
                    and context.confirms(rule.confirm_flag)
                ):
                    continue
                verdict = (
                    SafetyVerdict.ESCALATE
                    if rule.tier == "soft_require_confirm"
                    else SafetyVerdict.DENY
                )
                return SafetyResult(
                    verdict=verdict,
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
