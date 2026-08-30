"""Constitution parsing and compiled-rule registry for the safety layer."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal

import yaml

from dataforge.repairers.base import ProposedFix
from dataforge.verifier.schema import Schema

if TYPE_CHECKING:
    from dataforge.safety.filter import SafetyContext
else:  # pragma: no cover
    SafetyContext = Any

RuleTier = Literal["hard_never", "soft_require_confirm", "soft_prefer"]
SinglePredicate = Callable[[ProposedFix, Schema | None, SafetyContext], bool]
BatchPredicate = Callable[[list[ProposedFix]], bool]
PreferenceScorer = Callable[[ProposedFix, Schema | None, SafetyContext], int]


class ConstitutionError(ValueError):
    """Raised when a constitution file is malformed or references unknown rules."""


def _levenshtein_distance(left: str, right: str) -> int:
    """Return the Levenshtein edit distance between two strings."""
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)

    previous = list(range(len(right) + 1))
    for i, left_char in enumerate(left, start=1):
        current = [i]
        for j, right_char in enumerate(right, start=1):
            insert_cost = current[j - 1] + 1
            delete_cost = previous[j] + 1
            replace_cost = previous[j - 1] + (left_char != right_char)
            current.append(min(insert_cost, delete_cost, replace_cost))
        previous = current
    return previous[-1]


def _pii_overwrite(
    proposed_fix: ProposedFix,
    schema: Schema | None,
    context: SafetyContext,
) -> bool:
    """Return whether a fix touches a column marked as PII."""
    del context
    return schema is not None and proposed_fix.fix.column in schema.pii_columns


def _row_delete(
    proposed_fix: ProposedFix,
    schema: Schema | None,
    context: SafetyContext,
) -> bool:
    """Return whether a proposed fix is deleting a row."""
    del schema, context
    return proposed_fix.fix.operation == "delete_row"


def _primary_key_edit(
    proposed_fix: ProposedFix,
    schema: Schema | None,
    context: SafetyContext,
) -> bool:
    """Return whether a fix edits a declared primary-key column."""
    del context
    return schema is not None and proposed_fix.fix.column in schema.primary_key_columns


def _aggregate_sensitive(
    proposed_fix: ProposedFix,
    schema: Schema | None,
    context: SafetyContext,
) -> bool:
    """Return whether a fix edits a column used as an aggregate source."""
    del context
    return schema is not None and bool(schema.aggregate_dependencies_for(proposed_fix.fix.column))


def _llm_live_candidate(
    proposed_fix: ProposedFix,
    schema: Schema | None,
    context: SafetyContext,
) -> bool:
    """Return whether a candidate is an untrusted write needing confirmation.

    Both freshly generated (``llm_live``) and replayed-from-cache (``llm_cache``)
    values are LLM-origin: a cached value was still produced by a model, so it
    must clear the same unconfirmed-write escalation as a live generation.
    ``external`` values (proposed by an outside actor via ``verify_and_apply`` --
    an autonomous agent, another tool, or a human) are equally untrusted at the
    point of write and clear the same escalation. The rule id remains
    ``NO_UNCONFIRMED_LLM_WRITE`` for stability; it covers all untrusted origins.
    """
    del schema, context
    return proposed_fix.provenance.strip().lower() in {
        "llm",
        "llm_live",
        "live_llm",
        "llm_cache",
        "external",
    }


def _prompt_injection_text(
    proposed_fix: ProposedFix,
    schema: Schema | None,
    context: SafetyContext,
) -> bool:
    """Return whether a candidate writes text that resembles prompt injection."""
    del schema, context
    value = proposed_fix.fix.new_value.strip().lower()
    indicators = (
        "ignore previous instructions",
        "ignore all previous instructions",
        "system prompt",
        "developer message",
        "you are chatgpt",
        "reveal your instructions",
        "<script",
    )
    return any(indicator in value for indicator in indicators)


def _conflicting_cell_writes(fixes: list[ProposedFix]) -> bool:
    """Return whether multiple proposed fixes target the same cell differently."""
    seen: dict[tuple[int, str], str] = {}
    for fix in fixes:
        key = (fix.fix.row, fix.fix.column)
        existing = seen.get(key)
        if existing is not None and existing != fix.fix.new_value:
            return True
        seen[key] = fix.fix.new_value
    return False


#: Cells a batch may rewrite before auto-apply requires confirmation.
#:
#: Unchanged in value from the row-based threshold it replaces, deliberately. There is no
#: measurement that would justify a different number, and inventing one would be fitting a
#: parameter to nothing -- which is what `PRODUCT.md`:213-221 records refusing to do even when
#: the fitted constant looked like a clean win. What changed is the UNIT, which was a defect.
HIGH_VOLUME_CELL_BUDGET: Final = 100


def _high_volume_batch(fixes: list[ProposedFix]) -> bool:
    """Return whether a repair batch rewrites too many CELLS for auto-apply.

    Counted in cells, not distinct rows, since 2026-08-29. The row count was a defect with a
    measurable blind spot: a batch rewriting 90 rows across 50 columns is 4,500 cells and
    passed, because it touched 90 rows. Blast radius is what a budget is for, and blast radius
    is cells -- a cell is the unit this product writes, reverts, proves and attests.

    The change is strictly more refusing: any batch the row rule escalated still escalates,
    because cells >= distinct rows. That direction is required. Every predicate here ships as
    a new way to say no; a budget change that let something through would be a loosening
    dressed as a fix, and would need its own evidence.

    Two things this deliberately does NOT do, both because they would require a number nobody
    has measured:

    * **No table fraction.** 101 cells is treated the same in a 200-row table and a
      5,000,000-row one, which is obviously crude. But a fraction needs a threshold, and
      there is no measurement to choose one; the corpora here cannot supply it, since the only
      one large enough is tax and it mines four dependencies that write 643 cells.
    * **No cross-batch accumulation.** A sequence of batches each just under budget still
      passes. The pipeline path is unaffected -- ``engine/repair.py`` evaluates the whole
      accepted set in one call, so its batch IS its transaction -- but the agent path issues
      many batches and can walk under the budget indefinitely. Fixing that means either
      mutable state on ``SafetyFilter`` or a second budget at ``apply_transaction``, and the
      second needs a threshold that would refuse hospital's legitimate 567-cell repair at any
      value this guard uses. So it is named here as an open gap rather than closed with a
      guessed constant.

    A witness count is not used here even though ``dataforge/witness.py`` computes blast
    radius, because the two answer different questions: the witness predicts what a *candidate
    premise* would rewrite before it is accepted, while this counts what an *already-proposed
    batch* would write. Routing this through the witness would manufacture a dependency that
    does not exist.
    """
    return len({(fix.fix.row, fix.fix.column) for fix in fixes}) > HIGH_VOLUME_CELL_BUDGET


def _minimal_edit_distance(
    proposed_fix: ProposedFix,
    schema: Schema | None,
    context: SafetyContext,
) -> int:
    """Score a candidate by edit distance from the original value."""
    del schema, context
    return _levenshtein_distance(proposed_fix.fix.old_value, proposed_fix.fix.new_value)


_SINGLE_PREDICATES: dict[str, SinglePredicate] = {
    "pii_overwrite": _pii_overwrite,
    "row_delete": _row_delete,
    "primary_key_edit": _primary_key_edit,
    "aggregate_sensitive": _aggregate_sensitive,
    "llm_live_candidate": _llm_live_candidate,
    "prompt_injection_text": _prompt_injection_text,
}
_BATCH_PREDICATES: dict[str, BatchPredicate] = {
    "conflicting_cell_writes": _conflicting_cell_writes,
    "high_volume_batch": _high_volume_batch,
}
_SCORERS: dict[str, PreferenceScorer] = {
    "minimal_edit_distance": _minimal_edit_distance,
}


@dataclass(frozen=True)
class CompiledSingleRule:
    """Compiled single-fix safety rule."""

    rule_id: str
    description: str
    tier: RuleTier
    predicate: SinglePredicate
    override_flag: str | None = None
    confirm_flag: str | None = None


@dataclass(frozen=True)
class CompiledBatchRule:
    """Compiled batch safety rule."""

    rule_id: str
    description: str
    tier: RuleTier
    predicate: BatchPredicate
    confirm_flag: str | None = None


@dataclass(frozen=True)
class CompiledPreferenceRule:
    """Compiled candidate-preference rule."""

    rule_id: str
    description: str
    tier: RuleTier
    scorer: PreferenceScorer


@dataclass(frozen=True)
class Constitution:
    """Compiled constitution with rule registries by scope."""

    single_rules: tuple[CompiledSingleRule, ...]
    batch_rules: tuple[CompiledBatchRule, ...]
    preference_rules: tuple[CompiledPreferenceRule, ...]


def default_constitution_path() -> Path:
    """Return the shipped default constitution path."""
    return Path(str(resources.files("dataforge.safety").joinpath("constitutions/default.yaml")))


def _expect_mapping(payload: object, *, message: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ConstitutionError(message)
    return payload


def _build_single_rule(payload: dict[str, object], tier: RuleTier) -> CompiledSingleRule:
    rule_id = str(payload.get("id", "")).strip()
    description = str(payload.get("description", "")).strip()
    predicate_name = str(payload.get("predicate", "")).strip()
    if not rule_id or not description:
        raise ConstitutionError(f"Invalid rule entry for tier '{tier}'.")
    predicate = _SINGLE_PREDICATES.get(predicate_name)
    if predicate is None:
        raise ConstitutionError(f"Unknown predicate '{predicate_name}' in rule '{rule_id}'.")
    return CompiledSingleRule(
        rule_id=rule_id,
        description=description,
        tier=tier,
        predicate=predicate,
        override_flag=str(payload["override_flag"]) if payload.get("override_flag") else None,
        confirm_flag=str(payload["confirm_flag"]) if payload.get("confirm_flag") else None,
    )


def _build_batch_rule(payload: dict[str, object], tier: RuleTier) -> CompiledBatchRule:
    rule_id = str(payload.get("id", "")).strip()
    description = str(payload.get("description", "")).strip()
    predicate_name = str(payload.get("predicate", "")).strip()
    if not rule_id or not description:
        raise ConstitutionError(f"Invalid batch rule entry for tier '{tier}'.")
    predicate = _BATCH_PREDICATES.get(predicate_name)
    if predicate is None:
        raise ConstitutionError(f"Unknown predicate '{predicate_name}' in rule '{rule_id}'.")
    return CompiledBatchRule(
        rule_id=rule_id,
        description=description,
        tier=tier,
        predicate=predicate,
        confirm_flag=str(payload["confirm_flag"]) if payload.get("confirm_flag") else None,
    )


def _build_preference_rule(payload: dict[str, object], tier: RuleTier) -> CompiledPreferenceRule:
    rule_id = str(payload.get("id", "")).strip()
    description = str(payload.get("description", "")).strip()
    scorer_name = str(payload.get("scorer", "")).strip()
    if not rule_id or not description:
        raise ConstitutionError(f"Invalid preference rule entry for tier '{tier}'.")
    if not scorer_name:
        raise ConstitutionError(f"Preference rule '{rule_id}' must declare a scorer.")
    scorer = _SCORERS.get(scorer_name)
    if scorer is None:
        raise ConstitutionError(f"Unknown scorer '{scorer_name}' in rule '{rule_id}'.")
    return CompiledPreferenceRule(
        rule_id=rule_id,
        description=description,
        tier=tier,
        scorer=scorer,
    )


@lru_cache(maxsize=8)
def load_constitution(path: Path) -> Constitution:
    """Load and compile a constitution YAML file."""
    raw_payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    root = _expect_mapping(raw_payload or {}, message="Constitution must be a YAML mapping.")

    single_rules: list[CompiledSingleRule] = []
    batch_rules: list[CompiledBatchRule] = []
    preference_rules: list[CompiledPreferenceRule] = []

    for tier in ("hard_never", "soft_require_confirm", "soft_prefer"):
        raw_rules = root.get(tier, [])
        if not isinstance(raw_rules, list):
            raise ConstitutionError(f"Tier '{tier}' must be a YAML list.")
        for raw_rule in raw_rules:
            payload = _expect_mapping(
                raw_rule, message=f"Rule entries in '{tier}' must be mappings."
            )
            scope = str(payload.get("scope", "single")).strip().lower()
            if tier == "soft_prefer":
                preference_rules.append(_build_preference_rule(payload, tier))
                continue
            if scope == "batch":
                batch_rules.append(_build_batch_rule(payload, tier))
            else:
                single_rules.append(_build_single_rule(payload, tier))

    return Constitution(
        single_rules=tuple(single_rules),
        batch_rules=tuple(batch_rules),
        preference_rules=tuple(preference_rules),
    )
