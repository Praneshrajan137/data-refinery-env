"""PROTOTYPE: induce candidate constraints instead of proposing values.

Status: **prototype**. Not wired into any pipeline, not exposed on the CLI, and not
promotable without the human review path described below.

## The argument

DataForge's LLM corrector is stuck at propose-only for a measured reason:
``precision_at_auto_apply`` of 0.077-0.16 and ECE of 0.79-0.82, so
``release/corrector_gate.py`` keeps every class at the 1.01 abstain sentinel. That will not
be fixed by a better model, because the problem is not accuracy. **A proposed value has
nothing to check it against.** Whatever confidence accompanies it, the only available
verification is "does this value satisfy the constraints we already had", and if those
constraints were sufficient the corrector would not have been needed.

A proposed *constraint* is a different object. It can be:

* checked for internal consistency by ``verifier/smt.py`` and independently by
  ``verifier/direct.py``, cross-checked in ``verifier/differential.py``;
* checked against the data -- how many rows satisfy it, which ones do not;
* compiled by ``verifier/constraint_ir.py`` into a SQL proof query;
* read by a human in a few seconds, unlike a hundred individual cell proposals.

So the LLM moves from the value path, where its output is unfalsifiable, to the proposal
path, where its output is checkable. That is a change of kind, not of degree.

External support: ForestED/TreeED (arXiv:2512.07246, KDD 2026) reports +16.1% F1 by using an
LLM to *induce a decision tree* rather than to label cells, explicitly because
LLM-as-labeler is "an implicit black-box process with limited traceability". Independently,
arXiv:2606.02866 measures multi-agent debate *improving* error detection by +27.4pp F1 while
*degrading* generation by 1.6-15.5pp through critique-induced confusion. Both point the same
way: put the model where its output can be checked.

## The two hard gates

1. **An induced constraint is advisory until a human accepts it.** It may enter
   ``authoritative_columns`` only through ``cli/constraints.py``.
   ``docs/trust/authority-is-mutable.md`` is explicit that rewriting constraints rewrites
   the *premise* of provenness, which is why ``write_constraint_review_artifact_atomic`` is
   deliberately ungated -- it is a change to the authority, not a change under it. An
   inducer that could write its own premise would be able to manufacture proof.
2. **Advisory constraints never yield ``proven``.** Enforced by the existing
   inferred/authoritative split, not by anything here.

This module therefore returns candidates and **cannot write**. It has no filesystem access
and no path to a schema.

## Why the inducer is injected

``completion_fn`` is injectable for the same reason ``review/ranker.py`` injects it: the
tests must run offline, and a prototype that can only be exercised with a paid API is a
prototype nobody exercises.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from dataforge.verifier.schema import (
    AcceptedValues,
    DomainBound,
    RegexConstraint,
)

# Imported to document, in code, exactly which schema constraint kinds a reviewer could
# eventually accept a candidate as. Deliberately never *constructed* here: see
# CandidateConstraint.to_constraint.
_REVIEWABLE_AS = (RegexConstraint, AcceptedValues, DomainBound)

__all__ = [
    "CandidateConstraint",
    "InducedConstraintSet",
    "InductionError",
    "induce_column_constraints",
    "render_induction_prompt",
]

CompletionFn = Callable[[str], str]

_SUPPORTED_KINDS = ("regex", "accepted_values", "domain_bound")

# A candidate that holds for fewer than this fraction of non-empty values is describing a
# different column. Not a confidence threshold -- a relevance one.
_MIN_SUPPORT_FRACTION = 0.8

# Below this many values, "95% satisfy this" is two coincidences. Mirrors the floor in
# MissingValueDetector and SemanticDomainDetector.
_MIN_VALUES = 8


class InductionError(ValueError):
    """Raised when an induction response cannot be turned into a checkable candidate."""


@dataclass(frozen=True, slots=True)
class CandidateConstraint:
    """One induced constraint, with the evidence needed to review or reject it.

    ``violating_values`` is the whole point. A proposed value comes with a confidence and
    nothing else; a proposed constraint comes with the exact set of cells it would flag, so
    a reviewer can see the consequence before accepting the premise.

    ``strength`` is fixed to ``"advisory"`` and there is deliberately no way to construct
    this with any other value. An induced constraint becomes authoritative only by passing
    through human review, which produces a different object entirely.
    """

    kind: Literal["regex", "accepted_values", "domain_bound"]
    column: str
    rationale: str
    support: int
    total: int
    violating_values: tuple[str, ...]
    strength: Literal["advisory"] = "advisory"

    @property
    def support_fraction(self) -> float:
        """Fraction of non-empty values satisfying the candidate."""
        if self.total == 0:
            return 0.0
        return round(self.support / self.total, 4)

    def to_constraint(self, payload: object) -> object:
        """Return the schema-level constraint object, for review only.

        Deliberately a pass-through rather than a constructor. Building a
        :class:`~dataforge.verifier.schema.Schema` -- or even a bare constraint -- here
        would make it one line of caller code to treat an induced constraint as a declared
        one, and that line is the whole risk this module exists to avoid. The reviewable
        target kinds are recorded in ``_REVIEWABLE_AS`` for readers.
        """
        return payload


@dataclass(frozen=True, slots=True)
class InducedConstraintSet:
    """Candidates for one column, plus what was rejected and why.

    Rejections are returned rather than dropped. A caller that sees only survivors cannot
    tell a careful inducer from a lucky one.
    """

    column: str
    candidates: tuple[CandidateConstraint, ...]
    rejected: tuple[tuple[str, str], ...]
    model: str | None = None

    @property
    def admissible(self) -> bool:
        """Whether any candidate survived checking."""
        return bool(self.candidates)


def render_induction_prompt(column: str, values: Sequence[str], *, max_values: int = 40) -> str:
    """Render the induction prompt.

    Asks for a *constraint*, never a corrected value, and says so explicitly: the boundary
    is the point of the design, and a model that drifts into proposing values produces
    output this module will reject rather than silently accept.
    """
    sample = list(values[:max_values])
    return (
        "You are given the values of one column from a data table. Propose data-quality "
        "CONSTRAINTS that the column's correct values satisfy.\n\n"
        "Rules:\n"
        "- Propose constraints, NEVER corrected values. Do not suggest what any cell "
        "should be changed to.\n"
        "- A constraint must hold for the large majority of values. It is a description of "
        "the column, not of its exceptions.\n"
        f"- Allowed kinds: {', '.join(_SUPPORTED_KINDS)}.\n"
        "- Reply with JSON only: "
        '{"constraints": [{"kind": ..., "rationale": ..., '
        '"pattern": ... | "values": [...] | "min": ..., "max": ...}]}\n\n'
        f"Column name: {column}\n"
        f"Values ({len(sample)} of {len(values)} shown):\n"
        + "\n".join(f"- {value}" for value in sample)
    )


def _check_regex(spec: dict[str, object], values: Sequence[str]) -> list[str]:
    """Compile a regex candidate and return the values it would flag."""
    pattern = spec.get("pattern")
    if not isinstance(pattern, str) or not pattern:
        raise InductionError("regex candidate has no 'pattern'")
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        raise InductionError(f"regex candidate does not compile: {exc}") from exc
    return [value for value in values if not compiled.match(value)]


def _check_accepted_values(spec: dict[str, object], values: Sequence[str]) -> list[str]:
    """Return the values an accepted-values candidate would flag."""
    allowed = spec.get("values")
    if not isinstance(allowed, list) or not allowed:
        raise InductionError("accepted_values candidate has no non-empty 'values'")
    if not all(isinstance(item, str) for item in allowed):
        raise InductionError("accepted_values candidate holds a non-string value")
    allowed_set = set(allowed)
    return [value for value in values if value not in allowed_set]


def _check_domain_bound(spec: dict[str, object], values: Sequence[str]) -> list[str]:
    """Return the values a numeric-bound candidate would flag."""
    minimum, maximum = spec.get("min"), spec.get("max")
    if minimum is None and maximum is None:
        raise InductionError("domain_bound candidate has neither 'min' nor 'max'")
    try:
        low = None if minimum is None else float(minimum)  # type: ignore[arg-type]
        high = None if maximum is None else float(maximum)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise InductionError(f"domain_bound candidate has a non-numeric bound: {exc}") from exc

    violating: list[str] = []
    for value in values:
        try:
            numeric = float(value)
        except ValueError:
            # A non-numeric value in a bounded column is a violation, not a parse failure
            # to swallow: swallowing it would let a bound "hold" on a column of text.
            violating.append(value)
            continue
        if (low is not None and numeric < low) or (high is not None and numeric > high):
            violating.append(value)
    return violating


_CHECKERS = {
    "regex": _check_regex,
    "accepted_values": _check_accepted_values,
    "domain_bound": _check_domain_bound,
}


def induce_column_constraints(
    column: str,
    values: Sequence[str],
    *,
    completion_fn: CompletionFn,
    model: str | None = None,
) -> InducedConstraintSet:
    """Induce candidate constraints for one column and check each against the data.

    Every candidate is evaluated before being returned, so an unparseable, non-compiling or
    irrelevant proposal is rejected here rather than surfaced to a reviewer. That check is
    the difference from value proposal: it is possible.

    Args:
        column: Column name.
        values: The column's values.
        completion_fn: Maps a prompt to a completion. Injected so this runs offline.
        model: Model identifier, recorded for provenance.

    Returns:
        The :class:`InducedConstraintSet`, carrying survivors and rejections.

    Raises:
        InductionError: If the response is not JSON, or has no ``constraints`` list. A
            malformed response is a failure rather than an empty result: returning
            "no candidates" for a broken inducer would make a broken inducer look careful.
    """
    non_empty = [value for value in values if value and value.strip()]
    if len(non_empty) < _MIN_VALUES:
        return InducedConstraintSet(
            column=column,
            candidates=(),
            rejected=(("*", f"column has {len(non_empty)} values, need >= {_MIN_VALUES}"),),
            model=model,
        )

    raw = completion_fn(render_induction_prompt(column, non_empty))
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise InductionError(f"induction response is not JSON: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("constraints"), list):
        raise InductionError("induction response has no 'constraints' list")

    candidates: list[CandidateConstraint] = []
    rejected: list[tuple[str, str]] = []
    for entry in payload["constraints"]:
        if not isinstance(entry, dict):
            rejected.append(("?", "candidate is not an object"))
            continue
        kind = entry.get("kind")
        # A model asked for constraints will sometimes return a value anyway. Rejecting the
        # shape is how the boundary is enforced rather than merely requested.
        if {"new_value", "corrected_value", "value"} & set(entry):
            rejected.append((str(kind), "candidate proposes a value, not a constraint"))
            continue
        if kind not in _CHECKERS:
            rejected.append((str(kind), f"unsupported kind; allowed: {_SUPPORTED_KINDS}"))
            continue
        try:
            violating = _CHECKERS[kind](entry, non_empty)
        except InductionError as exc:
            rejected.append((str(kind), str(exc)))
            continue

        support = len(non_empty) - len(violating)
        if support / len(non_empty) < _MIN_SUPPORT_FRACTION:
            rejected.append(
                (
                    str(kind),
                    f"holds for {support}/{len(non_empty)} values, below the "
                    f"{_MIN_SUPPORT_FRACTION:.0%} relevance floor",
                )
            )
            continue
        if not violating:
            rejected.append((str(kind), "no value violates it, so accepting it would flag nothing"))
            continue

        candidates.append(
            CandidateConstraint(
                kind=kind,
                column=column,
                rationale=str(entry.get("rationale") or ""),
                support=support,
                total=len(non_empty),
                violating_values=tuple(dict.fromkeys(violating)),
            )
        )

    return InducedConstraintSet(
        column=column,
        candidates=tuple(candidates),
        rejected=tuple(rejected),
        model=model,
    )
