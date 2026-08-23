"""Tests for the constraint-induction prototype.

Two kinds of assertion, and the second matters more.

**Capability**: candidates are checked against the data before being returned, which is
possible for a constraint and impossible for a value.

**Boundary**: the module cannot write, cannot produce an authoritative constraint, and
rejects a response that drifts into proposing values. An induced constraint that could enter
``authoritative_columns`` on its own would be able to manufacture its own proof -- the
premise of provenness is mutable, which is exactly why
``write_constraint_review_artifact_atomic`` is deliberately ungated and lives behind human
review.
"""

from __future__ import annotations

import json

import pytest

from dataforge.repairers.induce import (
    CandidateConstraint,
    InductionError,
    induce_column_constraints,
    render_induction_prompt,
)

_MONTHS = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "1234",
]


def _responder(payload: dict[str, object]) -> object:
    """Return a completion_fn that always answers with the given payload."""

    def _complete(prompt: str) -> str:
        assert prompt, "the prompt must be non-empty"
        return json.dumps(payload)

    return _complete


class TestBoundary:
    """What the prototype refuses. This is the load-bearing half."""

    def test_a_candidate_is_always_advisory(self) -> None:
        """There is no way to construct an authoritative induced constraint."""
        result = induce_column_constraints(
            "month",
            _MONTHS,
            completion_fn=_responder(
                {"constraints": [{"kind": "regex", "pattern": "^[a-z]+$", "rationale": "text"}]}
            ),
        )
        assert result.candidates
        for candidate in result.candidates:
            assert candidate.strength == "advisory"

    def test_strength_is_not_a_settable_field(self) -> None:
        """A frozen literal, so a caller cannot promote a candidate by assignment."""
        candidate = CandidateConstraint(
            kind="regex",
            column="c",
            rationale="r",
            support=9,
            total=10,
            violating_values=("x",),
        )
        with pytest.raises((AttributeError, TypeError)):
            candidate.strength = "proven"  # type: ignore[misc]

    def test_a_response_proposing_a_value_is_rejected(self) -> None:
        """The boundary is enforced on shape, not merely requested in the prompt."""
        result = induce_column_constraints(
            "month",
            _MONTHS,
            completion_fn=_responder(
                {
                    "constraints": [
                        {
                            "kind": "regex",
                            "pattern": "^[a-z]+$",
                            "new_value": "september",
                            "rationale": "fix it",
                        }
                    ]
                }
            ),
        )
        assert not result.candidates
        assert any("proposes a value" in reason for _, reason in result.rejected)

    def test_module_has_no_write_capability(self) -> None:
        """A prototype that could write its own premise could manufacture proof."""
        import inspect

        from dataforge.repairers import induce

        source = inspect.getsource(induce)
        for forbidden in ("write_text", "write_bytes", "open(", "Path("):
            assert forbidden not in source, (
                f"induce.py must have no filesystem access; found {forbidden!r}"
            )

    def test_module_does_not_produce_a_schema(self) -> None:
        """One line returning a Schema is all it would take to treat this as declared."""
        import inspect

        from dataforge.repairers import induce

        source = inspect.getsource(induce)
        assert "Schema(" not in source

    def test_prototype_is_not_a_registered_repairer(self) -> None:
        """It proposes premises, not fixes, so it must not appear in the repairer registry."""
        from dataforge.repairers import build_repairers

        registry = build_repairers(cache_dir=None, allow_llm=True, model="x")
        assert "induce" not in registry
        assert "constraint_induction" not in registry


class TestChecking:
    """Every candidate is evaluated against the data before being returned."""

    def test_a_non_compiling_regex_is_rejected_not_returned(self) -> None:
        result = induce_column_constraints(
            "month",
            _MONTHS,
            completion_fn=_responder({"constraints": [{"kind": "regex", "pattern": "^[unclosed"}]}),
        )
        assert not result.candidates
        assert any("does not compile" in reason for _, reason in result.rejected)

    def test_a_candidate_below_the_relevance_floor_is_rejected(self) -> None:
        """A constraint holding for a minority is describing a different column."""
        result = induce_column_constraints(
            "month",
            _MONTHS,
            completion_fn=_responder({"constraints": [{"kind": "regex", "pattern": "^j"}]}),
        )
        assert not result.candidates
        assert any("relevance floor" in reason for _, reason in result.rejected)

    def test_a_candidate_flagging_nothing_is_rejected(self) -> None:
        """Accepting a premise that changes no outcome adds risk for no benefit."""
        result = induce_column_constraints(
            "month",
            _MONTHS,
            completion_fn=_responder({"constraints": [{"kind": "regex", "pattern": ".*"}]}),
        )
        assert not result.candidates
        assert any("no value violates it" in reason for _, reason in result.rejected)

    def test_candidate_carries_the_cells_it_would_flag(self) -> None:
        """The reviewer sees the consequence before accepting the premise.

        This is the property a proposed value cannot have: a value comes with a confidence,
        a constraint comes with its exact blast radius.
        """
        result = induce_column_constraints(
            "month",
            _MONTHS,
            completion_fn=_responder({"constraints": [{"kind": "regex", "pattern": "^[a-z]+$"}]}),
        )
        assert len(result.candidates) == 1
        candidate = result.candidates[0]
        assert candidate.violating_values == ("1234",)
        assert candidate.support == 8
        assert candidate.total == 9
        assert candidate.support_fraction == 0.8889

    def test_accepted_values_candidate_is_checked(self) -> None:
        result = induce_column_constraints(
            "status",
            ["open", "closed", "open", "closed", "open", "closed", "open", "pendng"],
            completion_fn=_responder(
                {"constraints": [{"kind": "accepted_values", "values": ["open", "closed"]}]}
            ),
        )
        assert len(result.candidates) == 1
        assert result.candidates[0].violating_values == ("pendng",)

    def test_domain_bound_treats_a_non_numeric_value_as_a_violation(self) -> None:
        """Swallowing the parse failure would let a bound 'hold' on a column of text."""
        result = induce_column_constraints(
            "score",
            ["1", "2", "3", "4", "5", "6", "7", "8", "N/A"],
            completion_fn=_responder(
                {"constraints": [{"kind": "domain_bound", "min": 0, "max": 10}]}
            ),
        )
        assert len(result.candidates) == 1
        assert result.candidates[0].violating_values == ("N/A",)

    def test_unsupported_kind_is_rejected_with_the_allowed_list(self) -> None:
        result = induce_column_constraints(
            "month",
            _MONTHS,
            completion_fn=_responder({"constraints": [{"kind": "vibes"}]}),
        )
        assert not result.candidates
        assert any("unsupported kind" in reason for _, reason in result.rejected)

    def test_rejections_are_returned_not_dropped(self) -> None:
        """A caller seeing only survivors cannot tell a careful inducer from a lucky one."""
        result = induce_column_constraints(
            "month",
            _MONTHS,
            completion_fn=_responder(
                {
                    "constraints": [
                        {"kind": "regex", "pattern": "^[a-z]+$"},
                        {"kind": "regex", "pattern": "^[unclosed"},
                        {"kind": "vibes"},
                    ]
                }
            ),
        )
        assert len(result.candidates) == 1
        assert len(result.rejected) == 2


class TestFailClosed:
    """A broken inducer must not look like a careful one."""

    def test_non_json_response_raises(self) -> None:
        with pytest.raises(InductionError, match="not JSON"):
            induce_column_constraints("c", _MONTHS, completion_fn=lambda _: "sorry, I cannot")

    def test_response_without_a_constraints_list_raises(self) -> None:
        with pytest.raises(InductionError, match="no 'constraints' list"):
            induce_column_constraints("c", _MONTHS, completion_fn=_responder({"answer": "none"}))

    def test_short_column_returns_no_candidates_without_calling_the_model(self) -> None:
        """'95% satisfy this' on four values is a coincidence, and a paid call for nothing."""

        def _must_not_be_called(prompt: str) -> str:
            raise AssertionError("the model must not be called on a column below the floor")

        result = induce_column_constraints("c", ["a", "b", "c"], completion_fn=_must_not_be_called)
        assert not result.candidates
        assert result.rejected


class TestPrompt:
    """The prompt states the boundary explicitly."""

    def test_prompt_forbids_value_proposals(self) -> None:
        prompt = render_induction_prompt("month", _MONTHS)
        assert "NEVER corrected values" in prompt
        assert "Do not suggest what any cell" in prompt

    def test_prompt_reports_truncation(self) -> None:
        """Silence about a truncated sample is how a partial view reads as a whole one."""
        values = [f"v{i}" for i in range(100)]
        prompt = render_induction_prompt("c", values, max_values=10)
        assert "10 of 100 shown" in prompt
