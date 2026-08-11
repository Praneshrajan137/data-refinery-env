"""The agent executor must engage the advisory inferred guard when no schema exists.

Before 2026-08-09 the executor called ``SMTVerifier.verify(df, [fix], schema)`` with
three positional arguments, never passing ``verification_schema``. With ``schema=None``
the verifier short-circuits to a vacuous ACCEPT -- it checks only that the row is in
bounds and the column exists -- so *any* agent value was reported as "verified by the
SMT verifier" without a single check against the value itself.

``propose_repairs`` had always passed the inferred guard. The agent surface had not.

Scope, stated honestly: this proves the guard is *engaged*, not that it is complete.
The inferred guard's reach is bounded by what can be inferred from dirty data, and those
bounds are enumerated in ``docs/trust/inferred-guard-gaps.md``. In particular a column
that still contains the dirty value infers as ``str`` and therefore carries no numeric
type or domain constraint at all -- see
``test_dirty_column_infers_as_str_so_the_guard_cannot_constrain_it`` below, which pins
that limitation rather than hiding it. This is why the proven-only gate, not the guard,
is what keeps such a value off disk.
"""

from __future__ import annotations

from pathlib import Path

from dataforge.agent.executor import VerifiedActionExecutor
from dataforge.agent.tool_actions import parse_action
from dataforge.safety import SafetyContext
from dataforge.schema_inference import infer_verification_schema
from dataforge.table import read_csv


def _table(tmp_path: Path, content: str) -> object:
    path = tmp_path / "data.csv"
    path.write_text(content, encoding="utf-8")
    return read_csv(path)


def _clean_numeric_table(tmp_path: Path) -> object:
    """``score`` is clean numeric; the dirty cell sits in ``note`` instead.

    Keeping the dirt out of ``score`` is what lets ``infer_verification_schema``
    infer ``score: int`` with a domain bound, which is the precondition for the
    guard being able to say anything at all about a proposed ``score`` value.
    """
    return _table(
        tmp_path,
        "id,score,note\n1,10,ok\n2,20,ok\n3,30,ok\n4,40,ok\n5,50,ok\n6,60,ok\n7,70,ok\n8,80,!!!\n",
    )


def _executor(table: object) -> VerifiedActionExecutor:
    return VerifiedActionExecutor(
        table,
        None,
        provenance="llm_live",
        safety_context=SafetyContext(confirm_escalations=True),
    )


def _fix(row: int, column: str, value: str):  # noqa: ANN201
    return parse_action(
        {
            "action_type": "FIX",
            "row": row,
            "column": column,
            "new_value": value,
            "justification": "test",
            "fix_type": "correct_value",
        }
    )


class TestInferredGuardIsEngaged:
    """A schema-less FIX is checked against the inferred guard, not waved through."""

    def test_non_numeric_value_is_rejected_in_an_inferred_numeric_column(
        self, tmp_path: Path
    ) -> None:
        outcome = _executor(_clean_numeric_table(tmp_path)).execute(
            _fix(3, "score", "not-a-number")
        )

        assert outcome.accepted is False
        assert outcome.rejection_reason is not None

    def test_out_of_domain_value_is_rejected(self, tmp_path: Path) -> None:
        # infer_verification_schema pads the observed range, so this is far outside it.
        outcome = _executor(_clean_numeric_table(tmp_path)).execute(_fix(3, "score", "999999999"))

        assert outcome.accepted is False

    def test_plausible_in_domain_value_is_still_accepted(self, tmp_path: Path) -> None:
        # The guard must not become a blanket refusal: a value consistent with the
        # inferred type and domain still passes, exactly as in the deterministic path.
        outcome = _executor(_clean_numeric_table(tmp_path)).execute(_fix(3, "score", "35"))

        assert outcome.accepted is True


class TestInferredGuardLimits:
    """Pin the documented limits so they cannot be mistaken for guarantees."""

    def test_dirty_column_infers_as_str_so_the_guard_cannot_constrain_it(
        self, tmp_path: Path
    ) -> None:
        # The single non-numeric cell drags the whole column to ``str``, which removes
        # both the type check and the domain bound. Documented as gaps 1-2 in
        # docs/trust/inferred-guard-gaps.md. Recorded here as a measured fact, because
        # the honest defence for this case is the proven-only gate, not the guard.
        table = _table(tmp_path, "id,score\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,abc\n")

        inferred = infer_verification_schema(table)

        assert inferred.columns["score"] == "str"
        assert all(bound.column != "score" for bound in inferred.domain_bounds)
        # Consequence: a garbage value in that column clears the guard.
        assert _executor(table).execute(_fix(7, "score", "not-a-number")).accepted is True
