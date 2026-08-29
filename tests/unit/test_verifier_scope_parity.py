"""The two verifiers must agree even where the SMT encoding no longer looks.

``tests/property/test_verifier_equivalence.py`` generates tables of 2-4 rows from well-typed
value pools. That is a good fence for constraint logic and a blind spot for *encoding scope*: with
so few rows, almost every row falls inside the candidate's footprint, and no generated value is
uncoercible to its declared type.

Scoping the SMT encoding on 2026-08-29 opened exactly that blind spot. ``DirectVerifier`` returns
UNKNOWN when any value in a relevant column cannot be coerced to its declared type, justified in
``direct.py`` on the grounds that "the primary verifier likewise cannot encode it". That was true
while every cell was asserted. Once only the footprint was asserted, an uncoercible value in a row
*outside* the footprint produced **SMT ACCEPT against Direct UNKNOWN** -- a real divergence between
two implementations whose whole purpose is to be independently right about the same thing.

The differential caught it and failed closed, so no unsound value could ever have been written.
That is the invariant working. It is still a defect: the N-version check is supposed to *detect*
disagreement, not absorb it silently on an entire input class.

These tests pin the parity directly, with tables large enough that footprint and table are not the
same set. They are deliberately not Hypothesis-driven: the case is specific and known, and a named
test that states the mechanism is worth more here than another random search over small frames.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix
from dataforge.verifier.differential import differential_verify
from dataforge.verifier.direct import DirectVerifier
from dataforge.verifier.result import VerificationVerdict
from dataforge.verifier.schema import DomainBound, FunctionalDependency, Schema
from dataforge.verifier.smt import SMTVerifier

_ROWS = 40
#: The candidate is row 0; the poison sits far from it so it is outside any footprint.
_POISON_ROW = 30


def _fix(column: str, old: str, new: str, row: int = 0) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(row=row, column=column, old_value=old, new_value=new, detector_id="p"),
        reason="candidate",
        confidence=1.0,
        provenance="deterministic",
    )


def _assert_agree(df: pd.DataFrame, schema: Schema, fix: ProposedFix, note: str) -> None:
    smt = SMTVerifier().verify(df, [fix], schema)
    direct = DirectVerifier().verify(df, [fix], schema)
    assert smt.verdict == direct.verdict, (
        f"{note}: the verifiers disagreed. smt={smt.verdict.name} ({smt.reason}) "
        f"direct={direct.verdict.name} ({direct.reason}). Scoping what the SMT encoding ASSERTS "
        "must not change what either verifier CONCLUDES."
    )
    combined = differential_verify(df, [fix], schema)
    assert combined.agreement, f"{note}: differential reported disagreement"


def test_uncoercible_value_outside_the_footprint_keeps_both_verifiers_in_step() -> None:
    """The exact case that diverged: garbage in a relevant column, far from the candidate."""
    amounts = [str(index + 1) for index in range(_ROWS)]
    amounts[_POISON_ROW] = "not-a-number"
    df = pd.DataFrame({"amount": amounts})
    schema = Schema(
        columns={"amount": "float"},
        domain_bounds=(DomainBound(column="amount", min_value=0.0, max_value=1_000.0),),
    )
    _assert_agree(df, schema, _fix("amount", "1", "3"), "uncoercible outside footprint")


def test_uncoercible_value_in_an_fd_column_outside_the_group_keeps_both_in_step() -> None:
    """Same shape, reached through a functional dependency's relevant-column set."""
    code = ["A", "A"] + [f"K{index}" for index in range(_ROWS - 2)]
    amount = [str(index + 1) for index in range(_ROWS)]
    amount[_POISON_ROW] = "still-not-a-number"
    df = pd.DataFrame({"code": code, "amount": amount})
    schema = Schema(
        columns={"code": "str", "amount": "float"},
        functional_dependencies=(FunctionalDependency(determinant=("code",), dependent="amount"),),
    )
    _assert_agree(df, schema, _fix("amount", "1", "2"), "uncoercible outside determinant group")


def test_a_clean_table_of_the_same_shape_still_reaches_a_real_verdict() -> None:
    """Non-vacuity: without the poison value the pair must decide, not abstain.

    Without this, the two tests above could pass by both verifiers returning UNKNOWN for some
    unrelated reason, which would make the parity assertion meaningless.
    """
    df = pd.DataFrame({"amount": [str(index + 1) for index in range(_ROWS)]})
    schema = Schema(
        columns={"amount": "float"},
        domain_bounds=(DomainBound(column="amount", min_value=0.0, max_value=1_000.0),),
    )
    fix = _fix("amount", "1", "3")
    smt = SMTVerifier().verify(df, [fix], schema)
    direct = DirectVerifier().verify(df, [fix], schema)
    assert smt.verdict == direct.verdict == VerificationVerdict.ACCEPT, (
        f"a clean table must yield ACCEPT from both, got smt={smt.verdict.name} "
        f"direct={direct.verdict.name}; otherwise the parity tests prove nothing"
    )


@pytest.mark.parametrize("poison", ["", "  ", "1,5", "1.2.3", "NaNaN", "1e", "--3"])
def test_parity_holds_across_several_uncoercible_shapes(poison: str) -> None:
    """One malformed literal is one sample; the class is what matters."""
    amounts = [str(index + 1) for index in range(_ROWS)]
    amounts[_POISON_ROW] = poison
    df = pd.DataFrame({"amount": amounts})
    schema = Schema(columns={"amount": "float"})
    _assert_agree(df, schema, _fix("amount", "1", "3"), f"poison={poison!r}")
