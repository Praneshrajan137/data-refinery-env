"""Tests for the fail-closed differential verifier (N-version cross-check)."""

from __future__ import annotations

import pandas as pd

from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix
from dataforge.verifier.differential import differential_verify
from dataforge.verifier.result import VerificationResult, VerificationVerdict
from dataforge.verifier.schema import DomainBound, Schema


class _FakeVerifier:
    def __init__(self, verdict: VerificationVerdict) -> None:
        self._verdict = verdict

    def verify(self, df, fixes, schema=None, *, verification_schema=None):  # noqa: ANN001, ANN201
        return VerificationResult(verdict=self._verdict, reason=f"fake {self._verdict.value}")


def _fix() -> ProposedFix:
    return ProposedFix(
        fix=CellFix(row=0, column="a", old_value="old", new_value="5", detector_id="t"),
        reason="c",
        confidence=1.0,
        provenance="llm_live",
    )


_A = VerificationVerdict.ACCEPT
_R = VerificationVerdict.REJECT
_U = VerificationVerdict.UNKNOWN


def _combined(primary: VerificationVerdict, secondary: VerificationVerdict) -> object:
    df = pd.DataFrame({"a": ["1"]})
    return differential_verify(
        df,
        [_fix()],
        Schema(columns={"a": "int"}),
        primary=_FakeVerifier(primary),
        secondary=_FakeVerifier(secondary),
    )


def test_both_accept_is_accept_and_agrees() -> None:
    result = _combined(_A, _A)
    assert result.verdict == _A
    assert result.agreement is True


def test_disagreement_fails_closed() -> None:
    for primary, secondary in [(_A, _R), (_R, _A), (_A, _U), (_U, _A)]:
        result = _combined(primary, secondary)
        assert result.verdict == _R, f"{primary}/{secondary} should fail closed"
        assert result.agreement is (primary == secondary)


def test_both_reject_is_reject_and_agrees() -> None:
    result = _combined(_R, _R)
    assert result.verdict == _R
    assert result.agreement is True


def test_real_smt_and_direct_agree_on_accept_and_reject() -> None:
    df = pd.DataFrame({"a": ["10", "20", "30"]})
    schema = Schema(
        columns={"a": "int"},
        domain_bounds=(DomainBound(column="a", min_value=0.0, max_value=100.0),),
    )
    ok = ProposedFix(
        fix=CellFix(row=0, column="a", old_value="10", new_value="50", detector_id="t"),
        reason="c",
        confidence=1.0,
        provenance="deterministic",
    )
    bad = ProposedFix(
        fix=CellFix(row=0, column="a", old_value="10", new_value="500", detector_id="t"),
        reason="c",
        confidence=1.0,
        provenance="deterministic",
    )
    accept = differential_verify(df, [ok], schema)
    assert accept.verdict == _A and accept.agreement is True
    reject = differential_verify(df, [bad], schema)
    assert reject.verdict == _R and reject.agreement is True
