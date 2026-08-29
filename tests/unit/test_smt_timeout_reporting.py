"""The solver budget must be reachable, and a timeout must not read as a violation.

Two defects motivated this file, both found on 2026-08-29.

**The budget was unreachable.** ``SchemaToSMT.__init__`` hard-coded ``timeout_ms=200``, and
``SMTVerifier.verify`` -- the only production caller -- does not forward the parameter. There was
no CLI flag, no environment variable and no config key. Because ``differential_verify`` collapses
UNKNOWN into REJECT, and because the whole-table encoding exhausted 200 ms on every fix at
1,000 rows, repairs were being dropped by a timeout that no operator could observe or raise.

**A timeout was indistinguishable from a constraint violation.** Both arrived as UNKNOWN with a
reason built from ``solver.reason_unknown()``. That string cannot be trusted to tell them apart:
z3 issue #445 records the same cause reporting "sometimes `timeout` and sometimes `unknown`", the
possible values are not enumerated in any documentation, and no stability guarantee is offered.
The budget belongs to us, so the measurement has to be ours.

One of these is fixable by giving the solver more time. The other means the repair was wrong.
Reporting them identically makes the difference invisible exactly where it matters.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix
from dataforge.verifier.result import VerificationVerdict
from dataforge.verifier.schema import FunctionalDependency, Schema
from dataforge.verifier.smt import (
    _DEFAULT_TIMEOUT_MS,
    _TIMEOUT_ENV_VAR,
    SchemaToSMT,
    _default_timeout_ms,
)

_SCHEMA = Schema(
    columns={"code": "str", "name": "str"},
    functional_dependencies=(FunctionalDependency(determinant=("code",), dependent="name"),),
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"code": ["A", "A", "A"], "name": ["X", "X", "Y"]})


def _fix() -> ProposedFix:
    return ProposedFix(
        fix=CellFix(row=2, column="name", old_value="Y", new_value="X", detector_id="fd_violation"),
        reason="candidate",
        confidence=1.0,
        provenance="deterministic",
    )


def test_default_budget_is_used_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset means the documented default, not zero and not unbounded."""
    monkeypatch.delenv(_TIMEOUT_ENV_VAR, raising=False)
    assert _default_timeout_ms() == _DEFAULT_TIMEOUT_MS


def test_budget_is_reachable_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """An operator must be able to raise it without editing source."""
    monkeypatch.setenv(_TIMEOUT_ENV_VAR, "4242")
    assert _default_timeout_ms() == 4242
    verifier = SchemaToSMT(_SCHEMA, _frame())
    assert verifier._timeout_ms == 4242, (
        "the constructed verifier must pick up the configured budget; a knob that only changes a "
        "helper's return value is not reachable"
    )


@pytest.mark.parametrize("bad", ["", "abc", "0", "-1", "1.5"])
def test_malformed_budget_falls_back_to_the_default(
    monkeypatch: pytest.MonkeyPatch, bad: str
) -> None:
    """A wrong environment variable must not stop the verifier from running.

    Refusing to start would fail open relative to the operator's intent: the verifier is what
    holds unproven fixes back, so a verifier that will not construct is strictly worse than one
    running on the documented default.
    """
    monkeypatch.setenv(_TIMEOUT_ENV_VAR, bad)
    assert _default_timeout_ms() == _DEFAULT_TIMEOUT_MS


def test_explicit_argument_still_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """The environment is a default, not an override."""
    monkeypatch.setenv(_TIMEOUT_ENV_VAR, "9999")
    assert SchemaToSMT(_SCHEMA, _frame(), timeout_ms=7)._timeout_ms == 7


def test_a_budget_exhausted_unknown_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    """A timeout must name itself a timeout and point at the knob that fixes it."""
    monkeypatch.delenv(_TIMEOUT_ENV_VAR, raising=False)

    class _NeverFinishes:
        """A solver that consumes the budget and returns unknown, as z3 does on timeout."""

        def __init__(self) -> None:
            self._budget_ms = 0

        def set(self, **kwargs: object) -> None:
            timeout = kwargs.get("timeout")
            self._budget_ms = int(timeout) if isinstance(timeout, int) else 0

        def add(self, *args: object) -> None:
            return None

        def assert_and_track(self, formula: object, label: object) -> None:
            return None

        def check(self) -> object:
            import time as _time

            _time.sleep(self._budget_ms / 1000.0)
            from z3 import unknown as _unknown  # type: ignore[import-untyped]

            return _unknown

        def reason_unknown(self) -> str:
            return "canceled"

    import dataforge.verifier.smt as smt_module

    monkeypatch.setattr(smt_module, "Solver", _NeverFinishes)
    result = SchemaToSMT(_SCHEMA, _frame(), timeout_ms=50).verify_fix(_fix())

    assert result.verdict == VerificationVerdict.UNKNOWN, "must stay fail-closed"
    assert "timeout" in result.reason.lower()
    assert "NOT a constraint violation" in result.reason, (
        "the whole point is that a reader can tell this apart from a real violation"
    )
    assert _TIMEOUT_ENV_VAR in result.reason, "say which knob fixes it"


def test_an_incompleteness_unknown_is_not_called_a_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Returning unknown well inside the budget is incompleteness; more time will not help."""
    monkeypatch.delenv(_TIMEOUT_ENV_VAR, raising=False)

    class _GivesUpImmediately:
        def set(self, **kwargs: object) -> None:
            return None

        def add(self, *args: object) -> None:
            return None

        def assert_and_track(self, formula: object, label: object) -> None:
            return None

        def check(self) -> object:
            from z3 import unknown as _unknown  # type: ignore[import-untyped]

            return _unknown

        def reason_unknown(self) -> str:
            return "(incomplete (theory arithmetic))"

    import dataforge.verifier.smt as smt_module

    monkeypatch.setattr(smt_module, "Solver", _GivesUpImmediately)
    result = SchemaToSMT(_SCHEMA, _frame(), timeout_ms=100_000).verify_fix(_fix())

    assert result.verdict == VerificationVerdict.UNKNOWN
    assert "incompleteness" in result.reason
    assert "more time will not help" in result.reason
    assert "budget exhausted" not in result.reason, (
        "misreporting incompleteness as a timeout would send an operator to raise a budget that "
        "cannot help, which is worse than saying nothing"
    )
