"""Differential verification: cross-check two independent constraint checkers.

Runs the primary z3-backed :class:`~dataforge.verifier.smt.SMTVerifier` and the
independently-written :class:`~dataforge.verifier.direct.DirectVerifier` on the
same fixes and combines their verdicts FAIL-CLOSED: a fix is accepted only when
BOTH implementations accept it. Any disagreement (or a non-accept from either)
yields a non-accept combined verdict and is recorded, so a bug in either
implementation is caught rather than silently trusted.

This is the N-version safety net: the diverse checker can only ever REDUCE what
auto-applies, never increase it.
"""

from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, Field

from dataforge.repairers.base import ProposedFix
from dataforge.table import TableLike
from dataforge.verifier.direct import DirectVerifier
from dataforge.verifier.result import VerificationResult, VerificationVerdict
from dataforge.verifier.schema import Schema


class ConstraintVerifier(Protocol):
    """Structural interface shared by every constraint-checker implementation."""

    def verify(
        self,
        df: TableLike,
        fixes: list[ProposedFix],
        schema: Schema | None = None,
        *,
        verification_schema: Schema | None = None,
    ) -> VerificationResult:
        """Return the verdict for one or more candidate fixes."""
        ...  # pragma: no cover


class DifferentialResult(BaseModel):
    """Combined, fail-closed verdict plus each implementation's raw verdict."""

    verdict: VerificationVerdict
    reason: str = Field(min_length=1)
    agreement: bool
    primary_verdict: VerificationVerdict
    secondary_verdict: VerificationVerdict
    unsat_core: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}


def _make_smt_verifier() -> ConstraintVerifier:
    # Imported lazily so callers that only need the diverse checker stay z3-free.
    from dataforge.verifier.smt import SMTVerifier

    return SMTVerifier()


def differential_verify(
    df: TableLike,
    fixes: list[ProposedFix],
    schema: Schema | None = None,
    *,
    verification_schema: Schema | None = None,
    primary: ConstraintVerifier | None = None,
    secondary: ConstraintVerifier | None = None,
) -> DifferentialResult:
    """Verify with two independent checkers and combine their verdicts fail-closed.

    Args:
        df: The working dataframe.
        fixes: Candidate fixes to verify.
        schema: Authoritative declared/reviewed schema (the diverse cross-check is
            meaningful when this is present).
        verification_schema: Advisory inferred schema (passed through unchanged).
        primary: The primary verifier (defaults to the z3-backed SMTVerifier).
        secondary: The diverse verifier (defaults to DirectVerifier).

    Returns:
        A :class:`DifferentialResult` whose ``verdict`` is ACCEPT only when both
        implementations accept; otherwise REJECT (fail-closed), never leaking an
        UNKNOWN into the accept path.
    """
    primary = primary if primary is not None else _make_smt_verifier()
    secondary = secondary if secondary is not None else DirectVerifier()

    primary_result = primary.verify(df, fixes, schema, verification_schema=verification_schema)
    secondary_result = secondary.verify(df, fixes, schema, verification_schema=verification_schema)

    agreement = primary_result.verdict == secondary_result.verdict
    both_accept = (
        primary_result.verdict == VerificationVerdict.ACCEPT
        and secondary_result.verdict == VerificationVerdict.ACCEPT
    )

    if both_accept:
        return DifferentialResult(
            verdict=VerificationVerdict.ACCEPT,
            reason="Both independent verifiers accepted the candidate fix.",
            agreement=True,
            primary_verdict=primary_result.verdict,
            secondary_verdict=secondary_result.verdict,
        )

    if not agreement:
        reason = (
            "Independent verifiers disagreed and the fix is held fail-closed "
            f"(primary={primary_result.verdict.value}: {primary_result.reason}; "
            f"diverse={secondary_result.verdict.value}: {secondary_result.reason})."
        )
    else:
        reason = (
            f"Both verifiers returned {primary_result.verdict.value}, not accept: "
            f"{primary_result.reason}"
        )

    # Prefer a concrete rejecting core for explainability.
    unsat_core = (
        primary_result.unsat_core
        if primary_result.verdict == VerificationVerdict.REJECT
        else secondary_result.unsat_core
    )
    return DifferentialResult(
        verdict=VerificationVerdict.REJECT,
        reason=reason,
        agreement=agreement,
        primary_verdict=primary_result.verdict,
        secondary_verdict=secondary_result.verdict,
        unsat_core=unsat_core,
    )
