"""Tests for the verification-only inferred-constraint guard (C0).

This guard closes the schema-less verification gap: when no authoritative
(declared or reviewed) schema is present, the engine infers constraints and the
verifier checks any proposed correction against inferred type/domain/regex/FD
*instead of auto-accepting it*. The guard is advisory and value-focused -- it
only rejects clear violations of the proposed value and never imposes inferred
constraints on the rest of the (possibly dirty) table.

It is strictly distinct from the reviewed-constraints-drive-repair path: these
inferred constraints never generate issues and never drive repairs; they only
gate corrections that would otherwise bypass semantic verification.
"""

from __future__ import annotations

import pandas as pd

from dataforge.repairers.base import ProposedFix
from dataforge.schema_inference import infer_verification_schema
from dataforge.transactions.txn import CellFix
from dataforge.verifier import SMTVerifier, VerificationVerdict


def _fix(*, row: int, column: str, old_value: str, new_value: str) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=row,
            column=column,
            old_value=old_value,
            new_value=new_value,
            detector_id="llm_corrector",
        ),
        reason="candidate",
        confidence=0.9,
        provenance="llm_live",
    )


class TestInferVerificationSchema:
    """The inferred verification schema captures high-confidence structure."""

    def test_numeric_column_infers_type_and_domain(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})

        schema = infer_verification_schema(df)

        assert schema.column_type("amount") in {"int", "float"}
        bounds = schema.domain_bounds_for("amount")
        assert bounds, "expected an inferred numeric domain bound"
        assert bounds[0].min_value == 9.0
        assert bounds[0].max_value == 13.0

    def test_digit_code_column_infers_regex(self) -> None:
        df = pd.DataFrame({"zip": ["02134", "10001", "94105", "60601", "30301"]})

        schema = infer_verification_schema(df)

        patterns = [rule.pattern for rule in schema.regex_constraints_for("zip")]
        assert patterns == [r"^\d{5}$"]

    def test_free_text_column_is_unconstrained(self) -> None:
        df = pd.DataFrame({"city": ["Boston", "New York", "Chicago", "Denver"]})

        schema = infer_verification_schema(df)

        assert schema.column_type("city") == "str"
        assert schema.domain_bounds_for("city") == ()
        assert schema.regex_constraints_for("city") == ()
        # No closed enum is inferred for categoricals: that would block the very
        # canonical-normalization corrections the LLM corrector exists to make.
        assert schema.accepted_values_for("city") == ()

    def test_functional_dependency_is_inferred_at_high_confidence(self) -> None:
        df = pd.DataFrame(
            {
                "zip": ["02134", "02134", "10001", "10001", "94105", "94105"],
                "city": ["Boston", "Boston", "NYC", "NYC", "SF", "SF"],
            }
        )

        schema = infer_verification_schema(df)

        fds = {(fd.determinant, fd.dependent) for fd in schema.functional_dependencies}
        assert (("zip",), "city") in fds


class TestVerifierWithoutSchemaIsUnchanged:
    """Parity: with no verification schema, behavior is structural-only."""

    def test_schema_none_and_no_guard_accepts_any_in_bounds_value(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        verifier = SMTVerifier()

        result = verifier.verify(
            df,
            [_fix(row=0, column="amount", old_value="10", new_value="not_a_number")],
            schema=None,
        )

        # No verification_schema supplied -> only structural checks run, exactly
        # as before this guard existed. The garbage value is NOT rejected here.
        assert result.verdict == VerificationVerdict.ACCEPT


class TestInferredGuardRejectsViolations:
    """With an inferred verification schema, clear violations are rejected."""

    def test_out_of_domain_value_is_rejected(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        verifier = SMTVerifier()
        verification_schema = infer_verification_schema(df)

        result = verifier.verify(
            df,
            [_fix(row=0, column="amount", old_value="10", new_value="999999")],
            schema=None,
            verification_schema=verification_schema,
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert "amount" in result.reason

    def test_type_violation_is_rejected(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        verifier = SMTVerifier()
        verification_schema = infer_verification_schema(df)

        result = verifier.verify(
            df,
            [_fix(row=0, column="amount", old_value="10", new_value="twelve")],
            schema=None,
            verification_schema=verification_schema,
        )

        assert result.verdict == VerificationVerdict.REJECT

    def test_regex_violation_is_rejected(self) -> None:
        df = pd.DataFrame({"zip": ["02134", "10001", "94105", "60601", "30301"]})
        verifier = SMTVerifier()
        verification_schema = infer_verification_schema(df)

        result = verifier.verify(
            df,
            [_fix(row=0, column="zip", old_value="02134", new_value="BADZIP")],
            schema=None,
            verification_schema=verification_schema,
        )

        assert result.verdict == VerificationVerdict.REJECT

    def test_fd_violation_is_rejected(self) -> None:
        df = pd.DataFrame(
            {
                "zip": ["02134", "02134", "10001", "10001", "94105", "94105"],
                "city": ["Boston", "Boston", "NYC", "NYC", "SF", "SF"],
            }
        )
        verifier = SMTVerifier()
        verification_schema = infer_verification_schema(df)

        # zip 02134 consistently determines city "Boston"; proposing "Atlanta"
        # for a 02134 row violates the inferred functional dependency.
        result = verifier.verify(
            df,
            [_fix(row=0, column="city", old_value="Boston", new_value="Atlanta")],
            schema=None,
            verification_schema=verification_schema,
        )

        assert result.verdict == VerificationVerdict.REJECT


class TestInferredGuardAcceptsValidCorrections:
    """The guard must not block legitimate corrections."""

    def test_in_domain_numeric_correction_is_accepted(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        verifier = SMTVerifier()
        verification_schema = infer_verification_schema(df)

        result = verifier.verify(
            df,
            [_fix(row=0, column="amount", old_value="10", new_value="11")],
            schema=None,
            verification_schema=verification_schema,
        )

        assert result.verdict == VerificationVerdict.ACCEPT

    def test_fd_consistent_correction_is_accepted(self) -> None:
        df = pd.DataFrame(
            {
                "zip": ["02134", "02134", "10001", "10001", "94105", "94105"],
                "city": ["Boston", "Bostan", "NYC", "NYC", "SF", "SF"],
            }
        )
        verifier = SMTVerifier()
        verification_schema = infer_verification_schema(df)

        # Correcting the typo "Bostan" -> "Boston" agrees with the FD consensus.
        result = verifier.verify(
            df,
            [_fix(row=1, column="city", old_value="Bostan", new_value="Boston")],
            schema=None,
            verification_schema=verification_schema,
        )

        assert result.verdict == VerificationVerdict.ACCEPT

    def test_categorical_normalization_to_unseen_value_is_accepted(self) -> None:
        # A canonical spelling not present in the dirty column must still pass:
        # the guard infers no closed enum for free-text categoricals.
        df = pd.DataFrame({"style": ["IPA", "ipa", "Lager", "lager", "Stout"]})
        verifier = SMTVerifier()
        verification_schema = infer_verification_schema(df)

        result = verifier.verify(
            df,
            [_fix(row=1, column="style", old_value="ipa", new_value="India Pale Ale")],
            schema=None,
            verification_schema=verification_schema,
        )

        assert result.verdict == VerificationVerdict.ACCEPT
