"""Tests for the per-issue CorrectionContract (C1).

The contract is the innovation that makes an LLM corrector safe: the detector
that found an error, plus the inferred (or declared) constraints, together
define what a *valid* correction looks like. The contract both (a) describes
that target for the corrector and (b) cheaply validates a candidate value
before the verifier / constitution gates spend budget on it.
"""

from __future__ import annotations

import pandas as pd

from dataforge.detectors.base import Issue, Severity
from dataforge.repairers.contract import CorrectionContract, build_correction_contract
from dataforge.schema_inference import infer_verification_schema


def _issue(
    *,
    row: int,
    column: str,
    issue_type: str,
    actual: str,
    expected: str | None = None,
    severity: Severity = Severity.REVIEW,
) -> Issue:
    return Issue(
        row=row,
        column=column,
        issue_type=issue_type,  # type: ignore[arg-type]
        severity=severity,
        confidence=0.9,
        expected=expected,
        actual=actual,
        reason="detected for test",
    )


class TestBuildContract:
    def test_build_returns_contract_for_issue(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

        contract = build_correction_contract(issue, schema)

        assert isinstance(contract, CorrectionContract)
        assert contract.issue is issue


class TestContractRejects:
    def test_rejects_no_op_equal_to_actual(self) -> None:
        df = pd.DataFrame({"city": ["Boston", "Denver", "Austin"]})
        schema = infer_verification_schema(df)
        issue = _issue(
            row=0, column="city", issue_type="categorical_normalization", actual="boston"
        )

        contract = build_correction_contract(issue, schema)
        result = contract.check("boston")

        assert result.ok is False
        assert "differ" in result.reason.lower() or "no-op" in result.reason.lower()

    def test_rejects_empty_correction(self) -> None:
        df = pd.DataFrame({"city": ["Boston", "Denver", "Austin"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=0, column="city", issue_type="missing_value", actual="")

        contract = build_correction_contract(issue, schema)
        result = contract.check("   ")

        assert result.ok is False
        assert "empty" in result.reason.lower()

    def test_rejects_non_numeric_for_numeric_column(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=0, column="amount", issue_type="type_mismatch", actual="oops")

        contract = build_correction_contract(issue, schema)
        result = contract.check("still_not_a_number")

        assert result.ok is False

    def test_rejects_out_of_domain_numeric(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

        contract = build_correction_contract(issue, schema)
        result = contract.check("999999")

        assert result.ok is False

    def test_rejects_regex_mismatch(self) -> None:
        df = pd.DataFrame({"zip": ["02134", "10001", "94105", "60601", "30301"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=0, column="zip", issue_type="format_violation", actual="2134")

        contract = build_correction_contract(issue, schema)
        result = contract.check("BADZIP")

        assert result.ok is False


class TestContractAccepts:
    def test_accepts_valid_in_domain_correction(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

        contract = build_correction_contract(issue, schema)
        result = contract.check("11")

        assert result.ok is True

    def test_accepts_canonical_normalization_to_unseen_value(self) -> None:
        df = pd.DataFrame({"style": ["IPA", "ipa", "Lager", "lager", "Stout"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=1, column="style", issue_type="categorical_normalization", actual="ipa")

        contract = build_correction_contract(issue, schema)
        result = contract.check("India Pale Ale")

        assert result.ok is True

    def test_accepts_missing_value_fill(self) -> None:
        df = pd.DataFrame({"city": ["Boston", "Denver", "Austin", "Reno"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=0, column="city", issue_type="missing_value", actual="")

        contract = build_correction_contract(issue, schema)
        result = contract.check("Seattle")

        assert result.ok is True


class TestContractDescribe:
    def test_describe_states_location_and_requirements(self) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=4, column="amount", issue_type="outlier", actual="9999")

        contract = build_correction_contract(issue, schema)
        text = contract.describe()

        assert "amount" in text
        assert "9999" in text
        # numeric requirement should be surfaced for a numeric column
        assert "number" in text.lower() or "numeric" in text.lower() or "int" in text.lower()

    def test_describe_mentions_pattern_for_structured_column(self) -> None:
        df = pd.DataFrame({"zip": ["02134", "10001", "94105", "60601", "30301"]})
        schema = infer_verification_schema(df)
        issue = _issue(row=0, column="zip", issue_type="format_violation", actual="2134")

        contract = build_correction_contract(issue, schema)
        text = contract.describe()

        assert r"\d{5}" in text
