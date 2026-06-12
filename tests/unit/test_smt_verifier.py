"""Unit tests for the Week 3 SMT verifier."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from dataforge.detectors.base import (
    AcceptedValues,
    DomainBound,
    FunctionalDependency,
    RegexConstraint,
    Schema,
)
from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix
from dataforge.verifier import (
    SchemaToSMT,
    SMTVerifier,
    VerificationVerdict,
    constraint_ir_from_schema,
    explain_unsat_core,
)


def _fix(*, row: int, column: str, old_value: str, new_value: str, detector_id: str) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=row,
            column=column,
            old_value=old_value,
            new_value=new_value,
            detector_id=detector_id,
        ),
        reason="candidate",
        confidence=0.9,
        provenance="deterministic",
    )


class TestSchemaToSMT:
    """Constraint-aware verification behavior."""

    def test_decimal_shift_fix_within_bounds_is_accepted(self) -> None:
        df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
        schema = Schema(
            columns={"amount": "float"},
            domain_bounds=[DomainBound(column="amount", min_value=0.0, max_value=5000.0)],
        )

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=3,
                column="amount",
                old_value="1020",
                new_value="102",
                detector_id="decimal_shift",
            )
        )

        assert result.verdict == VerificationVerdict.ACCEPT

    def test_domain_bound_violation_is_rejected_with_explanation(self) -> None:
        df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
        schema = Schema(
            columns={"amount": "float"},
            domain_bounds=[DomainBound(column="amount", min_value=0.0, max_value=5000.0)],
        )

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=3,
                column="amount",
                old_value="1020",
                new_value="-5",
                detector_id="decimal_shift",
            )
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert result.unsat_core
        explanation = explain_unsat_core(result.unsat_core, schema)
        assert "amount" in explanation
        assert "minimum" in explanation.lower()

    def test_not_null_violation_is_rejected(self) -> None:
        df = pd.DataFrame({"name": ["Ada", "Grace"]})
        schema = Schema(columns={"name": "str"}, not_null_columns={"name"})

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(row=1, column="name", old_value="Grace", new_value="", detector_id="manual")
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert "not-null" in result.reason.lower()

    def test_unique_violation_is_rejected(self) -> None:
        df = pd.DataFrame({"email": ["a@example.com", "b@example.com"]})
        schema = Schema(columns={"email": "str"}, unique_columns={"email"})

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=1,
                column="email",
                old_value="b@example.com",
                new_value="a@example.com",
                detector_id="manual",
            )
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert "unique" in result.reason.lower()

    def test_accepted_values_violation_is_rejected(self) -> None:
        df = pd.DataFrame({"status": ["placed", "shipped"]})
        schema = Schema(
            columns={"status": "str"},
            accepted_values=[AcceptedValues(column="status", values=("placed", "shipped"))],
        )

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=0,
                column="status",
                old_value="placed",
                new_value="lost",
                detector_id="manual",
            )
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert "accepted values" in result.reason.lower()

    def test_regex_violation_is_rejected(self) -> None:
        df = pd.DataFrame({"state": ["CA", "NY"]})
        schema = Schema(
            columns={"state": "str"},
            regex_constraints=[RegexConstraint(column="state", pattern=r"^[A-Z]{2}$")],
        )

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(row=1, column="state", old_value="NY", new_value="new york", detector_id="manual")
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert "regex" in result.reason.lower()

    def test_constraint_ir_includes_v1_dbt_style_constraints(self) -> None:
        schema = Schema(
            columns={"id": "str", "status": "str"},
            not_null_columns={"id"},
            unique_columns={"id"},
            primary_key_columns={"id"},
            accepted_values=[AcceptedValues(column="status", values=("placed", "shipped"))],
            regex_constraints=[RegexConstraint(column="status", pattern=r"^[a-z]+$")],
        )

        kinds = {constraint.kind for constraint in constraint_ir_from_schema(schema)}
        ids = {constraint.constraint_id for constraint in constraint_ir_from_schema(schema)}

        assert {"not_null", "unique", "accepted_values", "regex"}.issubset(kinds)
        assert "primary_key::id" in ids

    def test_fd_violation_is_rejected(self) -> None:
        df = pd.DataFrame(
            {
                "code": ["A", "A", "A"],
                "name": ["Alpha", "Alpha", "Beta"],
                "state": ["IL", "IL", "NY"],
            }
        )
        schema = Schema(
            columns={"code": "str", "name": "str", "state": "str"},
            functional_dependencies=[
                FunctionalDependency(determinant=["code"], dependent="name"),
                FunctionalDependency(determinant=["name"], dependent="state"),
            ],
        )

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=2,
                column="name",
                old_value="Beta",
                new_value="Alpha",
                detector_id="fd_violation",
            )
        )

        assert result.verdict == VerificationVerdict.REJECT
        explanation = explain_unsat_core(result.unsat_core, schema)
        assert "name" in explanation
        assert "state" in explanation
        assert "fd" in explanation.lower()

    def test_unsupported_type_returns_unknown(self) -> None:
        df = pd.DataFrame({"event_date": ["2026-04-20"]})
        schema = Schema(columns={"event_date": "date"})

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=0,
                column="event_date",
                old_value="2026-04-20",
                new_value="2026-04-21",
                detector_id="type_mismatch",
            )
        )

        assert result.verdict == VerificationVerdict.UNKNOWN
        assert "unsupported" in result.reason.lower()

    def test_delete_row_operation_is_rejected(self) -> None:
        df = pd.DataFrame({"amount": ["10"]})
        schema = Schema(columns={"amount": "float"})

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=0,
                column="amount",
                old_value="10",
                new_value="11",
                detector_id="manual",
            ).model_copy(
                update={
                    "fix": CellFix(
                        row=0,
                        column="amount",
                        old_value="10",
                        new_value="11",
                        detector_id="manual",
                        operation="delete_row",
                    )
                }
            )
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert "only cell updates" in result.reason.lower()

    def test_out_of_bounds_row_is_rejected(self) -> None:
        df = pd.DataFrame({"amount": ["10"]})
        schema = Schema(columns={"amount": "float"})

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=4,
                column="amount",
                old_value="10",
                new_value="11",
                detector_id="decimal_shift",
            )
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert "out of bounds" in result.reason.lower()

    def test_missing_column_is_rejected(self) -> None:
        df = pd.DataFrame({"amount": ["10"]})
        schema = Schema(columns={"amount": "float"})

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=0,
                column="missing",
                old_value="10",
                new_value="11",
                detector_id="decimal_shift",
            )
        )

        assert result.verdict == VerificationVerdict.REJECT
        assert "does not exist" in result.reason.lower()

    def test_encoding_failure_returns_unknown(self) -> None:
        df = pd.DataFrame({"amount": ["10"]})
        schema = Schema(columns={"amount": "int"})

        result = SchemaToSMT(schema, df).verify_fix(
            _fix(
                row=0,
                column="amount",
                old_value="10",
                new_value="not-an-int",
                detector_id="type_mismatch",
            )
        )

        assert result.verdict == VerificationVerdict.UNKNOWN
        assert "could not encode value" in result.reason.lower()

    def test_solver_unknown_status_is_propagated(self) -> None:
        df = pd.DataFrame({"amount": ["10"]})
        schema = Schema(columns={"amount": "float"})

        class FakeSolver:
            def set(self, **kwargs: object) -> None:
                del kwargs

            def add(self, *args: object) -> None:
                del args

            def assert_and_track(self, formula: object, label: object) -> None:
                del formula, label

            def check(self) -> object:
                from dataforge.verifier.smt import unknown

                return unknown

            def reason_unknown(self) -> str:
                return "timed out"

            def unsat_core(self) -> tuple[()]:
                return ()

        with patch("dataforge.verifier.smt.Solver", return_value=FakeSolver()):
            result = SchemaToSMT(schema, df).verify_fix(
                _fix(
                    row=0,
                    column="amount",
                    old_value="10",
                    new_value="11",
                    detector_id="decimal_shift",
                )
            )

        assert result.verdict == VerificationVerdict.UNKNOWN
        assert "timed out" in result.reason

    def test_wrapper_without_schema_rejects_invalid_coordinates(self) -> None:
        df = pd.DataFrame({"amount": ["10"]})
        verifier = SMTVerifier()

        row_result = verifier.verify(
            df,
            [
                _fix(
                    row=2,
                    column="amount",
                    old_value="10",
                    new_value="11",
                    detector_id="decimal_shift",
                )
            ],
            schema=None,
        )
        column_result = verifier.verify(
            df,
            [
                _fix(
                    row=0,
                    column="missing",
                    old_value="10",
                    new_value="11",
                    detector_id="decimal_shift",
                )
            ],
            schema=None,
        )

        assert row_result.verdict == VerificationVerdict.REJECT
        assert column_result.verdict == VerificationVerdict.REJECT

    def test_explain_unsat_core_handles_empty_and_unknown_labels(self) -> None:
        schema = Schema(columns={"amount": "float"})

        empty_message = explain_unsat_core((), schema)
        unknown_message = explain_unsat_core(("custom::rule",), schema)

        assert "did not expose" in empty_message.lower()
        assert "custom::rule" in unknown_message
