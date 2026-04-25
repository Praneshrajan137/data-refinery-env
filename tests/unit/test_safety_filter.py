"""Unit tests for Week 3 safety-filter behavior."""

from __future__ import annotations

from dataforge.detectors.base import AggregateDependency, Schema
from dataforge.repairers.base import ProposedFix
from dataforge.safety import SafetyContext, SafetyFilter, SafetyVerdict
from dataforge.transactions.txn import CellFix


def _fix(
    *,
    column: str = "amount",
    old_value: str = "1020",
    new_value: str = "102",
    detector_id: str = "decimal_shift",
    operation: str = "update",
) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=3,
            column=column,
            old_value=old_value,
            new_value=new_value,
            detector_id=detector_id,
            operation=operation,
        ),
        reason="candidate",
        confidence=0.9,
        provenance="deterministic",
    )


class TestSafetyFilter:
    """Week 3 verdict behavior."""

    def test_pii_overwrite_denied_without_override(self) -> None:
        schema = Schema(columns={"phone_number": "str"}, pii_columns={"phone_number"})

        result = SafetyFilter().evaluate(_fix(column="phone_number"), schema, SafetyContext())

        assert result.verdict == SafetyVerdict.DENY
        assert "NO_PII_OVERWRITE" in result.rule_ids

    def test_pii_overwrite_escalates_when_override_requested_without_confirmation(self) -> None:
        schema = Schema(columns={"phone_number": "str"}, pii_columns={"phone_number"})
        context = SafetyContext(allow_pii=True)

        result = SafetyFilter().evaluate(_fix(column="phone_number"), schema, context)

        assert result.verdict == SafetyVerdict.ESCALATE
        assert "confirmation" in result.reason.lower()

    def test_pii_overwrite_allows_when_override_confirmed(self) -> None:
        schema = Schema(columns={"phone_number": "str"}, pii_columns={"phone_number"})
        context = SafetyContext(allow_pii=True, confirm_pii=True)

        result = SafetyFilter().evaluate(_fix(column="phone_number"), schema, context)

        assert result.verdict == SafetyVerdict.ALLOW

    def test_row_delete_is_denied(self) -> None:
        result = SafetyFilter().evaluate(
            _fix(column="__row__", detector_id="manual", operation="delete_row"),
            Schema(columns={"id": "str"}),
            SafetyContext(),
        )

        assert result.verdict == SafetyVerdict.DENY
        assert "NO_ROW_DELETE" in result.rule_ids

    def test_aggregate_sensitive_edit_escalates_without_confirmation(self) -> None:
        schema = Schema(
            columns={"amount": "float"},
            aggregate_dependencies=[
                AggregateDependency(
                    source_column="amount",
                    aggregate="sum",
                    target_column="total_amount",
                )
            ],
        )

        result = SafetyFilter().evaluate(_fix(column="amount"), schema, SafetyContext())

        assert result.verdict == SafetyVerdict.ESCALATE
        assert "NO_AGGREGATE_BREAK" in result.rule_ids

    def test_aggregate_sensitive_edit_allows_when_confirmed(self) -> None:
        schema = Schema(
            columns={"amount": "float"},
            aggregate_dependencies=[
                AggregateDependency(
                    source_column="amount",
                    aggregate="sum",
                    target_column="total_amount",
                )
            ],
        )

        result = SafetyFilter().evaluate(
            _fix(column="amount"),
            schema,
            SafetyContext(confirm_escalations=True),
        )

        assert result.verdict == SafetyVerdict.ALLOW

    def test_minimal_edit_prefers_smallest_levenshtein_distance(self) -> None:
        candidates = [
            _fix(old_value="1020", new_value="101"),
            _fix(old_value="1020", new_value="102"),
        ]

        preferred = SafetyFilter().choose_preferred(
            candidates, Schema(columns={"amount": "float"}), SafetyContext()
        )

        assert preferred.fix.new_value == "102"

    def test_conflicting_batch_writes_are_denied(self) -> None:
        first = _fix(column="amount", new_value="102")
        second = _fix(column="amount", new_value="103")

        result = SafetyFilter().evaluate_batch([first, second])

        assert result.verdict == SafetyVerdict.DENY
        assert "NO_CONFLICTING_CELL_WRITES" in result.rule_ids
