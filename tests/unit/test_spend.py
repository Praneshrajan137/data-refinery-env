"""Tests for the accountable-spend layer (``dataforge.spend``).

These are the regression locks for the three independent guards: the pre-flight
USD estimate, the in-flight hard stop, and the after-the-fact receipt ledger.
Every test is offline -- no provider is contacted.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.spend import (
    PRICES,
    CostCapExceededError,
    ModelPrice,
    SpendMeter,
    append_receipt,
    cap_from_env,
    estimate_usd,
    load_ledger,
    meter_from_env,
    price_for,
    total_estimated_usd,
)

_PRICE = ModelPrice(usd_per_1k_input=0.005, usd_per_1k_output=0.015)


class TestModelPrice:
    """Per-call cost arithmetic."""

    def test_usd_for_charges_input_and_output_separately(self) -> None:
        # 1000 prompt tokens at $0.005/1k + 1000 completion at $0.015/1k.
        assert _PRICE.usd_for(1000, 1000) == pytest.approx(0.020)

    def test_usd_for_is_zero_without_tokens(self) -> None:
        assert _PRICE.usd_for(0, 0) == 0.0

    def test_price_for_is_case_insensitive(self) -> None:
        assert price_for("AZURE") == PRICES["azure"]

    def test_unpriced_provider_returns_none(self) -> None:
        # Groq/Cerebras are free-tier: a missing entry disables the USD guard
        # rather than inventing a number.
        assert price_for("groq") is None
        assert price_for("cerebras") is None


class TestEstimateUsd:
    """The pre-flight guard that was missing: prices x planned calls."""

    def test_estimate_multiplies_calls_by_per_call_cost(self) -> None:
        estimate = estimate_usd(
            calls=1000,
            avg_prompt_tokens=350,
            avg_completion_tokens=100,
            price=_PRICE,
        )
        # 350/1k*0.005 + 100/1k*0.015 = 0.00325 per call.
        assert estimate == pytest.approx(3.25)

    def test_unpriced_provider_yields_no_estimate(self) -> None:
        # No estimate means no refusal: we never fabricate a price to block on.
        assert (
            estimate_usd(
                calls=1000,
                avg_prompt_tokens=350,
                avg_completion_tokens=100,
                price=None,
            )
            is None
        )


class TestSpendMeter:
    """The single in-flight guard that replaced three duplicated copies."""

    def test_records_tokens_and_accumulates_cost(self) -> None:
        meter = SpendMeter(provider="azure", model="m", price=_PRICE)
        meter.record(prompt_tokens=1000, completion_tokens=1000)
        meter.record(prompt_tokens=1000, completion_tokens=1000)
        assert meter.calls == 2
        assert meter.cumulative_usd == pytest.approx(0.040)

    def test_hard_stops_when_cap_exceeded(self) -> None:
        meter = SpendMeter(provider="azure", model="m", price=_PRICE, max_usd=0.05)
        meter.record(prompt_tokens=1000, completion_tokens=1000)  # $0.020
        meter.record(prompt_tokens=1000, completion_tokens=1000)  # $0.040
        with pytest.raises(CostCapExceededError, match="azure spend guard tripped"):
            meter.record(prompt_tokens=1000, completion_tokens=1000)  # $0.060

    def test_cap_is_exclusive_so_landing_exactly_on_it_is_allowed(self) -> None:
        meter = SpendMeter(provider="azure", model="m", price=_PRICE, max_usd=0.020)
        meter.record(prompt_tokens=1000, completion_tokens=1000)
        assert meter.cumulative_usd == pytest.approx(0.020)

    def test_unpriced_meter_counts_but_never_trips(self) -> None:
        # This is the free-tier path: accounting without a USD guard, matching
        # the previous no-op behavior for Groq and Cerebras exactly.
        meter = SpendMeter(provider="groq", model="m", price=None, max_usd=0.0001)
        for _ in range(50):
            meter.record(prompt_tokens=10_000, completion_tokens=10_000)
        assert meter.calls == 50
        assert meter.cumulative_usd == 0.0

    def test_reasoning_tokens_are_recorded_for_visibility(self) -> None:
        meter = SpendMeter(provider="azure", model="m", price=_PRICE)
        meter.record(prompt_tokens=10, completion_tokens=200, reasoning_tokens=180)
        receipt = meter.receipt(run_id="r1")
        assert receipt.reasoning_tokens == 180
        # Reasoning tokens are billed inside completion_tokens, never added twice.
        assert receipt.completion_tokens == 200

    def test_missing_usage_fails_closed_on_a_metered_run(self) -> None:
        # A provider omitting usage would otherwise silently zero the guard and
        # let a capped run overspend undetected.
        meter = SpendMeter(
            provider="azure",
            model="m",
            price=_PRICE,
            max_usd=10.0,
            fail_on_missing_usage=True,
        )
        with pytest.raises(CostCapExceededError, match="no usage payload"):
            meter.record(prompt_tokens=0, completion_tokens=0, usage_present=False)

    def test_missing_usage_is_tolerated_when_not_metered(self) -> None:
        meter = SpendMeter(provider="groq", model="m", price=None)
        meter.record(prompt_tokens=0, completion_tokens=0, usage_present=False)
        assert meter.calls == 1


class TestCapFromEnv:
    """Cap resolution: provider-specific wins, global is the floor."""

    def test_provider_specific_cap_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATAFORGE_AZURE_MAX_USD", "15")
        monkeypatch.setenv("DATAFORGE_MAX_USD", "3")
        assert cap_from_env("azure") == 15.0

    def test_global_cap_applies_when_no_specific_cap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATAFORGE_AZURE_MAX_USD", raising=False)
        monkeypatch.setenv("DATAFORGE_MAX_USD", "7.5")
        assert cap_from_env("azure") == 7.5

    def test_no_cap_configured_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATAFORGE_AZURE_MAX_USD", raising=False)
        monkeypatch.delenv("DATAFORGE_MAX_USD", raising=False)
        assert cap_from_env("azure") is None

    def test_non_positive_disables_the_guard(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The established convention for "run unmetered".
        monkeypatch.setenv("DATAFORGE_AZURE_MAX_USD", "0")
        assert cap_from_env("azure") is None

    def test_malformed_value_falls_through(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATAFORGE_AZURE_MAX_USD", "not-a-number")
        monkeypatch.setenv("DATAFORGE_MAX_USD", "2")
        assert cap_from_env("azure") == 2.0

    def test_meter_from_env_wires_price_and_cap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATAFORGE_AZURE_MAX_USD", "1")
        meter = meter_from_env(provider="azure", model="dep")
        assert meter.max_usd == 1.0
        meter.record(prompt_tokens=1000, completion_tokens=0)
        assert meter.cumulative_usd == pytest.approx(0.005)


class TestLedger:
    """Receipts are append-only and auditable."""

    def test_append_creates_ledger_with_schema(self, tmp_path: Path) -> None:
        path = tmp_path / "spend_ledger.json"
        meter = SpendMeter(provider="azure", model="dep", price=_PRICE, max_usd=5.0)
        meter.record(prompt_tokens=1000, completion_tokens=1000)
        append_receipt(path, meter.receipt(run_id="probe", method="llm_corrector"))

        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["schema"] == "dataforge_spend_ledger_v1"
        assert len(payload["receipts"]) == 1
        receipt = payload["receipts"][0]
        assert receipt["run_id"] == "probe"
        assert receipt["provider"] == "azure"
        assert receipt["method"] == "llm_corrector"
        assert receipt["cap_usd"] == 5.0
        assert receipt["estimated_usd"] == pytest.approx(0.020)

    def test_append_is_additive_not_destructive(self, tmp_path: Path) -> None:
        path = tmp_path / "spend_ledger.json"
        for run_id in ("a", "b", "c"):
            meter = SpendMeter(provider="azure", model="dep", price=_PRICE)
            meter.record(prompt_tokens=1000, completion_tokens=0)
            append_receipt(path, meter.receipt(run_id=run_id))
        assert [r["run_id"] for r in load_ledger(path)] == ["a", "b", "c"]

    def test_total_sums_every_receipt(self, tmp_path: Path) -> None:
        path = tmp_path / "spend_ledger.json"
        for _ in range(4):
            meter = SpendMeter(provider="azure", model="dep", price=_PRICE)
            meter.record(prompt_tokens=1000, completion_tokens=0)  # $0.005 each
            append_receipt(path, meter.receipt(run_id="r"))
        assert total_estimated_usd(path) == pytest.approx(0.020)

    def test_absent_ledger_reads_as_empty(self, tmp_path: Path) -> None:
        path = tmp_path / "missing.json"
        assert load_ledger(path) == []
        assert total_estimated_usd(path) == 0.0

    def test_corrupt_ledger_reads_as_empty_rather_than_crashing(self, tmp_path: Path) -> None:
        path = tmp_path / "spend_ledger.json"
        path.write_text("{not json", encoding="utf-8")
        assert load_ledger(path) == []


class TestLedgerSummary:
    """A ledger total that is mostly reconstruction must not read as a measurement.

    The split is derived from ``calls == 0`` rather than a flag, because a receipt with
    no token counts cannot have had its USD observed -- and a flag is something a caller
    can forget to set.
    """

    def test_splits_measured_from_reconstructed(self, tmp_path: Path) -> None:
        from dataforge.spend import ledger_summary

        path = tmp_path / "ledger.json"
        path.write_text(
            json.dumps(
                {
                    "schema": "dataforge_spend_ledger_v1",
                    "receipts": [
                        {"run_id": "a", "calls": 10, "estimated_usd": 4.0},
                        {"run_id": "b", "calls": 0, "estimated_usd": 6.0},
                    ],
                }
            ),
            encoding="utf-8",
        )
        summary = ledger_summary(path)
        assert summary.measured_usd == pytest.approx(4.0)
        assert summary.estimated_usd == pytest.approx(6.0)
        assert summary.measured_receipts == 1
        assert summary.estimated_receipts == 1
        assert summary.total_usd == pytest.approx(10.0)
        assert summary.measured_fraction == pytest.approx(0.4)

    def test_missing_calls_key_counts_as_reconstruction(self, tmp_path: Path) -> None:
        from dataforge.spend import ledger_summary

        path = tmp_path / "ledger.json"
        path.write_text(
            json.dumps(
                {
                    "schema": "dataforge_spend_ledger_v1",
                    "receipts": [{"run_id": "a", "estimated_usd": 2.0}],
                }
            ),
            encoding="utf-8",
        )
        summary = ledger_summary(path)
        assert summary.estimated_receipts == 1
        assert summary.measured_usd == pytest.approx(0.0)

    def test_describe_states_the_measured_fraction(self, tmp_path: Path) -> None:
        from dataforge.spend import ledger_summary

        path = tmp_path / "ledger.json"
        path.write_text(
            json.dumps(
                {
                    "schema": "dataforge_spend_ledger_v1",
                    "receipts": [
                        {"run_id": "a", "calls": 5, "estimated_usd": 1.0},
                        {"run_id": "b", "calls": 0, "estimated_usd": 3.0},
                    ],
                }
            ),
            encoding="utf-8",
        )
        described = ledger_summary(path).describe()
        assert "measured" in described and "reconstructed" in described
        assert "25%" in described

    def test_empty_ledger_is_fully_measured_by_convention(self, tmp_path: Path) -> None:
        """No spend means nothing unverified; the fraction must not divide by zero."""
        from dataforge.spend import ledger_summary

        path = tmp_path / "ledger.json"
        path.write_text(
            json.dumps({"schema": "dataforge_spend_ledger_v1", "receipts": []}),
            encoding="utf-8",
        )
        summary = ledger_summary(path)
        assert summary.total_usd == pytest.approx(0.0)
        assert summary.measured_fraction == pytest.approx(1.0)

    def test_total_matches_total_estimated_usd(self, tmp_path: Path) -> None:
        """The split must not change the headline number, only explain it."""
        from dataforge.spend import ledger_summary, total_estimated_usd

        path = tmp_path / "ledger.json"
        path.write_text(
            json.dumps(
                {
                    "schema": "dataforge_spend_ledger_v1",
                    "receipts": [
                        {"run_id": "a", "calls": 3, "estimated_usd": 1.25},
                        {"run_id": "b", "calls": 0, "estimated_usd": 2.5},
                        {"run_id": "c", "calls": 9, "estimated_usd": 0.75},
                    ],
                }
            ),
            encoding="utf-8",
        )
        assert ledger_summary(path).total_usd == pytest.approx(total_estimated_usd(path))
