"""Tests for the Trust Ledger outcome metric.

The assertions target the three properties that make it honest rather than flattering: net
improvement can be negative, a rate is always paired with a bound, and the scope caveat
cannot be dropped.
"""

from __future__ import annotations

import pytest

from dataforge.metrics import TrustLedger, clopper_pearson_upper


def _ledger(**overrides: object) -> TrustLedger:
    base: dict[str, object] = {
        "cells_applied": 100,
        "corrections": 100,
        "corruptions": 0,
        "cells_held": 50,
        "cells_abstained": 10,
        "real_errors": 120,
        "reversibility_verified": True,
    }
    base.update(overrides)
    return TrustLedger(**base)  # type: ignore[arg-type]


def test_net_improvement_can_be_negative() -> None:
    """The measured NO-GO case must be expressible as a loss, not as progress.

    A repairer that fixed 23 cells and corrupted 25 was rejected on exactly this arithmetic.
    A gross correction count would have called it an improvement.
    """
    ledger = _ledger(cells_applied=48, corrections=23, corruptions=25)
    assert ledger.net_cells_improved == -2
    assert ledger.is_net_positive is False


def test_zero_corruptions_does_not_claim_a_zero_rate() -> None:
    """The point estimate is 0; the bound is not. That gap is the honesty."""
    ledger = _ledger(cells_applied=40, corrections=40, corruptions=0)
    assert ledger.corruption_exposure_per_10k == 0.0
    bound = ledger.corruption_exposure_upper_per_10k()
    assert bound > 0.0, "zero observed failures was reported as a zero rate"
    # With n=40 the 95% bound is around 700 per 10k; assert the order of magnitude rather
    # than an exact value so the test does not pin the arithmetic library.
    assert 100.0 < bound < 2000.0


def test_a_larger_sample_tightens_the_bound() -> None:
    """More evidence must buy a stronger claim, and nothing else should."""
    small = _ledger(cells_applied=40, corrections=40, corruptions=0)
    large = _ledger(cells_applied=4000, corrections=4000, corruptions=0)
    assert large.corruption_exposure_upper_per_10k() < small.corruption_exposure_upper_per_10k()


def test_review_effort_is_none_when_nothing_was_surfaced() -> None:
    """Absent is not zero: a run that surfaced nothing has no effort ratio to report."""
    ledger = _ledger(cells_applied=0, corrections=0, corruptions=0, cells_held=0)
    assert ledger.review_effort_per_real_error is None
    assert ledger.as_dict()["review_effort_per_real_error"] is None


def test_review_effort_matches_the_documented_exchange_rate() -> None:
    """549 cells surfaced for 308 real errors is the measured hospital default: 1.78."""
    ledger = _ledger(cells_applied=0, corrections=0, corruptions=0, cells_held=549, real_errors=308)
    effort = ledger.review_effort_per_real_error
    assert effort is not None
    assert effort == pytest.approx(1.7825, abs=0.001)


def test_serialised_form_always_carries_the_bound_and_the_scope() -> None:
    """A consumer must not be able to read the rate without its bound or its scope."""
    payload = _ledger().as_dict()
    assert "corruption_exposure_upper_95_per_10k" in payload
    assert "scope" in payload
    scope = payload["scope"]
    assert isinstance(scope, str)
    assert "do not predict behaviour on a different table" in scope
    assert "require ground truth" in scope


def test_a_ledger_that_does_not_balance_is_rejected() -> None:
    """Refuse to construct nonsense rather than reporting it."""
    with pytest.raises(ValueError, match="cannot exceed cells_applied"):
        TrustLedger(
            cells_applied=1,
            corrections=1,
            corruptions=1,
            cells_held=0,
            cells_abstained=0,
            real_errors=1,
            reversibility_verified=True,
        )


def test_negative_counts_are_rejected() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        TrustLedger(
            cells_applied=-1,
            corrections=0,
            corruptions=0,
            cells_held=0,
            cells_abstained=0,
            real_errors=0,
            reversibility_verified=True,
        )


def test_summary_leads_with_net_improvement() -> None:
    """Ordering is a claim: the first line a reader sees should be the outcome."""
    lines = _ledger(cells_applied=48, corrections=23, corruptions=25).summary_lines()
    assert lines[0].startswith("net cells improved:")
    assert "-2" in lines[0]


class TestClopperPearson:
    def test_zero_failures_closed_form(self) -> None:
        # 1 - 0.05 ** (1/10) for n=10.
        assert clopper_pearson_upper(0, 10) == pytest.approx(0.2589, abs=0.001)

    def test_known_value(self) -> None:
        # 1 failure in 10 trials: the exact 95% one-sided upper bound is ~0.3941.
        assert clopper_pearson_upper(1, 10) == pytest.approx(0.3941, abs=0.002)

    def test_all_failures_is_certain(self) -> None:
        assert clopper_pearson_upper(10, 10) == 1.0

    def test_no_trials_claims_nothing(self) -> None:
        """With no evidence the bound must be 1.0, not 0.0.

        This is the ``agent_fix_count: 0`` case: a run with an empty denominator establishes
        nothing, and must not be reportable as a clean result.
        """
        assert clopper_pearson_upper(0, 0) == 1.0

    def test_bound_is_monotonic_in_sample_size(self) -> None:
        bounds = [clopper_pearson_upper(0, n) for n in (10, 100, 1000, 10000)]
        assert bounds == sorted(bounds, reverse=True)
