"""Tests for the contamination audit verdict rule.

Offline and arithmetic. No model, no network, no spend. The probe script gathers evidence;
this file pins what the evidence is allowed to mean.

The tests worth reading are the ones asserting the audit **cannot** produce a convenient
answer: an unavailable probe must not become a clean one, a flagged negative control must
void rather than resolve, and a verdict over nothing must raise.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.bench.contamination import (
    ALPHA,
    C2_MIN_DELTA,
    ContaminationAuditError,
    ProbeOutcome,
    binomial_p_value_greater,
    decide_verdict,
    exchangeability_available,
    majority_base_rate,
    paired_signflip_p_value,
)

_CLEAN_CONTROL = ProbeOutcome(
    probe="C4", available=True, p_value=0.9, effect=0.0, detail="synthetic control, no signal"
)


def _probe(name: str, *, p: float | None, effect: float | None = None) -> ProbeOutcome:
    """Build an available probe outcome."""
    return ProbeOutcome(
        probe=name,  # type: ignore[arg-type]
        available=True,
        p_value=p,
        effect=effect,
        detail="fixture",
    )


def _decide(outcomes: dict[str, ProbeOutcome]) -> object:
    """Decide with fixed binding fields."""
    return decide_verdict(outcomes, model="gpt-5.6-sol", seed=0, reference_sha256="a" * 64)


class TestAnUnavailableProbeIsNotACleanOne:
    """L1, made unavoidable. This is the failure the whole module exists to prevent."""

    def test_asking_an_unavailable_probe_for_its_flag_raises(self) -> None:
        missing = ProbeOutcome(
            probe="C1",
            available=False,
            p_value=None,
            effect=None,
            detail="deployment rejected logprobs",
        )
        with pytest.raises(ContaminationAuditError, match="not a clean result"):
            _ = missing.flagged

    def test_an_unavailable_c1_is_excluded_from_the_count_not_counted_as_passing(self) -> None:
        outcomes = {
            "C1": ProbeOutcome(
                probe="C1", available=False, p_value=None, effect=None, detail="no logprobs"
            ),
            "C2": _probe("C2", p=0.0001, effect=0.30),
            "C3": _probe("C3", p=0.0001),
            "C4": _CLEAN_CONTROL,
        }
        verdict = _decide(outcomes)
        # Two of the two probes that RAN flagged, so the kill criterion fires. Had C1 been
        # scored as "did not flag", this would still be CONTAMINATED -- so assert the
        # availability bookkeeping directly too.
        assert verdict.verdict == "CONTAMINATED"  # type: ignore[attr-defined]
        assert verdict.unavailable_probes == ("C1",)  # type: ignore[attr-defined]
        assert verdict.exchangeability_available is False  # type: ignore[attr-defined]

    def test_exchangeability_available_is_recorded_when_c1_runs(self) -> None:
        outcomes = {
            "C1": _probe("C1", p=0.5),
            "C2": _probe("C2", p=0.5),
            "C3": _probe("C3", p=0.5),
            "C4": _CLEAN_CONTROL,
        }
        assert _decide(outcomes).exchangeability_available is True  # type: ignore[attr-defined]


class TestTheNegativeControlDominates:
    """C4 tests the probes, not the model. A firing control voids everything."""

    def test_a_flagged_control_voids_rather_than_resolving(self) -> None:
        outcomes = {
            "C2": _probe("C2", p=0.5),
            "C3": _probe("C3", p=0.5),
            "C4": ProbeOutcome(
                probe="C4",
                available=True,
                p_value=0.0001,
                effect=0.4,
                detail="fired on generated columns",
            ),
        }
        with pytest.raises(ContaminationAuditError, match="VOID"):
            _decide(outcomes)

    def test_a_flagged_control_voids_even_when_no_probe_flagged(self) -> None:
        """VOID is evaluated before the count, so a clean count cannot rescue it."""
        outcomes = {
            "C1": _probe("C1", p=0.99),
            "C2": _probe("C2", p=0.99),
            "C3": _probe("C3", p=0.99),
            "C4": ProbeOutcome(
                probe="C4", available=True, p_value=0.001, effect=0.9, detail="fired"
            ),
        }
        with pytest.raises(ContaminationAuditError, match="own prompt design"):
            _decide(outcomes)

    def test_an_absent_control_raises(self) -> None:
        with pytest.raises(ContaminationAuditError, match="negative control"):
            _decide({"C2": _probe("C2", p=0.5), "C3": _probe("C3", p=0.5)})

    def test_an_unavailable_control_raises(self) -> None:
        outcomes = {
            "C2": _probe("C2", p=0.5),
            "C4": ProbeOutcome(
                probe="C4", available=False, p_value=None, effect=None, detail="cap hit"
            ),
        }
        with pytest.raises(ContaminationAuditError, match="cannot be interpreted"):
            _decide(outcomes)


class TestVerdictCounts:
    """The pre-registered kill criterion, as arithmetic."""

    def test_zero_flagged_is_clean(self) -> None:
        outcomes = {
            "C1": _probe("C1", p=0.4),
            "C2": _probe("C2", p=0.4),
            "C3": _probe("C3", p=0.4),
            "C4": _CLEAN_CONTROL,
        }
        verdict = _decide(outcomes)
        assert verdict.verdict == "CLEAN"  # type: ignore[attr-defined]
        assert verdict.contamination_suspected is False  # type: ignore[attr-defined]
        assert verdict.cancels_wild_column_measurement is False  # type: ignore[attr-defined]

    def test_one_flagged_is_suspected_and_proceeds(self) -> None:
        outcomes = {
            "C1": _probe("C1", p=0.4),
            "C2": _probe("C2", p=0.4),
            "C3": _probe("C3", p=0.0001),
            "C4": _CLEAN_CONTROL,
        }
        verdict = _decide(outcomes)
        assert verdict.verdict == "SUSPECTED"  # type: ignore[attr-defined]
        assert verdict.flagged_probes == ("C3",)  # type: ignore[attr-defined]
        assert verdict.contamination_suspected is True  # type: ignore[attr-defined]
        assert verdict.cancels_wild_column_measurement is False  # type: ignore[attr-defined]

    def test_two_flagged_cancels_the_wild_column_measurement(self) -> None:
        outcomes = {
            "C1": _probe("C1", p=0.0001),
            "C2": _probe("C2", p=0.4),
            "C3": _probe("C3", p=0.0001),
            "C4": _CLEAN_CONTROL,
        }
        verdict = _decide(outcomes)
        assert verdict.verdict == "CONTAMINATED"  # type: ignore[attr-defined]
        assert verdict.cancels_wild_column_measurement is True  # type: ignore[attr-defined]

    def test_a_verdict_over_zero_executed_probes_raises(self) -> None:
        """Non-vacuity: CLEAN from nothing is the all_parity failure."""
        outcomes = {
            "C1": ProbeOutcome(
                probe="C1", available=False, p_value=None, effect=None, detail="no logprobs"
            ),
            "C4": _CLEAN_CONTROL,
        }
        with pytest.raises(ContaminationAuditError, match="zero executed probes"):
            _decide(outcomes)

    def test_the_verdict_carries_what_produced_it(self) -> None:
        """P4: a verdict for one corpus may not be reported for another."""
        verdict = _decide({"C3": _probe("C3", p=0.5), "C4": _CLEAN_CONTROL})
        assert verdict.model == "gpt-5.6-sol"  # type: ignore[attr-defined]
        assert verdict.seed == 0  # type: ignore[attr-defined]
        assert verdict.reference_sha256 == "a" * 64  # type: ignore[attr-defined]


class TestC2NeedsBothClauses:
    """Significance alone is prompt-format asymmetry, not memory."""

    def test_significant_but_negligible_delta_does_not_flag(self) -> None:
        probe = _probe("C2", p=0.0001, effect=C2_MIN_DELTA - 0.01)
        assert probe.flagged is False

    def test_significant_and_material_delta_flags(self) -> None:
        assert _probe("C2", p=0.0001, effect=C2_MIN_DELTA).flagged is True

    def test_material_but_insignificant_delta_does_not_flag(self) -> None:
        assert _probe("C2", p=0.5, effect=0.40).flagged is False

    def test_a_missing_effect_size_cannot_flag_c2(self) -> None:
        assert _probe("C2", p=0.0001, effect=None).flagged is False

    def test_other_probes_need_only_significance(self) -> None:
        assert _probe("C3", p=ALPHA / 2, effect=None).flagged is True


class TestBaseRate:
    """C3 compares against majority-class guessing, never 0.5."""

    def test_rt_bench_marginal_split(self) -> None:
        assert round(majority_base_rate(41, 35), 4) == 0.5395

    def test_st_bench_marginal_split_is_far_from_one_half(self) -> None:
        """Testing against 0.5 here would manufacture a finding."""
        assert round(majority_base_rate(47, 77), 4) == 0.6210

    def test_a_base_rate_over_zero_items_raises(self) -> None:
        with pytest.raises(ContaminationAuditError, match="undefined"):
            majority_base_rate(0, 0)


class TestExchangeabilityAvailability:
    """Whether the provable method can run. Fail-closed on every uncertain path."""

    def test_a_rejecting_deployment_is_unavailable(self, tmp_path: Path) -> None:
        artifact = tmp_path / "azure_capability_probe.json"
        artifact.write_text(
            json.dumps(
                {
                    "model": "gpt-5.6-sol",
                    "probes": {
                        "logprobs": {"accepted": False, "error_kind": "http_400"},
                    },
                }
            ),
            encoding="utf-8",
        )
        available, reason = exchangeability_available(artifact)
        assert available is False
        assert "gpt-5.6-sol" in reason
        assert "not as a clean result" in reason

    def test_an_absent_artifact_is_unavailable_not_assumed_present(self, tmp_path: Path) -> None:
        available, reason = exchangeability_available(tmp_path / "nope.json")
        assert available is False
        assert "absent" in reason

    def test_a_corrupt_artifact_is_unavailable(self, tmp_path: Path) -> None:
        artifact = tmp_path / "bad.json"
        artifact.write_text("{not json", encoding="utf-8")
        available, reason = exchangeability_available(artifact)
        assert available is False
        assert "unreadable" in reason

    def test_an_artifact_with_no_logprobs_probe_is_unavailable(self, tmp_path: Path) -> None:
        artifact = tmp_path / "partial.json"
        artifact.write_text(json.dumps({"probes": {"baseline": {"accepted": True}}}), "utf-8")
        available, reason = exchangeability_available(artifact)
        assert available is False
        assert "no logprobs probe" in reason

    def test_an_accepting_deployment_is_available_but_carries_a_caveat(
        self, tmp_path: Path
    ) -> None:
        """Accepting the parameter is necessary, not sufficient: chat logprobs are generative."""
        artifact = tmp_path / "ok.json"
        artifact.write_text(
            json.dumps({"model": "x", "probes": {"logprobs": {"accepted": True}}}), "utf-8"
        )
        available, reason = exchangeability_available(artifact)
        assert available is True
        assert "generated" in reason

    def test_the_committed_artifact_records_c1_as_unavailable(self) -> None:
        """The live measured state on this deployment, as of 2026-08-24."""
        artifact = (
            Path(__file__).resolve().parents[2] / "eval" / "results" / "azure_capability_probe.json"
        )
        if not artifact.exists():
            pytest.skip("capability artifact not present in this checkout")
        available, reason = exchangeability_available(artifact)
        assert available is False, (
            "gpt-5.6-sol rejects logprobs, so the contamination audit must run on the two "
            f"behavioural probes only. Reason recorded: {reason}"
        )


class TestPairedSignflipTest:
    """The named paired test. Pre-registered at 20,000 resamples, seed 0."""

    def test_a_large_consistent_positive_shift_is_significant(self) -> None:
        deltas = [0.25] * 30
        assert paired_signflip_p_value(deltas) < ALPHA

    def test_noise_around_zero_is_not_significant(self) -> None:
        deltas = [0.1, -0.1, 0.05, -0.05, 0.2, -0.2, 0.0, 0.15, -0.15, 0.02, -0.02]
        assert paired_signflip_p_value(deltas) > 0.1

    def test_a_negative_shift_is_never_significant_one_sided(self) -> None:
        """The general arm beating the guided arm is not evidence of memorisation."""
        assert paired_signflip_p_value([-0.3] * 25) > 0.9

    def test_it_is_deterministic_across_calls(self) -> None:
        """A Monte Carlo p-value that moves between runs invites re-rolling."""
        deltas = [0.1, 0.2, -0.05, 0.3, 0.0, 0.15]
        first = paired_signflip_p_value(deltas)
        second = paired_signflip_p_value(deltas)
        assert first == second

    def test_it_never_returns_exactly_zero(self) -> None:
        """Add-one correction: a finite resample cannot evidence p == 0."""
        assert paired_signflip_p_value([1.0] * 40) > 0.0

    def test_all_zero_deltas_raise_rather_than_returning_one(self) -> None:
        """Identical arms is a finding, not a p-value."""
        with pytest.raises(ContaminationAuditError, match="identical output"):
            paired_signflip_p_value([0.0, 0.0, 0.0])

    def test_zero_pairs_raise(self) -> None:
        with pytest.raises(ContaminationAuditError, match="zero pairs"):
            paired_signflip_p_value([])

    def test_nonpositive_resamples_raise(self) -> None:
        with pytest.raises(ContaminationAuditError, match="positive"):
            paired_signflip_p_value([0.1, 0.2], resamples=0)

    def test_a_single_large_outlier_does_not_reach_significance(self) -> None:
        """With n=1 the sign-flip null has two points, so p cannot go below 0.5."""
        assert paired_signflip_p_value([0.9]) > ALPHA


class TestCommittedArtifact:
    """The spec binds each limit to an artifact field. A limit only in prose rots.

    These assertions are what make L1-L5 real: a future run that drops a field, or reports a
    verdict inconsistent with its own probe outcomes, fails here rather than being read as
    evidence.
    """

    @pytest.fixture
    def artifact(self) -> dict[str, object]:
        path = Path(__file__).resolve().parents[2] / "eval" / "results" / "contamination_audit.json"
        if not path.exists():
            pytest.skip("contamination audit artifact not present in this checkout")
        return json.loads(path.read_text(encoding="utf-8"))

    def test_the_envelope_carries_provenance_and_limits(self, artifact: dict) -> None:
        for key in (
            "schema_version",
            "generated_at",
            "provenance",
            "limitations",
            "thresholds",
            "status",
            "model",
            "seed",
            "reference",
        ):
            assert key in artifact, f"artifact is missing {key!r}"
        assert artifact["provenance"]["git_commit"]
        assert artifact["thresholds"]["pre_registration"] == (
            "eval/preregistration/contamination_audit.md"
        )

    def test_l1_exchangeability_availability_is_recorded(self, artifact: dict) -> None:
        """The strongest instrument's availability must be a field, not a footnote."""
        assert "exchangeability_available" in artifact
        assert isinstance(artifact["exchangeability_available"], bool)
        assert artifact["exchangeability_reason"]

    def test_an_unavailable_c1_is_never_listed_as_a_flagged_or_passing_probe(
        self, artifact: dict
    ) -> None:
        if artifact["exchangeability_available"]:
            pytest.skip("C1 was available in this run")
        assert "C1" not in artifact.get("flagged_probes", [])
        assert "C1" in artifact.get("unavailable_probes", [])
        assert "C1" not in artifact["methods_implemented"]

    def test_every_spec_limit_appears_in_the_limitations_array(self, artifact: dict) -> None:
        joined = " ".join(str(item) for item in artifact["limitations"])
        for marker in ("L1:", "L2:", "L3:", "L4:", "L5:"):
            assert marker in joined, f"{marker} is missing from limitations"

    def test_the_verdict_is_consistent_with_its_own_flag_count(self, artifact: dict) -> None:
        """The kill criterion, checked against the recorded probes rather than trusted."""
        status = artifact["status"]
        flagged = artifact.get("flagged_probes", [])
        if status == "VOID":
            pytest.skip("a void audit has no flag-count invariant")
        expected = (
            "CLEAN" if not flagged else ("SUSPECTED" if len(flagged) == 1 else "CONTAMINATED")
        )
        assert status == expected, f"status {status} disagrees with flagged {flagged}"
        assert artifact["cancels_wild_column_measurement"] is (status == "CONTAMINATED")
        assert artifact["contamination_suspected"] is (status != "CLEAN")

    def test_the_negative_control_passed_or_the_audit_is_void(self, artifact: dict) -> None:
        """C4 firing means the probes measure their own prompt design."""
        control = artifact["probes"]["C4"]
        if artifact["status"] == "VOID":
            pytest.skip("already void")
        p_value = control.get("p_value")
        assert p_value is not None, "the control must have produced a p-value"
        flagged = (
            p_value < artifact["alpha"]
            and (control.get("mean_delta") or 0) >= (artifact["min_delta"])
        )
        assert not flagged, (
            "C4 flagged but the audit is not VOID: a probe that fires on synthetic content "
            "the model has not seen cannot support any verdict"
        )

    def test_failed_calls_are_reported_not_hidden(self, artifact: dict) -> None:
        assert "failed_calls" in artifact
        assert artifact["calls"] > 0, "a verdict over zero calls is not a measurement"


class TestBinomialPValue:
    """Exact, because the tail that matters is where the approximation is worst."""

    def test_chance_performance_is_not_significant(self) -> None:
        assert binomial_p_value_greater(54, 100, 0.5395) > 0.1

    def test_perfect_recovery_is_significant(self) -> None:
        assert binomial_p_value_greater(100, 100, 0.5395) < ALPHA

    def test_all_outcomes_sums_to_one(self) -> None:
        assert round(binomial_p_value_greater(0, 20, 0.5), 10) == 1.0

    def test_degenerate_inputs_raise(self) -> None:
        with pytest.raises(ContaminationAuditError):
            binomial_p_value_greater(1, 0, 0.5)
        with pytest.raises(ContaminationAuditError):
            binomial_p_value_greater(5, 3, 0.5)
        with pytest.raises(ContaminationAuditError):
            binomial_p_value_greater(1, 3, 1.5)

    def test_it_is_monotone_in_successes(self) -> None:
        values = [binomial_p_value_greater(k, 50, 0.6) for k in range(51)]
        assert values == sorted(values, reverse=True)
