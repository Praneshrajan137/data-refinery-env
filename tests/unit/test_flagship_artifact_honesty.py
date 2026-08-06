"""The flagship artifact must never overclaim.

This run was pre-registered to expect a NULL: certification at
`alpha = delta = 0.05` needs >= 59 all-correct accepted samples, and the budget buys
roughly a quarter of that. The danger with a near-miss is that a *measurement* gets
read as a *certificate*, so these tests lock the properties that keep the two apart:

* certified auto-apply is only ever claimed when a threshold is actually reachable
  (<= 1.0), and the release gate independently refuses to enable it without
  committed passing evidence;
* the artifact is mode-keyed, so structured-mode maps can never be confused with (or
  overwrite) the free-text `corrector_calibration.json` fit on a different
  distribution;
* nothing is pooled across seeds -- pooled certification is sound only for a null;
* the recorded deviation from the pre-registration is present and visible.

All tests read committed artifacts only. No provider is contacted.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

_RESULTS = Path(__file__).resolve().parents[2] / "eval" / "results"
_ARTIFACT = _RESULTS / "corrector_calibration_structured.json"
_FREETEXT_ARTIFACT = _RESULTS / "corrector_calibration.json"


@pytest.fixture(scope="module")
def flagship() -> dict[str, Any]:
    """Return the committed structured-mode flagship artifact."""
    if not _ARTIFACT.exists():
        pytest.skip("flagship artifact not committed")
    payload = json.loads(_ARTIFACT.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


class TestProvenanceAndScope:
    """The artifact says what it is, what it ran, and how it deviated."""

    def test_schema_and_arm_are_recorded(self, flagship: dict[str, Any]) -> None:
        assert flagship["schema"] == "dataforge_corrector_calibration_structured_v1"
        assert flagship["arm"] == "B_structured_k9"
        assert flagship["k"] == 9

    def test_preregistration_and_deviation_are_linked(self, flagship: dict[str, Any]) -> None:
        assert flagship["preregistration"].endswith("api_phase_certification.md")
        # The override of the pre-registered tie-break must travel with the result.
        assert "AMENDMENT 2" in flagship["deviation"]

    def test_is_not_pooled_across_seeds(self, flagship: dict[str, Any]) -> None:
        # Pooling across seeds assumes exchangeability and is only sound for a null.
        assert flagship["conformal"]["pooled_across_seeds"] is False

    def test_conformal_parameters_match_the_preregistration(self, flagship: dict[str, Any]) -> None:
        conformal = flagship["conformal"]
        assert conformal["alpha"] == 0.05
        assert conformal["delta"] == 0.05
        assert conformal["min_support"] == 30
        assert conformal["calib_fraction"] == 0.5
        assert conformal["split_seed"] == 20260804

    def test_does_not_collide_with_the_freetext_artifact(self) -> None:
        # Structured and free-text confidences are different distributions; sharing
        # a calibration artifact would silently apply the wrong map.
        assert _ARTIFACT.name != _FREETEXT_ARTIFACT.name
        if _FREETEXT_ARTIFACT.exists():
            freetext = json.loads(_FREETEXT_ARTIFACT.read_text(encoding="utf-8"))
            assert freetext.get("schema") != "dataforge_corrector_calibration_structured_v1"


class TestNoOverclaim:
    """A measurement is not a certificate."""

    def test_certified_flag_is_consistent_with_the_thresholds(
        self, flagship: dict[str, Any]
    ) -> None:
        analysis = flagship["analysis"]
        if analysis.get("status") != "analysed":
            pytest.skip("no samples analysed")
        reachable = [
            issue_type
            for issue_type, data in analysis["by_issue_type"].items()
            if data["threshold_is_reachable"]
        ]
        # `certified` may only be true when some threshold is actually reachable.
        assert flagship["certified"] == bool(reachable)

    def test_unreachable_thresholds_carry_an_honest_reason(self, flagship: dict[str, Any]) -> None:
        analysis = flagship["analysis"]
        if analysis.get("status") != "analysed":
            pytest.skip("no samples analysed")
        for issue_type, data in analysis["by_issue_type"].items():
            if not data["threshold_is_reachable"]:
                reason = data["uncertified_reason"]
                assert reason, f"{issue_type} is uncertified with no recorded reason"
                # The distinction that makes a null informative.
                assert "insufficient_support" in reason or "precision_below_target" in reason, (
                    f"{issue_type}: unrecognised reason {reason!r}"
                )

    def test_clean_slice_is_reported_against_the_certification_requirement(
        self, flagship: dict[str, Any]
    ) -> None:
        analysis = flagship["analysis"]
        if analysis.get("status") != "analysed":
            pytest.skip("no samples analysed")
        needed = analysis["samples_needed_to_certify"]
        # min_samples_for_certification(0.05, 0.05) == 59.
        assert needed == 59
        slice_info = analysis["largest_all_correct_slice"]
        # Reporting the clean slice next to the requirement is what stops "the top
        # N were perfect" from being mistaken for a guarantee.
        assert "n" in slice_info

    def test_release_gate_still_refuses_auto_apply(self) -> None:
        from dataforge.release.corrector_gate import check_corrector_release_gate

        # Independent of anything this artifact says, the gate must not have been
        # unlocked: no corrector class may be promoted without committed evidence.
        result = check_corrector_release_gate()
        assert result.passed is True
        assert result.enabled_classes == []

    def test_policy_thresholds_are_all_disabled(self, flagship: dict[str, Any]) -> None:
        analysis = flagship["analysis"]
        if analysis.get("status") != "analysed":
            pytest.skip("no samples analysed")
        policy = analysis["policy"]
        for issue_type, threshold in policy.get("auto_apply_thresholds", {}).items():
            assert threshold > 1.0, (
                f"{issue_type} has a reachable threshold {threshold}; enabling "
                "auto-apply requires committed promotion evidence and an explicit "
                "maintainer decision, not a benchmark artifact."
            )
        assert policy["default_threshold"] > 1.0


class TestEceIsMeasuredOnHeldOutData:
    """Calibration quality reported on the data it was fit to measures nothing."""

    def test_ece_is_reported_before_and_after_on_the_test_half(
        self, flagship: dict[str, Any]
    ) -> None:
        analysis = flagship["analysis"]
        if analysis.get("status") != "analysed":
            pytest.skip("no samples analysed")
        assert "ece_test_before" in analysis
        assert "ece_test_after" in analysis


_SWEEP_ARTIFACT = _RESULTS / "corrector_arm_sweep.json"
_TRIAGE_ARTIFACT = _RESULTS / "triage_scorer_comparison.json"
_LEDGER = _RESULTS / "spend_ledger.json"
_DOCS = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def sweep() -> dict[str, Any]:
    """Return the committed arm-sweep artifact, which carries the load-bearing claim."""
    if not _SWEEP_ARTIFACT.exists():
        pytest.skip("sweep artifact not committed")
    payload = json.loads(_SWEEP_ARTIFACT.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


class TestCentralClaimRestsOnDiscrimination:
    """The free-text-vs-structured claim must rest on ROC-AUC, not on ECE.

    ECE is a weighted mean of ``|mean_confidence - accuracy|``, so at low accuracy any
    uniformly-lower score improves it with zero gain in ordering. The first writeup of
    this phase cited ECE 0.68 -> 0.46 as evidence of value; that was withdrawn. These
    tests make the withdrawal structural rather than editorial.
    """

    def test_every_arm_reports_roc_auc_and_a_confidence_interval(
        self, sweep: dict[str, Any]
    ) -> None:
        for name, arm in sweep["arms"].items():
            assert arm.get("roc_auc") is not None, f"{name} has no roc_auc"
            lo, hi = arm["roc_auc_ci95"]
            assert lo is not None and hi is not None, f"{name} has no CI"
            assert 0.0 <= lo <= arm["roc_auc"] <= hi <= 1.0, f"{name} CI does not bracket"

    def test_ece_is_labelled_as_secondary_and_not_evidence(self, sweep: dict[str, Any]) -> None:
        for name, arm in sweep["arms"].items():
            assert "ece_secondary_not_evidence" in arm, (
                f"{name} must carry ECE under a name that forbids citing it as evidence"
            )

    def test_freetext_confidence_has_no_usable_signal(self, sweep: dict[str, Any]) -> None:
        """Free-text's CI must include chance -- that is the actual finding."""
        arm = sweep["arms"]["A_freetext_k3"]
        lo, _hi = arm["roc_auc_ci95"]
        assert lo <= 0.52, f"free-text CI lower bound {lo} no longer includes chance"

    def test_structured_separates_from_freetext(self, sweep: dict[str, Any]) -> None:
        """The structured arm's CI must not overlap free-text's, or the claim is dead."""
        free_hi = sweep["arms"]["A_freetext_k3"]["roc_auc_ci95"][1]
        struct_lo = sweep["arms"]["B_structured_k9"]["roc_auc_ci95"][0]
        assert struct_lo > free_hi, (
            f"structured CI [{struct_lo}, ..] overlaps free-text [.., {free_hi}]"
        )


class TestPrecisionGradientRefutesTheSampleSizeStory:
    """More data would most likely produce a firmer NO, not a certificate.

    The retracted claim was that only accepted-set *sample size* stood in the way. The
    precision gradient refutes it: the top tier's precision level is itself below the
    bar, so this test fails if anyone reinstates the sample-size framing by finding a
    large all-correct slice that does not exist.
    """

    def test_no_slice_large_enough_to_certify_is_all_correct(self, sweep: dict[str, Any]) -> None:
        from dataforge.conformal import min_samples_for_certification

        needed = min_samples_for_certification(0.05, 0.05)
        for name, arm in sweep["arms"].items():
            pairs = sorted(
                (
                    (float(c), bool(ok))
                    for pairs_ in arm["samples_by_type"].values()
                    for c, ok in pairs_
                ),
                key=lambda pair: -pair[0],
            )
            run = 0
            for _conf, ok in pairs:
                if not ok:
                    break
                run += 1
            assert run < needed, (
                f"{name} has an all-correct prefix of {run} >= {needed}; "
                "certification may now be reachable and the recorded NULL must be revisited"
            )

    def test_top_tier_precision_is_below_the_auto_apply_bar(self, sweep: dict[str, Any]) -> None:
        """A ~0.80 top-decile precision is the real blocker, and must stay recorded."""
        arm = sweep["arms"]["B_structured_k9"]
        pairs = [
            (float(c), bool(ok)) for pairs_ in arm["samples_by_type"].values() for c, ok in pairs_
        ]
        pairs.sort(key=lambda pair: -pair[0])
        top10 = pairs[:10]
        precision = sum(1 for _c, ok in top10 if ok) / len(top10)
        assert precision < 0.95, (
            f"top-10 precision {precision} now clears the 0.95 bar; the NULL needs revisiting"
        )


class TestLedgerReportsWhatItActuallyMeasured:
    """A ledger total that is mostly reconstruction must never be presented as fact."""

    def test_summary_splits_measured_from_reconstructed(self) -> None:
        from dataforge.spend import ledger_summary

        if not _LEDGER.exists():
            pytest.skip("ledger not committed")
        summary = ledger_summary(_LEDGER)
        assert summary.measured_receipts > 0
        assert summary.estimated_receipts > 0, (
            "this phase's ledger contains reconstructions; if that changed, update the docs"
        )
        assert summary.total_usd == pytest.approx(
            summary.measured_usd + summary.estimated_usd, abs=1e-6
        )

    def test_zero_call_receipts_are_counted_as_reconstructions(self) -> None:
        """The split must be derived from missing token counts, not a manual flag.

        Scoped to receipts that claim money: a zero-call receipt with zero USD made no
        billable call and is a no-op, not an estimate. Counting those as reconstructions
        would overstate how much of the ledger is unverified.
        """
        from dataforge.spend import ledger_summary, load_ledger

        if not _LEDGER.exists():
            pytest.skip("ledger not committed")
        claimed_without_measurement = [
            r
            for r in load_ledger(_LEDGER)
            if not r.get("calls") and float(r.get("estimated_usd") or 0.0) > 0.0
        ]
        assert ledger_summary(_LEDGER).estimated_receipts == len(claimed_without_measurement)

    def test_every_reconstruction_states_its_method(self) -> None:
        """A receipt claiming spend it did not measure must say how it was derived.

        Scoped to receipts that actually claim money: a zero-call receipt with zero USD is
        a no-op run (every request rejected, say) and has no method to state.
        """
        from dataforge.spend import load_ledger

        if not _LEDGER.exists():
            pytest.skip("ledger not committed")
        for receipt in load_ledger(_LEDGER):
            if receipt.get("calls"):
                continue
            if not float(receipt.get("estimated_usd") or 0.0) > 0.0:
                continue
            notes = " ".join(receipt.get("notes") or ()).upper()
            assert "RECONSTRUCT" in notes or "ESTIMAT" in notes, (
                f"receipt {receipt.get('run_id')} claims spend with no token counts but "
                "does not say it is a reconstruction"
            )

    def test_zero_call_zero_dollar_receipts_are_not_counted_as_estimates(self) -> None:
        """A run that made no billable call must not inflate the reconstructed share."""
        from dataforge.spend import ledger_summary, load_ledger

        if not _LEDGER.exists():
            pytest.skip("ledger not committed")
        noop = [
            r
            for r in load_ledger(_LEDGER)
            if not r.get("calls") and not float(r.get("estimated_usd") or 0.0) > 0.0
        ]
        assert ledger_summary(_LEDGER).noop_receipts == len(noop)


class TestTriageComparisonStaysHonest:
    """The corrector-vs-ranker comparison must keep its own caveats attached."""

    @pytest.fixture(scope="class")
    def triage(self) -> dict[str, Any]:
        if not _TRIAGE_ARTIFACT.exists():
            pytest.skip("triage comparison artifact not committed")
        payload = json.loads(_TRIAGE_ARTIFACT.read_text(encoding="utf-8"))
        assert isinstance(payload, dict)
        return payload

    def test_both_scorers_saw_the_same_cells(self, triage: dict[str, Any]) -> None:
        """Matched population is the entire point; unequal n means invalid again."""
        counts = {s["n"] for s in triage["scorers"] if s.get("n")}
        assert len(counts) == 1, f"scorers saw different cell counts: {counts}"

    def test_enriched_samples_suppress_precision_at_k(self, triage: dict[str, Any]) -> None:
        """ROC-AUC survives class-balance changes; precision@k does not."""
        if not triage.get("enriched"):
            pytest.skip("artifact is a natural-base-rate run")
        for scorer in triage["scorers"]:
            assert "precision_at_top_10pct" not in scorer, (
                f"{scorer['scorer']} reports precision@k from an enriched sample"
            )
            assert "precision_at_k_suppressed" in scorer

    def test_no_equivalence_is_claimed_when_the_delta_straddles_zero(
        self, triage: dict[str, Any]
    ) -> None:
        """A CI containing zero is a failure to detect a difference, not equivalence."""
        lo, hi = triage["paired_auc_delta_ci95"]
        if lo is None:
            pytest.skip("delta CI undefined at this n")
        if lo < 0 < hi:
            assert triage["abstention_handling"], "abstention handling must stay documented"


class TestRetractionsAreVisibleNotOverwritten:
    """A corrected record must show that it was corrected."""

    @pytest.mark.parametrize(
        "relative",
        ["DECISIONS.md", "docs/STRATEGY.md", "eval/preregistration/api_phase_certification.md"],
    )
    def test_document_marks_its_retractions(self, relative: str) -> None:
        text = (_DOCS / relative).read_text(encoding="utf-8")
        assert "RETRACT" in text.upper(), f"{relative} corrects claims without marking them"

    def test_no_document_still_asserts_the_withdrawn_sample_size_story(self) -> None:
        """The withdrawn framing may only appear inside a retraction, never as a claim."""
        for relative in (
            "DECISIONS.md",
            "docs/STRATEGY.md",
            "eval/preregistration/api_phase_certification.md",
        ):
            text = (_DOCS / relative).read_text(encoding="utf-8")
            for line in text.splitlines():
                lowered = line.lower()
                if "binding constraint" not in lowered:
                    continue
                if "sample size" in lowered or "accepted-set" in lowered:
                    assert any(
                        marker in lowered
                        for marker in ("retract", "no longer", "was wrong", "withdraw")
                    ), f"{relative} still asserts the withdrawn framing: {line.strip()}"


class TestClaimScopeMatchesEvidenceScope:
    """A claim can be arithmetically correct about its sample and false about the world.

    The guards above check artifact *fields* and *numbers*. None of them caught a
    redundancy conclusion generalised from one dataset while a committed artifact in the
    same session recorded the LLM ranker at chance on another. Scope is the axis that was
    unguarded, so it is guarded here.
    """

    _PROBE = _RESULTS / "review_gate_probe.json"

    @pytest.fixture(scope="class")
    def probe(self) -> dict[str, Any]:
        if not self._PROBE.exists():
            pytest.skip("review gate probe artifact not committed")
        payload = json.loads(self._PROBE.read_text(encoding="utf-8"))
        assert isinstance(payload, dict)
        return payload

    def test_the_disconfirming_dataset_is_still_recorded(self, probe: dict[str, Any]) -> None:
        """flights must stay in the record: it is the counterexample to redundancy."""
        findings = probe["findings"]
        assert "flights" in findings, "the dataset that refutes the general claim was removed"
        flights_auc = findings["flights"]["measured_outcome"]["llm_roc_auc"]
        assert flights_auc < 0.6, (
            f"flights LLM ROC-AUC is now {flights_auc}; if the ranker stopped being at "
            "chance there, the retraction in DECISIONS.md must be revisited"
        )

    def test_triage_artifact_is_single_dataset(self) -> None:
        """The comparison covers one dataset; that is why its claim must be scoped."""
        if not _TRIAGE_ARTIFACT.exists():
            pytest.skip("triage comparison artifact not committed")
        payload = json.loads(_TRIAGE_ARTIFACT.read_text(encoding="utf-8"))
        assert isinstance(payload.get("dataset"), str), "artifact must name its dataset"

    @pytest.mark.parametrize(
        "relative",
        [
            "DECISIONS.md",
            "docs/STRATEGY.md",
            "eval/preregistration/api_phase_certification.md",
        ],
    )
    def test_no_unscoped_redundancy_claim(self, relative: str) -> None:
        """Feature-redundancy claims must name their dataset or sit in a retraction.

        Matched narrowly on purpose: "redundancy" also legitimately describes *data*
        properties (entity consensus exploits cross-row redundancy), a different claim
        entirely that must not trip this guard.
        """
        pattern = re.compile(
            r"redundant as|redundancy question|substantially redundant|"
            r"features.{0,30}redundan|redundan.{0,30}(?:ranker|scorer)",
            re.IGNORECASE,
        )
        lines = (_DOCS / relative).read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            if not pattern.search(line):
                continue
            window = "\n".join(lines[max(0, index - 4) : index + 5]).lower()
            assert any(
                marker in window
                for marker in ("hospital", "retract", "withdraw", "single dataset", "one dataset")
            ), (
                f"{relative}:{index + 1} claims scorer redundancy without naming its "
                f"dataset or marking a retraction: {line.strip()}"
            )

    @pytest.mark.parametrize(
        "relative",
        [
            "DECISIONS.md",
            "docs/STRATEGY.md",
            "eval/preregistration/api_phase_certification.md",
        ],
    )
    def test_the_runtime_regime_problem_is_disclosed(self, relative: str) -> None:
        """The binding constraint must not be buried in an artifact string again.

        Whether confidence discriminates depends on correlation with correctness, which
        needs ground truth unavailable at runtime. Confidence dispersion was tested as a
        proxy and refuted on rayyan. Any document reporting the triage result must say so.
        """
        text = (_DOCS / relative).read_text(encoding="utf-8").lower()
        if "review_gate_probe" not in text and "triage" not in text:
            pytest.skip(f"{relative} does not report the triage result")
        assert "runtime" in text, (
            f"{relative} reports triage without disclosing that its value cannot be "
            "predicted at runtime"
        )


class TestDetectorRegimeScope:
    """Precision claims must name their detector regime.

    The queue that justified the paid triager exists only under inferred FD constraints,
    and only on hospital. Stating its precision as a property of "the detector queue" was
    the third scope error of this phase; this guard makes the fourth impossible.
    """

    _COMPOSITION = _RESULTS / "detector_queue_composition.json"

    @pytest.fixture(scope="class")
    def composition(self) -> dict[str, Any]:
        if not self._COMPOSITION.exists():
            pytest.skip("detector queue composition artifact not committed")
        payload = json.loads(self._COMPOSITION.read_text(encoding="utf-8"))
        assert isinstance(payload, dict)
        return payload

    def test_both_regimes_are_measured(self, composition: dict[str, Any]) -> None:
        for name, entry in composition["datasets"].items():
            assert set(entry["regimes"]) == {"default", "inferred_constraints"}, (
                f"{name} must be measured in both regimes or the scope cannot be checked"
            )

    def test_the_default_regime_is_not_flooded(self, composition: dict[str, Any]) -> None:
        """If the shipped default were itself flooded, the retraction would be wrong."""
        for name, entry in composition["datasets"].items():
            precision = entry["regimes"]["default"]["precision"]
            assert precision > 0.2, (
                f"{name} default-regime precision is {precision}; the claim that review is "
                "already efficient without inferred constraints must be revisited"
            )

    def test_inferred_constraints_flood_only_hospital(self, composition: dict[str, Any]) -> None:
        """The hospital-only nature of the flooding is what makes it a config choice."""
        for name in ("flights", "rayyan"):
            regimes = composition["datasets"][name]["regimes"]
            assert (
                regimes["default"]["flagged_cells"]
                == regimes["inferred_constraints"]["flagged_cells"]
            ), f"{name} now differs between regimes; the hospital-only finding is stale"
        hospital = composition["datasets"]["hospital"]["regimes"]
        assert (
            hospital["inferred_constraints"]["flagged_cells"]
            > 5 * hospital["default"]["flagged_cells"]
        ), "hospital no longer floods under inferred constraints; re-scope the docs"

    def test_ground_truth_completeness_is_recorded(self, composition: dict[str, Any]) -> None:
        """The precision numbers are only interpretable alongside this statement."""
        note = composition["ground_truth_completeness"].upper()
        assert "COMPLETE BY CONSTRUCTION" in note

    @pytest.mark.parametrize(
        "relative",
        [
            "DECISIONS.md",
            "docs/STRATEGY.md",
            "eval/preregistration/api_phase_certification.md",
        ],
    )
    def test_low_precision_claims_name_their_regime(self, relative: str) -> None:
        """A 4.4%-style precision figure may only appear with its regime named."""
        pattern = re.compile(r"4\.4%|4\.5%|95% of flagged|95\.6%", re.IGNORECASE)
        lines = (_DOCS / relative).read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            if not pattern.search(line):
                continue
            window = "\n".join(lines[max(0, index - 4) : index + 6]).lower()
            assert any(
                marker in window
                for marker in ("inferred", "regime", "retract", "re-scope", "re-scoped")
            ), (
                f"{relative}:{index + 1} states a flooded-queue precision without naming "
                f"the inferred-constraint regime: {line.strip()}"
            )


class TestFreeRankerTransferFinding:
    """The free ranker's value claim must stay bounded by its transfer failure.

    In-sample it is near-perfect; out-of-sample it is anti-correlated on rayyan. Quoting
    the in-sample number as achievable performance would be the exact error this phase
    keeps making, so the artifact must keep both and the docs must not promote the free
    ranker to a default.
    """

    _FREE = _RESULTS / "free_vs_llm_ranker.json"

    @pytest.fixture(scope="class")
    def free(self) -> dict[str, Any]:
        if not self._FREE.exists():
            pytest.skip("free ranker artifact not committed")
        payload = json.loads(self._FREE.read_text(encoding="utf-8"))
        assert isinstance(payload, dict)
        return payload

    def test_transfer_and_in_sample_are_both_reported(self, free: dict[str, Any]) -> None:
        """Reporting only one of the two is how a transfer gap gets hidden."""
        for regime, block in free["regimes"].items():
            for name, entry in block["leave_one_dataset_out"].items():
                if entry.get("skipped"):
                    continue
                assert "free_transfer_roc_auc" in entry, f"{regime}/{name} missing transfer"
                assert "free_in_sample_roc_auc" in entry, f"{regime}/{name} missing in-sample"
                assert "transfer_gap" in entry, f"{regime}/{name} missing the gap itself"

    def test_the_in_sample_number_is_labelled_unusable(self, free: dict[str, Any]) -> None:
        method = free["method"].lower()
        assert "never be quoted" in method or "must never" in method, (
            "the in-sample score must carry an explicit warning against quoting it"
        )

    def test_transfer_still_fails_somewhere(self, free: dict[str, Any]) -> None:
        """If transfer stopped failing, the decision to keep paying must be revisited."""
        transfers = [
            entry["free_transfer_roc_auc"]
            for block in free["regimes"].values()
            for entry in block["leave_one_dataset_out"].values()
            if not entry.get("skipped")
        ]
        assert min(transfers) < 0.6, (
            f"minimum free-ranker transfer AUC is now {min(transfers)}; the free ranker may "
            "no longer be unsafe as a default and DECISIONS.md must be revisited"
        )

    def test_llm_comparison_is_marked_unpaired(self, free: dict[str, Any]) -> None:
        """The LLM reference came from a different setup; that must stay disclosed."""
        note = free["llm_reference_note"].lower()
        assert "not strictly comparable" in note or "not a paired" in note


class TestProductMetricIsReported:
    """AUC alone cannot answer "will this save me time?"; effort curves must survive.

    The rayyan curve is the load-bearing one: a ranker whose transfer AUC is 0.27 makes
    review WORSE than no ranking. If that stops being recorded, the decision not to default
    to the free ranker loses its evidence.
    """

    _FREE = _RESULTS / "free_vs_llm_ranker.json"

    @pytest.fixture(scope="class")
    def free(self) -> dict[str, Any]:
        if not self._FREE.exists():
            pytest.skip("free ranker artifact not committed")
        return dict(json.loads(self._FREE.read_text(encoding="utf-8")))

    def test_effort_curves_exist_at_natural_rate(self, free: dict[str, Any]) -> None:
        for regime, block in free["regimes"].items():
            for name, entry in block["leave_one_dataset_out"].items():
                if entry.get("skipped"):
                    continue
                curve = entry["effort_curve_free_transfer"]
                assert curve, f"{regime}/{name} has no effort curve"
                assert all("recall_at_k" in point for point in curve)
                assert entry["unranked_cells_per_true_error"] is not None

    def test_a_harmful_ranking_case_is_still_recorded(self, free: dict[str, Any]) -> None:
        """At least one dataset must show ranking below its own unranked base rate."""
        harmful = []
        for block in free["regimes"].values():
            for name, entry in block["leave_one_dataset_out"].items():
                if entry.get("skipped"):
                    continue
                base = entry["natural_precision"]
                top5 = next(
                    (
                        p
                        for p in entry["effort_curve_free_transfer"]
                        if p["effort_fraction"] == 0.05
                    ),
                    None,
                )
                if top5 is not None and top5["precision_at_k"] < base:
                    harmful.append(name)
        assert harmful, (
            "no dataset now shows the free ranker performing worse than no ranking; the "
            "decision to keep paying for the LLM ranker must be re-examined"
        )


class TestCrossDatasetRankerFinding:
    """Two findings from the paired cross-dataset run must not silently rot.

    The first replaces a retracted single-dataset claim. The second is a refuted hypothesis
    of mine -- feeding detector evidence to the ranker -- that turned out actively harmful,
    and which someone will otherwise re-propose as an obvious improvement.
    """

    _XDS = _RESULTS / "ranker_arms_cross_dataset.json"

    @pytest.fixture(scope="class")
    def xds(self) -> dict[str, Any]:
        if not self._XDS.exists():
            pytest.skip("cross-dataset ranker artifact not committed")
        return dict(json.loads(self._XDS.read_text(encoding="utf-8")))

    def test_all_three_datasets_are_measured(self, xds: dict[str, Any]) -> None:
        """A single-dataset artifact is what produced the retracted claim."""
        assert set(xds["datasets"]) >= {"hospital", "rayyan", "flights"}

    def test_the_run_records_its_model(self, xds: dict[str, Any]) -> None:
        """gpt-5-mini numbers are not comparable to gpt-5.6-sol ones; the model must be named."""
        assert isinstance(xds.get("model"), str) and xds["model"]

    def test_the_run_records_its_detector_regime(self, xds: dict[str, Any]) -> None:
        assert "default" in xds["regime"]

    def test_triage_quality_still_varies_across_datasets(self, xds: dict[str, Any]) -> None:
        """If it stopped varying, the non-generalisation retraction would need revisiting."""
        bare = {
            name: entry["arms"][0]["roc_auc"]
            for name, entry in xds["datasets"].items()
            if entry["arms"][0].get("n")
        }
        assert max(bare.values()) - min(bare.values()) > 0.15, (
            f"ranker AUC no longer varies materially across datasets ({bare}); the "
            "non-generalisation finding must be re-examined"
        )

    def test_detector_evidence_is_still_recorded_as_harmful(self, xds: dict[str, Any]) -> None:
        """The load-bearing negative result: evidence must remain measurably damaging."""
        rayyan = xds["datasets"]["rayyan"]
        bare = rayyan["arms"][0]["roc_auc"]
        with_evidence = rayyan["arms"][1]["roc_auc"]
        assert with_evidence < bare, (
            "detector evidence no longer degrades rayyan ranking; if that is real, the "
            "MEASURED HARMFUL warning on ReviewRanker.rank must be revisited"
        )
        low, high = rayyan["paired_delta_ci95_evidence_minus_bare"]
        assert low is not None and high is not None and high < 0.0, (
            f"the paired delta CI {[low, high]} no longer excludes zero, so the harm is no "
            "longer statistically detectable"
        )

    def test_the_harm_warning_is_at_the_call_site(self) -> None:
        """A reader enabling `evidence=` must meet the evidence before the code."""
        source = (
            Path(__file__).resolve().parents[2] / "dataforge" / "review" / "ranker.py"
        ).read_text(encoding="utf-8")
        assert "MEASURED HARMFUL" in source
        assert "anchoring" in source.lower()
        assert "independent" in source.lower()

    def test_raw_pairs_are_persisted_for_free_reanalysis(self, xds: dict[str, Any]) -> None:
        """The enriched triage artifact saved only summaries and could never be reweighted."""
        for name, entry in xds["datasets"].items():
            if not entry.get("cells_scored"):
                continue
            records = entry.get("records")
            assert isinstance(records, list) and records, f"{name} persisted no raw pairs"
            first = records[0]
            for field in ("label", "score_evidence_free", "score_with_evidence"):
                assert field in first, f"{name} records are missing {field}"
