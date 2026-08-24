"""Certification on human labels must bound the labeller, or refuse.

Why this file exists
--------------------
Per-table certification rests on two assumptions, and only one of them was ever enforced.
Exchangeability holds by construction, because the calibration data *is* the table. But the
labels must also be **right**, and a human ratifying a machine's proposed value does not produce
right labels -- they produce labels with an asymmetric error rate. Writing ``beta`` for the
false-accept rate (human calls a wrong repair correct) and ``gamma`` for the false-reject rate::

    p_tilde = p (1 - beta) + (1 - p) gamma        =>        p <= p_tilde / (1 - beta)

taking the conservative ``gamma = 0``. So certifying a measured 0.05 delivers 0.0625 at
``beta = 0.2`` and **0.10 at ``beta = 0.5``**. Automation bias drives the noise in exactly that
direction, because being shown an answer and asked "is this right?" is an acquiescence-biased
task. An unmeasured ``beta`` is therefore not a conservative simplification -- it is the one
direction that silently doubles the advertised error budget.

The tests here pin three things:

1. **Human labels without measured controls REFUSE to certify.** Fail-closed, because a silent bad
   write is unrecoverable while a refusal is not.
2. **The adjustment only ever tightens.** A noise-adjusted run can never certify something the
   unadjusted run rejected.
3. **The scope caveat travels in the artifact.** ``beta`` is estimated on the *plant*
   distribution; if plants are easier to reject than real corrector mistakes it is understated,
   and the bound is still anti-conservative. A guarantee that hides its own scope is the failure
   mode this project has retracted six claims for.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from dataforge.calibration_session import (
    CalibrationSessionArtifact,
    PlantedControl,
    build_calibration_session,
    certify_from_session,
    label_repair_sample,
)
from dataforge.conformal import (
    certify_threshold,
    certify_threshold_under_label_noise,
    label_noise_adjusted_bound,
    min_samples_under_label_noise,
)
from dataforge.detectors.base import Issue, Severity

_SHA = "a" * 64
_GRID = (0.99, 0.98, 0.97, 0.96, 0.95, 0.9, 0.8, 0.7, 0.6)


def _issues(count: int, issue_type: str = "type_mismatch") -> list[Issue]:
    return [
        Issue(
            row=i,
            column="c",
            issue_type=issue_type,
            severity=Severity.REVIEW,
            confidence=0.9,
            actual="x",
            reason="r",
        )
        for i in range(count)
    ]


def _control(row: int, *, accepted: bool | None) -> PlantedControl:
    decision = "pending" if accepted is None else ("correct" if accepted else "error")
    return PlantedControl(
        row=row,
        column="c",
        issue_type="type_mismatch",
        flagged_value="x",
        # Drawn from the column's own values so the plant is as plausible as a real mistake.
        planted_value="plausible-but-wrong",
        withheld_truth="the-real-value",
        origin="column_distribution",
        repair_decision=decision,
    )


def _human_session(
    *,
    labels: int = 120,
    controls: int = 30,
    false_accepts: int = 0,
    all_correct: bool = True,
) -> CalibrationSessionArtifact:
    artifact = build_calibration_session(
        _issues(labels),
        source_path=Path("t.csv"),
        source_sha256=_SHA,
        row_count=labels,
        columns=["c"],
        table_fingerprint="fp",
        fd_detection_source="none",
        per_class=labels,
        label_source="human",
    )
    for index, sample in enumerate(list(artifact.samples)):
        artifact = label_repair_sample(
            artifact,
            row=sample.row,
            column=sample.column,
            decision="correct" if all_correct or index % 5 else "error",
            proposed_repair="v",
            repair_confidence=0.97,
        )
    planted = [_control(10_000 + i, accepted=i < false_accepts) for i in range(controls)]
    return artifact.model_copy(update={"planted_controls": planted})


class TestHumanLabelsFailClosed:
    """The load-bearing safety property."""

    def test_human_labels_without_controls_refuse_to_certify(self) -> None:
        artifact = _human_session(controls=0)
        with pytest.raises(ValueError, match="without labelled planted controls"):
            certify_from_session(artifact)

    def test_pending_controls_do_not_count_as_measured(self) -> None:
        """An unlabelled plant measures nothing, so it must not unlock certification."""
        artifact = _human_session(controls=0)
        unlabelled = [_control(20_000 + i, accepted=None) for i in range(30)]
        artifact = artifact.model_copy(update={"planted_controls": unlabelled})
        with pytest.raises(ValueError, match="without labelled planted controls"):
            certify_from_session(artifact)

    def test_oracle_labels_do_not_require_controls(self) -> None:
        """Retained ground truth has no labeller to bound, and says so in the certificate."""
        artifact = build_calibration_session(
            _issues(120),
            source_path=Path("t.csv"),
            source_sha256=_SHA,
            row_count=120,
            columns=["c"],
            table_fingerprint="fp",
            fd_detection_source="none",
            per_class=120,
            label_source="oracle",
        )
        for sample in list(artifact.samples):
            artifact = label_repair_sample(
                artifact,
                row=sample.row,
                column=sample.column,
                decision="correct",
                proposed_repair="v",
                repair_confidence=0.97,
            )
        result = certify_from_session(artifact)
        assert result.label_source == "oracle"
        assert result.label_noise_adjusted is False
        assert result.beta_upper is None
        assert result.certified_classes == ["type_mismatch"]

    def test_label_source_has_no_default_so_it_cannot_be_forgotten(self) -> None:
        """A default would be wrong either way; requiring it is the fail-closed choice."""
        with pytest.raises(ValidationError, match="label_source"):
            CalibrationSessionArtifact(  # type: ignore[call-arg]
                source_path="t.csv",
                source_sha256=_SHA,
                row_count=1,
                columns=["c"],
                table_fingerprint="fp",
                flagged_cells_total=1,
                fd_detection_source="none",
                seed=1,
            )


class TestTheAdjustmentOnlyTightens:
    """Pruning bought power; the noise term spends it. Net effect must be stricter."""

    def test_adjusted_bound_is_never_below_the_measured_bound(self) -> None:
        for n in (30, 59, 92, 200):
            for errors in (0, 1, 3):
                if errors >= n:
                    continue
                for controls, false_accepts in ((10, 0), (30, 0), (30, 3), (100, 1)):
                    measured, beta, adjusted = label_noise_adjusted_bound(
                        errors, n, false_accepts=false_accepts, controls=controls
                    )
                    assert adjusted >= measured
                    assert 0.0 <= beta <= 1.0

    def test_noise_adjusted_certification_never_beats_unadjusted(self) -> None:
        samples = [(0.97, True)] * 120
        unadjusted = certify_threshold(samples, alpha=0.05, grid=_GRID, prune_infeasible=True)
        adjusted = certify_threshold_under_label_noise(
            samples, alpha=0.05, controls_by_class={"column_distribution": (0, 30)}, grid=_GRID
        )
        assert unadjusted is not None
        # Adjusted may refuse, or certify no *lower* (more permissive) threshold than unadjusted.
        assert adjusted is None or adjusted >= unadjusted

    def test_a_labeller_who_accepts_plants_blocks_certification(self) -> None:
        """The measurement that could embarrass us: bad controls must cost coverage."""
        clean = _human_session(labels=120, controls=30, false_accepts=0)
        sloppy = _human_session(labels=120, controls=30, false_accepts=8)
        assert certify_from_session(clean).certified_classes == ["type_mismatch"]
        result = certify_from_session(sloppy)
        assert result.certified_classes == []
        assert result.beta_upper is not None and result.beta_upper > 0.2
        assert "label_noise_blocked" in result.reasons["type_mismatch"]

    def test_zero_controls_in_the_bound_helper_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one planted control"):
            label_noise_adjusted_bound(0, 100, false_accepts=0, controls=0)


class TestSampleBudget:
    """The honest data budget, roughly double the naive one and still affordable."""

    @pytest.mark.parametrize(
        ("alpha", "controls", "expected_n"),
        [
            (0.05, 30, 82),
            (0.05, 20, 87),
            (0.10, 30, 40),
            (0.20, 30, 19),
        ],
    )
    def test_required_samples_match_the_preregistered_frontier(
        self, alpha: float, controls: int, expected_n: int
    ) -> None:
        assert min_samples_under_label_noise(alpha, controls=controls) == expected_n

    def test_the_adjusted_budget_exceeds_the_naive_one(self) -> None:
        from dataforge.conformal import min_samples_for_certification

        for alpha in (0.05, 0.10, 0.20):
            naive = min_samples_for_certification(alpha, 0.05)
            adjusted = min_samples_under_label_noise(alpha, controls=30)
            assert adjusted is not None and adjusted > naive


class TestPruningUsesTheAdjustedFloor:
    """The pruning floor must match the bound the loop actually tests.

    Until 2026-08-22 ``certify_threshold_under_label_noise`` passed the bare ``alpha`` to
    ``feasible_candidate_sequence``, so pruning used the NAIVE Clopper-Pearson floor (59 at
    alpha=delta=0.05) while the walk tested the noise-adjusted bound (floor 82 with 30 clean
    controls). Grid points with 59-81 accepted samples were retained despite being
    arithmetically incapable of certifying.

    The cost was power, not soundness: a fixed sequence halts on its first tested failure,
    so retaining an unachievable point lets the walk break there and never reach a coarser
    threshold that would have passed. The bug silently cancelled the feature it shipped
    with.
    """

    @staticmethod
    def _calibration(n_high: int, n_low: int) -> list[tuple[float, bool]]:
        """``n_high`` perfect samples at 0.99 and ``n_low`` perfect ones at 0.90."""
        return [(0.99, True)] * n_high + [(0.90, True)] * n_low

    def test_a_point_below_the_adjusted_floor_is_pruned(self) -> None:
        """70 samples clears the naive floor of 59 but not the adjusted floor of 82.

        With pruning correct, ``t=0.99`` (70 samples) is dropped and the walk reaches
        ``t=0.90`` (140 samples), which can certify. With the naive floor it would be
        retained, fail the adjusted bound, and break the sequence at the first step.
        """
        calibration = self._calibration(n_high=70, n_low=70)

        certified = certify_threshold_under_label_noise(
            calibration,
            alpha=0.05,
            controls_by_class={"column_distribution": (0, 30)},
            grid=[0.99, 0.90],
            min_support=30,
        )

        assert certified == pytest.approx(0.90), (
            f"the walk must skip the infeasible 0.99 point and certify at 0.90; got {certified}"
        )

    def test_the_naive_floor_would_have_broken_the_sequence(self) -> None:
        """Pins the regression directly: 70 is above the naive floor, below the adjusted.

        If someone reverts the fix, ``feasible_candidate_sequence`` keeps ``0.99`` and the
        assertion above fails. This test makes the arithmetic that distinguishes the two
        floors explicit, so the failure message points at the cause rather than at a
        surprising ``None``.
        """
        from dataforge.conformal import min_samples_for_certification

        naive = min_samples_for_certification(0.05, 0.05)
        adjusted = min_samples_under_label_noise(0.05, controls=30, false_accepts=0)
        assert adjusted is not None
        assert naive < 70 < adjusted, (
            f"this test's fixture depends on 70 sitting between the naive floor ({naive}) "
            f"and the adjusted floor ({adjusted}); adjust n_high if the constants moved"
        )

    def test_an_infeasible_control_set_returns_none_rather_than_walking(self) -> None:
        """When no finite n can certify, say so instead of certifying nothing quietly.

        ``min_samples_under_label_noise`` returns ``None`` once the controls admit so much
        false-accept rate that the target is unreachable at any sample size. That is a
        signal to collect more CONTROLS, not more labels, and it must not be confused with
        "walked the grid and found nothing".
        """
        certified = certify_threshold_under_label_noise(
            self._calibration(n_high=500, n_low=500),
            alpha=0.05,
            controls_by_class={"column_distribution": (30, 30)},
            grid=[0.99, 0.90],
            min_support=30,
        )

        assert certified is None


class TestScopeTravelsInTheArtifact:
    """A guarantee that hides its own scope is how six claims got retracted."""

    def test_the_plant_distribution_caveat_is_recorded(self) -> None:
        result = certify_from_session(_human_session())
        assert result.beta_scope_note is not None
        assert "PLANTED-CONTROL distribution" in result.beta_scope_note
        assert "anti-conservative" in result.beta_scope_note

    def test_control_counts_are_recorded(self) -> None:
        result = certify_from_session(_human_session(controls=30, false_accepts=2))
        assert result.label_noise_controls == 30
        assert result.label_noise_false_accepts == 2
        assert result.label_noise_adjusted is True

    def test_oracle_certificates_carry_no_beta_note(self) -> None:
        artifact = build_calibration_session(
            _issues(120),
            source_path=Path("t.csv"),
            source_sha256=_SHA,
            row_count=120,
            columns=["c"],
            table_fingerprint="fp",
            fd_detection_source="none",
            per_class=120,
            label_source="oracle",
        )
        for sample in list(artifact.samples):
            artifact = label_repair_sample(
                artifact,
                row=sample.row,
                column=sample.column,
                decision="correct",
                proposed_repair="v",
                repair_confidence=0.97,
            )
        assert certify_from_session(artifact).beta_scope_note is None


class TestPlantsNeverEnterCertification:
    """A plant is known-wrong by construction. Certifying on one would be circular."""

    def test_planted_controls_are_not_counted_as_repair_labels(self) -> None:
        artifact = _human_session(labels=120, controls=30)
        result = certify_from_session(artifact)
        assert result.repair_labels_used == 120, "plants must not inflate the label count"

    def test_plants_live_in_a_separate_list(self) -> None:
        artifact = _human_session(labels=120, controls=30)
        sample_rows = {sample.row for sample in artifact.samples}
        control_rows = {control.row for control in artifact.planted_controls}
        assert not (sample_rows & control_rows)


def test_mutation_witnesses_are_documented() -> None:
    """Mutants executed against ``conformal.py`` / ``calibration_session.py``, each turning the
    named test red. Recorded because an undocumented mutation claim is indistinguishable from an
    untested one.

    **Method note, learned the hard way twice in one session.** A mutation applied by string
    replacement can silently fail to apply -- wrong indentation, wrong line endings, a quote
    mangled by the shell -- and the suite then passes for the most boring possible reason. Twice
    a mutant here "survived" and was actually never applied. So a mutation run must assert that
    the file *changed* before it asserts anything about the tests. An unverified no-op mutant is
    worse than no mutant: it certifies a guard that was never challenged.
    """
    witnesses = {
        "fail-closed branch removed (human with 0 controls proceeds)": (
            "TestHumanLabelsFailClosed::test_human_labels_without_controls_refuse_to_certify"
        ),
        "pending controls counted as labelled": (
            "TestHumanLabelsFailClosed::test_pending_controls_do_not_count_as_measured"
        ),
        "beta_upper forced to 0.0": (
            "TestTheAdjustmentOnlyTightens::test_a_labeller_who_accepts_plants_blocks_certification"
        ),
        "adjusted bound returns `measured` unchanged": (
            "TestTheAdjustmentOnlyTightens::test_a_labeller_who_accepts_plants_blocks_certification"
        ),
        "delta not split (full delta to each bound)": (
            "TestSampleBudget::test_required_samples_match_the_preregistered_frontier"
        ),
        "scope note dropped from the certificate": (
            "TestScopeTravelsInTheArtifact::test_the_plant_distribution_caveat_is_recorded"
        ),
        "plants merged into `samples`": (
            "TestPlantsNeverEnterCertification::test_planted_controls_are_not_counted_as_repair_labels"
        ),
    }
    module = globals()
    for mutation, target in witnesses.items():
        class_name, _, method = target.partition("::")
        assert class_name in module, f"{mutation}: no class {class_name}"
        assert hasattr(module[class_name], method), f"{mutation}: no test {target}"


class TestTheLivePathStratifiesRatherThanPooling:
    """The consequence of docs/trust/stratified-label-noise-result.md, enforced in the product.

    `certify_session` used to pool every planted control into one `beta`. The two origins
    measure different things and their measured false-accept rates differ by 7.5x, so pooling
    understated `beta` by enough to change whether a pre-registered kill criterion fired.
    These tests pin the fix at the level that ships, not just in the arithmetic helper.
    """

    @staticmethod
    def _mixed(
        *, distribution: tuple[int, int], corrector: tuple[int, int]
    ) -> CalibrationSessionArtifact:
        """A session whose controls span BOTH origins, with per-origin (false_accepts, n)."""
        artifact = _human_session(labels=120, controls=0)
        planted: list[PlantedControl] = []
        row = 10_000
        for origin, (accepted, total) in (
            ("column_distribution", distribution),
            ("corrector_generated", corrector),
        ):
            for index in range(total):
                control = _control(row, accepted=index < accepted)
                planted.append(control.model_copy(update={"origin": origin}))
                row += 1
        return artifact.model_copy(update={"planted_controls": planted})

    def test_controls_are_grouped_by_origin_not_pooled(self) -> None:
        artifact = self._mixed(distribution=(2, 30), corrector=(4, 8))
        assert artifact.controls_by_origin() == {
            "column_distribution": (2, 30),
            "corrector_generated": (4, 8),
        }
        # The pooled accessor still exists for reporting, and still pools.
        assert artifact.observed_false_accepts() == 6
        assert len(artifact.labelled_controls()) == 38

    def test_the_worst_origin_binds_the_certified_beta(self) -> None:
        """The real measured split: 2/30 against 4/8.

        Pooled this gives beta_upper 0.3125, below the pre-registered 0.35 kill threshold.
        Stratified the binding class gives 0.8712, above it. The session must report the
        latter, because that is the bound the labelling process actually supports.
        """
        artifact = self._mixed(distribution=(2, 30), corrector=(4, 8))
        result = certify_from_session(artifact)
        assert result.beta_upper is not None
        assert result.beta_upper == pytest.approx(0.8712, abs=0.001)
        assert result.beta_upper > 0.35, "the pre-registered kill criterion must fire here"

    def test_pooling_would_have_reported_a_bound_below_the_kill_threshold(self) -> None:
        """Pins the size of the defect, so a revert is visibly a regression and not a tidy-up."""
        pooled = label_noise_adjusted_bound(0, 1, false_accepts=6, controls=38)[1]
        assert pooled == pytest.approx(0.3125, abs=0.001)
        assert pooled <= 0.35
        stratified = certify_from_session(
            self._mixed(distribution=(2, 30), corrector=(4, 8))
        ).beta_upper
        assert stratified is not None and stratified > pooled

    def test_a_dirty_origin_cannot_be_hidden_by_a_clean_one(self) -> None:
        """Many clean plants plus a few accepted hard ones must not average out.

        This is the failure mode pooling permits: pad the control set with easy plants and the
        false-accept rate on the hard class disappears into the denominator.
        """
        artifact = self._mixed(distribution=(0, 200), corrector=(4, 8))
        result = certify_from_session(artifact)
        assert result.beta_upper is not None
        assert result.beta_upper > 0.35
        assert result.certified_classes == [], (
            "a binding beta above the kill threshold must certify nothing"
        )

    def test_a_single_origin_is_arithmetically_unchanged(self) -> None:
        """The migration must not move any number it was not meant to move.

        With one class the union correction divides delta/2 by 1, so the stratified bound is
        bit-identical to the pooled one. That keeps the blast radius exactly at genuinely
        pooled multi-origin sets.
        """
        single = certify_from_session(_human_session(labels=120, controls=30, false_accepts=2))
        expected = label_noise_adjusted_bound(0, 1, false_accepts=2, controls=30)[1]
        assert single.beta_upper == pytest.approx(expected, abs=1e-12)
