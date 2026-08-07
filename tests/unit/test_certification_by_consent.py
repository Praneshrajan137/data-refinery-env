"""Certification must consume repair verdicts, never detection verdicts.

The plan for this feature said to "feed the labelled session into
``certify_thresholds_by_class``". Taken literally that is a **category error**, and the
consequence is silent data loss.

``certify_threshold`` takes ``(confidence, was_correct)`` pairs where ``was_correct`` means
*the applied repair was correct*, and its output populates ``auto_apply_thresholds`` -- the
gate that decides whether a proposed value gets written to the user's file. A calibration
session's ``decision`` field answers a different question: *was this cell genuinely an
error?* That is detection precision.

The two come apart concretely. On hospital, row 3 ``City`` holds ``'birminghxm'`` and should
be ``'birmingham'``. A corrector proposing ``'Boston'`` is wrong on a cell that was correctly
flagged. So detection precision 1.0 is fully compatible with corrector accuracy 0.0, and
certifying auto-apply on detection labels would authorize overwriting cells with values no
one ever validated.

These tests exist so that conflation cannot be reintroduced by someone reading the plan.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.calibration_session import (
    CERTIFICATION_GRID,
    CalibrationSessionArtifact,
    build_calibration_session,
    certify_from_session,
    label_calibration_sample,
    label_repair_sample,
    repair_labelled_samples,
)
from dataforge.conformal import ABSTAIN_THRESHOLD
from dataforge.detectors.base import Issue, Severity

_SHA = "b" * 64


def _issues(count: int, issue_type: str = "missing_value") -> list[Issue]:
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


def _session(count: int = 80, issue_type: str = "missing_value") -> CalibrationSessionArtifact:
    return build_calibration_session(
        _issues(count, issue_type),
        source_path=Path("t.csv"),
        source_sha256=_SHA,
        row_count=count,
        columns=["c"],
        table_fingerprint="fp",
        fd_detection_source="none",
        per_class=count,
    )


def _label_all_repairs(
    artifact: CalibrationSessionArtifact,
    *,
    correct_every: int = 1,
    confidence: float = 0.97,
) -> CalibrationSessionArtifact:
    """Label every sample's repair; ``correct_every=1`` means all correct."""
    for index, sample in enumerate(list(artifact.samples)):
        artifact = label_repair_sample(
            artifact,
            row=sample.row,
            column=sample.column,
            decision="correct" if index % correct_every == 0 else "error",
            proposed_repair="v",
            repair_confidence=confidence,
        )
    return artifact


class TestDetectionLabelsCannotCertify:
    """The central guard. Everything else in this file is secondary to it."""

    def test_detection_labels_alone_are_refused(self) -> None:
        artifact = _session()
        for sample in list(artifact.samples):
            artifact = label_calibration_sample(
                artifact, row=sample.row, column=sample.column, decision="error"
            )
        with pytest.raises(ValueError, match="no repair verdicts"):
            certify_from_session(artifact)

    def test_the_refusal_explains_the_distinction(self) -> None:
        """A bare failure would invite someone to "fix" it by passing detection labels."""
        artifact = _session()
        artifact = label_calibration_sample(artifact, row=0, column="c", decision="error")
        with pytest.raises(ValueError, match="is the proposed replacement right"):
            certify_from_session(artifact)

    def test_detection_labels_are_not_counted_as_repair_labels(self) -> None:
        artifact = _session(10)
        for sample in list(artifact.samples):
            artifact = label_calibration_sample(
                artifact, row=sample.row, column=sample.column, decision="error"
            )
        assert repair_labelled_samples(artifact) == []

    def test_an_unlabelled_repair_is_not_counted(self) -> None:
        """A proposal with no verdict is not evidence."""
        artifact = _session(10)
        assert repair_labelled_samples(artifact) == []

    def test_a_verdict_without_a_proposal_is_refused(self) -> None:
        """A verdict on nothing would enter certification as a real observation."""
        artifact = _session(10)
        with pytest.raises(ValueError, match="no proposed repair"):
            label_repair_sample(artifact, row=0, column="c", decision="correct")

    def test_a_verdict_with_no_proposal_is_excluded_from_the_usable_set(self) -> None:
        """Belt and braces: the filter must hold even if such a sample is constructed.

        The confidence is deliberately SET here so the proposal check is the only thing
        that can exclude this sample. Leaving both fields empty made this test pass for the
        wrong reason and let a mutant of the proposal check survive.
        """
        artifact = _session(1)
        orphan = artifact.samples[0].model_copy(
            update={
                "repair_decision": "correct",
                "proposed_repair": None,
                "repair_confidence": 0.9,
            }
        )
        artifact = artifact.model_copy(update={"samples": [orphan]})
        assert repair_labelled_samples(artifact) == []

    def test_a_verdict_with_no_confidence_is_excluded(self) -> None:
        """There is nothing to compare a threshold against."""
        artifact = _session(1)
        orphan = artifact.samples[0].model_copy(
            update={
                "repair_decision": "correct",
                "proposed_repair": "v",
                "repair_confidence": None,
            }
        )
        artifact = artifact.model_copy(update={"samples": [orphan]})
        assert repair_labelled_samples(artifact) == []


class TestCertificationFromRepairLabels:
    def test_all_correct_repairs_certify_the_class(self) -> None:
        result = certify_from_session(_label_all_repairs(_session()))
        assert result.certified_classes == ["missing_value"]
        assert result.thresholds["missing_value"] <= 0.97

    def test_an_imprecise_corrector_is_not_certified(self) -> None:
        """30% wrong must not certify at alpha = 0.05, whatever the sample size."""
        result = certify_from_session(_label_all_repairs(_session(), correct_every=10))
        assert result.certified_classes == []
        assert result.thresholds["missing_value"] == ABSTAIN_THRESHOLD

    def test_a_failure_carries_a_reason(self) -> None:
        result = certify_from_session(_label_all_repairs(_session(), correct_every=10))
        assert "missing_value" in result.reasons
        assert result.reasons["missing_value"]

    def test_too_few_labels_cannot_certify(self) -> None:
        """59 all-correct accepted samples is the floor; 10 cannot clear it."""
        result = certify_from_session(_label_all_repairs(_session(10)))
        assert result.certified_classes == []

    def test_certification_is_per_class_not_pooled(self) -> None:
        """A good class must not be dragged down by a bad one, or vice versa."""
        good = _issues(80, "missing_value")
        # Distinct rows: one issue per cell is an invariant of the queue, and
        # build_calibration_session enforces it, so overlapping rows would be collapsed.
        bad = [
            Issue(
                row=100 + i,
                column="c",
                issue_type="type_mismatch",
                severity=Severity.REVIEW,
                confidence=0.9,
                actual="x",
                reason="r",
            )
            for i in range(80)
        ]
        artifact = build_calibration_session(
            good + bad,
            source_path=Path("t.csv"),
            source_sha256=_SHA,
            row_count=180,
            columns=["c"],
            table_fingerprint="fp",
            fd_detection_source="none",
            per_class=80,
        )
        for sample in list(artifact.samples):
            healthy = sample.issue_type == "missing_value"
            artifact = label_repair_sample(
                artifact,
                row=sample.row,
                column=sample.column,
                decision="correct" if healthy or sample.row % 3 else "error",
                proposed_repair="v",
                repair_confidence=0.97,
            )
        result = certify_from_session(artifact)
        assert "missing_value" in result.certified_classes
        assert "type_mismatch" not in result.certified_classes

    def test_labels_used_is_reported(self) -> None:
        result = certify_from_session(_label_all_repairs(_session(70)))
        assert result.repair_labels_used == 70


class TestOneSamplePerCell:
    """(row, column) is the label key, so it has to be unique in the session."""

    def test_a_cell_flagged_twice_is_sampled_once(self) -> None:
        """Otherwise one verdict labels two samples and double-counts in the estimate."""
        collision = [
            Issue(
                row=0,
                column="c",
                issue_type=kind,
                severity=Severity.REVIEW,
                confidence=confidence,
                actual="x",
                reason="r",
            )
            for kind, confidence in (("outlier", 0.4), ("type_mismatch", 0.95))
        ]
        artifact = build_calibration_session(
            collision,
            source_path=Path("t.csv"),
            source_sha256=_SHA,
            row_count=1,
            columns=["c"],
            table_fingerprint="fp",
            fd_detection_source="none",
        )
        assert len(artifact.samples) == 1

    def test_the_higher_confidence_issue_wins_the_cell(self) -> None:
        """Mirrors the queue, where tier-0 precedence displaces weaker findings."""
        collision = [
            Issue(
                row=0,
                column="c",
                issue_type=kind,
                severity=Severity.REVIEW,
                confidence=confidence,
                actual="x",
                reason="r",
            )
            for kind, confidence in (("outlier", 0.4), ("type_mismatch", 0.95))
        ]
        artifact = build_calibration_session(
            collision,
            source_path=Path("t.csv"),
            source_sha256=_SHA,
            row_count=1,
            columns=["c"],
            table_fingerprint="fp",
            fd_detection_source="none",
        )
        assert artifact.samples[0].issue_type == "type_mismatch"

    def test_precedence_is_by_confidence_not_input_order(self) -> None:
        """Last-wins would be order-dependent, so the same queue could yield different
        sessions depending on detector registration order."""
        collision = [
            Issue(
                row=0,
                column="c",
                issue_type=kind,
                severity=Severity.REVIEW,
                confidence=confidence,
                actual="x",
                reason="r",
            )
            for kind, confidence in (("type_mismatch", 0.95), ("outlier", 0.4))
        ]
        artifact = build_calibration_session(
            collision,
            source_path=Path("t.csv"),
            source_sha256=_SHA,
            row_count=1,
            columns=["c"],
            table_fingerprint="fp",
            fd_detection_source="none",
        )
        assert len(artifact.samples) == 1
        assert artifact.samples[0].issue_type == "type_mismatch"

    def test_flagged_total_counts_cells_not_issues(self) -> None:
        """The field is named for cells; counting issues overstates the queue."""
        collision = [
            Issue(
                row=0,
                column="c",
                issue_type=kind,
                severity=Severity.REVIEW,
                confidence=0.5,
                actual="x",
                reason="r",
            )
            for kind in ("outlier", "type_mismatch", "format_violation")
        ]
        artifact = build_calibration_session(
            collision,
            source_path=Path("t.csv"),
            source_sha256=_SHA,
            row_count=1,
            columns=["c"],
            table_fingerprint="fp",
            fd_detection_source="none",
        )
        assert artifact.flagged_cells_total == 1


class TestGridIsPreSpecified:
    """A data-dependent grid is a validity weakness, not just a power one."""

    def test_the_grid_is_a_fixed_constant(self) -> None:
        result = certify_from_session(_label_all_repairs(_session()))
        assert result.grid == list(CERTIFICATION_GRID)

    def test_the_grid_is_strictly_descending(self) -> None:
        """Fixed sequential testing requires purest-first ordering."""
        assert list(CERTIFICATION_GRID) == sorted(CERTIFICATION_GRID, reverse=True)
        assert len(set(CERTIFICATION_GRID)) == len(CERTIFICATION_GRID)

    def test_the_grid_does_not_depend_on_observed_confidences(self) -> None:
        low = certify_from_session(_label_all_repairs(_session(), confidence=0.61))
        high = certify_from_session(_label_all_repairs(_session(), confidence=0.99))
        assert low.grid == high.grid

    def test_the_certified_threshold_comes_from_the_fixed_grid(self) -> None:
        """Asserting the *reported* grid is not enough -- the report could simply echo the
        constant while the certification used data-derived candidates. With every repair
        correct, the lowest certifiable grid point wins, and that value (0.60) exists only in
        the pre-specified grid. If candidates came from the observed confidences instead, the
        threshold would land on the observed value (0.97).
        """
        result = certify_from_session(_label_all_repairs(_session(), confidence=0.97))
        assert result.thresholds["missing_value"] == min(CERTIFICATION_GRID)

    def test_the_threshold_is_not_the_observed_confidence(self) -> None:
        result = certify_from_session(_label_all_repairs(_session(), confidence=0.97))
        assert result.thresholds["missing_value"] != 0.97


class TestCertificationIsAnchoredToTheTable:
    def test_the_source_hash_travels_with_the_certificate(self) -> None:
        """A certificate that forgets its table can be applied to another one."""
        result = certify_from_session(_label_all_repairs(_session()))
        assert result.source_sha256 == _SHA

    def test_the_table_fingerprint_travels_with_the_certificate(self) -> None:
        result = certify_from_session(_label_all_repairs(_session()))
        assert result.table_fingerprint == "fp"

    def test_alpha_and_delta_are_recorded(self) -> None:
        """A threshold is meaningless without the risk level it was certified at."""
        result = certify_from_session(_label_all_repairs(_session()), alpha=0.1, delta=0.01)
        assert (result.alpha, result.delta) == (0.1, 0.01)
