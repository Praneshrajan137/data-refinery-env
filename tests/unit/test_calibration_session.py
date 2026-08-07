"""A calibration session must measure honestly or not at all.

Three failure modes would make this feature worse than absent, because each produces a
number that *looks* like evidence:

1. **A rank-ordered sample.** Offering the highest-confidence cells inflates measured
   precision. This project already had to retract a claim to selected-extremum error once.
2. **Cross-table label credit.** Labels gathered on one file reported as a measurement of
   different bytes.
3. **Silent round-trip loss.** An artifact that writes but cannot be read, so labels vanish.

Number 3 is not hypothetical: the first implementation used `tuple` fields, and under
`strict=True` Pydantic refuses to coerce a JSON array back to a tuple, so every session was
write-only. The regression test is `test_round_trips_through_json`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.calibration_session import (
    CalibrationSessionArtifact,
    build_calibration_session,
    clopper_pearson_interval,
    dump_calibration_session,
    label_calibration_sample,
    load_calibration_session,
    summarize_calibration,
)
from dataforge.detectors.base import Issue, Severity

_SHA = "a" * 64


def _issue(row: int, column: str, issue_type: str, confidence: float) -> Issue:
    return Issue(
        row=row,
        column=column,
        issue_type=issue_type,
        severity=Severity.REVIEW,
        confidence=confidence,
        actual=f"v{row}",
        reason=f"{issue_type} at row {row}",
    )


def _population(count: int = 100, issue_type: str = "outlier") -> list[Issue]:
    """Confidence rises with row index, so a rank-ordered sample is detectable."""
    return [_issue(i, "c", issue_type, 0.5 + 0.005 * i) for i in range(count)]


def _build(issues: list[Issue], **kwargs: object) -> CalibrationSessionArtifact:
    params: dict[str, object] = {
        "source_path": Path("t.csv"),
        "source_sha256": _SHA,
        "row_count": 100,
        "columns": ["c"],
        "table_fingerprint": "fp",
        "fd_detection_source": "none",
    }
    params.update(kwargs)
    return build_calibration_session(issues, **params)  # type: ignore[arg-type]


class TestSampleIsRandomNotRankOrdered:
    """The whole estimate is worthless if the sample is drawn by confidence."""

    def test_sample_is_not_the_highest_confidence_cells(self) -> None:
        issues = _population()
        artifact = _build(issues, per_class=12)
        sampled_rows = {s.row for s in artifact.samples}
        top_rows = {i.row for i in sorted(issues, key=lambda i: -i.confidence)[:12]}
        assert sampled_rows != top_rows, "sample equals the top-confidence cells"

    def test_sample_is_not_the_lowest_confidence_cells(self) -> None:
        issues = _population()
        artifact = _build(issues, per_class=12)
        sampled_rows = {s.row for s in artifact.samples}
        bottom_rows = {i.row for i in sorted(issues, key=lambda i: i.confidence)[:12]}
        assert sampled_rows != bottom_rows

    def test_sample_mean_confidence_tracks_the_population(self) -> None:
        """A random sample's mean confidence should sit near the population's.

        Averaged over many seeds so this asserts the sampler's behaviour rather than one
        lucky draw. A rank-ordered sampler fails this by a wide margin.
        """
        issues = _population()
        population_mean = sum(i.confidence for i in issues) / len(issues)
        sample_means = []
        for seed in range(60):
            artifact = _build(issues, per_class=12, seed=seed)
            sample_means.append(
                sum(s.detector_confidence for s in artifact.samples) / len(artifact.samples)
            )
        assert abs(sum(sample_means) / len(sample_means) - population_mean) < 0.02

    def test_different_seeds_draw_different_samples(self) -> None:
        issues = _population()
        first = {s.row for s in _build(issues, per_class=12, seed=1).samples}
        second = {s.row for s in _build(issues, per_class=12, seed=2).samples}
        assert first != second

    def test_the_same_seed_is_reproducible(self) -> None:
        issues = _population()
        first = [s.row for s in _build(issues, per_class=12, seed=7).samples]
        second = [s.row for s in _build(issues, per_class=12, seed=7).samples]
        assert first == second

    def test_strategy_is_recorded_so_a_biased_draw_cannot_hide(self) -> None:
        assert _build(_population()).sampling_strategy == "random_within_class"


class TestStratification:
    def test_every_class_is_represented(self) -> None:
        issues = _population(50, "outlier") + _population(3, "type_mismatch")
        artifact = _build(issues, per_class=12)
        assert {s.issue_type for s in artifact.samples} == {"outlier", "type_mismatch"}

    def test_a_small_class_is_taken_whole_not_padded(self) -> None:
        issues = _population(50, "outlier") + _population(3, "type_mismatch")
        artifact = _build(issues, per_class=12)
        rare = [s for s in artifact.samples if s.issue_type == "type_mismatch"]
        assert len(rare) == 3

    def test_flagged_total_records_the_whole_queue_not_the_sample(self) -> None:
        artifact = _build(_population(100), per_class=12)
        assert artifact.flagged_cells_total == 100
        assert len(artifact.samples) == 12


class TestLabelling:
    def test_labelling_an_unsampled_cell_is_refused(self) -> None:
        """Accepting it would silently break the random-sampling guarantee."""
        artifact = _build(_population(), per_class=5)
        with pytest.raises(KeyError, match="not part of this calibration session"):
            label_calibration_sample(artifact, row=99999, column="c", decision="error")

    def test_labelling_is_pure(self) -> None:
        artifact = _build(_population(), per_class=5)
        target = artifact.samples[0]
        updated = label_calibration_sample(
            artifact, row=target.row, column=target.column, decision="error"
        )
        assert artifact.samples[0].decision == "pending"
        assert updated.samples[0].decision == "error"

    def test_samples_start_pending(self) -> None:
        assert all(s.decision == "pending" for s in _build(_population()).samples)


class TestSummary:
    def test_pending_samples_are_excluded_not_assumed(self) -> None:
        artifact = _build(_population(), per_class=10)
        target = artifact.samples[0]
        artifact = label_calibration_sample(
            artifact, row=target.row, column=target.column, decision="error"
        )
        [entry] = summarize_calibration(artifact)
        assert entry.labelled == 1
        assert entry.precision == 1.0

    def test_an_unlabelled_class_reports_no_precision_rather_than_zero(self) -> None:
        """Reporting 0.0 for an unmeasured class would invent a measurement."""
        [entry] = summarize_calibration(_build(_population(), per_class=5))
        assert entry.precision is None
        assert entry.precision_ci95 is None

    def test_precision_is_the_labelled_error_rate(self) -> None:
        artifact = _build(_population(), per_class=10)
        for index, sample in enumerate(artifact.samples):
            artifact = label_calibration_sample(
                artifact,
                row=sample.row,
                column=sample.column,
                decision="error" if index < 4 else "correct",
            )
        [entry] = summarize_calibration(artifact)
        assert entry.labelled == 10
        assert entry.real_errors == 4
        assert entry.precision == pytest.approx(0.4)

    def test_queue_counts_are_reported_beside_the_estimate(self) -> None:
        """Precision without queue size cannot tell a user how much work is wasted."""
        artifact = _build(_population(), per_class=5)
        [entry] = summarize_calibration(artifact, queue_counts={"outlier": 100})
        assert entry.flagged_cells_in_queue == 100

    def test_certification_shortfall_is_reported(self) -> None:
        artifact = _build(_population(), per_class=5)
        [entry] = summarize_calibration(artifact)
        assert entry.samples_short_of_certification_floor == 59


class TestClopperPearson:
    @pytest.mark.parametrize(
        ("successes", "total"), [(0, 10), (5, 10), (10, 10), (1, 3), (59, 59), (8, 12)]
    )
    def test_interval_contains_the_estimate_and_stays_in_range(
        self, successes: int, total: int
    ) -> None:
        low, high = clopper_pearson_interval(successes, total)
        assert 0.0 <= low <= successes / total <= high <= 1.0

    def test_no_data_yields_the_whole_interval(self) -> None:
        assert clopper_pearson_interval(0, 0) == (0.0, 1.0)

    def test_more_data_narrows_the_interval(self) -> None:
        narrow = clopper_pearson_interval(50, 100)
        wide = clopper_pearson_interval(5, 10)
        assert (narrow[1] - narrow[0]) < (wide[1] - wide[0])

    def test_all_successes_still_admits_doubt(self) -> None:
        """12/12 is not proof of 1.0, and the interval has to say so."""
        low, _ = clopper_pearson_interval(12, 12)
        assert low < 1.0


class TestScopeAndSerialization:
    def test_round_trips_through_json(self) -> None:
        """Regression: tuple fields under strict mode made sessions write-only."""
        artifact = _build(_population(), per_class=6)
        restored = CalibrationSessionArtifact.model_validate(
            json.loads(dump_calibration_session(artifact))
        )
        assert restored == artifact

    def test_labels_survive_a_round_trip(self, tmp_path: Path) -> None:
        artifact = _build(_population(), per_class=6)
        target = artifact.samples[0]
        artifact = label_calibration_sample(
            artifact, row=target.row, column=target.column, decision="error", note="typo"
        )
        path = tmp_path / "session.json"
        path.write_text(dump_calibration_session(artifact), encoding="utf-8")
        restored = load_calibration_session(path)
        assert restored.labelled()[0].decision == "error"
        assert restored.labelled()[0].note == "typo"

    def test_the_source_hash_is_recorded_so_labels_cannot_move_tables(self) -> None:
        assert _build(_population()).source_sha256 == _SHA

    def test_a_malformed_hash_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            _build(_population(), source_sha256="not-a-hash")

    def test_fd_regime_is_recorded_beside_the_estimate(self) -> None:
        """A precision number is meaningless without the regime that produced the queue."""
        assert _build(_population(), fd_detection_source="declared").fd_detection_source == (
            "declared"
        )
