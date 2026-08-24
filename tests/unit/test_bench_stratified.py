"""Tests for the stratified queue-filter projection.

Offline arithmetic. No model, no network, no spend.

The tests that matter are the ones asserting the projection cannot flatter a filter: an
unsampled stratum must not vanish from the denominator, recall retention must be visible next to
any precision gain, and a projection where nothing was measured must raise rather than returning
the baseline as a result.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.bench.abstention import ThreeWayScore, score_detection_three_way
from dataforge.bench.stratified import (
    StratifiedProjectionError,
    StratumSample,
    project_queue_filter,
    stratified_precision,
    wilson_interval,
)


def _stratum(
    name: str,
    *,
    true_pop: int,
    false_pop: int,
    true_kept: int = 0,
    true_sampled: int = 0,
    false_kept: int = 0,
    false_sampled: int = 0,
) -> StratumSample:
    """Build a stratum sample."""
    return StratumSample(
        name=name,
        true_population=true_pop,
        false_population=false_pop,
        true_kept=true_kept,
        true_sampled=true_sampled,
        false_kept=false_kept,
        false_sampled=false_sampled,
    )


class TestTheRayyanShape:
    """The real queue: a 0.0649 stratum to clean and a 1.0000 stratum to protect."""

    def test_a_filter_that_helps_one_stratum_and_destroys_another_shows_both(self) -> None:
        """The whole reason this module exists rather than a pooled number.

        A filter that removes 90% of missing_value false positives while rejecting 40% of
        perfect date_transposition detections raises pooled precision. The per-stratum view
        shows it is discarding 255 real errors to do it.
        """
        projection = project_queue_filter(
            [
                _stratum(
                    "missing_value",
                    true_pop=75,
                    false_pop=1080,
                    true_kept=60,
                    true_sampled=75,
                    false_kept=12,
                    false_sampled=125,
                ),
                _stratum(
                    "date_transposition",
                    true_pop=637,
                    false_pop=0,
                    true_kept=60,
                    true_sampled=100,
                ),
            ],
            total_true_errors_in_table=948,
        )
        assert projection.baseline_precision == round(712 / 1792, 4)
        # Precision rises...
        assert projection.projected_precision is not None
        assert projection.projected_precision > projection.baseline_precision
        # ...and the per-stratum view shows what it cost.
        by_name = {s.name: s for s in projection.per_stratum}
        assert by_name["date_transposition"].keep_true_rate == 0.6
        assert by_name["date_transposition"].true_errors_lost == pytest.approx(254.8, abs=0.1)
        assert projection.recall_retained is not None
        assert projection.recall_retained < 0.7, "the safety term must expose the damage"

    def test_a_filter_that_only_removes_false_positives_is_pure_gain(self) -> None:
        projection = project_queue_filter(
            [
                _stratum(
                    "missing_value",
                    true_pop=75,
                    false_pop=1080,
                    true_kept=75,
                    true_sampled=75,
                    false_kept=0,
                    false_sampled=125,
                ),
            ],
            total_true_errors_in_table=948,
        )
        assert projection.recall_retained == 1.0
        assert projection.projected_precision == 1.0
        assert projection.true_errors_lost == 0.0


class TestUnsampledStrataAreCarriedNotDropped:
    """Dropping a stratum shrinks the denominator and flatters the filter."""

    def test_an_unsampled_stratum_stays_in_the_projection_unfiltered(self) -> None:
        projection = project_queue_filter(
            [
                _stratum(
                    "missing_value",
                    true_pop=75,
                    false_pop=1080,
                    true_kept=75,
                    true_sampled=75,
                    false_kept=0,
                    false_sampled=125,
                ),
                # type_mismatch: 64 cells, 2 true, never sampled.
                _stratum("type_mismatch", true_pop=2, false_pop=62),
            ],
            total_true_errors_in_table=948,
        )
        assert projection.uncovered_strata == ("type_mismatch",)
        by_name = {s.name: s for s in projection.per_stratum}
        # Carried at rate 1.0 on both sides: no claim is made about cells never shown.
        assert by_name["type_mismatch"].projected_true_kept == 2.0
        assert by_name["type_mismatch"].projected_false_kept == 62.0
        assert "none sampled" in by_name["type_mismatch"].note, (
            "an unsampled stratum must say so on its own row, not only in the aggregate"
        )
        assert by_name["type_mismatch"].keep_true_rate is None
        # Its 62 false positives remain in the denominator.
        assert projection.projected_false_kept == 62.0

    def test_a_projection_where_nothing_was_sampled_raises(self) -> None:
        """Otherwise it reports the unfiltered baseline as a filtered result."""
        with pytest.raises(StratifiedProjectionError, match="never measured"):
            project_queue_filter(
                [_stratum("a", true_pop=10, false_pop=90)],
                total_true_errors_in_table=100,
            )

    def test_zero_strata_raises(self) -> None:
        with pytest.raises(StratifiedProjectionError, match="zero strata"):
            project_queue_filter([], total_true_errors_in_table=1)

    def test_a_stratum_with_true_cells_but_no_true_draws_is_noted(self) -> None:
        """The safety term is unestimated there, and the note says so."""
        projection = project_queue_filter(
            [_stratum("tail", true_pop=2, false_pop=107, false_kept=3, false_sampled=60)],
            total_true_errors_in_table=948,
        )
        tail = projection.per_stratum[0]
        assert tail.covered is True
        assert tail.keep_true_rate is None
        assert "none sampled" in tail.note
        assert tail.projected_true_kept == 2.0, "carried unfiltered, not assumed rejected"


class TestRatesAndIntervals:
    """An unmeasured rate is None, never 0.0."""

    def test_an_unsampled_rate_is_none_not_zero(self) -> None:
        stratum = _stratum("x", true_pop=5, false_pop=5)
        assert stratum.keep_true.rate is None
        assert stratum.keep_false.rate is None

    def test_an_unsampled_rate_has_a_maximally_ignorant_interval(self) -> None:
        assert wilson_interval(0, 0) == (0.0, 1.0)

    def test_wilson_stays_inside_the_unit_interval_at_the_boundary(self) -> None:
        low, high = wilson_interval(60, 60)
        assert 0.0 <= low <= 1.0
        assert high == 1.0
        low_zero, high_zero = wilson_interval(0, 60)
        assert low_zero == 0.0
        assert high_zero <= 1.0

    def test_the_interval_narrows_with_more_trials(self) -> None:
        narrow = wilson_interval(50, 100)
        wide = wilson_interval(5, 10)
        assert (narrow[1] - narrow[0]) < (wide[1] - wide[0])

    def test_baseline_precision_comes_from_known_counts(self) -> None:
        stratum = _stratum("missing_value", true_pop=75, false_pop=1080)
        assert stratum.baseline_precision == 0.0649


class TestImpossibleSamplesRaise:
    """A sample that cannot exist must not silently produce a rate."""

    def test_sampling_more_true_cells_than_exist_raises(self) -> None:
        with pytest.raises(StratifiedProjectionError, match="population of"):
            _stratum("x", true_pop=5, false_pop=10, true_kept=6, true_sampled=6)

    def test_sampling_more_false_cells_than_exist_raises(self) -> None:
        with pytest.raises(StratifiedProjectionError, match="population of"):
            _stratum("x", true_pop=5, false_pop=10, false_kept=1, false_sampled=11)

    def test_keeping_more_than_was_sampled_raises(self) -> None:
        with pytest.raises(StratifiedProjectionError, match="kept more cells"):
            _stratum("x", true_pop=5, false_pop=10, true_kept=4, true_sampled=3)

    def test_a_negative_population_raises(self) -> None:
        with pytest.raises(StratifiedProjectionError, match="negative"):
            _stratum("x", true_pop=-1, false_pop=10)


class TestStratifiedPrecision:
    """Only the false-positive term is projected. Recall is a census and has no interval."""

    def _census(self, *, tp: int, fp: int, fn: int, debatable: int = 0) -> ThreeWayScore:
        return score_detection_three_way(
            distinct_values=[f"v{i}" for i in range(100)],
            ground_truth=[f"v{i}" for i in range(tp + fn)],
            debatable=[f"d{i}" for i in range(debatable)],
            predicted=[f"v{i}" for i in range(tp)] + [f"x{i}" for i in range(fp)],
        )

    def test_recall_is_exact_because_all_ground_truth_is_in_the_census(self) -> None:
        estimate = stratified_precision(
            census_score=self._census(tp=10, fp=5, fn=30),
            per_column_fp=[1, 0, 2, 0, 1],
            population_columns=1000,
        )
        assert estimate.recall == 0.25, "10 of 40, exactly, with no sampling error"

    def test_the_projection_scales_the_per_column_rate(self) -> None:
        estimate = stratified_precision(
            census_score=self._census(tp=10, fp=5, fn=30),
            per_column_fp=[2] * 50,
            population_columns=1000,
        )
        assert estimate.scale == 20.0
        assert estimate.projected_fp == 2000.0
        assert estimate.total_fp == 2005.0
        assert estimate.precision == round(10 / 2015, 4)

    def test_zero_flag_columns_must_lower_the_rate(self) -> None:
        """Dropping unflagged columns would inflate the per-column rate."""
        with_zeros = stratified_precision(
            census_score=self._census(tp=10, fp=0, fn=0),
            per_column_fp=[4, 0, 0, 0],
            population_columns=100,
        )
        without_zeros = stratified_precision(
            census_score=self._census(tp=10, fp=0, fn=0),
            per_column_fp=[4],
            population_columns=100,
        )
        assert with_zeros.projected_fp == 100.0
        assert without_zeros.projected_fp == 400.0
        assert with_zeros.precision is not None
        assert without_zeros.precision is not None
        assert with_zeros.precision > without_zeros.precision

    def test_the_upper_fp_bound_gives_the_lower_precision_bound(self) -> None:
        estimate = stratified_precision(
            census_score=self._census(tp=20, fp=10, fn=10),
            per_column_fp=[3, 1, 0, 2, 5, 1, 0, 2],
            population_columns=500,
        )
        low, high = estimate.precision_ci
        assert low is not None and high is not None
        assert low < (estimate.precision or 0) < high, "the point estimate sits inside"
        assert estimate.projected_fp_ci[0] < estimate.projected_fp < estimate.projected_fp_ci[1]

    def test_a_zero_variance_sample_still_yields_an_interval(self) -> None:
        estimate = stratified_precision(
            census_score=self._census(tp=5, fp=0, fn=5),
            per_column_fp=[1] * 20,
            population_columns=200,
        )
        # Every column flagged exactly once, so variance is 0 and the interval collapses.
        assert estimate.projected_fp == 200.0
        assert estimate.projected_fp_ci == (200.0, 200.0)

    def test_no_unlabelled_sample_raises_when_the_population_is_nonempty(self) -> None:
        """Otherwise precision is projected over thousands of unscored columns."""
        with pytest.raises(StratifiedProjectionError, match="fabricated"):
            stratified_precision(
                census_score=self._census(tp=10, fp=1, fn=1),
                per_column_fp=[],
                population_columns=1131,
            )

    def test_an_empty_population_needs_no_sample(self) -> None:
        estimate = stratified_precision(
            census_score=self._census(tp=10, fp=5, fn=0),
            per_column_fp=[],
            population_columns=0,
        )
        assert estimate.projected_fp == 0.0
        assert estimate.precision == round(10 / 15, 4)

    def test_precision_is_none_when_nothing_was_flagged(self) -> None:
        estimate = stratified_precision(
            census_score=self._census(tp=0, fp=0, fn=40),
            per_column_fp=[0] * 10,
            population_columns=100,
        )
        assert estimate.precision is None
        assert estimate.recall == 0.0

    def test_debatable_predictions_are_carried_not_scored(self) -> None:
        """The three-way rule: a flagged debatable value costs neither term."""
        census = score_detection_three_way(
            distinct_values=["a", "b", "c"],
            ground_truth=["a"],
            debatable=["b"],
            predicted=["a", "b"],
        )
        estimate = stratified_precision(
            census_score=census, per_column_fp=[0] * 10, population_columns=100
        )
        assert estimate.census_tp == 1
        assert estimate.census_fp == 0, "the debatable flag is not a false positive"
        assert estimate.census_debatable_predicted == 1
        assert estimate.precision == 1.0

    """A filter that keeps everything must reproduce the baseline exactly."""

    def test_keeping_everything_reproduces_the_baseline(self) -> None:
        projection = project_queue_filter(
            [
                _stratum(
                    "a",
                    true_pop=799,
                    false_pop=1515,
                    true_kept=50,
                    true_sampled=50,
                    false_kept=50,
                    false_sampled=50,
                ),
            ],
            total_true_errors_in_table=948,
        )
        assert projection.projected_precision == projection.baseline_precision
        assert projection.recall_retained == 1.0
        assert projection.true_errors_lost == 0.0


class TestCommittedWildColumnArtifact:
    """The measurement's limits are bound to fields, and the gating is enforced.

    The gate matters most: this measurement is authorised by the contamination audit, and an
    artifact claiming a result while the audit says CONTAMINATED would be exactly the failure
    the audit exists to prevent.
    """

    @pytest.fixture
    def artifact(self) -> dict:
        path = (
            Path(__file__).resolve().parents[2] / "eval" / "results" / "wild_column_detection.json"
        )
        if not path.exists():
            pytest.skip("wild column artifact not present in this checkout")
        return json.loads(path.read_text(encoding="utf-8"))

    def test_it_records_the_contamination_gate_it_passed(self, artifact: dict) -> None:
        gate = artifact["contamination_audit"]
        assert gate["status"] == "CLEAN", (
            "a wild-column result may only be published under a CLEAN or SUSPECTED audit; "
            f"got {gate['status']!r}"
        )
        assert gate["contamination_suspected"] is False
        # The audit's own weakness travels with the result that depends on it.
        assert "exchangeability_available" in gate

    def test_the_baseline_excludes_every_non_evaluable_detector(self, artifact: dict) -> None:
        """Including a frequency-dependent detector would corrupt the baseline."""
        from dataforge.bench.detection import DECLARED_APPLICABILITY, EVALUABLE_ON_DISTINCT_VALUES

        for name in artifact["baseline_detectors"]:
            assert DECLARED_APPLICABILITY[name] in EVALUABLE_ON_DISTINCT_VALUES, name
        for name in artifact["excluded_detectors"]:
            assert DECLARED_APPLICABILITY[name] not in EVALUABLE_ON_DISTINCT_VALUES, name
        assert set(artifact["baseline_detectors"]) | set(artifact["excluded_detectors"]) == set(
            DECLARED_APPLICABILITY
        ), "every declared detector must be either in the baseline or explicitly excluded"

    def test_the_unit_and_axis_are_recorded(self, artifact: dict) -> None:
        """L1 and L3: distinct values, detection only."""
        assert artifact["scoring_unit"] == "distinct_value"
        assert artifact["axis"] == "detection"
        assert artifact["scoring_spec"] == "specs/SPEC_abstention_scoring.md"

    def test_every_limit_appears_in_the_limitations_array(self, artifact: dict) -> None:
        joined = " ".join(str(item) for item in artifact["limitations"])
        for marker in ("L1:", "L2:", "L3:", "L4:", "L5:", "L6:", "L7:"):
            assert marker in joined, f"{marker} is missing from limitations"

    def test_the_census_and_sample_partition_each_corpus(self, artifact: dict) -> None:
        """89 debatable-only columns fell in neither stratum in a first draft of this probe."""
        expected = {"rt_bench": 1200, "st_bench": 1197}
        for corpus, result in artifact["per_corpus"].items():
            total = result["census_columns"] + result["unlabelled_population"]
            assert total == expected[corpus], (
                f"{corpus}: census {result['census_columns']} + unlabelled "
                f"{result['unlabelled_population']} != {expected[corpus]} columns"
            )

    def test_recall_is_reported_as_exact_and_precision_with_an_interval(
        self, artifact: dict
    ) -> None:
        """Recall is a census; precision is projected. The artifact must not blur them."""
        for result in artifact["per_corpus"].values():
            for arm in ("llm", "heuristic_evaluable"):
                entry = result[arm]
                if not entry.get("available"):
                    continue
                assert entry["recall_exact"] is not None
                low, high = entry["projected_precision_ci95"]
                assert low <= entry["projected_precision"] <= high

    def test_failed_calls_are_reported(self, artifact: dict) -> None:
        assert "failed_calls" in artifact
        assert artifact["calls"] > 0


class TestIdentity:
    """A filter that keeps everything must reproduce the baseline exactly."""

    def test_keeping_everything_reproduces_the_baseline(self) -> None:
        projection = project_queue_filter(
            [
                _stratum(
                    "a",
                    true_pop=799,
                    false_pop=1515,
                    true_kept=50,
                    true_sampled=50,
                    false_kept=50,
                    false_sampled=50,
                ),
            ],
            total_true_errors_in_table=948,
        )
        assert projection.projected_precision == projection.baseline_precision
        assert projection.recall_retained == 1.0
        assert projection.true_errors_lost == 0.0
