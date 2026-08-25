"""Tests for the stratified label-noise bound.

Offline arithmetic. The tests that matter encode the reason this function exists: pooling
heterogeneous control classes changes whether a pre-registered kill criterion fires, so the
stratified bound must be the one that binds and the pooled figure must be incapable of driving a
decision.

Real numbers throughout, from `docs/trust/local-certification-result.md`:
`column_distribution` 2/30, `corrector_generated` 4/8.
"""

from __future__ import annotations

import pytest

from dataforge.conformal import (
    _min_samples_given_beta,
    label_noise_adjusted_bound,
    label_noise_adjusted_bound_stratified,
)

#: The two measured control classes. A 7.5x gap in raw rate: 0.0667 against 0.5000.
_REAL = {"column_distribution": (2, 30), "corrector_generated": (4, 8)}


class TestPoolingChangesTheDecision:
    """The finding this function was built for."""

    def test_the_worst_class_binds_not_the_average(self) -> None:
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL)
        assert bound.binding_class == "corrector_generated"
        assert bound.beta_upper == pytest.approx(0.8712, abs=0.001)
        assert bound.beta_upper > bound.pooled_beta_upper

    def test_the_kill_criterion_fires_stratified_and_not_pooled(self) -> None:
        """The consequence, stated as an assertion.

        Pre-registered in eval/preregistration/human_label_noise.md: beta_upper > 0.35 means
        per-table certification at alpha=0.05 is dead. Pooling kept it nominally alive.
        """
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL)
        assert bound.kill_criterion_fires(0.35) is True
        assert bound.pooled_beta_upper <= 0.35, (
            "if the pooled bound ever exceeds 0.35 this test loses its point, but the finding "
            "would then be moot rather than wrong"
        )

    def test_the_pooled_figure_matches_the_old_pooled_path(self) -> None:
        """So the comparison in the artifact is against what was actually being reported."""
        _, pooled_beta, _ = label_noise_adjusted_bound(2, 60, false_accepts=6, controls=38)
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL)
        assert bound.pooled_beta_upper == pytest.approx(pooled_beta, abs=1e-9)

    def test_heterogeneity_compares_the_classes_to_each_other(self) -> None:
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL)
        assert bound.heterogeneity_ratio == pytest.approx(3.56, abs=0.05)
        assert bound.pooling_would_have_understated is True

    def test_the_stratified_vs_pooled_ratio_is_reported_separately(self) -> None:
        """It contains the union penalty, so it is not a heterogeneity measure."""
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL)
        assert bound.stratified_vs_pooled_ratio == pytest.approx(2.79, abs=0.02)

    def test_identical_classes_show_no_heterogeneity(self) -> None:
        """The case that caught the first version of the metric.

        Two classes agreeing exactly still make the stratified bound wider than the pooled one,
        because pooling gains sample size while stratifying pays a union correction. That is not
        heterogeneity, and the two quantities are now separate properties.
        """
        bound = label_noise_adjusted_bound_stratified(
            2, 60, controls_by_class={"a": (3, 30), "b": (3, 30)}
        )
        assert bound.beta_by_class["a"] == bound.beta_by_class["b"]
        assert bound.heterogeneity_ratio == 1.0
        assert bound.pooling_would_have_understated is False
        assert bound.stratified_vs_pooled_ratio > 1.0, (
            "stratifying is still wider on identical classes; that is the union penalty, "
            "not disagreement"
        )


class TestTheUnionCorrection:
    """Adding a class costs power, which removes any incentive to split one."""

    def test_splitting_a_class_cannot_soften_the_bound(self) -> None:
        """Half the controls each, same rate: the split bound must not be tighter."""
        single = label_noise_adjusted_bound_stratified(2, 60, controls_by_class={"all": (4, 40)})
        split = label_noise_adjusted_bound_stratified(
            2, 60, controls_by_class={"a": (2, 20), "b": (2, 20)}
        )
        assert split.beta_upper > single.beta_upper

    def test_more_classes_widen_each_bound(self) -> None:
        two = label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL)
        four = label_noise_adjusted_bound_stratified(
            2,
            60,
            controls_by_class={**_REAL, "c": (2, 30), "d": (2, 30)},
        )
        assert four.beta_by_class["column_distribution"] > two.beta_by_class["column_distribution"]

    def test_a_single_class_is_wider_than_the_unstratified_primitive(self) -> None:
        """One class still pays delta/2 divided by one, so it matches the primitive."""
        _, primitive_beta, _ = label_noise_adjusted_bound(2, 60, false_accepts=4, controls=8)
        single = label_noise_adjusted_bound_stratified(2, 60, controls_by_class={"only": (4, 8)})
        assert single.beta_upper == pytest.approx(primitive_beta, abs=1e-9)


class TestRefusals:
    """Nothing may silently substitute for a missing control class."""

    def test_no_classes_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one control class"):
            label_noise_adjusted_bound_stratified(2, 60, controls_by_class={})

    def test_a_class_with_zero_controls_raises_rather_than_being_dropped(self) -> None:
        """A dropped class cannot bind, and its absence reads as evidence of low noise."""
        with pytest.raises(ValueError, match="might bind"):
            label_noise_adjusted_bound_stratified(
                2, 60, controls_by_class={"a": (2, 30), "empty": (0, 0)}
            )

    def test_more_false_accepts_than_controls_raises(self) -> None:
        with pytest.raises(ValueError, match="false_accepts must be in"):
            label_noise_adjusted_bound_stratified(2, 60, controls_by_class={"a": (5, 4)})

    def test_nonpositive_n_raises(self) -> None:
        with pytest.raises(ValueError, match="n must be positive"):
            label_noise_adjusted_bound_stratified(0, 0, controls_by_class=_REAL)

    def test_delta_outside_the_unit_interval_raises(self) -> None:
        with pytest.raises(ValueError, match="delta must be in"):
            label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL, delta=1.5)


class TestArithmetic:
    """The adjustment itself, and its saturation."""

    def test_a_beta_at_one_saturates_the_bound(self) -> None:
        """One false accept out of one control says nothing, so the bound is vacuous."""
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class={"a": (1, 1)})
        assert bound.beta_upper == 1.0
        assert bound.adjusted_bound == 1.0

    def test_zero_false_accepts_still_yields_a_nonzero_beta(self) -> None:
        """Clean controls bound beta below 1 but never at 0; n is finite."""
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class={"a": (0, 30)})
        assert 0.0 < bound.beta_upper < 1.0
        assert bound.adjusted_bound > bound.measured_bound

    def test_the_adjustment_inflates_the_measured_bound(self) -> None:
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL)
        assert bound.adjusted_bound > bound.measured_bound
        assert bound.adjusted_bound == pytest.approx(
            bound.measured_bound / (1 - bound.beta_upper), abs=1e-9
        )

    def test_the_delta_it_used_is_recorded(self) -> None:
        bound = label_noise_adjusted_bound_stratified(2, 60, controls_by_class=_REAL, delta=0.1)
        assert bound.delta == 0.1


class TestTheInflationFactorDoesNotCreateAnAsymptote:
    """A correction to a claim this project published three times.

    ``docs/trust/stratified-label-noise-result.md``, ``DECISIONS.md`` and
    ``eval/preregistration/blind_elicitation.md`` all asserted that an inflation factor of
    ``1/(1 - 0.8712) = 7.76`` put ``alpha = 0.05`` "out of reach at any sample size". It does not.
    The measured bound ``1 - (delta/2)^(1/n)`` tends to zero as ``n`` grows, so
    ``measured / (1 - beta)`` can be driven below any positive alpha; the factor multiplies the
    sample cost and nothing more.

    The pre-registered kill criterion is unaffected -- it was defined as "alpha=0.05 unreachable
    inside ~200 judgements", and the real floor is 572 -- so the verdict stands on budget rather
    than on impossibility. These tests exist so the corrected numbers cannot drift back.
    """

    #: Computed at the measured binding beta with ``delta = 0.05`` and zero observed errors.
    EXPECTED = {0.01: 2863, 0.05: 572, 0.10: 285, 0.20: 142, 0.30: 94, 0.50: 56}

    def _measured_beta(self) -> float:
        bound = label_noise_adjusted_bound_stratified(
            0,
            1,
            controls_by_class={"column_distribution": (2, 30), "corrector_generated": (4, 8)},
        )
        assert bound.beta_upper == pytest.approx(0.8712332766265144, abs=1e-12)
        return bound.beta_upper

    def test_every_alpha_is_reachable_at_a_finite_sample_size(self) -> None:
        beta = self._measured_beta()
        for alpha in self.EXPECTED:
            floor = _min_samples_given_beta(alpha, beta_upper=beta, delta=0.05)
            assert floor is not None, (
                f"alpha={alpha} reported unreachable at beta={beta}. An inflation factor "
                "multiplies the sample cost; it cannot make a positive alpha unreachable"
            )

    def test_the_published_alpha_table_is_exact(self) -> None:
        beta = self._measured_beta()
        measured = {
            alpha: _min_samples_given_beta(alpha, beta_upper=beta, delta=0.05)
            for alpha in self.EXPECTED
        }
        assert measured == self.EXPECTED

    def test_the_kill_criterion_still_fires_on_budget(self) -> None:
        """The verdict must not depend on the arithmetic error that was corrected."""
        beta = self._measured_beta()
        floor = _min_samples_given_beta(0.05, beta_upper=beta, delta=0.05)
        assert floor is not None and floor > 200, (
            "the criterion was pre-registered as 'alpha=0.05 unreachable inside ~200 judgements'; "
            "if the floor ever falls inside 200 the kill criterion no longer fires and the "
            "published verdict must be revisited rather than reasserted"
        )

    def test_the_factor_multiplies_the_cost_by_roughly_itself(self) -> None:
        """Why the floor rose to 572: the sample cost scales with 1/(1-beta), it does not diverge.

        The agreement is approximate, not exact. Both floors are integers produced by a ceiling, so
        the ratio carries up to one label of rounding at each end -- 572/72 is 7.944 against a factor
        of 7.766, a 2.3% gap. The point being pinned is the *scaling law*, not an identity: an
        inflation factor buys proportionally more labels rather than an unreachable target.
        """
        beta = self._measured_beta()
        negligible = _min_samples_given_beta(0.05, beta_upper=0.0, delta=0.05)
        inflated = _min_samples_given_beta(0.05, beta_upper=beta, delta=0.05)
        assert negligible is not None and inflated is not None
        ratio = inflated / negligible
        assert 1.0 / (1.0 - beta) == pytest.approx(ratio, rel=0.05), (
            "the ratio of sample floors should track the inflation factor"
        )

    def test_alpha_020_is_reachable_but_needs_a_flawless_run(self) -> None:
        """The fallback DECISIONS.md names is arithmetically live and practically blocked.

        142 labels is inside the ~200 budget, so alpha=0.20 is not dead on sample size. Every one
        of the 142 must carry zero observed errors, and they come from the labeller whose measured
        false-accept rate is bounded at 0.8712 -- one who accepted 4 of 8 planted wrong proposals.
        """
        beta = self._measured_beta()
        assert _min_samples_given_beta(0.20, beta_upper=beta, delta=0.05) == 142
        assert 142 < 200, "the fallback is inside the pre-registered budget"
