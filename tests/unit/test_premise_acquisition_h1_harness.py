"""Guard: the leave-one-table-out harness must be able to reach both verdicts.

H1 says no in-table measure transfers. The harness reported exactly that. A harness that
could only ever report that is worthless, so the tests below pin the falsifying path as
carefully as the supported one: given a measure that genuinely separates on every fold, K1
must fire and H1 must be reported as falsified.

The threshold rule is also pinned. It is fitted openly and generously on purpose -- the claim
is that even a well-fitted threshold does not transfer -- so a rule that quietly fitted badly
would manufacture the conclusion.
"""

from __future__ import annotations

from scripts.bench.measure_premise_acquisition_h1 import _apply, _fit_threshold


class TestThresholdFitting:
    def test_a_perfectly_separable_training_set_yields_a_separating_threshold(self) -> None:
        scored = [(0.9, True), (0.95, True), (0.1, False), (0.2, False)]
        fit = _fit_threshold(scored)

        assert fit["fitted"] is True
        assert fit["train_youden_j"] == 1.0
        assert fit["train_true_admitted"] == 2
        assert fit["train_false_admitted"] == 0
        assert 0.2 <= float(fit["threshold"]) < 0.9

    def test_ties_break_toward_the_more_conservative_threshold(self) -> None:
        """Equal J must pick the LARGER threshold, which admits fewer false dependencies.

        A premise gate should err toward refusing a spurious constraint, and the tie-break is
        the only place that preference is expressed. If it inverted, the harness would be
        systematically generous to false dependencies.
        """
        # Any threshold in [0.2, 0.9) achieves J = 1.0 here.
        scored = [(0.9, True), (0.1, False), (0.2, False)]
        fit = _fit_threshold(scored)

        assert fit["train_youden_j"] == 1.0
        # The largest tying threshold is the score just below the positive.
        assert float(fit["threshold"]) == 0.2

    def test_an_inseparable_training_set_still_fits_and_reports_a_poor_j(self) -> None:
        """Overlapping classes must not crash, and must not claim a good fit."""
        scored = [(0.5, True), (0.5, False), (0.6, True), (0.6, False)]
        fit = _fit_threshold(scored)

        assert fit["fitted"] is True
        assert float(fit["train_youden_j"]) == 0.0

    def test_a_single_class_training_fold_refuses_to_fit(self) -> None:
        """A fold with one label class can neither fit nor test, and must say so."""
        assert _fit_threshold([(0.9, True), (0.8, True)])["fitted"] is False
        assert _fit_threshold([])["fitted"] is False


class TestApplyingAThresholdToAHeldOutTable:
    def test_a_clean_fold_reports_separation(self) -> None:
        result = _apply(0.5, [(0.9, True), (0.1, False)])

        assert result["discarded_true"] == 0
        assert result["admitted_false"] == 0
        assert result["separates"] is True

    def test_losing_a_true_dependency_is_not_separation(self) -> None:
        """The column this result leans on: a gate that destroys a real constraint."""
        result = _apply(0.95, [(0.9, True), (0.1, False)])

        assert result["discarded_true"] == 1
        assert result["admitted_false"] == 0
        assert result["separates"] is False

    def test_admitting_a_false_dependency_is_not_separation(self) -> None:
        result = _apply(0.05, [(0.9, True), (0.1, False)])

        assert result["discarded_true"] == 0
        assert result["admitted_false"] == 1
        assert result["separates"] is False

    def test_the_gate_is_strictly_greater_than_the_threshold(self) -> None:
        """A candidate scoring exactly at the threshold is REFUSED, not admitted.

        Pinned because the artifact documents `score > threshold`, and an off-by-one on the
        boundary would silently change which dependencies carry write authority.
        """
        at_boundary = _apply(0.9, [(0.9, True), (0.9, False)])

        assert at_boundary["discarded_true"] == 1
        assert at_boundary["admitted_false"] == 0


class TestTheVerdictCanGoBothWays:
    """A verdict that can only come out one way is not a measurement."""

    def test_a_measure_that_transfers_is_detected_as_clean_on_every_fold(self) -> None:
        """K1's path: a measure that genuinely transfers must produce clean folds.

        Note what the fixture has to satisfy, because it is the whole finding in miniature:
        both tables must put the two label classes on the SAME SCALE. My first attempt used
        false scores of 0.10/0.20 in one table and 0.05/0.08 in the other, and the fold failed
        -- a threshold fitted on one admitted a false dependency in the other, even though each
        table was perfectly separable on its own. Per-table separability is not transfer, and
        real tables do not share a scale, which is why H1 stands.
        """
        table_a = [(0.99, True), (0.05, False), (0.06, False)]
        table_b = [(0.98, True), (0.05, False), (0.06, False)]

        # Hold out B: fit on A, test on B.
        fit_on_a = _fit_threshold(table_a)
        on_b = _apply(float(fit_on_a["threshold"]), table_b)
        # Hold out A: fit on B, test on A.
        fit_on_b = _fit_threshold(table_b)
        on_a = _apply(float(fit_on_b["threshold"]), table_a)

        assert on_b["separates"] is True, "a transferable measure must produce a clean fold"
        assert on_a["separates"] is True
        # This is the shape that would make K1 fire and falsify H1.

    def test_per_table_separability_is_not_transfer(self) -> None:
        """Each table separable alone, yet the fold fails. This is the measured situation.

        On the real corpus `mu_plus` separates perfectly on 4 of 10 tables, and its threshold
        barely moves (0.991374 on nine folds). What moves is the tables underneath it.
        """
        # Both tables are perfectly separable in isolation...
        table_a = [(0.99, True), (0.10, False), (0.20, False)]
        table_b = [(0.98, True), (0.15, False), (0.05, False)]
        assert min(s for s, t in table_a if t) > max(s for s, t in table_a if not t)
        assert min(s for s, t in table_b if t) > max(s for s, t in table_b if not t)

        # ...but a threshold fitted on B admits a false dependency in A.
        fit_on_b = _fit_threshold(table_b)
        on_a = _apply(float(fit_on_b["threshold"]), table_a)

        assert on_a["separates"] is False
        assert on_a["admitted_false"] == 1

    def test_a_measure_whose_scale_shifts_between_tables_fails_to_transfer(self) -> None:
        """The measured reality: the threshold barely moves and the tables move under it."""
        table_a = [(0.99, True), (0.10, False)]
        # Table B's TRUE dependency scores below table A's FALSE one.
        table_b = [(0.05, True), (0.99, False)]

        fit_on_a = _fit_threshold(table_a)
        on_b = _apply(float(fit_on_a["threshold"]), table_b)

        assert on_b["separates"] is False
        assert on_b["discarded_true"] == 1
        assert on_b["admitted_false"] == 1
