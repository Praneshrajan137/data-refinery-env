"""Premise-quality measures, tested against hand-derived arithmetic.

Every expected value below is computed **by hand from the closed form** in
`specs/SPEC_premise_quality.md` section 4, and the derivation is written out in the test so
a reader can check it without running anything. That matters more than usual here: the whole
point of `mu+` is that its decision point is 0 rather than a fitted constant, so a test that
merely restates whatever the implementation returns would verify nothing about the property
the pre-registration stakes its kill criteria on.

Written before the implementation, per `eval/preregistration/premise_quality_measure.md`.
"""

from __future__ import annotations

import pytest

from dataforge.premise_quality import g3_prime, mu_plus


def _groups(determinant: list[str], dependent: list[str]) -> dict[str, list[str]]:
    """Build the determinant-group mapping the miner already computes."""
    groups: dict[str, list[str]] = {}
    for det, dep in zip(determinant, dependent, strict=True):
        groups.setdefault(det, []).append(dep)
    return groups


class TestExactDependency:
    """An exact FD on a non-constant dependent must score 1.0."""

    def test_perfect_fd_scores_one(self) -> None:
        # N=4, X=[a,a,b,b], Y=[1,1,2,2], dom_X=2.
        #   pdep(X->Y) = (1/4)[ (1/2)(2^2) + (1/2)(2^2) ] = (1/4)(2+2) = 1.0
        #   pdep(Y)    = (1/16)(2^2 + 2^2) = 8/16 = 0.5
        #   mu = 1 - [(1-1.0)/(1-0.5)] * [(4-1)/(4-2)] = 1 - 0 = 1.0
        groups = _groups(["a", "a", "b", "b"], ["1", "1", "2", "2"])
        assert mu_plus(groups, ["1", "1", "2", "2"]) == pytest.approx(1.0)


class TestTheSingletonCorrection:
    """The property the whole measure exists for."""

    def test_all_singleton_groups_score_zero_and_do_not_raise(self) -> None:
        """|dom_X| == N makes the correction term divide by zero.

        This is not an exotic input. It is the near-key determinant the gate exists to
        reject, and the reference implementation raises here.
        """
        groups = _groups(["a", "b", "c", "d"], ["1", "2", "3", "4"])
        assert mu_plus(groups, ["1", "2", "3", "4"]) == 0.0

    def test_an_exact_dependency_scores_one_however_few_rows_could_falsify_it(self) -> None:
        """A limitation, discovered by this test falsifying my own assumption.

        The first version of this test asserted that a mostly-singleton determinant scores
        BELOW a well-grouped one on an exact dependency. It does not: both score exactly
        1.0. The reason is structural -- for an exact dependency `pdep(X->Y) == 1`, so the
        numerator `(1 - pdep(X->Y))` is zero and the singleton correction, however large, is
        multiplied by zero.

        So `mu+` discriminates among APPROXIMATE dependencies, not exact ones. An exact
        dependency resting on two testable rows is indistinguishable from one resting on a
        thousand. What guards that case is the pre-existing near-key rejection
        (`_MAX_DETERMINANT_UNIQUE_FRACTION`), which C3 does not remove and now must not be
        described as redundant.
        """
        # Grouped and exact: X=[a,a,b,b,c,c], dom_X=3, SUM_x max = 6, so pdep(X->Y) = 1.
        grouped = _groups(["a", "a", "b", "b", "c", "c"], ["1", "1", "2", "2", "3", "3"])
        # Sparse and exact: X=[a,a,b,c,d,e], dom_X=5. Four singleton groups each contribute
        # (1/1)(1^2) = 1 and the pair contributes (1/2)(2^2) = 2, so pdep(X->Y) = 6/6 = 1.
        sparse = _groups(["a", "a", "b", "c", "d", "e"], ["1", "1", "2", "3", "4", "5"])
        assert mu_plus(grouped, ["1", "1", "2", "2", "3", "3"]) == pytest.approx(1.0)
        assert mu_plus(sparse, ["1", "1", "2", "3", "4", "5"]) == pytest.approx(1.0)

    def test_with_violations_present_falsifiability_dominates_the_score(self) -> None:
        """The mechanism, demonstrated where it actually operates.

        Both candidates below have exactly ONE violation. They differ only in how many rows
        could have falsified them, and the scores differ by more than half the range. This
        is the ZipCode -> HospitalName case in miniature, and it is why the correction is
        worth having despite the exact-dependency blind spot above.
        """
        # Grouped: N=6, X=[a,a,b,b,c,c], Y=[1,2,2,2,3,3], dom_X=3.
        #   pdep(X->Y) = (1/6)[(1/2)(1+1) + (1/2)(4) + (1/2)(4)] = (1/6)(5) = 0.8333
        #   pdep(Y)    = (1/36)(1^2 + 3^2 + 2^2) = 14/36 = 0.3889
        #   mu = 1 - [(1-0.8333)/(1-0.3889)] * [(6-1)/(6-3)] = 1 - 0.27273*1.6667 = 0.5455
        grouped_dependent = ["1", "2", "2", "2", "3", "3"]
        grouped = _groups(["a", "a", "b", "b", "c", "c"], grouped_dependent)
        grouped_score = mu_plus(grouped, grouped_dependent)
        assert grouped_score == pytest.approx(0.5455, abs=1e-4)

        # Sparse: N=6, X=[a,a,b,c,d,e], Y=[1,2,2,3,4,5], dom_X=5.
        #   pdep(X->Y) = (1/6)[(1/2)(1+1) + 1 + 1 + 1 + 1] = (1/6)(5) = 0.8333
        #   pdep(Y)    = (1/36)(1 + 4 + 1 + 1 + 1) = 8/36 = 0.2222
        #   mu = 1 - [(1-0.8333)/(1-0.2222)] * [(6-1)/(6-5)] = 1 - 0.21429*5 = -0.0714 -> 0
        sparse_dependent = ["1", "2", "2", "3", "4", "5"]
        sparse = _groups(["a", "a", "b", "c", "d", "e"], sparse_dependent)
        assert mu_plus(sparse, sparse_dependent) == 0.0

        assert grouped_score > 0.5, (
            "identical violation counts must not score identically when one dependency was "
            "far less falsifiable than the other"
        )


class TestTheNullIsCalibratedAtZero:
    """mu == 0 exactly when the determinant adds nothing, without any fitted constant."""

    def test_single_group_determinant_scores_zero(self) -> None:
        # N=4, X=[a,a,a,a], Y=[1,1,1,2], dom_X=1.
        #   pdep(X->Y) = (1/4)[(1/4)(3^2 + 1^2)] = (1/4)(10/4) = 0.625
        #   pdep(Y)    = (1/16)(3^2 + 1^2) = 10/16 = 0.625
        #   mu = 1 - [(1-0.625)/(1-0.625)] * [(4-1)/(4-1)] = 1 - 1 = 0.0
        dependent = ["1", "1", "1", "2"]
        groups = _groups(["a", "a", "a", "a"], dependent)
        assert mu_plus(groups, dependent) == pytest.approx(0.0)

    def test_information_below_chance_clamps_to_zero(self) -> None:
        # N=6, X=[a,a,a,b,b,b], Y=[1,1,2,2,2,1], dom_X=2.
        #   pdep(X->Y) = (1/6)[(1/3)(2^2+1^2) + (1/3)(2^2+1^2)] = (1/6)(10/3) = 0.5556
        #   pdep(Y)    = (1/36)(3^2 + 3^2) = 0.5
        #   mu = 1 - [(1-0.5556)/(1-0.5)] * [(6-1)/(6-2)]
        #      = 1 - (0.8889 * 1.25) = 1 - 1.1111 = -0.1111  -> clamped to 0.0
        dependent = ["1", "1", "2", "2", "2", "1"]
        groups = _groups(["a", "a", "a", "b", "b", "b"], dependent)
        assert mu_plus(groups, dependent) == 0.0


class TestDegenerateInputsReturnZeroRatherThanRaising:
    """Each of these divides by zero in the closed form."""

    def test_constant_dependent_scores_zero(self) -> None:
        """pdep(Y) == 1 zeroes the left denominator.

        The miner rejects constant dependents upstream; this must not depend on that.
        """
        dependent = ["1", "1", "1", "1"]
        groups = _groups(["a", "a", "b", "b"], dependent)
        assert mu_plus(groups, dependent) == 0.0

    def test_single_row_scores_zero(self) -> None:
        groups = _groups(["a"], ["1"])
        assert mu_plus(groups, ["1"]) == 0.0

    def test_empty_input_scores_zero(self) -> None:
        assert mu_plus({}, []) == 0.0


class TestInvariants:
    """Properties that must hold for any input."""

    @pytest.mark.parametrize(
        "determinant,dependent",
        [
            (["a", "a", "b", "b"], ["1", "1", "2", "2"]),
            (["a", "a", "a", "b", "b", "b"], ["1", "1", "2", "2", "2", "1"]),
            (["a", "a", "b", "c", "d", "e"], ["1", "1", "2", "3", "4", "5"]),
            (["x", "x", "x", "y", "y", "z", "z", "z"], ["p", "p", "q", "q", "q", "r", "r", "p"]),
        ],
    )
    def test_score_is_in_the_unit_interval(
        self, determinant: list[str], dependent: list[str]
    ) -> None:
        score = mu_plus(_groups(determinant, dependent), dependent)
        assert 0.0 <= score <= 1.0

    def test_row_order_does_not_change_the_score(self) -> None:
        determinant = ["a", "a", "a", "b", "b", "c"]
        dependent = ["1", "1", "2", "2", "2", "3"]
        forward = mu_plus(_groups(determinant, dependent), dependent)
        reverse = mu_plus(
            _groups(list(reversed(determinant)), list(reversed(dependent))),
            list(reversed(dependent)),
        )
        assert forward == pytest.approx(reverse)

    def test_relabelling_values_does_not_change_the_score(self) -> None:
        """The measure depends on the partition, not on the labels."""
        base = mu_plus(
            _groups(["a", "a", "b", "b", "c"], ["1", "1", "2", "2", "3"]),
            ["1", "1", "2", "2", "3"],
        )
        relabelled = mu_plus(
            _groups(["Q", "Q", "R", "R", "S"], ["x", "x", "y", "y", "z"]),
            ["x", "x", "y", "y", "z"],
        )
        assert base == pytest.approx(relabelled)


class TestG3Prime:
    """Reported, never gated. Included so the choice of mu+ stays auditable."""

    def test_perfect_fd_scores_one(self) -> None:
        # N=4, X=[a,a,b,b]: SUM_x max_y c_xy = 2 + 2 = 4, dom_X = 2.
        #   g3' = (4 - 2) / (4 - 2) = 1.0
        groups = _groups(["a", "a", "b", "b"], ["1", "1", "2", "2"])
        assert g3_prime(groups) == pytest.approx(1.0)

    def test_all_singletons_score_zero_and_do_not_raise(self) -> None:
        """The |dom_X| == N floor that Giannella and Robertson subtract."""
        groups = _groups(["a", "b", "c", "d"], ["1", "2", "3", "4"])
        assert g3_prime(groups) == 0.0

    def test_a_violation_lowers_the_score(self) -> None:
        # N=6, X=[a,a,a,b,b,b], Y=[1,1,2,2,2,1]:
        #   SUM_x max_y c_xy = 2 + 2 = 4, dom_X = 2
        #   g3' = (4 - 2) / (6 - 2) = 0.5
        groups = _groups(["a", "a", "a", "b", "b", "b"], ["1", "1", "2", "2", "2", "1"])
        assert g3_prime(groups) == pytest.approx(0.5)

    def test_empty_input_scores_zero(self) -> None:
        assert g3_prime({}) == 0.0
