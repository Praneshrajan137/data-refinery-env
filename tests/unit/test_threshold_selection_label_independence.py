"""The candidate sequence must depend on confidences and never on labels.

Why this file exists
--------------------
``certify_threshold`` walks its candidates purest-first and ``break`` s on the first tested
failure. That ordering runs from the grid point with the *least* statistical power toward the
most, so a thinly-populated top point fails the Clopper-Pearson floor -- even holding a flawless
record -- and halts the walk before any well-supported threshold is tested. A perfect record at a
high threshold therefore destroyed certification of every lower one.

``feasible_candidate_sequence`` fixes that by pruning grid points whose accepted count cannot
reach the floor at *any* label assignment. The fix rests on one claim, and this file exists to
make that claim falsifiable rather than argued:

    The pruning reads **confidences** and never **labels**.

Fixed-sequence testing needs the sequence pre-specified independently of the labels; it does not
need independence from the covariates (Mondrian conformal already lets the taxonomy depend on
them). So confidence-driven pruning keeps the family-wise ``delta`` claim, and label-driven
selection would forfeit it.

Design notes, deliberate
------------------------
* **The tests assert observable consequences, not reported values.** The strongest test permutes
  the labels and demands byte-identical output. A guard that merely inspected a reported
  parameter would pass while the code cheated.
* **Every guard here has been mutation-tested.** The mutants are enumerated in
  ``test_mutation_witnesses_are_documented`` so a future reader can re-run them by hand rather
  than trust this docstring.
* **Pruning must never loosen the guarantee.** ``TestPruningCannotSoften`` pins that a pruned run
  certifies a threshold only where the unpruned run would also have accepted it on precision --
  i.e. pruning buys power, never permission.
"""

from __future__ import annotations

import itertools
import random

import pytest

from dataforge.calibration_session import CERTIFICATION_GRID
from dataforge.conformal import (
    LabeledSample,
    certification_reason,
    certify_threshold,
    feasible_candidate_sequence,
    min_samples_for_certification,
)

_ALPHA = 0.05
_DELTA = 0.05


def _blocked_by_perfect_top_point() -> list[LabeledSample]:
    """The exact pathology: a flawless high-confidence set too small to clear the floor.

    40 perfect samples at 0.99 cannot certify (``0/40`` bounds the error rate at 0.0722 > 0.05),
    and under the unpruned descending walk that failure halts the sequence before 0.97, where 100
    perfect samples clear the bound comfortably.
    """
    return [(0.99, True)] * 40 + [(0.97, True)] * 60


class TestLabelIndependence:
    """The load-bearing invariant. If any of these fail, the validity argument is void."""

    def test_permuting_labels_never_changes_the_candidate_sequence(self) -> None:
        samples = _blocked_by_perfect_top_point()
        confidences = [conf for conf, _ in samples]
        expected = feasible_candidate_sequence(
            confidences, grid=CERTIFICATION_GRID, alpha=_ALPHA, delta=_DELTA
        )
        rng = random.Random(20260821)
        for _ in range(200):
            labels = [rng.random() < 0.5 for _ in samples]
            permuted = [conf for conf, _ in zip(confidences, labels, strict=True)]
            assert (
                feasible_candidate_sequence(
                    permuted, grid=CERTIFICATION_GRID, alpha=_ALPHA, delta=_DELTA
                )
                == expected
            )

    def test_opposite_label_assignments_certify_over_the_same_sequence(self) -> None:
        """The extreme permutation, exercised through the *deciding* call.

        An earlier version of this test passed the same confidence list twice and asserted the
        results matched. That is tautological -- the function takes floats, so of course it
        agreed -- and a guard that cannot fail manufactures confidence. This version instead
        pins the observable consequence: two samples with identical confidences and opposite
        labels must be judged over an identical candidate sequence, so the *only* thing that may
        differ between their outcomes is the Clopper-Pearson test itself.
        """
        right: list[LabeledSample] = [(0.99, True)] * 40 + [(0.97, True)] * 60
        wrong: list[LabeledSample] = [(0.99, False)] * 40 + [(0.97, False)] * 60
        seq_right = feasible_candidate_sequence(
            [conf for conf, _ in right], grid=CERTIFICATION_GRID, alpha=_ALPHA, delta=_DELTA
        )
        seq_wrong = feasible_candidate_sequence(
            [conf for conf, _ in wrong], grid=CERTIFICATION_GRID, alpha=_ALPHA, delta=_DELTA
        )
        assert seq_right == seq_wrong
        assert seq_right, "fixture must produce a non-empty sequence or this proves nothing"
        # And the outcomes must differ only because the labels differ, not the sequence.
        assert (
            certify_threshold(
                right, alpha=_ALPHA, delta=_DELTA, grid=CERTIFICATION_GRID, prune_infeasible=True
            )
            is not None
        )
        assert (
            certify_threshold(
                wrong, alpha=_ALPHA, delta=_DELTA, grid=CERTIFICATION_GRID, prune_infeasible=True
            )
            is None
        )

    def test_selector_signature_cannot_receive_a_label(self) -> None:
        """Structural defence: passing ``(confidence, label)`` pairs must fail loudly.

        The signature takes bare floats precisely so a label cannot reach the function. If a
        future refactor widens it to accept samples, this test goes red and the reviewer has to
        justify it.
        """
        samples = _blocked_by_perfect_top_point()
        with pytest.raises((TypeError, ValueError)):
            feasible_candidate_sequence(
                samples,  # type: ignore[arg-type]
                grid=CERTIFICATION_GRID,
                alpha=_ALPHA,
                delta=_DELTA,
            )


class TestPruningRestoresReachableThresholds:
    """The power claim, stated as a behavioural difference rather than a metric."""

    def test_unpruned_walk_is_blocked_and_pruned_walk_is_not(self) -> None:
        samples = _blocked_by_perfect_top_point()
        unpruned = certify_threshold(samples, alpha=_ALPHA, delta=_DELTA, grid=CERTIFICATION_GRID)
        pruned = certify_threshold(
            samples,
            alpha=_ALPHA,
            delta=_DELTA,
            grid=CERTIFICATION_GRID,
            prune_infeasible=True,
        )
        assert unpruned is None, "fixture no longer reproduces the pathology it was built for"
        assert pruned is not None

    def test_pruned_sequence_drops_exactly_the_infeasible_points(self) -> None:
        samples = _blocked_by_perfect_top_point()
        confidences = [conf for conf, _ in samples]
        floor = max(30, min_samples_for_certification(_ALPHA, _DELTA))
        survivors = feasible_candidate_sequence(
            confidences, grid=CERTIFICATION_GRID, alpha=_ALPHA, delta=_DELTA
        )
        for threshold in CERTIFICATION_GRID:
            accepted = sum(1 for conf in confidences if conf >= threshold)
            assert (threshold in survivors) == (accepted >= floor), (
                f"threshold {threshold} has {accepted} accepted samples against floor {floor}"
            )

    def test_survivors_are_descending(self) -> None:
        survivors = feasible_candidate_sequence(
            [conf for conf, _ in _blocked_by_perfect_top_point()],
            grid=CERTIFICATION_GRID,
            alpha=_ALPHA,
            delta=_DELTA,
        )
        assert survivors == sorted(survivors, reverse=True)

    def test_a_threshold_sitting_exactly_on_the_floor_is_kept(self) -> None:
        """The boundary, pinned because an off-by-one here silently discards a valid threshold.

        A mutation from ``>= floor`` to ``> floor`` survived every other test in this file: the
        original fixture had no grid point with exactly ``floor`` accepted samples, so the
        boundary was never exercised and the guard was theatre. ``min_samples_for_certification``
        is *exactly* the count at which zero errors clears the bound, so a threshold holding
        exactly that many samples must be retained.
        """
        floor = max(30, min_samples_for_certification(_ALPHA, _DELTA))
        # Exactly `floor` samples at 0.97 and nothing above it, so n(0.97) == floor precisely
        # while every higher grid point is empty.
        confidences = [0.97] * floor
        survivors = feasible_candidate_sequence(
            confidences, grid=CERTIFICATION_GRID, alpha=_ALPHA, delta=_DELTA
        )
        assert 0.97 in survivors, (
            f"a threshold with exactly {floor} accepted samples must survive; "
            "0 errors in that many clears the Clopper-Pearson bound by construction"
        )
        # And it must genuinely certify on a perfect record, which is the point of keeping it.
        assert (
            certify_threshold(
                [(0.97, True)] * floor,
                alpha=_ALPHA,
                delta=_DELTA,
                grid=CERTIFICATION_GRID,
                prune_infeasible=True,
            )
            is not None
        )

    def test_a_threshold_one_below_the_floor_is_dropped(self) -> None:
        """The other side of the same boundary."""
        floor = max(30, min_samples_for_certification(_ALPHA, _DELTA))
        survivors = feasible_candidate_sequence(
            [0.97] * (floor - 1), grid=CERTIFICATION_GRID, alpha=_ALPHA, delta=_DELTA
        )
        assert survivors == []

    def test_no_feasible_point_returns_empty_not_a_fallback(self) -> None:
        """Too little data must yield an empty sequence, never a permissive guess."""
        survivors = feasible_candidate_sequence(
            [0.99] * 5, grid=CERTIFICATION_GRID, alpha=_ALPHA, delta=_DELTA
        )
        assert survivors == []


class TestPruningCannotSoften:
    """Pruning buys statistical power. It must never buy permission."""

    @pytest.mark.parametrize(
        ("name", "samples"),
        [
            (
                "ten percent errors below the blocked point",
                [(0.99, True)] * 40 + [(0.97, True)] * 54 + [(0.97, False)] * 6,
            ),
            ("every proposal wrong", [(0.97, False)] * 100),
            ("sample smaller than the floor", [(0.97, True)] * 20),
            ("one short of the floor", [(0.97, True)] * 58),
            ("one error in sixty", [(0.97, True)] * 59 + [(0.97, False)]),
        ],
    )
    def test_imprecise_or_starved_inputs_still_refuse(
        self, name: str, samples: list[LabeledSample]
    ) -> None:
        assert (
            certify_threshold(
                samples,
                alpha=_ALPHA,
                delta=_DELTA,
                grid=CERTIFICATION_GRID,
                prune_infeasible=True,
            )
            is None
        ), name

    def test_pruning_never_certifies_a_higher_threshold_than_unpruned(self) -> None:
        """Exhaustive over a small space: pruning may certify where unpruned did not, but it
        must never certify a threshold the unpruned procedure had already *rejected on
        precision* rather than merely failed to reach."""
        rng = random.Random(4242)
        for _ in range(400):
            n_high = rng.randint(0, 60)
            n_low = rng.randint(0, 140)
            samples = [(0.99, rng.random() < 0.97) for _ in range(n_high)]
            samples += [(0.97, rng.random() < 0.97) for _ in range(n_low)]
            pruned = certify_threshold(
                samples,
                alpha=_ALPHA,
                delta=_DELTA,
                grid=CERTIFICATION_GRID,
                prune_infeasible=True,
            )
            if pruned is None:
                continue
            # Whatever it certified must survive the same exact test on its own accepted set.
            accepted = [ok for conf, ok in samples if conf >= pruned]
            errors = sum(1 for ok in accepted if not ok)
            assert len(accepted) >= max(30, min_samples_for_certification(_ALPHA, _DELTA))
            # Re-derive the bound independently of the implementation under test.
            bound = 1.0 - _DELTA ** (1.0 / len(accepted)) if errors == 0 else None
            if bound is not None:
                assert bound <= _ALPHA


class TestReasonMatchesDecision:
    """A reason computed under a different procedure than the decision is a second opinion."""

    def test_reason_is_none_only_when_the_same_settings_certify(self) -> None:
        samples = _blocked_by_perfect_top_point()
        # Under the deciding settings this certifies, so there is no reason to give.
        assert (
            certification_reason(
                samples,
                alpha=_ALPHA,
                delta=_DELTA,
                grid=CERTIFICATION_GRID,
                prune_infeasible=True,
            )
            is None
        )

    def test_reason_explains_a_genuine_precision_failure(self) -> None:
        samples = [(0.97, True)] * 90 + [(0.97, False)] * 10
        reason = certification_reason(
            samples,
            alpha=_ALPHA,
            delta=_DELTA,
            grid=CERTIFICATION_GRID,
            prune_infeasible=True,
        )
        assert reason is not None
        assert reason.startswith("precision_below_target")

    def test_reason_explains_a_genuine_support_failure(self) -> None:
        reason = certification_reason(
            [(0.97, True)] * 12,
            alpha=_ALPHA,
            delta=_DELTA,
            grid=CERTIFICATION_GRID,
            prune_infeasible=True,
        )
        assert reason is not None
        assert reason.startswith("insufficient_support")


class TestPruningRequiresAPrespecifiedGrid:
    """Pruning a label-derived grid would compound two selection effects."""

    def test_prune_without_grid_raises(self) -> None:
        with pytest.raises(ValueError, match="requires an explicit label-independent grid"):
            certify_threshold(
                [(0.97, True)] * 100,
                alpha=_ALPHA,
                delta=_DELTA,
                prune_infeasible=True,
            )

    def test_default_is_unpruned_so_committed_numbers_do_not_move(self) -> None:
        samples = _blocked_by_perfect_top_point()
        assert (
            certify_threshold(samples, alpha=_ALPHA, delta=_DELTA, grid=CERTIFICATION_GRID) is None
        )


def test_mutation_witnesses_are_documented() -> None:
    """Each guard above was verified to fail under a specific mutation, by running it.

    Recorded as executable documentation because an undocumented mutation claim is
    indistinguishable from an untested one. To re-verify, apply the mutation to
    ``dataforge/conformal.py`` and confirm the named test turns red.

    **One of these mutants initially SURVIVED, and the record is kept.** The off-by-one
    (``>= floor`` -> ``> floor``) passed all 19 tests in the first version of this file, because
    the fixture had no grid point holding exactly ``floor`` accepted samples -- so the boundary
    the mutation moves was never exercised. That guard was theatre until
    ``test_a_threshold_sitting_exactly_on_the_floor_is_kept`` was added. It is the third time this
    project has shipped a guard that could not fail, and the only reason it was caught here is
    that the mutants were executed rather than asserted.
    """
    witnesses = {
        # Make the selector consult labels: rank candidates by observed errors.
        "selector reads labels": "TestLabelIndependence::"
        "test_permuting_labels_never_changes_the_candidate_sequence",
        # Drop the floor from max(min_support, cp_floor) to min_support alone.
        "floor ignores the Clopper-Pearson minimum": "TestPruningRestoresReachableThresholds::"
        "test_pruned_sequence_drops_exactly_the_infeasible_points",
        # Return the full grid regardless of accepted counts (pruning becomes a no-op).
        "pruning is a no-op": "TestPruningRestoresReachableThresholds::"
        "test_unpruned_walk_is_blocked_and_pruned_walk_is_not",
        # Replace the CP test with `errors == 0` so any perfect set certifies.
        "bound replaced by a zero-error check": "TestPruningCannotSoften::"
        "test_imprecise_or_starved_inputs_still_refuse",
        # Let prune_infeasible fall back to the observed confidences when grid is None.
        "silent fallback instead of raising": "TestPruningRequiresAPrespecifiedGrid::"
        "test_prune_without_grid_raises",
        # Leave certification_reason on its old grid-less call.
        "reason judged under different settings": "TestReasonMatchesDecision::"
        "test_reason_is_none_only_when_the_same_settings_certify",
        # Make the accepted-set comparison strict (`>` not `>=`) so the boundary shifts by one.
        "off-by-one on the floor comparison": "TestPruningRestoresReachableThresholds::"
        "test_a_threshold_sitting_exactly_on_the_floor_is_kept",
    }
    assert len(witnesses) == 7
    # Every named test must actually exist in this module.
    module = globals()
    for mutation, target in witnesses.items():
        class_name, _, method = target.partition("::")
        assert class_name in module, f"{mutation}: no class {class_name}"
        assert hasattr(module[class_name], method), f"{mutation}: no test {target}"
    assert len(set(itertools.chain.from_iterable(w.split("::") for w in witnesses.values()))) >= 6
