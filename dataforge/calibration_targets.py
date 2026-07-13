"""Ground-truth-derived calibration targets for repair proposals.

The measured failure of exact-value correction is *miscalibration*, not raw
accuracy (frontier teachers show ECE 0.82-0.96). A model can only learn to be
calibrated if its supervised confidence target is trustworthy. The only
trustworthy target is one derived from **ground truth**, never from a teacher's
self-reported confidence -- distilling a miscalibrated teacher would reproduce
the very failure we are trying to fix.

This module derives, for each proposed cell repair, the confidence a
well-calibrated model *should* have assigned and whether it *should* have
abstained. It is pure and deterministic: no model, no network, no I/O. The
inferability class (already labelled in the trajectory schema) decides whether a
cell is answerable from the prompt at all; abstention is the correct behaviour
on the non-inferable slices.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from pydantic import BaseModel, Field

from dataforge.evaluation_contract import ABSTENTION_SLICES, InferabilityLabel

__all__ = [
    "CalibrationTarget",
    "calibration_samples",
    "derive_cell_target",
    "derive_targets_for_fixes",
    "strict_cell_value",
]


def strict_cell_value(value: str) -> str:
    """Official exact-match value normalization.

    Mirrors ``repair_contract._strict_cell_value`` so target derivation and
    scoring never disagree about what counts as a match.
    """
    return str(value).rstrip()


class CalibrationTarget(BaseModel):
    """The confidence/abstention a well-calibrated model should have produced.

    ``target_confidence`` is the confidence that *should* attach to the proposed
    value: 1.0 when the proposal exactly matches ground truth, 0.0 otherwise.
    ``should_abstain`` is True on the non-inferable slices, where the correct
    behaviour is to propose nothing rather than guess.
    """

    inferability: InferabilityLabel
    correct: bool
    should_abstain: bool
    target_confidence: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(min_length=1)

    model_config = {"frozen": True}


def derive_cell_target(
    *,
    proposed_value: str | None,
    clean_value: str | None,
    inferability: InferabilityLabel,
) -> CalibrationTarget:
    """Derive the calibration target for one cell from ground truth.

    ``proposed_value`` is the value the model proposed (``None`` if it abstained
    / proposed nothing for this cell). ``clean_value`` is the ground-truth value
    (``None`` when no ground-truth repair exists for the cell).
    """
    should_abstain = inferability in ABSTENTION_SLICES

    if should_abstain:
        # Correct behaviour on a non-inferable cell is to propose nothing.
        if proposed_value is None:
            return CalibrationTarget(
                inferability=inferability,
                correct=True,
                should_abstain=True,
                target_confidence=0.0,
                rationale="non-inferable cell correctly abstained",
            )
        return CalibrationTarget(
            inferability=inferability,
            correct=False,
            should_abstain=True,
            target_confidence=0.0,
            rationale="non-inferable cell should not have been guessed",
        )

    if proposed_value is None:
        return CalibrationTarget(
            inferability=inferability,
            correct=False,
            should_abstain=False,
            target_confidence=0.0,
            rationale="inferable cell was left unrepaired",
        )

    if clean_value is not None and strict_cell_value(proposed_value) == strict_cell_value(
        clean_value
    ):
        return CalibrationTarget(
            inferability=inferability,
            correct=True,
            should_abstain=False,
            target_confidence=1.0,
            rationale="proposal matches ground truth",
        )

    return CalibrationTarget(
        inferability=inferability,
        correct=False,
        should_abstain=False,
        target_confidence=0.0,
        rationale="proposal does not match ground truth",
    )


def derive_targets_for_fixes(
    *,
    proposed_by_cell: Mapping[tuple[int, str], str],
    clean_by_cell: Mapping[tuple[int, str], str],
    inferability_by_cell: Mapping[tuple[int, str], InferabilityLabel],
    default_inferability: InferabilityLabel = "context_derivable",
) -> dict[tuple[int, str], CalibrationTarget]:
    """Derive calibration targets for every proposed or ground-truth cell.

    The union of proposed cells and ground-truth cells is scored: proposals with
    no ground truth are over-repairs (incorrect), ground-truth cells with no
    proposal are misses (incorrect unless the cell was correctly abstained).
    ``inferability_by_cell`` carries the per-cell class; cells absent from it
    fall back to ``default_inferability``.
    """
    cells: set[tuple[int, str]] = set(proposed_by_cell) | set(clean_by_cell)
    targets: dict[tuple[int, str], CalibrationTarget] = {}
    for cell in cells:
        targets[cell] = derive_cell_target(
            proposed_value=proposed_by_cell.get(cell),
            clean_value=clean_by_cell.get(cell),
            inferability=inferability_by_cell.get(cell, default_inferability),
        )
    return targets


def calibration_samples(
    targets: Iterable[CalibrationTarget],
) -> list[tuple[float, bool]]:
    """Project targets into ``(target_confidence, correct)`` calibration pairs.

    These pairs are the exact shape consumed by ``dataforge.conformal`` and
    ``dataforge.calibration`` for certified-coverage measurement.
    """
    return [(target.target_confidence, target.correct) for target in targets]
