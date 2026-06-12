"""Exact-match cell-diff grading for data-quality repair evaluation.

The grader is the sole source of truth for scoring. Agents return
candidate Fix objects; the grader computes precision, recall, and F1
by exact string match against canonical ground-truth dirty-to-clean
cell corrections.

Scoring rules:
- Last-write-wins: duplicate predictions for the same (row, column)
  are collapsed to the final prediction.
- Exact match: ``fix.new_value`` must exactly equal ``ground_truth.clean_value``
  after trailing whitespace normalization.
- Wrong value on correct cell: counts as both FP (wrong prediction)
  and FN (correct cell left uncorrected).
- Malformed fixes: fixes with negative row indices or empty columns
  are rejected by Pydantic validation; if they bypass validation, they
  are counted as FP and logged as warnings.
"""

from __future__ import annotations

import logging
from collections import OrderedDict

from pydantic import BaseModel, Field

from dataforge_evals.agents.base import Fix, GroundTruthCell

logger = logging.getLogger(__name__)


class Grade(BaseModel):
    """Exact-match repair score for one evaluated run.

    Attributes:
        tp: True positives — fixes that exactly match a ground-truth correction.
        fp: False positives — fixes that do not match any ground-truth correction.
        fn: False negatives — ground-truth corrections not matched by any fix.
        precision: TP / (TP + FP), or 0.0 when no predictions are made.
        recall: TP / (TP + FN), or 0.0 when no ground truth exists.
        f1: Harmonic mean of precision and recall, or 0.0 when both are zero.
    """

    tp: int = Field(ge=0)
    fp: int = Field(ge=0)
    fn: int = Field(ge=0)
    precision: float = Field(ge=0.0, le=1.0)
    recall: float = Field(ge=0.0, le=1.0)
    f1: float = Field(ge=0.0, le=1.0)

    model_config = {"frozen": True}

    def __repr__(self) -> str:
        return (
            f"Grade(tp={self.tp}, fp={self.fp}, fn={self.fn}, "
            f"P={self.precision:.4f}, R={self.recall:.4f}, F1={self.f1:.4f})"
        )


def normalize_fixes(fixes: list[Fix]) -> list[Fix]:
    """Collapse proposed fixes to the final prediction per cell.

    When multiple fixes target the same ``(row, column)`` pair, only the
    last prediction is retained. This implements last-write-wins semantics.

    Args:
        fixes: Ordered list of proposed cell repairs.

    Returns:
        Deduplicated fix list preserving insertion order of final predictions.
    """
    by_cell: OrderedDict[tuple[int, str], Fix] = OrderedDict()
    for fix in fixes:
        key = (fix.row, fix.column)
        if key in by_cell:
            del by_cell[key]
        by_cell[key] = fix
    return list(by_cell.values())


def _normalize_value(value: str) -> str:
    """Normalize a cell value for comparison.

    Strips trailing whitespace to prevent spurious mismatches from
    formatting differences between agent output and ground truth.

    Args:
        value: Raw cell value string.

    Returns:
        Whitespace-normalized string.
    """
    return value.rstrip()


def grade_fixes(
    ground_truth: tuple[GroundTruthCell, ...] | list[GroundTruthCell],
    fixes: list[Fix],
) -> Grade:
    """Grade proposed fixes against canonical exact dirty-to-clean cell diffs.

    Scoring semantics:
    - A fix is a true positive when ``(row, column, normalized_new_value)``
      exactly matches a ground-truth correction's ``(row, column, clean_value)``.
    - A fix targeting a cell with no ground-truth issue is a false positive.
    - A fix targeting the correct cell but proposing the wrong value is both
      a false positive and a false negative.
    - Ground-truth corrections with no matching fix are false negatives.

    Args:
        ground_truth: Authoritative cell corrections.
        fixes: Agent-proposed cell repairs (will be deduplicated via last-write-wins).

    Returns:
        Immutable Grade with TP, FP, FN, precision, recall, and F1.
    """
    normalized = normalize_fixes(fixes)
    expected: dict[tuple[int, str], str] = {
        (cell.row, cell.column): _normalize_value(cell.clean_value) for cell in ground_truth
    }
    matched: set[tuple[int, str]] = set()
    tp = 0
    fp = 0
    for fix in normalized:
        key = (fix.row, fix.column)
        expected_value = expected.get(key)
        if expected_value is not None and _normalize_value(fix.new_value) == expected_value:
            tp += 1
            matched.add(key)
        else:
            fp += 1
            if expected_value is not None:
                logger.debug(
                    "Wrong value on correct cell (%d, %s): expected %r, got %r",
                    fix.row,
                    fix.column,
                    expected_value,
                    fix.new_value,
                )

    fn = len(expected) - len(matched)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return Grade(
        tp=tp,
        fp=fp,
        fn=fn,
        precision=round(precision, 4),
        recall=round(recall, 4),
        f1=round(f1, 4),
    )
