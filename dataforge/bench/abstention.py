"""Abstention-neutral detection scoring.

Implements ``specs/SPEC_abstention_scoring.md``: the three-way scoring rule published
with the ``RT-bench``/``ST-bench`` benchmarks of Auto-Test (Chen et al., SIGMOD 2025,
arXiv:2504.10762), under which a *debatable* label costs nothing to flag and nothing to
miss.

Why this module exists, in one measurement: on ``flights`` the heuristic method scores
correction F1 0.0000 with 92 false positives and 4,920 false negatives. The same flight's
arrival time appears upstream as 10:30/10:31/10:28/10:39, so the ground truth encodes an
arbitrary convention, and a system that declines to invent one truth is scored identically
to one that guesses wrong. ``docs/trust/accuracy-frontier.md`` argues the 0.0000 is honest
abstention; before this module, nothing in the repository could tell the two apart.

Three deliberate choices, each with a shipped precedent behind it:

* **Undefined stays undefined.** ``precision`` at a zero denominator is ``None``, never
  1.0. Reporting 1.0 would let a system that flags nothing report perfect precision --
  the vacuous-assertion failure this project has already had to retract once.
* **Aggregation pools counts.** Macro-averaging per-column rates would weight a
  3-value column like a 900-value one.
* **Risk lives in one place.** The frontier delegates to
  :func:`dataforge.conformal.risk_coverage_frontier`, which shares its definition of
  selective risk with the certification gate that acts on it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from pydantic import BaseModel, Field

from dataforge.conformal import (
    RISK_COVERAGE_GRID,
    LabeledSample,
    risk_coverage_frontier,
)

__all__ = [
    "ThreeWayScore",
    "AbstentionScoringError",
    "score_detection_three_way",
    "aggregate_three_way",
    "detection_risk_coverage_frontier",
    "RISK_COVERAGE_GRID",
]

# Rounding matches RepairScore/ClassScore in dataforge.bench.core so a reader
# comparing a detection number to a correction number is not comparing precisions.
_PRECISION_DIGITS = 4


class AbstentionScoringError(ValueError):
    """Raised when a benchmark row or an aggregate cannot be scored honestly.

    A distinct type because these are *refusals*, not merely bad inputs: each one
    marks a case where returning a number would be worse than returning nothing.
    """


class ThreeWayScore(BaseModel):
    """Detection metrics for one column, or pooled over columns.

    ``precision``/``recall``/``f1`` are ``None`` where genuinely undefined rather
    than filled with 1.0 or 0.0; see ``specs/SPEC_abstention_scoring.md``. A
    ``None`` is excluded from an aggregate's denominator by
    :func:`aggregate_three_way`, so it can never contribute a free perfect score.

    ``debatable_predicted`` and ``debatable_missed`` are recorded but contribute to
    no metric. They exist so a reader can see *how much* of a result the neutral
    zone absorbed: a score whose neutral zone swallowed most of the column is a
    weaker claim than the same score on a column with an empty ``D``, and that
    difference is invisible in precision and recall by construction.
    """

    tp: int = Field(ge=0)
    fp: int = Field(ge=0)
    fn: int = Field(ge=0)
    precision: float | None = Field(default=None, ge=0.0, le=1.0)
    recall: float | None = Field(default=None, ge=0.0, le=1.0)
    f1: float | None = Field(default=None, ge=0.0, le=1.0)
    debatable_predicted: int = Field(default=0, ge=0)
    debatable_missed: int = Field(default=0, ge=0)
    n_distinct_values: int = Field(default=0, ge=0)
    n_predicted: int = Field(default=0, ge=0)
    coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    abstention_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    columns_scored: int = Field(default=1, ge=0)

    model_config = {"frozen": True}


def _ratio(numerator: int, denominator: int) -> float | None:
    """Return the ratio, or None when the denominator is zero.

    None rather than 0.0 or 1.0 on purpose: both fillers are wrong in a way that
    flatters a system which decided nothing.
    """
    if denominator == 0:
        return None
    return round(numerator / denominator, _PRECISION_DIGITS)


def _f1(precision: float | None, recall: float | None) -> float | None:
    """Harmonic mean, propagating None and refusing a zero denominator."""
    if precision is None or recall is None:
        return None
    total = precision + recall
    if total == 0:
        return 0.0
    return round(2 * precision * recall / total, _PRECISION_DIGITS)


def score_detection_three_way(
    *,
    distinct_values: Iterable[str],
    ground_truth: Iterable[str],
    debatable: Iterable[str] = (),
    predicted: Iterable[str],
) -> ThreeWayScore:
    """Score one column's flagged values under the three-way rule.

    ``TP = |P & G|``, ``FP = |P - (G | D)|``, ``FN = |G - P|``. ``D`` appears in no
    term: it is subtracted from the false-positive set and never added to the
    false-negative set, which is what makes flagging *and* abstaining on a
    contested value both free.

    Args:
        distinct_values: The column's full distinct-value list (``dist_val``).
        ground_truth: Values labelled unambiguously erroneous (``G``).
        debatable: Values labelled debatable (``D``). Defaults to empty.
        predicted: Values the detector flagged (``P``).

    Returns:
        The column's :class:`ThreeWayScore`.

    Raises:
        AbstentionScoringError: If ``G`` and ``D`` intersect. A value cannot be both
            unambiguous and debatable, so an intersection means a corrupted or
            misparsed row. Silently preferring one label would break the property
            that ``D`` contributes to no term, with no signal that it had.
    """
    values = set(distinct_values)
    truth = set(ground_truth)
    debated = set(debatable)
    flagged = set(predicted)

    overlap = truth & debated
    if overlap:
        raise AbstentionScoringError(
            "ground_truth and debatable must be disjoint; a value cannot be both "
            f"unambiguous and debatable. Overlapping: {sorted(overlap)!r}"
        )

    true_positives = flagged & truth
    false_positives = flagged - truth - debated
    false_negatives = truth - flagged

    precision = _ratio(len(true_positives), len(true_positives) + len(false_positives))
    recall = _ratio(len(true_positives), len(true_positives) + len(false_negatives))
    coverage = _ratio(len(flagged), len(values))

    return ThreeWayScore(
        tp=len(true_positives),
        fp=len(false_positives),
        fn=len(false_negatives),
        precision=precision,
        recall=recall,
        f1=_f1(precision, recall),
        debatable_predicted=len(flagged & debated),
        debatable_missed=len(debated - flagged),
        n_distinct_values=len(values),
        n_predicted=len(flagged),
        coverage=coverage,
        abstention_rate=None if coverage is None else round(1.0 - coverage, _PRECISION_DIGITS),
        columns_scored=1,
    )


def aggregate_three_way(scores: Sequence[ThreeWayScore]) -> ThreeWayScore:
    """Pool per-column scores into one score by summing counts.

    Pooled rather than macro-averaged because a 3-distinct-value column and a
    900-distinct-value column are not equally informative, and because pooled is
    the form Auto-Test publishes.

    Args:
        scores: Per-column scores.

    Returns:
        The pooled :class:`ThreeWayScore`, with ``columns_scored`` set to the number
        of inputs.

    Raises:
        AbstentionScoringError: If ``scores`` is empty. An aggregate over zero
            columns must not return zeros: this project has already shipped a
            parity check that reduced to ``0 == 0 and 0 == 0`` and would have
            certified an agent that dropped every fix.
    """
    if not scores:
        raise AbstentionScoringError(
            "cannot aggregate zero columns: an aggregate over an empty corpus would "
            "report zeros, which is indistinguishable from a measured result"
        )

    tp = sum(score.tp for score in scores)
    fp = sum(score.fp for score in scores)
    fn = sum(score.fn for score in scores)
    n_values = sum(score.n_distinct_values for score in scores)
    n_predicted = sum(score.n_predicted for score in scores)

    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    coverage = _ratio(n_predicted, n_values)

    return ThreeWayScore(
        tp=tp,
        fp=fp,
        fn=fn,
        precision=precision,
        recall=recall,
        f1=_f1(precision, recall),
        debatable_predicted=sum(score.debatable_predicted for score in scores),
        debatable_missed=sum(score.debatable_missed for score in scores),
        n_distinct_values=n_values,
        n_predicted=n_predicted,
        coverage=coverage,
        abstention_rate=None if coverage is None else round(1.0 - coverage, _PRECISION_DIGITS),
        columns_scored=len(scores),
    )


def detection_risk_coverage_frontier(
    samples: Sequence[LabeledSample],
    *,
    grid: Sequence[float] = RISK_COVERAGE_GRID,
    delta: float = 0.05,
) -> list[dict[str, float]]:
    """Bounded risk-coverage frontier for a confidence-emitting detector.

    A thin, deliberate delegation to :func:`dataforge.conformal.risk_coverage_frontier`.
    The indirection earns its keep by being the only detection-side entry point, so
    a future contributor reaching for "risk on the benchmark" cannot arrive at a
    second definition of it.

    A ``sample`` is ``(confidence, was_correct)``, where ``was_correct`` means the
    flagged value is in ``G``. Values in ``D`` must be **excluded** from ``samples``
    before calling: including them as either outcome would reintroduce the penalty
    the three-way rule removes.

    Args:
        samples: ``(confidence, was_correct)`` pairs over flagged values, with
            debatable values already excluded.
        grid: Pre-specified threshold grid.
        delta: Failure probability for the one-sided upper risk bound.

    Returns:
        Frontier points as documented on
        :func:`dataforge.conformal.risk_coverage_frontier`.
    """
    return risk_coverage_frontier(samples, grid=grid, delta=delta)
