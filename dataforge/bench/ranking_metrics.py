"""Pure ranking metrics for the review-queue ranker (order quality).

These score a ranked list of review-queue candidates, each a
``(score, is_true_error)`` pair, where a higher ``score`` should mean "more
likely a genuine error". They mirror the style of
``dataforge.bench.error_classes`` helpers: simple sequences in, rounded floats
out, and safe vacuous defaults so an empty or degenerate input never raises.

The headline metric is ``roc_auc`` (threshold-free ordering quality: the
probability a random true error is ranked above a random false flag). The
operational metrics are ``precision_at_k`` / ``recall_at_k`` (what a human sees
in the top ``k`` of the queue) and ``queue_precision_lift`` (how much cleaner the
top ``k`` is than the raw base rate). The point of these is to compare a ranker
against a baseline ranker over the SAME candidates - the LLM ranker earns its
keep only by its lift over the free detector-confidence order.
"""

from __future__ import annotations

from collections.abc import Sequence

Sample = tuple[float, bool]


def _sorted_desc(ranked: Sequence[Sample]) -> list[Sample]:
    """Return samples sorted by score descending (stable)."""
    return sorted(ranked, key=lambda pair: pair[0], reverse=True)


def precision_at_k(ranked: Sequence[Sample], k: int) -> float:
    """Fraction of the top-``k`` ranked candidates that are true errors.

    ``0.0`` for empty input or ``k <= 0``. If fewer than ``k`` candidates exist,
    precision is over all of them.
    """
    if k <= 0 or not ranked:
        return 0.0
    top = _sorted_desc(ranked)[:k]
    if not top:
        return 0.0
    return round(sum(1 for _, is_true in top if is_true) / len(top), 4)


def recall_at_k(ranked: Sequence[Sample], k: int) -> float:
    """Fraction of ALL true errors captured within the top-``k``.

    ``0.0`` when there are no true errors in the input.
    """
    total_true = sum(1 for _, is_true in ranked if is_true)
    if total_true == 0 or k <= 0:
        return 0.0
    top = _sorted_desc(ranked)[:k]
    return round(sum(1 for _, is_true in top if is_true) / total_true, 4)


def roc_auc(samples: Sequence[Sample]) -> float:
    """Threshold-free ordering quality (tie-aware Mann-Whitney AUC).

    The probability that a random true error outranks a random false flag; 0.5
    means no discriminating signal. Returns 0.5 when the metric is undefined
    (no positives or no negatives), matching the "no information" convention.
    """
    n_pos = sum(1 for _, is_true in samples if is_true)
    n_neg = len(samples) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5
    # Average-rank assignment (ties share the mean of their rank block).
    ordered = sorted(samples, key=lambda pair: pair[0])
    ranks: list[float] = [0.0] * len(ordered)
    i = 0
    while i < len(ordered):
        j = i
        while j + 1 < len(ordered) and ordered[j + 1][0] == ordered[i][0]:
            j += 1
        avg_rank = (i + j) / 2 + 1  # 1-based average rank for the tie block
        for idx in range(i, j + 1):
            ranks[idx] = avg_rank
        i = j + 1
    rank_sum_pos = sum(rank for rank, (_, is_true) in zip(ranks, ordered, strict=True) if is_true)
    auc = (rank_sum_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)
    return round(auc, 4)


def queue_precision_lift(ranked: Sequence[Sample], k: int, base_rate: float) -> float:
    """Multiplicative lift of top-``k`` precision over the raw base rate.

    E.g. a base rate of 0.05 lifted to 0.40 precision@k returns 8.0. Returns 0.0
    when the base rate is 0 (no lift is definable).
    """
    if base_rate <= 0.0:
        return 0.0
    return round(precision_at_k(ranked, k) / base_rate, 4)
