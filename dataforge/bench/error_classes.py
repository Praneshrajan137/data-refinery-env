"""Per-error-class measurement instrument for the DataForge benchmark.

This module is the honesty foundation for the coverage work: it makes visible
*which* error classes the repair stack catches and misses, instead of a single
aggregate F1 that one strong dataset can mask. It provides:

- A transparent, documented ground-truth labeler (:func:`classify_error_cell`)
  that maps each ``(dirty, clean)`` correction to a canonical error class.
- Per-class precision/recall scoring (:func:`score_repairs_by_class`).
- A coverage matrix + regression gate over benchmark records.
- Calibration utilities (ECE, precision@auto-apply) used by the calibration
  layer once confidence-bearing predictions exist.

The labeler is a heuristic, and is deliberately simple and inspectable. It is
versioned (:data:`LABELER_VERSION`) so reported numbers are reproducible and any
change to the labeling rules is explicit. Per-class *recall* is objective (it
counts ground-truth cells correctly repaired); per-class *precision* is reported
only for predictions that land on a labeled ground-truth cell.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from typing import Any

from dataforge.bench.core import BenchmarkRepair, ClassScore, normalize_repairs
from dataforge.datasets.real_world import GroundTruthCell

__all__ = [
    "BENCH_ERROR_CLASSES",
    "LABELER_VERSION",
    "classify_error_cell",
    "class_coverage_matrix",
    "score_repairs_by_class",
    "expected_calibration_error",
    "precision_at_auto_apply",
    "check_coverage_regression",
]

LABELER_VERSION = "v1"

# Canonical, mutually exclusive error classes the labeler assigns. Chosen to
# align with the RAHA dataset error taxonomy (typo, missing_value, formatting,
# datetime, normalization) and the DataForge detector families.
BENCH_ERROR_CLASSES: tuple[str, ...] = (
    "missing_value",
    "numeric",
    "datetime_format",
    "value_format",
    "text_normalization",
    "other",
)

_MISSING_SENTINELS = frozenset(
    {"", "n/a", "na", "null", "none", "nan", "nil", "-", "unknown", "not available", "?"}
)
_DATE_PATTERN = re.compile(r"^\s*\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}([ T]\d{1,2}:\d{2}(:\d{2})?)?\s*$")
_ALNUM = re.compile(r"[0-9a-z]+")


def _is_missing(value: str) -> bool:
    """Return whether a value reads as missing/sentinel."""
    return value.strip().lower() in _MISSING_SENTINELS


def _is_number(value: str) -> bool:
    """Return whether a value parses as a float."""
    try:
        float(value.replace(",", "").strip())
    except (TypeError, ValueError):
        return False
    return True


def _looks_like_date(value: str) -> bool:
    """Return whether a value matches a common date/datetime shape."""
    return bool(_DATE_PATTERN.match(value))


def _alnum_skeleton(value: str) -> str:
    """Return the lowercase alphanumeric content, ignoring punctuation/spacing."""
    return "".join(_ALNUM.findall(value.lower()))


def _strip_ws(value: str) -> str:
    """Return the value with all whitespace removed."""
    return re.sub(r"\s+", "", value)


def classify_error_cell(dirty_value: str, clean_value: str) -> str:
    """Classify a single dirty-to-clean correction into a canonical error class.

    The rules are ordered and documented (labeler ``v1``):

    1. ``missing_value`` - the dirty cell is blank/sentinel but the clean cell
       holds a real value (or vice versa).
    2. ``numeric`` - both dirty and clean parse as numbers.
    3. ``datetime_format`` - the clean (or dirty) value looks like a date/datetime.
    4. ``text_normalization`` - dirty and clean are equal after removing case and
       whitespace, OR are a near-typo (Levenshtein distance <= 2).
    5. ``value_format`` - same alphanumeric content but a punctuation/structure
       reformat (e.g. phone "15551234567" -> "+1 (555) 123-4567"), or a large
       formatting overlap not captured above.
    6. ``other`` - anything else (semantic replacement, lookup correction).

    Args:
        dirty_value: The erroneous value.
        clean_value: The ground-truth corrected value.

    Returns:
        One of :data:`BENCH_ERROR_CLASSES`.
    """
    if _is_missing(dirty_value) != _is_missing(clean_value):
        return "missing_value"
    if _is_number(dirty_value) and _is_number(clean_value):
        return "numeric"
    if _looks_like_date(clean_value) or _looks_like_date(dirty_value):
        return "datetime_format"

    # Pure case/whitespace difference (no punctuation reformat).
    if _strip_ws(dirty_value).lower() == _strip_ws(clean_value).lower() and _strip_ws(dirty_value):
        return "text_normalization"

    dirty_skeleton = _alnum_skeleton(dirty_value)
    clean_skeleton = _alnum_skeleton(clean_value)
    # Same alphanumeric content but a punctuation/structure reformat (e.g. phone).
    if dirty_skeleton == clean_skeleton and dirty_skeleton:
        return "value_format"
    # Near-typo on short strings.
    if _levenshtein_le(dirty_value.strip().lower(), clean_value.strip().lower(), 2):
        return "text_normalization"
    if dirty_skeleton and clean_skeleton and _alnum_overlap(dirty_skeleton, clean_skeleton):
        return "value_format"
    return "other"


def _alnum_overlap(left: str, right: str) -> bool:
    """Return whether two skeletons share most of their characters (format diff)."""
    shorter, longer = (left, right) if len(left) <= len(right) else (right, left)
    if not shorter:
        return False
    return shorter in longer or _levenshtein_le(left, right, max(2, len(longer) // 3))


def _levenshtein_le(a: str, b: str, max_distance: int) -> bool:
    """Return whether the edit distance between a and b is <= max_distance."""
    if abs(len(a) - len(b)) > max_distance:
        return False
    previous = list(range(len(b) + 1))
    for i, char_a in enumerate(a, start=1):
        current = [i]
        row_min = i
        for j, char_b in enumerate(b, start=1):
            cost = 0 if char_a == char_b else 1
            value = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost)
            current.append(value)
            row_min = min(row_min, value)
        if row_min > max_distance:
            return False
        previous = current
    return previous[-1] <= max_distance


def score_repairs_by_class(
    ground_truth: tuple[GroundTruthCell, ...] | list[GroundTruthCell],
    repairs: list[BenchmarkRepair],
    detected_cells: set[tuple[int, str]] | None = None,
) -> dict[str, ClassScore]:
    """Score repairs (and optionally detections) per error class.

    Args:
        ground_truth: Cell-level dirty-to-clean corrections.
        repairs: Predicted repairs (normalized last-write-wins per cell).
        detected_cells: ``(row, column)`` cells a detector flagged, regardless of
            repair. When provided, detection recall is scored per class; when
            ``None``, detection metrics are zero (correction-only mode).

    Returns:
        A mapping from error class to :class:`ClassScore` for every class that
        has ground-truth support.
    """
    gt_class: dict[tuple[int, str], str] = {}
    gt_clean: dict[tuple[int, str], str] = {}
    support: OrderedDict[str, int] = OrderedDict((cls, 0) for cls in BENCH_ERROR_CLASSES)
    for cell in ground_truth:
        key = (cell.row, cell.column)
        cls = classify_error_cell(cell.dirty_value, cell.clean_value)
        gt_class[key] = cls
        gt_clean[key] = cell.clean_value
        support[cls] += 1

    tp_by_class: dict[str, int] = dict.fromkeys(BENCH_ERROR_CLASSES, 0)
    predicted_on_class: dict[str, int] = dict.fromkeys(BENCH_ERROR_CLASSES, 0)
    correct_on_class: dict[str, int] = dict.fromkeys(BENCH_ERROR_CLASSES, 0)
    detected_by_class: dict[str, int] = dict.fromkeys(BENCH_ERROR_CLASSES, 0)

    if detected_cells is not None:
        for key in detected_cells:
            detected_cls = gt_class.get(key)
            if detected_cls is not None:
                detected_by_class[detected_cls] += 1

    for repair in normalize_repairs(repairs):
        key = (repair.row, repair.column)
        repair_cls = gt_class.get(key)
        if repair_cls is None:
            continue  # spurious prediction on a non-error cell; not class-attributable
        predicted_on_class[repair_cls] += 1
        if repair.new_value == gt_clean[key]:
            correct_on_class[repair_cls] += 1
            tp_by_class[repair_cls] += 1

    scores: dict[str, ClassScore] = {}
    for cls in BENCH_ERROR_CLASSES:
        cls_support = support[cls]
        if cls_support == 0 and predicted_on_class[cls] == 0 and detected_by_class[cls] == 0:
            continue
        tp = tp_by_class[cls]
        fn = cls_support - tp
        recall = tp / cls_support if cls_support else 0.0
        predicted = predicted_on_class[cls]
        precision = correct_on_class[cls] / predicted if predicted else 0.0
        detected = detected_by_class[cls]
        detection_recall = detected / cls_support if cls_support else 0.0
        scores[cls] = ClassScore(
            error_class=cls,
            support=cls_support,
            detected=detected,
            detection_recall=round(detection_recall, 4),
            tp=tp,
            fn=fn,
            recall=round(recall, 4),
            predicted_on_class=predicted,
            precision_on_class=round(precision, 4),
        )
    return scores


def class_coverage_matrix(
    records: list[Any],  # list[SeedBenchmarkResult]; typed loosely to avoid an import cycle
) -> dict[tuple[str, str], dict[str, ClassScore]]:
    """Collapse seed records into a ``(method, dataset) -> {class: ClassScore}`` matrix.

    Deterministic methods produce identical per-class scores across seeds, so the
    first OK record per ``(method, dataset)`` with ``by_class`` is used.
    """
    matrix: dict[tuple[str, str], dict[str, ClassScore]] = {}
    for record in records:
        if getattr(record, "status", None) != "ok" or record.by_class is None:
            continue
        key = (record.method, record.dataset)
        matrix.setdefault(key, record.by_class)
    return matrix


def expected_calibration_error(samples: list[tuple[float, bool]], *, bins: int = 10) -> float:
    """Compute the Expected Calibration Error of confidence-labeled predictions.

    Args:
        samples: ``(confidence, was_correct)`` pairs with confidence in [0, 1].
        bins: Number of equal-width confidence bins.

    Returns:
        ECE in [0, 1]; 0.0 for an empty input.
    """
    if not samples:
        return 0.0
    bin_totals = [0] * bins
    bin_conf = [0.0] * bins
    bin_correct = [0] * bins
    for confidence, correct in samples:
        clamped = min(max(confidence, 0.0), 1.0)
        index = min(int(clamped * bins), bins - 1)
        bin_totals[index] += 1
        bin_conf[index] += clamped
        bin_correct[index] += 1 if correct else 0
    total = len(samples)
    ece = 0.0
    for index in range(bins):
        count = bin_totals[index]
        if count == 0:
            continue
        avg_conf = bin_conf[index] / count
        accuracy = bin_correct[index] / count
        ece += (count / total) * abs(avg_conf - accuracy)
    return round(ece, 4)


def precision_at_auto_apply(samples: list[tuple[bool, bool]]) -> float:
    """Precision among predictions the policy chose to auto-apply.

    Args:
        samples: ``(auto_applied, was_correct)`` pairs.

    Returns:
        correct / auto_applied, or 1.0 when nothing was auto-applied (vacuously
        safe: the tool corrupted nothing).
    """
    applied = [correct for auto_applied, correct in samples if auto_applied]
    if not applied:
        return 1.0
    return round(sum(1 for c in applied if c) / len(applied), 4)


def check_coverage_regression(
    records: list[Any],  # list[SeedBenchmarkResult]
    thresholds: dict[str, dict[str, float]],
) -> tuple[bool, list[str]]:
    """Check per-(method/dataset/class) recall floors against committed thresholds.

    A class key may carry an ``@detection`` suffix to assert a *detection* recall
    floor (did we flag the error); a plain class key asserts a *correction* recall
    floor (did we produce the exact value).

    Args:
        records: Benchmark seed records carrying ``by_class``.
        thresholds: ``{"method/dataset": {"error_class[@detection]": min_recall}}``.

    Returns:
        ``(passed, failures)`` where failures describe each floor that was missed.
    """
    matrix = class_coverage_matrix(records)
    failures: list[str] = []
    for key, class_floors in thresholds.items():
        method, _, dataset = key.partition("/")
        scores = matrix.get((method, dataset))
        if scores is None:
            failures.append(f"{key}: no benchmark record produced per-class scores")
            continue
        for raw_class, min_recall in class_floors.items():
            error_class, _, mode = raw_class.partition("@")
            score = scores.get(error_class)
            if mode == "detection":
                actual = score.detection_recall if score is not None else 0.0
            else:
                actual = score.recall if score is not None else 0.0
            if actual + 1e-9 < min_recall:
                failures.append(f"{key}/{raw_class}: recall {actual:.4f} < floor {min_recall:.4f}")
    return (not failures), failures
