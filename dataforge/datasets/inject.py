"""Fidelity-gated error injection.

Implements ``specs/SPEC_error_fidelity.md``: a generated corpus is admissible as evidence
only if its per-detector metric vector agrees with a real-error reference within
pre-registered bounds, and is **refused** otherwise -- never downgraded.

Refused rather than downgraded because a downgraded corpus is still cited. ``hospital``
was nominally caveated for months while being the declared flagship, and its entire error
model was one substituted character.

The gate is deliberately the same shape as :mod:`dataforge.release.corrector_gate`: a
fallible source earns evidential standing from a committed measurement clearing a canonical
verdict, fails closed on a malformed artifact, and a refusal is a valid outcome rather than
an error.

Why injection is permitted at all: there is no real-error corpus for the *correction* axis
and there will not be one. REIN's authors built a benchmark to fix that and found two
usable real-error datasets out of fourteen. ``RT-bench``/``ST-bench`` ship no clean values,
so they can score detection and never a repair. The choice is not real versus injected, it
is injected with a measured fidelity or injected with an assumed one.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from dataforge.bench.detection import DetectionRunResult, measure_column_benchmark
from dataforge.datasets.column_corpus import BenchmarkColumn, ColumnBenchmark
from dataforge.datasets.registry import ColumnBenchmarkMetadata

__all__ = [
    "FIDELITY_MIN_RANK_CORRELATION",
    "FIDELITY_MAX_PRECISION_GAP",
    "FIDELITY_MAX_COVERAGE_MISMATCH",
    "FIDELITY_MIN_FIRING_DETECTORS",
    "FidelityVerdict",
    "FidelityError",
    "GeneratedCorpus",
    "assess_fidelity",
    "generate_character_noise_corpus",
]

# Pre-registered in eval/preregistration/error_fidelity.md before any generator existed.
# Raising one of these to admit a specific generator is forbidden: the remedy for a failing
# generator is a better generator, not a lower bar.
FIDELITY_MIN_RANK_CORRELATION = 0.70
FIDELITY_MAX_PRECISION_GAP = 0.25
FIDELITY_MAX_COVERAGE_MISMATCH = 1
# Non-vacuity. A corpus nothing fires on correlates perfectly with any other such corpus,
# so without this the gate would certify the empty-oracle bug this project already shipped.
FIDELITY_MIN_FIRING_DETECTORS = 3


class FidelityError(RuntimeError):
    """Raised when a fidelity assessment cannot be made honestly.

    Distinct from a ``REFUSED`` verdict: a refusal is a measured outcome, while this is the
    absence of a valid measurement. Both block admission; only one is a finding.
    """


@dataclass(frozen=True, slots=True)
class GeneratedCorpus:
    """An injected corpus plus the provenance that binds a fidelity verdict to it.

    ``generator_id`` and ``seed`` are carried because a verdict is bound to the
    ``(generator_id, seed, reference)`` triple. Varying any of them to find a passing
    combination is threshold-shopping by another route.
    """

    benchmark: ColumnBenchmark
    generator_id: str
    seed: int
    injected_values: int


@dataclass(frozen=True, slots=True)
class FidelityVerdict:
    """The admissibility decision for one generated corpus.

    ``status`` is ``"PASSED"`` or ``"REFUSED"``. ``failed_conditions`` names every
    condition that did not hold, so a refusal is diagnostic rather than a bare no.
    """

    status: Literal["PASSED", "REFUSED"]
    generator_id: str
    seed: int
    reference: str
    reference_sha256: str
    rank_correlation: float | None
    max_precision_gap: float | None
    coverage_mismatch: int
    firing_detectors: int
    per_detector: tuple[dict[str, object], ...]
    failed_conditions: tuple[str, ...]
    limitations: tuple[str, ...]

    @property
    def admissible(self) -> bool:
        """Whether the corpus may be used as a diagnostic instrument."""
        return self.status == "PASSED"


def _spearman(first: Sequence[float], second: Sequence[float]) -> float | None:
    """Spearman rank correlation, with ties averaged. None when undefined.

    Returns None rather than 0.0 or 1.0 when either input has zero rank variance: a
    constant vector has no ordering to correlate, and filling a value would let a corpus
    on which every detector scored identically pass F1 by accident.
    """
    if len(first) != len(second) or len(first) < 2:
        return None

    def ranks(values: Sequence[float]) -> list[float]:
        order = sorted(range(len(values)), key=lambda index: values[index])
        result = [0.0] * len(values)
        position = 0
        while position < len(order):
            end = position
            while end + 1 < len(order) and values[order[end + 1]] == values[order[position]]:
                end += 1
            average = (position + end) / 2.0 + 1.0
            for index in range(position, end + 1):
                result[order[index]] = average
            position = end + 1
        return result

    first_ranks, second_ranks = ranks(first), ranks(second)
    n = len(first_ranks)
    mean_first = sum(first_ranks) / n
    mean_second = sum(second_ranks) / n
    covariance = sum(
        (a - mean_first) * (b - mean_second) for a, b in zip(first_ranks, second_ranks, strict=True)
    )
    var_first = sum((a - mean_first) ** 2 for a in first_ranks)
    var_second = sum((b - mean_second) ** 2 for b in second_ranks)
    if var_first == 0.0 or var_second == 0.0:
        return None
    return round(float(covariance / ((var_first**0.5) * (var_second**0.5))), 6)


def _precision_by_detector(result: DetectionRunResult) -> dict[str, float | None]:
    """Map detector name to measured precision, or None where it never fired."""
    return {
        measurement.detector: (
            float(measurement.score.precision)
            if measurement.score is not None and measurement.score.precision is not None
            else None
        )
        for measurement in result.per_detector
    }


def assess_fidelity(
    generated: GeneratedCorpus,
    reference: ColumnBenchmark,
) -> FidelityVerdict:
    """Assess whether a generated corpus is admissible against a real-error reference.

    Args:
        generated: The injected corpus with its generator provenance.
        reference: A loaded, hash-verified real-error corpus.

    Returns:
        The :class:`FidelityVerdict`. A ``REFUSED`` status is a measured finding, not an
        error, and is publishable as such.

    Raises:
        FidelityError: If the reference is not a natural-error corpus. Fidelity against
            injected data would be circular, and is the exact mistake that let
            ``hospital`` stand in for real errors for months.
    """
    if reference.metadata.error_provenance != "natural":
        raise FidelityError(
            f"reference corpus {reference.metadata.name!r} has error_provenance "
            f"{reference.metadata.error_provenance!r}; fidelity must be measured against "
            "natural errors or the comparison is circular"
        )

    generated_result = measure_column_benchmark(generated.benchmark)
    reference_result = measure_column_benchmark(reference)

    generated_precision = _precision_by_detector(generated_result)
    reference_precision = _precision_by_detector(reference_result)

    detectors = sorted(set(generated_precision) | set(reference_precision))
    fired_either = [
        name
        for name in detectors
        if generated_precision.get(name) is not None or reference_precision.get(name) is not None
    ]
    fired_both = [
        name
        for name in fired_either
        if generated_precision.get(name) is not None and reference_precision.get(name) is not None
    ]
    coverage_mismatch = len(fired_either) - len(fired_both)

    rank_correlation = _spearman(
        [generated_precision[name] or 0.0 for name in fired_both],
        [reference_precision[name] or 0.0 for name in fired_both],
    )
    max_gap = (
        max(
            abs((generated_precision[name] or 0.0) - (reference_precision[name] or 0.0))
            for name in fired_both
        )
        if fired_both
        else None
    )

    failed: list[str] = []
    # F4 is evaluated first and reported even when the others would pass, so a vacuous
    # corpus never presents a clean correlation as evidence.
    if len(fired_either) < FIDELITY_MIN_FIRING_DETECTORS:
        failed.append(
            f"F4 non-vacuity: {len(fired_either)} detector(s) fired, need "
            f">= {FIDELITY_MIN_FIRING_DETECTORS}"
        )
    if rank_correlation is None:
        failed.append("F1 rank correlation: undefined (fewer than two detectors, or no variance)")
    elif rank_correlation < FIDELITY_MIN_RANK_CORRELATION:
        failed.append(f"F1 rank correlation {rank_correlation} < {FIDELITY_MIN_RANK_CORRELATION}")
    if max_gap is None:
        failed.append("F2 precision gap: no detector fired on both corpora")
    elif max_gap > FIDELITY_MAX_PRECISION_GAP:
        failed.append(f"F2 max precision gap {round(max_gap, 4)} > {FIDELITY_MAX_PRECISION_GAP}")
    if coverage_mismatch > FIDELITY_MAX_COVERAGE_MISMATCH:
        failed.append(
            f"F3 coverage mismatch {coverage_mismatch} > {FIDELITY_MAX_COVERAGE_MISMATCH}"
        )

    return FidelityVerdict(
        status="REFUSED" if failed else "PASSED",
        generator_id=generated.generator_id,
        seed=generated.seed,
        reference=reference.metadata.name,
        reference_sha256=reference.sha256,
        rank_correlation=rank_correlation,
        max_precision_gap=None if max_gap is None else round(max_gap, 6),
        coverage_mismatch=coverage_mismatch,
        firing_detectors=len(fired_either),
        per_detector=tuple(
            {
                "detector": name,
                "generated_precision": generated_precision.get(name),
                "reference_precision": reference_precision.get(name),
            }
            for name in detectors
        ),
        failed_conditions=tuple(failed),
        limitations=(
            "The reference measures DETECTION only. A fidelity verdict about detection "
            "metrics is not evidence that the corpus reproduces real CORRECTION difficulty.",
            "Reference precision rests on 88 unambiguous error values, so m(R) is itself "
            "noisy and F2's bound absorbs part of that noise.",
            "Rank correlation over 3-8 points is low-powered. This is a floor against "
            "grossly wrong error populations, not a certificate of realism.",
            "PASSED does not promote the corpus above diagnostic tier, does not authorise a "
            "headline claim, and may not add a detector to CONSTRAINT_CHECKABLE_DETECTORS.",
            f"Bound to (generator_id={generated.generator_id}, seed={generated.seed}, "
            f"reference={reference.metadata.name}). Varying any of these requires a new "
            "verdict, not a confirmation.",
        ),
    )


def generate_character_noise_corpus(
    reference: ColumnBenchmark,
    *,
    seed: int = 20260823,
    rate: float = 0.02,
) -> GeneratedCorpus:
    """Inject uniform single-character substitutions into a reference corpus's values.

    The deliberately naive baseline generator, present so the gate has something to
    **refuse**. It reproduces the ``hospital`` error model -- a substituted character -- and
    the pre-registration predicts on the record that it will fail F1, because an injected
    character is found far more readily than a real error is.

    A gate that has never refused anything is not known to be a gate.

    Args:
        reference: Corpus whose clean values are corrupted. Its own labels are discarded;
            only the value populations are reused.
        seed: Seed for reproducibility.
        rate: Fraction of values to corrupt per column.

    Returns:
        The :class:`GeneratedCorpus`.

    Raises:
        FidelityError: If no value was corrupted. An empty injection would sail through
            any metric comparison while proving nothing, which is precisely the vacuous
            oracle F4 exists to catch -- caught here too so the failure is attributed to
            the generator rather than to the gate.
    """
    rng = random.Random(seed)
    columns: list[BenchmarkColumn] = []
    injected = 0

    for column in reference.columns:
        # Only values the reference does not already label, so an injected error is never
        # confused with a natural one.
        clean_values = [
            value
            for value in column.distinct_values
            if value not in column.ground_truth and value not in column.debatable and value
        ]
        if len(clean_values) < 2:
            continue
        target_count = max(1, int(len(clean_values) * rate))
        targets = set(rng.sample(range(len(clean_values)), min(target_count, len(clean_values))))

        values: list[str] = []
        truth: set[str] = set()
        for index, value in enumerate(clean_values):
            if index in targets:
                position = rng.randrange(len(value))
                corrupted = value[:position] + "x" + value[position + 1 :]
                if corrupted != value and corrupted not in clean_values:
                    values.append(corrupted)
                    truth.add(corrupted)
                    injected += 1
                    continue
            values.append(value)

        if not truth:
            continue
        columns.append(
            BenchmarkColumn(
                index=column.index,
                header=column.header,
                distinct_values=tuple(values),
                ground_truth=frozenset(truth),
                debatable=frozenset(),
                declared_value_count=len(set(values)),
            )
        )

    if not columns or injected == 0:
        raise FidelityError(
            "generator injected zero errors; an empty corpus would compare favourably "
            "against anything while proving nothing"
        )

    metadata: ColumnBenchmarkMetadata = reference.metadata.model_copy(
        update={
            "name": f"generated_character_noise_seed{seed}",
            "tier": "diagnostic",
            "tier_reason": (
                "Injected single-character substitutions. Diagnostic only: a fidelity "
                "verdict shows a corpus is shaped like real data, not that it is real data."
            ),
        }
    )
    return GeneratedCorpus(
        benchmark=ColumnBenchmark(
            metadata=metadata,
            columns=tuple(columns),
            quarantined=(),
            sha256=reference.sha256,
            padded_rows_discarded=0,
            value_count_mismatches=0,
        ),
        generator_id="character_noise_v1",
        seed=seed,
        injected_values=injected,
    )
