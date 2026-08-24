"""Stratified projection of a review-queue filter onto a known population.

Built for the rayyan queue-filter measurement and reused by the wild-column work, because both
face the same problem: a uniform sample of a heterogeneous queue spends most of its calls on the
stratum with the fewest true errors, and estimates the rate that matters least.

The estimator here is deliberately narrow. Population composition is **known exactly** -- the
RAHA corpora ship complete ground truth -- so nothing about the population is estimated. The only
sampled quantities are two conditional keep-rates per stratum:

* ``P(filter keeps the cell | the cell is a true error)`` -- recall retention, the safety term.
* ``P(filter keeps the cell | the cell is a false positive)`` -- the removal term.

Those are projected onto known counts. That is why enriching a sample within a stratum is
legitimate here and would not be if the composition were unknown: enrichment changes which cells
are drawn, not the population being projected onto.

The failure this module exists to prevent is pooling. Measured on rayyan, half the queue is a
detector at 0.0649 precision with 1,080 false positives to remove, and a quarter is a detector at
1.0000 with 637 correct detections to lose. A pooled lift figure adds the gain to the loss and
reports the sum, so a filter that destroys a perfect detector can look like an improvement.
:meth:`QueueProjection.per_stratum` is therefore not optional output.
"""

from __future__ import annotations

from dataclasses import dataclass

from dataforge.bench.abstention import ThreeWayScore

__all__ = [
    "StratifiedProjectionError",
    "KeepRate",
    "StratumSample",
    "StratumProjection",
    "QueueProjection",
    "StratifiedPrecision",
    "wilson_interval",
    "project_queue_filter",
    "stratified_precision",
]

_ROUND = 4
_Z = 1.96


class StratifiedProjectionError(RuntimeError):
    """Raised when a projection is requested that the sample cannot support."""


def wilson_interval(successes: int, trials: int) -> tuple[float, float]:
    """95% Wilson score interval.

    Wilson rather than normal-approximate because several strata here have rates near 0 or 1
    -- the zero-precision tail should be rejected almost always -- where the normal interval
    runs outside [0, 1] and understates uncertainty at small n.

    Args:
        successes: Observed successes.
        trials: Total trials.

    Returns:
        ``(low, high)``, clamped to [0, 1]. ``(0.0, 1.0)`` when ``trials`` is zero, which is
        maximal ignorance rather than a point estimate.
    """
    if trials <= 0:
        return 0.0, 1.0
    proportion = successes / trials
    denominator = 1 + _Z * _Z / trials
    centre = (proportion + _Z * _Z / (2 * trials)) / denominator
    half = (
        _Z * ((proportion * (1 - proportion) / trials + _Z * _Z / (4 * trials * trials)) ** 0.5)
    ) / denominator
    return (
        round(max(0.0, centre - half), _ROUND),
        round(min(1.0, centre + half), _ROUND),
    )


@dataclass(frozen=True, slots=True)
class KeepRate:
    """A sampled conditional keep-rate with its interval."""

    kept: int
    sampled: int

    @property
    def rate(self) -> float | None:
        """The point estimate, or None when nothing was sampled.

        None rather than 0.0: a stratum with no draws has no measured rate, and returning 0.0
        would project it as "the filter rejects everything here", which is a claim.
        """
        if self.sampled <= 0:
            return None
        return round(self.kept / self.sampled, _ROUND)

    @property
    def interval(self) -> tuple[float, float]:
        """95% Wilson interval on the rate."""
        return wilson_interval(self.kept, self.sampled)


@dataclass(frozen=True, slots=True)
class StratumSample:
    """One stratum: known population counts, plus what the filter did to a sample of it.

    ``true_population`` and ``false_population`` are counts over the **whole** stratum, taken
    from ground truth. ``true_kept`` / ``true_sampled`` and ``false_kept`` / ``false_sampled``
    describe the sample only.
    """

    name: str
    true_population: int
    false_population: int
    true_kept: int
    true_sampled: int
    false_kept: int
    false_sampled: int

    def __post_init__(self) -> None:
        if self.true_population < 0 or self.false_population < 0:
            raise StratifiedProjectionError(f"{self.name}: negative population count")
        if self.true_sampled > self.true_population:
            raise StratifiedProjectionError(
                f"{self.name}: sampled {self.true_sampled} true cells from a population of "
                f"{self.true_population}"
            )
        if self.false_sampled > self.false_population:
            raise StratifiedProjectionError(
                f"{self.name}: sampled {self.false_sampled} false cells from a population of "
                f"{self.false_population}"
            )
        if self.true_kept > self.true_sampled or self.false_kept > self.false_sampled:
            raise StratifiedProjectionError(f"{self.name}: kept more cells than were sampled")

    @property
    def population(self) -> int:
        """Total cells this stratum contributes to the queue."""
        return self.true_population + self.false_population

    @property
    def keep_true(self) -> KeepRate:
        """Recall retention within this stratum."""
        return KeepRate(kept=self.true_kept, sampled=self.true_sampled)

    @property
    def keep_false(self) -> KeepRate:
        """False-positive retention within this stratum. Lower is better."""
        return KeepRate(kept=self.false_kept, sampled=self.false_sampled)

    @property
    def baseline_precision(self) -> float | None:
        """Unfiltered precision of this stratum, from known counts."""
        if self.population == 0:
            return None
        return round(self.true_population / self.population, _ROUND)


@dataclass(frozen=True, slots=True)
class StratumProjection:
    """What the filter is projected to do to a whole stratum."""

    name: str
    population: int
    baseline_precision: float | None
    keep_true_rate: float | None
    keep_true_ci: tuple[float, float]
    keep_false_rate: float | None
    keep_false_ci: tuple[float, float]
    projected_true_kept: float
    projected_false_kept: float
    true_errors_lost: float
    covered: bool
    note: str

    @property
    def projected_precision(self) -> float | None:
        """Precision of this stratum after filtering."""
        total = self.projected_true_kept + self.projected_false_kept
        if total <= 0:
            return None
        return round(self.projected_true_kept / total, _ROUND)


@dataclass(frozen=True, slots=True)
class QueueProjection:
    """The whole-queue projection, and the per-stratum detail that must accompany it."""

    per_stratum: tuple[StratumProjection, ...]
    baseline_flagged: int
    baseline_true: int
    projected_true_kept: float
    projected_false_kept: float
    total_true_errors_in_table: int

    @property
    def baseline_precision(self) -> float | None:
        """Unfiltered queue precision."""
        if self.baseline_flagged == 0:
            return None
        return round(self.baseline_true / self.baseline_flagged, _ROUND)

    @property
    def projected_precision(self) -> float | None:
        """Queue precision after filtering."""
        total = self.projected_true_kept + self.projected_false_kept
        if total <= 0:
            return None
        return round(self.projected_true_kept / total, _ROUND)

    @property
    def recall_retained(self) -> float | None:
        """Fraction of the queue's true errors the filter keeps.

        The safety term. A filter that raises precision by discarding true errors has not
        improved the queue, and this is the number that says so.
        """
        if self.baseline_true == 0:
            return None
        return round(self.projected_true_kept / self.baseline_true, _ROUND)

    @property
    def true_errors_lost(self) -> float:
        """Absolute count of true errors the filter is projected to discard."""
        return round(self.baseline_true - self.projected_true_kept, 2)

    @property
    def uncovered_strata(self) -> tuple[str, ...]:
        """Strata carried at their unfiltered rate because they were not sampled."""
        return tuple(s.name for s in self.per_stratum if not s.covered)


def project_queue_filter(
    strata: list[StratumSample],
    *,
    total_true_errors_in_table: int,
) -> QueueProjection:
    """Project sampled keep-rates onto known population counts.

    An unsampled stratum is carried through **unfiltered** rather than dropped or assumed
    rejected. Dropping it would silently shrink the queue and inflate precision; assuming
    rejection would silently discard true errors. Carrying it unfiltered is the only choice
    that makes no claim about cells the filter was never shown, and each such stratum is named
    on :attr:`QueueProjection.uncovered_strata`.

    Args:
        strata: One entry per detector stratum, with known populations and sampled outcomes.
        total_true_errors_in_table: Ground-truth error cells in the whole table, for context.

    Returns:
        The :class:`QueueProjection`.

    Raises:
        StratifiedProjectionError: If no strata are supplied, or if every stratum is uncovered
            -- a projection in which the filter was never measured is not a measurement.
    """
    if not strata:
        raise StratifiedProjectionError("a projection over zero strata is undefined")

    projections: list[StratumProjection] = []
    for stratum in strata:
        keep_true, keep_false = stratum.keep_true, stratum.keep_false
        true_rate, false_rate = keep_true.rate, keep_false.rate
        # Covered means at least one side was measured. A stratum with true cells but no true
        # draws cannot have its safety term estimated, and says so.
        covered = (true_rate is not None and stratum.true_population > 0) or (
            false_rate is not None and stratum.false_population > 0
        )
        effective_true = true_rate if true_rate is not None else 1.0
        effective_false = false_rate if false_rate is not None else 1.0
        note = ""
        if stratum.true_population > 0 and true_rate is None:
            note = "true cells present but none sampled; carried unfiltered"
        elif stratum.false_population > 0 and false_rate is None:
            note = "false cells present but none sampled; carried unfiltered"
        projected_true = stratum.true_population * effective_true
        projected_false = stratum.false_population * effective_false
        projections.append(
            StratumProjection(
                name=stratum.name,
                population=stratum.population,
                baseline_precision=stratum.baseline_precision,
                keep_true_rate=true_rate,
                keep_true_ci=keep_true.interval,
                keep_false_rate=false_rate,
                keep_false_ci=keep_false.interval,
                projected_true_kept=round(projected_true, 2),
                projected_false_kept=round(projected_false, 2),
                true_errors_lost=round(stratum.true_population - projected_true, 2),
                covered=covered,
                note=note,
            )
        )

    if not any(projection.covered for projection in projections):
        raise StratifiedProjectionError(
            "no stratum was sampled, so the filter was never measured. A projection built "
            "entirely from unfiltered carry-through would report the baseline as a result."
        )

    return QueueProjection(
        per_stratum=tuple(projections),
        baseline_flagged=sum(stratum.population for stratum in strata),
        baseline_true=sum(stratum.true_population for stratum in strata),
        projected_true_kept=round(sum(p.projected_true_kept for p in projections), 2),
        projected_false_kept=round(sum(p.projected_false_kept for p in projections), 2),
        total_true_errors_in_table=total_true_errors_in_table,
    )


@dataclass(frozen=True, slots=True)
class StratifiedPrecision:
    """Population precision when only the false-positive term needs projecting.

    The structure this exploits is specific to a sparsely-labelled detection corpus, and it is
    worth stating because it removes most of the sampling error:

    * **Every ground-truth error lives in a labelled column.** So if the labelled columns are
      scored as a census, ``tp``, ``fn`` and therefore **recall are exact** -- no sampling, no
      interval.
    * Only false positives on *unlabelled* columns need estimating, because there are far too
      many such columns to score them all.

    So the single projected quantity is the false-positive count over unlabelled columns, and
    the precision interval derives from that one term.
    """

    census_columns: int
    census_tp: int
    census_fp: int
    census_fn: int
    census_debatable_predicted: int
    sampled_columns: int
    population_columns: int
    sampled_fp: int
    projected_fp: float
    projected_fp_ci: tuple[float, float]

    @property
    def scale(self) -> float:
        """How far the sampled unlabelled columns are scaled up."""
        if self.sampled_columns <= 0:
            return 0.0
        return round(self.population_columns / self.sampled_columns, 4)

    @property
    def total_fp(self) -> float:
        """Census false positives plus projected unlabelled ones."""
        return round(self.census_fp + self.projected_fp, 2)

    @property
    def precision(self) -> float | None:
        """Projected population precision, or None when nothing was flagged."""
        denominator = self.census_tp + self.total_fp
        if denominator <= 0:
            return None
        return round(self.census_tp / denominator, 4)

    @property
    def precision_ci(self) -> tuple[float | None, float | None]:
        """Interval induced by the projected false-positive interval.

        Note the inversion: the *upper* bound on false positives gives the *lower* bound on
        precision.
        """
        low_fp, high_fp = self.projected_fp_ci
        high_denominator = self.census_tp + self.census_fp + high_fp
        low_denominator = self.census_tp + self.census_fp + low_fp
        low = round(self.census_tp / high_denominator, 4) if high_denominator > 0 else None
        high = round(self.census_tp / low_denominator, 4) if low_denominator > 0 else None
        return low, high

    @property
    def recall(self) -> float | None:
        """Exact recall over unambiguous errors. No interval: this is a census."""
        denominator = self.census_tp + self.census_fn
        if denominator <= 0:
            return None
        return round(self.census_tp / denominator, 4)


def stratified_precision(
    *,
    census_score: ThreeWayScore,
    per_column_fp: list[int],
    population_columns: int,
) -> StratifiedPrecision:
    """Estimate population precision from a labelled census plus an unlabelled sample.

    Args:
        census_score: A :class:`~dataforge.bench.abstention.ThreeWayScore` over **all** columns
            containing a ground-truth error. Read for ``tp``, ``fp``, ``fn`` and
            ``debatable_predicted``.
        per_column_fp: False positives observed on each sampled unlabelled column. One entry
            per column, so the list length is the sample size. Zeros must be included --
            dropping unflagged columns would inflate the per-column rate.
        population_columns: Total unlabelled columns in the corpus.

    Returns:
        The :class:`StratifiedPrecision`.

    Raises:
        StratifiedProjectionError: If no unlabelled columns were sampled while the population
            is non-empty. Projecting zero false positives onto thousands of unscored columns
            would report a precision the measurement cannot support.
    """
    if population_columns > 0 and not per_column_fp:
        raise StratifiedProjectionError(
            f"{population_columns} unlabelled columns exist but none were sampled; a "
            "precision projected over them would be fabricated"
        )
    sampled = len(per_column_fp)
    sampled_fp = sum(per_column_fp)
    if sampled == 0:
        projected, interval = 0.0, (0.0, 0.0)
    else:
        mean = sampled_fp / sampled
        projected = mean * population_columns
        # Normal-approximate interval on the total, from the per-column variance. Stated
        # rather than hidden: this assumes columns are exchangeable draws, which is why the
        # sample must be random and why zero-flag columns must be retained.
        if sampled > 1:
            variance = sum((count - mean) ** 2 for count in per_column_fp) / (sampled - 1)
            standard_error = (variance / sampled) ** 0.5
        else:
            standard_error = float(mean)
        half = _Z * standard_error * population_columns
        interval = (max(0.0, round(projected - half, 2)), round(projected + half, 2))
    return StratifiedPrecision(
        census_columns=census_score.columns_scored,
        census_tp=census_score.tp,
        census_fp=census_score.fp,
        census_fn=census_score.fn,
        census_debatable_predicted=census_score.debatable_predicted,
        sampled_columns=sampled,
        population_columns=population_columns,
        sampled_fp=sampled_fp,
        projected_fp=round(projected, 2),
        projected_fp_ci=interval,
    )
