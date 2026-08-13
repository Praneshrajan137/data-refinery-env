"""The Trust Ledger: the end-to-end outcome metric the product did not have.

Every published number measured a DETECTOR: correction F1, precision, recall, ROC-AUC,
ECE. None measured what a user actually receives from a run -- how many cells got better,
how many got worse, and how much review the run cost. ``docs/STRATEGY.md`` even proposes
"trust metrics, not the F1" and names the headline it wants ("zero incorrect fixes were
auto-applied"), but the committed instance of that experiment recorded
``agent_fix_count: 0``: a zero denominator.

This module defines the missing quantities, and defines them so they cannot flatter.

Three rules, each earned from a correction in this repo's own history
--------------------------------------------------------------------
**A rate is reported with a bound, never as a bare point estimate.** Zero failures out of
a small n does not mean the rate is zero; it means the rate is below a bound that depends
on n. A "n=6 at 100%" figure was published here and retracted as a selected extremum.

**Improvement is net, not gross.** ``corrections - corruptions``. A tool that fixes 23
cells and corrupts 25 has made the data worse, and a gross count would call that progress.
That exact ratio was measured for one candidate repairer and is why it was rejected.

**Scope travels with the number.** A ledger describes the run it was computed from. It says
nothing about a different table, because the product cannot predict in advance whether it
will help an unseen one -- ``docs/STRATEGY.md`` calls that "the highest-value open
problem". :meth:`TrustLedger.scope_caveat` carries that sentence with the figures so the
number cannot be quoted without it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Final

__all__ = [
    "TrustLedger",
    "clopper_pearson_upper",
]

_PER: Final = 10_000


def clopper_pearson_upper(failures: int, trials: int, confidence: float = 0.95) -> float:
    """Exact (Clopper-Pearson) upper confidence bound on a failure rate.

    Chosen over a normal approximation because the interesting case here is a very small
    number of failures out of a modest n, which is exactly where the approximation is worst
    and can even produce a bound of zero.

    The ``failures == 0`` case has the closed form ``1 - alpha ** (1 / trials)``. The
    general case inverts the binomial tail by bisection, so no SciPy dependency is needed
    for a number that appears in published claims.
    """
    if trials <= 0:
        return 1.0
    if failures >= trials:
        return 1.0
    alpha = 1.0 - confidence
    if failures == 0:
        return float(1.0 - alpha ** (1.0 / trials))

    def tail_at_most(probability: float) -> float:
        total = 0.0
        for count in range(failures + 1):
            total += (
                math.comb(trials, count)
                * probability**count
                * (1.0 - probability) ** (trials - count)
            )
        return total

    low, high = 0.0, 1.0
    for _ in range(200):
        mid = (low + high) / 2.0
        if tail_at_most(mid) > alpha:
            low = mid
        else:
            high = mid
    return high


@dataclass(frozen=True)
class TrustLedger:
    """What one run did, in terms a user can act on.

    Args:
        cells_applied: Cells actually written.
        corrections: Applied cells whose new value matches ground truth.
        corruptions: Applied cells whose new value does NOT match ground truth. Requires
            ground truth, so this is measurable on a benchmark and NOT at runtime -- which
            is itself the honest limit, recorded rather than papered over.
        cells_held: Detected cells surfaced for review instead of written.
        cells_abstained: Cells where no value was derivable at all.
        real_errors: Ground-truth errors present in the table.
        reversibility_verified: Whether the applied set was proven to revert byte-for-byte.
    """

    cells_applied: int
    corrections: int
    corruptions: int
    cells_held: int
    cells_abstained: int
    real_errors: int
    reversibility_verified: bool

    def __post_init__(self) -> None:
        # Negativity first: it is the more fundamental error, and checking the balance
        # first would report a confusing "does not balance" for a negative count.
        for name in (
            "cells_applied",
            "corrections",
            "corruptions",
            "cells_held",
            "cells_abstained",
            "real_errors",
        ):
            if getattr(self, name) < 0:
                message = f"{name} cannot be negative"
                raise ValueError(message)
        if self.corrections + self.corruptions > self.cells_applied:
            message = (
                "corrections + corruptions cannot exceed cells_applied; a ledger that does "
                "not balance is not evidence"
            )
            raise ValueError(message)

    @property
    def net_cells_improved(self) -> int:
        """Corrections minus corruptions. The quantity absent from every prior report.

        Can be negative, deliberately. A tool that repairs 23 cells and corrupts 25 has
        made the data worse, and the metric must be able to say so.
        """
        return self.corrections - self.corruptions

    @property
    def corruption_exposure_per_10k(self) -> float:
        """Point estimate of corruptions per 10,000 applied cells.

        Never publish this alone; pair it with :meth:`corruption_exposure_upper_per_10k`.
        """
        if self.cells_applied == 0:
            return 0.0
        return self.corruptions / self.cells_applied * _PER

    def corruption_exposure_upper_per_10k(self, confidence: float = 0.95) -> float:
        """Upper confidence bound on corruptions per 10,000 applied cells.

        This is the honest headline. With zero corruptions over 40 applied cells the point
        estimate is 0, but the bound is roughly 700 per 10,000 -- which is the difference
        between "we measured no corruption" and "corruption is impossible".
        """
        return clopper_pearson_upper(self.corruptions, self.cells_applied, confidence) * _PER

    @property
    def review_effort_per_real_error(self) -> float | None:
        """Cells a human must read per genuine error found. None when nothing was flagged.

        The existing metric worth keeping: it moved from 1.78 to 22.80 on hospital when
        inferred constraints were accepted, which is the exchange rate that made queue
        flooding visible as a configuration choice rather than a dataset property.
        """
        surfaced = self.cells_applied + self.cells_held
        if surfaced == 0 or self.real_errors == 0:
            return None
        return surfaced / self.real_errors

    @property
    def is_net_positive(self) -> bool:
        return self.net_cells_improved > 0

    def scope_caveat(self) -> str:
        """The sentence that must travel with these figures."""
        return (
            "Measured on this run only. These figures describe the table, schema and "
            "configuration used; they do not predict behaviour on a different table, "
            "because whether a repair helps an unseen table cannot be determined at "
            "runtime without ground truth. Corruption counts require ground truth and are "
            "therefore measurable on a benchmark, not in production."
        )

    def as_dict(self) -> dict[str, object]:
        """Serialisable form, with the bound and the caveat included by construction.

        The bound and the caveat are not optional extras: emitting the point estimate alone
        is how a measured result becomes an overclaim.
        """
        return {
            "cells_applied": self.cells_applied,
            "corrections": self.corrections,
            "corruptions": self.corruptions,
            "cells_held": self.cells_held,
            "cells_abstained": self.cells_abstained,
            "real_errors": self.real_errors,
            "reversibility_verified": self.reversibility_verified,
            "net_cells_improved": self.net_cells_improved,
            "corruption_exposure_per_10k": round(self.corruption_exposure_per_10k, 4),
            "corruption_exposure_upper_95_per_10k": round(
                self.corruption_exposure_upper_per_10k(), 4
            ),
            "review_effort_per_real_error": (
                round(self.review_effort_per_real_error, 4)
                if self.review_effort_per_real_error is not None
                else None
            ),
            "scope": self.scope_caveat(),
        }

    def summary_lines(self) -> list[str]:
        """Human-readable summary. Leads with net improvement, not with a rate."""
        effort = self.review_effort_per_real_error
        effort_text = f"{effort:.2f} cells per real error" if effort is not None else "n/a"
        return [
            f"net cells improved:      {self.net_cells_improved:+d} "
            f"({self.corrections} corrected - {self.corruptions} corrupted)",
            f"cells written:           {self.cells_applied}",
            f"cells held for review:   {self.cells_held}",
            f"cells abstained:         {self.cells_abstained}",
            f"corruption exposure:     {self.corruption_exposure_per_10k:.1f} per 10k "
            f"(95% upper bound {self.corruption_exposure_upper_per_10k():.1f} per 10k)",
            f"review effort:           {effort_text}",
            f"reversibility verified:  {self.reversibility_verified}",
        ]
