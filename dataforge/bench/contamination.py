"""Contamination audit verdict logic.

Implements `specs/SPEC_contamination_audit.md`. Separated from
``scripts/bench/probe_contamination.py`` so the verdict rule is unit-testable without
spending money: the probe script gathers evidence, this module decides what the evidence
means, and the decision is the part that must not drift.

The rule this exists to make unavoidable: an audit whose result determines whether expensive
work proceeds has an incentive gradient pointing at inconclusiveness. So the stopping
conditions are computed here, from pre-registered thresholds, with no path that turns a
missing measurement into a clean one.

Three probes, from published methods (citations in the spec):

* ``C1`` exchangeability -- Oren et al. The only *provable* one, and the one most likely to
  be unavailable, because reasoning deployments commonly reject ``logprobs``.
* ``C2`` guided vs general instruction -- Golchin & Surdeanu.
* ``C3`` slot guessing on a capability-free target -- Deng et al.

Plus ``C4``, a negative control on synthetic never-published content. C4 is not a fourth
contamination probe; it tests the *probes*. If the suite fires on content the model cannot
have seen, no verdict can be read off it.
"""

from __future__ import annotations

import json
import random
from collections.abc import Sequence
from dataclasses import dataclass
from math import comb
from pathlib import Path
from typing import Literal

__all__ = [
    "ContaminationAuditError",
    "ProbeOutcome",
    "AuditVerdict",
    "Verdict",
    "binomial_p_value_greater",
    "exchangeability_available",
    "majority_base_rate",
    "paired_signflip_p_value",
    "decide_verdict",
]

Verdict = Literal["CLEAN", "SUSPECTED", "CONTAMINATED", "VOID"]

#: Pre-registered alpha for every test. Fixed in
#: ``eval/preregistration/contamination_audit.md``; a module constant for the same reason
#: ``RISK_COVERAGE_GRID`` is one -- a threshold chosen after seeing the data is a validity
#: problem, not a power problem.
ALPHA = 0.01

#: C2 requires a minimum effect size as well as significance. A large paired sample makes a
#: negligible delta significant, and prompt-format asymmetry is a negligible delta.
C2_MIN_DELTA = 0.05


class ContaminationAuditError(RuntimeError):
    """Raised when a verdict is requested that the evidence cannot support."""


@dataclass(frozen=True, slots=True)
class ProbeOutcome:
    """One probe's result.

    ``available`` is the field that carries L1. A probe that could not run is **excluded
    from the flag count**, never recorded as having failed to flag: an audit that returns
    "clean" because its strongest instrument was missing is reporting an absence of
    measurement as an absence of contamination.
    """

    probe: Literal["C1", "C2", "C3", "C4"]
    available: bool
    p_value: float | None
    effect: float | None
    detail: str

    @property
    def flagged(self) -> bool:
        """Whether this probe detected memorisation under its pre-registered rule.

        Raises:
            ContaminationAuditError: If the probe did not run. Callers must consult
                :attr:`available` first; a boolean here would silently become ``False``.
        """
        if not self.available:
            raise ContaminationAuditError(
                f"{self.probe} did not run ({self.detail}); it has no flag state. "
                "An unavailable probe is a recorded limit, not a clean result."
            )
        if self.p_value is None:
            raise ContaminationAuditError(f"{self.probe} ran but reported no p-value")
        if self.p_value >= ALPHA:
            return False
        if self.probe == "C2":
            # Both clauses, per the spec. Significance alone is not enough.
            return self.effect is not None and self.effect >= C2_MIN_DELTA
        return True


@dataclass(frozen=True, slots=True)
class AuditVerdict:
    """The audit's decision, bound to what produced it."""

    verdict: Verdict
    flagged_probes: tuple[str, ...]
    unavailable_probes: tuple[str, ...]
    model: str
    seed: int
    reference_sha256: str
    exchangeability_available: bool

    @property
    def contamination_suspected(self) -> bool:
        """Whether downstream artifacts must carry a suspicion flag."""
        return self.verdict in {"SUSPECTED", "CONTAMINATED"}

    @property
    def cancels_wild_column_measurement(self) -> bool:
        """Whether the pre-registered kill criterion fired."""
        return self.verdict == "CONTAMINATED"


def majority_base_rate(class_a_count: int, class_b_count: int) -> float:
    """Return the accuracy of always guessing the larger class.

    C3's comparison point. Testing against 0.5 instead would manufacture a finding on
    ST-bench, whose 47/124 split makes constant "debatable" answering worth 0.6210.

    Args:
        class_a_count: Items in the first class.
        class_b_count: Items in the second class.

    Returns:
        The majority-class share.

    Raises:
        ContaminationAuditError: If there are no items, which has no base rate.
    """
    total = class_a_count + class_b_count
    if total <= 0:
        raise ContaminationAuditError("a base rate over zero items is undefined")
    return max(class_a_count, class_b_count) / total


def binomial_p_value_greater(successes: int, trials: int, base_rate: float) -> float:
    """Exact one-sided binomial p-value for observing at least this many successes.

    Exact rather than normal-approximate because the counts here are small (200 items for
    C3, split across two corpora) and the approximation is poor in the tail that matters.

    Args:
        successes: Observed successes.
        trials: Total trials.
        base_rate: Null-hypothesis success probability.

    Returns:
        ``P(X >= successes)`` under ``Binomial(trials, base_rate)``.

    Raises:
        ContaminationAuditError: On a degenerate input that has no p-value.
    """
    if trials <= 0:
        raise ContaminationAuditError("a p-value over zero trials is undefined")
    if not 0 <= successes <= trials:
        raise ContaminationAuditError(f"successes {successes} outside 0..{trials}")
    if not 0.0 <= base_rate <= 1.0:
        raise ContaminationAuditError(f"base rate {base_rate} outside [0, 1]")
    if base_rate == 0.0:
        return 1.0 if successes == 0 else 0.0
    if base_rate == 1.0:
        return 1.0
    total = 0.0
    for k in range(successes, trials + 1):
        total += comb(trials, k) * (base_rate**k) * ((1 - base_rate) ** (trials - k))
    return min(1.0, total)


def paired_signflip_p_value(
    deltas: Sequence[float],
    *,
    resamples: int = 20_000,
    seed: int = 0,
) -> float:
    """One-sided p-value that the mean paired delta exceeds zero, by sign-flip permutation.

    Under the null that corpus identification changes nothing, each pair's delta is equally
    likely to carry either sign, so the sign vector is exchangeable. Flipping signs at random
    builds the null distribution of the mean directly.

    Chosen over a paired t-test because these deltas are bounded, discrete and skewed -- a
    recall difference over an 8-to-40 value held-out set is nothing like normal -- and over a
    sign test because that discards magnitude, which is the second pre-registered clause.
    Fixed ``resamples`` and ``seed``, both pre-registered, because a Monte Carlo p-value that
    moves between runs invites re-rolling.

    Args:
        deltas: Paired differences, guided minus general, one per item.
        resamples: Sign-flip resamples. Pre-registered at 20,000.
        seed: Fixed RNG seed.

    Returns:
        The proportion of resampled means at least as large as the observed mean, with the
        observed configuration counted (add-one), so the p-value is never exactly 0.

    Raises:
        ContaminationAuditError: If there are no deltas, or all are exactly zero. All-zero
            deltas have no sign to flip and would return a meaningless 1.0 from a degenerate
            null; the caller must recognise "the arms were identical" as its own outcome.
    """
    if not deltas:
        raise ContaminationAuditError("a paired test over zero pairs is undefined")
    if resamples <= 0:
        raise ContaminationAuditError("resamples must be positive")
    if all(delta == 0.0 for delta in deltas):
        raise ContaminationAuditError(
            "every paired delta is exactly zero: the arms produced identical output, which "
            "is a finding to report rather than a p-value to compute"
        )

    observed = sum(deltas) / len(deltas)
    rng = random.Random(seed)
    at_least_as_extreme = 0
    for _ in range(resamples):
        total = 0.0
        for delta in deltas:
            total += delta if rng.random() < 0.5 else -delta
        if total / len(deltas) >= observed:
            at_least_as_extreme += 1
    # Add-one so a p-value is never reported as exactly zero from a finite resample.
    return (at_least_as_extreme + 1) / (resamples + 1)


def exchangeability_available(capability_artifact: Path) -> tuple[bool, str]:
    """Return whether Oren et al.'s exchangeability test can run, and why not if it cannot.

    Read from the committed Azure capability artifact rather than hardcoded, so the answer
    tracks the deployment. **Fail-closed on every uncertain path**: a missing, unreadable or
    silent artifact returns ``False``. Assuming the capability is present and emitting a
    p-value computed from nothing is the dangerous direction, and a p-value is exactly the
    kind of output a reader will trust without checking its premise.

    Measured on ``gpt-5.6-sol`` at api-version ``2025-04-01-preview``, 2026-08-24: the
    deployment returns HTTP 400 ``unsupported_parameter`` for ``logprobs``.

    There is also a structural reason no workaround exists, recorded so nobody spends a day
    looking for one. Oren et al. need the log-likelihood of a **provided** ordering, which
    requires teacher-forced scoring of text the caller supplies. Chat Completions returns
    logprobs for tokens the model **generated**, so even where the parameter is accepted the
    quantity the test needs is not obtainable. Asking the model to reproduce the canonical
    order instead is a different experiment -- it is C2.

    Args:
        capability_artifact: Path to ``eval/results/azure_capability_probe.json``.

    Returns:
        ``(available, reason)``. ``reason`` is always populated, including on success, so an
        artifact can record why the decision went the way it did.
    """
    if not capability_artifact.exists():
        return False, (
            f"capability artifact {capability_artifact.name} is absent; logprob support is "
            "unknown and is assumed unavailable rather than presumed"
        )
    try:
        payload = json.loads(capability_artifact.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return False, f"capability artifact unreadable ({type(exc).__name__}); assumed unavailable"

    probe = (payload.get("probes") or {}).get("logprobs")
    if not isinstance(probe, dict):
        return False, "capability artifact records no logprobs probe; assumed unavailable"
    if probe.get("accepted") is not True:
        model = payload.get("model", "unknown")
        kind = probe.get("error_kind", "rejected")
        return False, (
            f"deployment {model} rejects logprobs ({kind}), so the log-likelihood of a "
            "canonical ordering cannot be obtained. Oren et al.'s provable test is "
            "unavailable and this is recorded as a limit, not as a clean result."
        )
    return True, (
        "deployment accepts logprobs. NOTE: chat-completions logprobs cover generated "
        "tokens only, so confirm teacher-forced scoring of a provided ordering is genuinely "
        "available before treating C1 as implementable."
    )


def decide_verdict(
    outcomes: dict[str, ProbeOutcome],
    *,
    model: str,
    seed: int,
    reference_sha256: str,
) -> AuditVerdict:
    """Apply the pre-registered verdict rule.

    Args:
        outcomes: Probe id to outcome. Must contain ``C4``.
        model: Deployment identifier the verdict is bound to.
        seed: Seed the verdict is bound to.
        reference_sha256: Corpus digest the verdict is bound to.

    Returns:
        The :class:`AuditVerdict`.

    Raises:
        ContaminationAuditError: If ``C4`` is absent (non-vacuity requirement 1), if no
            contamination probe ran (requirement 3), or if ``C4`` flagged, which voids the
            audit rather than producing a verdict.
    """
    if "C4" not in outcomes:
        raise ContaminationAuditError(
            "C4 (negative control) is absent. A probe suite with no negative control "
            "cannot distinguish a contaminated corpus from a leading prompt."
        )

    control = outcomes["C4"]
    if not control.available:
        raise ContaminationAuditError(
            f"C4 did not run ({control.detail}); the audit cannot be interpreted without it"
        )
    if control.flagged:
        # VOID dominates, evaluated before the C1-C3 count.
        raise ContaminationAuditError(
            "C4 flagged: the probes fire on synthetic content the model has not seen, so "
            "they are measuring their own prompt design. Verdict is VOID and no "
            f"contamination conclusion may be drawn. Detail: {control.detail}"
        )

    probes = [outcomes[name] for name in ("C1", "C2", "C3") if name in outcomes]
    available = [probe for probe in probes if probe.available]
    if not available:
        raise ContaminationAuditError(
            "no contamination probe ran. A verdict over zero executed probes would report "
            "CLEAN from nothing, which is the all_parity failure this project has shipped "
            "once already."
        )

    flagged = tuple(probe.probe for probe in available if probe.flagged)
    unavailable = tuple(probe.probe for probe in probes if not probe.available)

    if len(flagged) >= 2:
        verdict: Verdict = "CONTAMINATED"
    elif len(flagged) == 1:
        verdict = "SUSPECTED"
    else:
        verdict = "CLEAN"

    return AuditVerdict(
        verdict=verdict,
        flagged_probes=flagged,
        unavailable_probes=unavailable,
        model=model,
        seed=seed,
        reference_sha256=reference_sha256,
        exchangeability_available="C1" in outcomes and outcomes["C1"].available,
    )
