"""Accountable LLM spend: one price table, one guard, one receipt.

DataForge's trust doctrine is that nothing is written without proof you can
re-verify independently. Paid inference was the one part of the system with none
of that: the product provider path returned a bare string (no tokens, no cost),
the bench clients accumulated an estimate and then discarded it at process exit,
and a call-count guard stood in for a spend guard even though prices and a call
estimate both already existed. This module closes that gap.

Three layers, deliberately independent:

* **Pre-flight** -- :func:`estimate_usd` multiplies a planned call count by the
  price table so an over-budget plan is refused *before* the first billable call.
* **In-flight** -- :class:`SpendMeter` accumulates real token usage and raises
  :class:`CostCapExceededError` the moment the cap is crossed. It is the single
  implementation; the previously triplicated per-client guards delegate to it.
* **After the fact** -- :class:`SpendReceipt` is appended to a committed ledger so
  every paid run leaves an auditable artifact, mirroring the data-mutation
  certificate.

Prices are deliberately **conservative (high)** so the guard trips early rather
than late. An estimate that is too pessimistic costs a re-run; one that is too
optimistic costs real money.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

_LEDGER_SCHEMA = "dataforge_spend_ledger_v1"

# Env var naming mirrors the existing per-provider convention
# (DATAFORGE_AZURE_MAX_USD etc.); DATAFORGE_MAX_USD is the global default that
# also covers the product/agent path, which had no cap at all before.
_GLOBAL_CAP_ENV = "DATAFORGE_MAX_USD"


class CostCapExceededError(RuntimeError):
    """Raised when cumulative estimated spend crosses the configured USD cap.

    This is a hard stop: once raised, no further billable calls are made. The
    estimate uses conservative (high) per-token prices so the guard trips early
    rather than late.

    Defined here as the canonical location and re-exported from
    ``dataforge.bench.groq_client`` for backward compatibility.
    """


@dataclass(frozen=True, slots=True)
class ModelPrice:
    """Conservative per-1k-token prices for one provider or model.

    Args:
        usd_per_1k_input: USD per 1000 prompt tokens.
        usd_per_1k_output: USD per 1000 completion tokens. Reasoning tokens are
            billed as output tokens, so they are charged at this rate.
    """

    usd_per_1k_input: float
    usd_per_1k_output: float

    def usd_for(self, prompt_tokens: int, completion_tokens: int) -> float:
        """Return the estimated USD cost of one call's token usage."""
        return (prompt_tokens / 1000.0) * self.usd_per_1k_input + (
            completion_tokens / 1000.0
        ) * self.usd_per_1k_output


# Single source of truth. Before this module, these numbers lived in bench-client
# constructor defaults, which is why the guard was triplicated and why Gemini --
# whose client took no price arguments at all -- was entirely unmetered.
#
# Groq and Cerebras are intentionally absent: they are used on free tiers, and a
# missing entry means "unpriced", which disables the USD guard rather than
# inventing a number. That preserves the previous no-op behavior exactly.
PRICES: dict[str, ModelPrice] = {
    "azure": ModelPrice(usd_per_1k_input=0.005, usd_per_1k_output=0.015),
    "bedrock": ModelPrice(usd_per_1k_input=0.003, usd_per_1k_output=0.015),
    "grok": ModelPrice(usd_per_1k_input=0.002, usd_per_1k_output=0.006),
    "gemini": ModelPrice(usd_per_1k_input=0.002, usd_per_1k_output=0.006),
}

# Per-MODEL prices, keyed ``(provider, model)``. Provider-level pricing is not merely
# imprecise here, it is a financial hazard: measured from this repo's own ledger,
# gpt-5.6-sol costs $0.00372/call and gpt-5-mini $0.00008/call -- a factor of 46. So a
# deployment swap that left the provider price in place would under-meter by 46x, and a
# $15 cap would authorise roughly $700 of real spend before tripping.
#
# That is not hypothetical. `.env` carried DATAFORGE_AZURE_USD_PER_1K_INPUT=0.00025
# (gpt-5-mini's rate) while the plan was to point DATAFORGE_AZURE_MODEL at gpt-5.6-sol.
#
# Values are deliberately conservative (rounded UP where uncertain) so that a wrong price
# causes a run to stop EARLY rather than overspend. The Azure retail prices API returns
# zero meters for these deployments, so these are list-rate estimates, not API-verified
# figures -- which is exactly why erring high is the right direction.
MODEL_PRICES: dict[tuple[str, str], ModelPrice] = {
    ("azure", "gpt-5.6-sol"): ModelPrice(usd_per_1k_input=0.005, usd_per_1k_output=0.015),
    ("azure", "gpt-5"): ModelPrice(usd_per_1k_input=0.005, usd_per_1k_output=0.015),
    ("azure", "gpt-5.5"): ModelPrice(usd_per_1k_input=0.005, usd_per_1k_output=0.015),
    ("azure", "gpt-5-mini"): ModelPrice(usd_per_1k_input=0.00025, usd_per_1k_output=0.002),
    ("azure", "gpt-5-nano"): ModelPrice(usd_per_1k_input=0.00005, usd_per_1k_output=0.0004),
}


def price_for(provider: str, model: str | None = None) -> ModelPrice | None:
    """Return the conservative price for a provider/model, or None if unpriced.

    Resolution order: exact ``(provider, model)`` match first, then the provider-level
    fallback. The per-model table exists because provider-level pricing silently
    mis-meters by up to 46x across Azure deployments (see :data:`MODEL_PRICES`).

    The provider fallback is retained rather than made strict because removing it would
    turn an unknown model into an *unpriced* one, and unpriced disables the cap entirely --
    a strictly worse failure. Instead the fallback uses the provider's conservative (high)
    number, so an unrecognised Azure deployment over-estimates cost and stops early.
    Callers that need certainty should use :func:`require_price_for`.

    Args:
        provider: Provider identifier (e.g. ``"azure"``).
        model: Optional model/deployment name.

    Returns:
        The :class:`ModelPrice`, or ``None`` when the provider is unpriced (free
        tier), in which case the USD guard is disabled by design.
    """
    key = provider.strip().lower()
    if model:
        exact = MODEL_PRICES.get((key, model.strip()))
        if exact is not None:
            return exact
    return PRICES.get(key)


def is_model_priced(provider: str, model: str | None) -> bool:
    """Return whether an EXACT per-model price exists, not just a provider fallback.

    Lets a paid run assert it knows what it is spending before it spends it, instead of
    discovering afterwards that it was metered at a neighbouring deployment's rate.
    """
    if not model:
        return False
    return (provider.strip().lower(), model.strip()) in MODEL_PRICES


def require_price_for(provider: str, model: str | None) -> ModelPrice:
    """Return the exact per-model price, or raise.

    Fails closed for paid experiments: metering a frontier deployment at a cheaper
    sibling's rate is how a capped run overspends, so a missing price is an error rather
    than a silent fallback.

    Raises:
        ValueError: If no exact ``(provider, model)`` price is registered.
    """
    if not is_model_priced(provider, model):
        raise ValueError(
            f"no per-model price registered for provider={provider!r} model={model!r}; "
            "add it to dataforge.spend.MODEL_PRICES (conservative/high) or set "
            f"DATAFORGE_{provider.strip().upper()}_USD_PER_1K_INPUT and _OUTPUT "
            "explicitly. Refusing to meter a paid run at a fallback rate."
        )
    price = price_for(provider, model)
    assert price is not None  # guaranteed by is_model_priced
    return price


def cap_from_env(provider: str) -> float | None:
    """Resolve the USD cap for a provider from the environment.

    Precedence is provider-specific first, then the global default, so an
    existing ``DATAFORGE_AZURE_MAX_USD`` still wins for Azure runs while
    ``DATAFORGE_MAX_USD`` provides a floor for every other path -- including the
    product/agent path, which previously had no cap of any kind.

    Args:
        provider: Provider identifier.

    Returns:
        The cap in USD, or ``None`` when no cap is configured.
    """
    specific = f"DATAFORGE_{provider.strip().upper()}_MAX_USD"
    for name in (specific, _GLOBAL_CAP_ENV):
        raw = os.environ.get(name, "").strip()
        if not raw:
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        # A non-positive value is the established way to disable the guard.
        return value if value > 0 else None
    return None


def estimate_usd(
    *,
    calls: int,
    avg_prompt_tokens: int,
    avg_completion_tokens: int,
    price: ModelPrice | None,
) -> float | None:
    """Estimate the USD cost of a planned run before making any call.

    This is the pre-flight half that was missing: the codebase already estimated
    call counts and already knew prices, but never multiplied them, so a bounded
    run against a frontier deployment passed the call-count guard unexamined.

    Args:
        calls: Planned number of billable calls.
        avg_prompt_tokens: Expected prompt tokens per call.
        avg_completion_tokens: Expected completion tokens per call.
        price: Provider price, or ``None`` when unpriced.

    Returns:
        The estimated spend in USD, or ``None`` when the provider is unpriced
        (no estimate is possible, so no refusal is made).

    Example:
        >>> estimate_usd(
        ...     calls=1000,
        ...     avg_prompt_tokens=350,
        ...     avg_completion_tokens=100,
        ...     price=PRICES["azure"],
        ... )
        3.25
    """
    if price is None:
        return None
    return round(calls * price.usd_for(avg_prompt_tokens, avg_completion_tokens), 6)


@dataclass(frozen=True, slots=True, kw_only=True)
class SpendReceipt:
    """An auditable record of one run's estimated spend.

    Every field is either measured (tokens, calls) or declared (cap, provenance).
    ``estimated_usd`` is explicitly an *estimate* at conservative prices, not a
    billed amount -- the honest name matters, because the authoritative figure
    lives in the provider's billing portal, not here.
    """

    run_id: str
    utc: str
    provider: str
    model: str
    calls: int
    prompt_tokens: int
    completion_tokens: int
    reasoning_tokens: int
    estimated_usd: float
    cap_usd: float | None
    method: str | None = None
    dataset: str | None = None
    git_sha: str | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-serializable mapping for the ledger."""
        payload = asdict(self)
        payload["notes"] = list(self.notes)
        return payload


class SpendMeter:
    """Thread-safe cumulative spend accounting with a hard stop.

    This is the single in-flight guard. It replaces three byte-similar copies
    (the OpenAI-compatible client's ``_enforce_cost_guard`` plus inline blocks in
    the Azure and Bedrock clients) whose prices lived in constructor defaults.

    Args:
        provider: Provider identifier, used in the error message.
        model: Model/deployment name, recorded on the receipt.
        price: Conservative price, or ``None`` to disable the USD guard (free
            tiers) while still counting calls and tokens.
        max_usd: Hard cap in USD, or ``None`` for no cap.
        fail_on_missing_usage: When true, a call reporting no usage payload
            raises instead of silently contributing zero cost. Metered runs set
            this, because a provider omitting usage would otherwise zero the
            guard and let a capped run overspend undetected.
    """

    def __init__(
        self,
        *,
        provider: str,
        model: str = "",
        price: ModelPrice | None = None,
        max_usd: float | None = None,
        fail_on_missing_usage: bool = False,
    ) -> None:
        self._provider = provider
        self._model = model
        self._price = price
        self._max_usd = max_usd
        self._fail_on_missing_usage = fail_on_missing_usage
        self._lock = threading.Lock()
        self._cumulative_usd = 0.0
        self._calls = 0
        self._prompt_tokens = 0
        self._completion_tokens = 0
        self._reasoning_tokens = 0

    @property
    def cumulative_usd(self) -> float:
        """Return the cumulative estimated spend so far (0 when unpriced)."""
        return self._cumulative_usd

    @property
    def calls(self) -> int:
        """Return the number of recorded billable calls."""
        return self._calls

    @property
    def max_usd(self) -> float | None:
        """Return the configured hard cap, if any."""
        return self._max_usd

    def record(
        self,
        *,
        prompt_tokens: int,
        completion_tokens: int,
        reasoning_tokens: int = 0,
        usage_present: bool = True,
    ) -> float:
        """Record one call's usage and hard-stop if the cap is crossed.

        Args:
            prompt_tokens: Prompt tokens billed for this call.
            completion_tokens: Completion tokens billed for this call. Reasoning
                tokens are already included by providers that report them.
            reasoning_tokens: Hidden reasoning tokens, recorded for visibility.
            usage_present: Whether the provider returned a usage payload.

        Returns:
            The cumulative estimated spend after recording this call.

        Raises:
            CostCapExceededError: If cumulative spend now exceeds the cap, or if
                usage was missing on a run configured to fail closed.
        """
        with self._lock:
            self._calls += 1
            self._prompt_tokens += prompt_tokens
            self._completion_tokens += completion_tokens
            self._reasoning_tokens += reasoning_tokens
            if self._price is not None:
                self._cumulative_usd += self._price.usd_for(prompt_tokens, completion_tokens)
            cumulative = self._cumulative_usd
            over_cap = self._max_usd is not None and cumulative > self._max_usd

        if not usage_present and self._fail_on_missing_usage and self._price is not None:
            raise CostCapExceededError(
                f"{self._provider} returned no usage payload on a metered run, so "
                "spend cannot be accounted for. Refusing to continue blind "
                f"(estimated ${cumulative:.4f} so far). Set "
                f"DATAFORGE_{self._provider.upper()}_MAX_USD=0 to run unmetered."
            )
        if over_cap:
            assert self._max_usd is not None  # narrowed by over_cap
            raise CostCapExceededError(
                f"{self._provider} spend guard tripped: estimated "
                f"${cumulative:.4f} exceeds cap ${self._max_usd:.2f}. "
                "No further calls will be made."
            )
        return cumulative

    def receipt(
        self,
        *,
        run_id: str,
        method: str | None = None,
        dataset: str | None = None,
        git_sha: str | None = None,
        notes: tuple[str, ...] = (),
    ) -> SpendReceipt:
        """Snapshot this meter as an auditable receipt."""
        return SpendReceipt(
            run_id=run_id,
            utc=datetime.now(UTC).isoformat(timespec="seconds"),
            provider=self._provider,
            model=self._model,
            calls=self._calls,
            prompt_tokens=self._prompt_tokens,
            completion_tokens=self._completion_tokens,
            reasoning_tokens=self._reasoning_tokens,
            estimated_usd=round(self._cumulative_usd, 6),
            cap_usd=self._max_usd,
            method=method,
            dataset=dataset,
            git_sha=git_sha,
            notes=notes,
        )


def meter_from_env(
    *,
    provider: str,
    model: str = "",
    fail_on_missing_usage: bool = False,
) -> SpendMeter:
    """Build a :class:`SpendMeter` from the price table and environment caps."""
    return SpendMeter(
        provider=provider,
        model=model,
        price=price_for(provider, model),
        max_usd=cap_from_env(provider),
        fail_on_missing_usage=fail_on_missing_usage,
    )


def load_ledger(path: Path) -> list[dict[str, object]]:
    """Return the receipts recorded in a ledger file (empty when absent)."""
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, dict):
        return []
    receipts = payload.get("receipts")
    return [r for r in receipts if isinstance(r, dict)] if isinstance(receipts, list) else []


def append_receipt(path: Path, receipt: SpendReceipt) -> Path:
    """Append a receipt to the committed spend ledger, creating it if needed.

    The ledger is append-only: it is a record of what was spent, so rewriting
    history would defeat its purpose. Callers commit it alongside the run's
    result artifact.

    Args:
        path: Ledger path (conventionally ``eval/results/spend_ledger.json``).
        receipt: The receipt to append.

    Returns:
        The ledger path, for convenience.
    """
    receipts = load_ledger(path)
    receipts.append(receipt.to_payload())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"schema": _LEDGER_SCHEMA, "receipts": receipts}, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def total_estimated_usd(path: Path) -> float:
    """Return the summed estimated spend across every receipt in a ledger.

    This is the number to check against an overall campaign budget. Prefer
    :func:`ledger_summary` when reporting it to a human: a bare total hides how
    much of it was actually measured.
    """
    total = 0.0
    for entry in load_ledger(path):
        value = entry.get("estimated_usd")
        if isinstance(value, int | float):
            total += float(value)
    return round(total, 6)


@dataclass(frozen=True, slots=True)
class LedgerSummary:
    """Measured versus reconstructed spend in a ledger.

    A receipt with ``calls == 0`` but nonzero USD carries no measured token counts, so its
    USD is a reconstruction rather than an observation. Reporting a single total conceals
    that distinction -- and during the phase that built this module, most recorded spend was
    reconstruction, which a bare total presented as fact.

    A receipt with ``calls == 0`` **and** zero USD is neither: it is a no-op run that made
    no billable call (for example one whose every request was rejected). Counting those as
    reconstructions would overstate how much of the ledger is estimated, so they are
    tracked separately.
    """

    measured_usd: float
    estimated_usd: float
    measured_receipts: int
    estimated_receipts: int
    noop_receipts: int = 0

    @property
    def total_usd(self) -> float:
        """Return the combined total."""
        return round(self.measured_usd + self.estimated_usd, 6)

    @property
    def measured_fraction(self) -> float:
        """Return the share of recorded spend that was actually measured."""
        total = self.total_usd
        return round(self.measured_usd / total, 4) if total else 1.0

    def describe(self) -> str:
        """Return a one-line honest summary suitable for a report."""
        noop = f"; {self.noop_receipts} no-op receipts" if self.noop_receipts else ""
        return (
            f"${self.total_usd:.2f} total = ${self.measured_usd:.2f} measured "
            f"({self.measured_receipts} receipts) + ${self.estimated_usd:.2f} "
            f"reconstructed ({self.estimated_receipts} receipts); "
            f"{self.measured_fraction:.0%} measured{noop}"
        )


def ledger_summary(path: Path) -> LedgerSummary:
    """Split a ledger's spend into measured and reconstructed halves.

    Args:
        path: Ledger path.

    Returns:
        A :class:`LedgerSummary`. A receipt with ``calls == 0`` and nonzero USD is a
        reconstruction, because without token counts its USD cannot have been derived from
        an observation. A receipt with ``calls == 0`` and zero USD made no billable call at
        all and is counted as a no-op rather than an estimate.
    """
    measured = estimated = 0.0
    n_measured = n_estimated = n_noop = 0
    for entry in load_ledger(path):
        value = entry.get("estimated_usd")
        usd = float(value) if isinstance(value, int | float) else 0.0
        calls = entry.get("calls")
        if isinstance(calls, int) and calls > 0:
            measured += usd
            n_measured += 1
        elif usd > 0.0:
            estimated += usd
            n_estimated += 1
        else:
            n_noop += 1
    return LedgerSummary(
        measured_usd=round(measured, 6),
        estimated_usd=round(estimated, 6),
        measured_receipts=n_measured,
        estimated_receipts=n_estimated,
        noop_receipts=n_noop,
    )


def prices_from_env(provider: str, model: str | None = None) -> ModelPrice | None:
    """Return the prices a real run actually charges itself, honouring env overrides.

    Resolution: exact per-model price, else provider fallback, then
    ``DATAFORGE_<PROVIDER>_USD_PER_1K_INPUT`` / ``_OUTPUT`` on top. Passing ``model`` matters --
    omitting it silently meters every Azure deployment at one rate, and the measured spread is
    46x (gpt-5.6-sol $0.00372/call vs gpt-5-mini $0.00008/call).

    A receipt written from the table rather than from the overrides would disagree with the cap that
    was actually enforced during the run. Resolving both in one place is what keeps the ledger and
    the guard talking about the same money.

    Returns ``None`` for unpriced providers, matching ``price_for``, so callers keep the documented
    "no price means no USD guard" behaviour instead of inventing a number.
    """
    base = price_for(provider, model)
    if base is None:
        return None
    prefix = f"DATAFORGE_{provider.strip().upper()}_USD_PER_1K"

    def _override(suffix: str, fallback: float) -> float:
        raw = os.environ.get(f"{prefix}_{suffix}", "").strip()
        if not raw:
            return fallback
        try:
            value = float(raw)
        except ValueError:
            return fallback
        return value if value >= 0 else fallback

    return ModelPrice(
        usd_per_1k_input=_override("INPUT", base.usd_per_1k_input),
        usd_per_1k_output=_override("OUTPUT", base.usd_per_1k_output),
    )
