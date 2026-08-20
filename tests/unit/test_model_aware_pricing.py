"""A model swap must not be able to silently under-meter a paid run.

`price_for(provider, model)` used to do `del model`. Pricing was provider-keyed, so every
Azure deployment metered at one rate. Measured from this repo's own ledger
(`eval/results/spend_ledger.json`), gpt-5.6-sol costs $0.00372/call and gpt-5-mini
$0.00008/call -- a factor of 46.

That gap was live, not theoretical. `.env` held `DATAFORGE_AZURE_USD_PER_1K_INPUT=0.00025`
(gpt-5-mini's rate) with `DATAFORGE_AZURE_MAX_USD=15`, and the next step was to point
`DATAFORGE_AZURE_MODEL` at gpt-5.6-sol. Changing only the model would have left a $15 cap
authorising hundreds of dollars of real spend, because `SpendMeter` bills each call at
whatever price it was handed.

Two directional choices are load-bearing and are asserted below:

* An **unknown** model falls back to the provider's *conservative (high)* rate, so an
  unrecognised deployment over-estimates and stops early. Falling back low, or returning
  None, would be worse -- None disables the cap entirely.
* `require_price_for` **fails closed** so a paid experiment can assert it knows what it is
  spending before it spends it.
"""

from __future__ import annotations

import pytest

from dataforge.spend import (
    MODEL_PRICES,
    PRICES,
    ModelPrice,
    SpendMeter,
    is_model_priced,
    price_for,
    prices_from_env,
    require_price_for,
)


class TestPerModelPricesAreDistinct:
    def test_sol_and_mini_do_not_share_a_price(self) -> None:
        """The whole point: one number cannot be right for both."""
        sol = price_for("azure", "gpt-5.6-sol")
        mini = price_for("azure", "gpt-5-mini")
        assert sol != mini

    def test_sol_is_the_more_expensive_of_the_two(self) -> None:
        """Directionality matters: inverting these would under-meter the frontier model."""
        sol = price_for("azure", "gpt-5.6-sol")
        mini = price_for("azure", "gpt-5-mini")
        assert sol is not None and mini is not None
        assert sol.usd_per_1k_input > mini.usd_per_1k_input
        assert sol.usd_per_1k_output > mini.usd_per_1k_output

    def test_the_measured_ratio_is_at_least_ten_fold(self) -> None:
        """A swap metered at the wrong rate is off by this factor, so it cannot be small."""
        sol = price_for("azure", "gpt-5.6-sol")
        mini = price_for("azure", "gpt-5-mini")
        assert sol is not None and mini is not None
        sol_call = sol.usd_for(600, 30)
        mini_call = mini.usd_for(600, 30)
        assert sol_call / mini_call >= 10.0

    @pytest.mark.parametrize(
        "model", ["gpt-5.6-sol", "gpt-5", "gpt-5.5", "gpt-5-mini", "gpt-5-nano"]
    )
    def test_every_deployment_in_use_has_an_exact_price(self, model: str) -> None:
        assert is_model_priced("azure", model)


class TestFallbackIsConservative:
    def test_an_unknown_model_falls_back_to_the_provider_rate(self) -> None:
        assert price_for("azure", "some-future-deployment") == PRICES["azure"]

    def test_the_fallback_is_not_cheaper_than_any_registered_azure_model(self) -> None:
        """Falling back cheap would make an unrecognised deployment under-meter."""
        fallback = PRICES["azure"]
        azure_models = [p for (prov, _), p in MODEL_PRICES.items() if prov == "azure"]
        assert fallback.usd_per_1k_input >= min(p.usd_per_1k_input for p in azure_models)
        assert fallback.usd_per_1k_input == max(p.usd_per_1k_input for p in azure_models)

    def test_an_unknown_model_is_not_reported_as_exactly_priced(self) -> None:
        assert not is_model_priced("azure", "some-future-deployment")

    def test_no_model_is_not_reported_as_exactly_priced(self) -> None:
        assert not is_model_priced("azure", None)

    def test_an_unpriced_provider_stays_unpriced(self) -> None:
        """Groq/Cerebras are free-tier; a missing entry must keep disabling the guard."""
        assert price_for("groq", "llama-3.1-70b-versatile") is None


class TestRequirePriceFailsClosed:
    def test_a_registered_model_is_returned(self) -> None:
        assert require_price_for("azure", "gpt-5.6-sol") == MODEL_PRICES[("azure", "gpt-5.6-sol")]

    def test_an_unregistered_model_raises(self) -> None:
        with pytest.raises(ValueError, match="no per-model price registered"):
            require_price_for("azure", "made-up-deployment")

    def test_a_missing_model_raises(self) -> None:
        with pytest.raises(ValueError, match="no per-model price registered"):
            require_price_for("azure", None)

    def test_the_error_names_the_override_env_vars(self) -> None:
        """The message has to say how to fix it, or it just blocks work."""
        with pytest.raises(ValueError, match="DATAFORGE_AZURE_USD_PER_1K_INPUT"):
            require_price_for("azure", "made-up-deployment")


class TestEnvOverridesRespectTheModel:
    def test_the_model_reaches_the_env_price_resolver(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DATAFORGE_AZURE_USD_PER_1K_INPUT", raising=False)
        monkeypatch.delenv("DATAFORGE_AZURE_USD_PER_1K_OUTPUT", raising=False)
        assert prices_from_env("azure", "gpt-5-mini") == MODEL_PRICES[("azure", "gpt-5-mini")]
        assert prices_from_env("azure", "gpt-5.6-sol") == MODEL_PRICES[("azure", "gpt-5.6-sol")]

    def test_an_explicit_override_still_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The escape hatch must keep working for a deployment we have not priced."""
        monkeypatch.setenv("DATAFORGE_AZURE_USD_PER_1K_INPUT", "0.111")
        monkeypatch.setenv("DATAFORGE_AZURE_USD_PER_1K_OUTPUT", "0.222")
        price = prices_from_env("azure", "gpt-5.6-sol")
        assert price == ModelPrice(usd_per_1k_input=0.111, usd_per_1k_output=0.222)


class TestTheCapActuallyBindsAtTheRightRate:
    def test_a_sol_run_trips_a_cap_that_a_mini_run_would_not(self) -> None:
        """The concrete failure this prevents: same tokens, same cap, different verdicts.

        Metered at mini's rate the run continues; metered correctly it stops. Before the
        fix both arms used one provider price, so the sol arm was the one that continued.
        """
        tokens = (600_000, 30_000)  # ~1000 calls' worth
        cap = 1.0

        sol_meter = SpendMeter(
            provider="azure", price=require_price_for("azure", "gpt-5.6-sol"), max_usd=cap
        )
        mini_meter = SpendMeter(
            provider="azure", price=require_price_for("azure", "gpt-5-mini"), max_usd=cap
        )

        from dataforge.spend import CostCapExceededError

        with pytest.raises(CostCapExceededError):
            sol_meter.record(prompt_tokens=tokens[0], completion_tokens=tokens[1])

        mini_meter.record(prompt_tokens=tokens[0], completion_tokens=tokens[1])
        assert mini_meter.cumulative_usd < cap
