"""The environment contract: every billable knob must be documented.

Before ``.env.example`` existed, the variables a user must configure were
described only in prose across two runbooks, so there was no machine-checkable
list and a newly added metered provider could ship with no documented cap.

These tests derive their expectations **from the code**, not from a hand-copied
list, so they cannot silently drift:

* every priced (billable) provider in :data:`dataforge.spend.PRICES` must have a
  documented ``DATAFORGE_<PROVIDER>_MAX_USD`` cap;
* every implemented provider must have its credential variable documented;
* the template must never contain a real secret.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from dataforge.agent.providers import _OPENAI_COMPAT_CONFIG
from dataforge.spend import _GLOBAL_CAP_ENV, PRICES

_TEMPLATE = Path(__file__).resolve().parents[2] / ".env.example"

# Credential variable per implemented provider. Kept explicit because the
# provider modules read these directly rather than from a shared table.
_CREDENTIAL_ENV = {
    "azure": "AZURE_API_KEY",
    "bedrock": "AWS_BEARER_TOKEN_BEDROCK",
    "gemini": "GEMINI_API_KEY",
}


@pytest.fixture(scope="module")
def template_text() -> str:
    """Return the committed environment template."""
    assert _TEMPLATE.exists(), f"{_TEMPLATE} is missing"
    return _TEMPLATE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def documented(template_text: str) -> set[str]:
    """Return every variable name mentioned in the template."""
    return set(re.findall(r"(?m)^#?\s*([A-Z][A-Z0-9_]*)=", template_text))


class TestSpendContract:
    """Billable providers cannot ship without a documented cap."""

    def test_every_priced_provider_has_a_documented_cap(self, documented: set[str]) -> None:
        # This is the invariant that matters: `cap_from_env` builds the name
        # dynamically, so a static grep would miss a new provider. Deriving the
        # expectation from PRICES means adding a metered provider without
        # documenting its cap fails here.
        missing = {
            f"DATAFORGE_{provider.upper()}_MAX_USD"
            for provider in PRICES
            if f"DATAFORGE_{provider.upper()}_MAX_USD" not in documented
        }
        assert not missing, f"Priced providers with no documented USD cap: {sorted(missing)}"

    def test_global_cap_is_documented(self, documented: set[str]) -> None:
        assert _GLOBAL_CAP_ENV in documented

    def test_provider_selector_is_documented(self, documented: set[str]) -> None:
        assert "DATAFORGE_LLM_PROVIDER" in documented


class TestCredentialContract:
    """Every implemented provider's credential is discoverable."""

    def test_openai_compatible_credentials_are_documented(self, documented: set[str]) -> None:
        missing = {
            config.api_key_env
            for config in _OPENAI_COMPAT_CONFIG.values()
            if config.api_key_env not in documented
        }
        assert not missing, f"Undocumented provider credentials: {sorted(missing)}"

    @pytest.mark.parametrize(("provider", "env_var"), sorted(_CREDENTIAL_ENV.items()))
    def test_native_provider_credentials_are_documented(
        self, provider: str, env_var: str, documented: set[str]
    ) -> None:
        assert env_var in documented, f"{provider} credential {env_var} is undocumented"

    def test_azure_requires_endpoint_and_deployment(self, documented: set[str]) -> None:
        # Azure needs three variables, not one; omitting either of these is the
        # most common setup failure.
        assert "AZURE_OPENAI_ENDPOINT" in documented
        assert "DATAFORGE_AZURE_MODEL" in documented


class TestTemplateSafety:
    """The template is committed, so it must never hold a real secret."""

    @pytest.mark.parametrize(
        "pattern",
        [
            r"sk-[A-Za-z0-9]{20,}",  # OpenAI-style
            r"xai-[A-Za-z0-9]{20,}",  # xAI
            r"gsk_[A-Za-z0-9]{20,}",  # Groq
            r"hf_[A-Za-z0-9]{20,}",  # Hugging Face
            r"AIza[A-Za-z0-9_\-]{30,}",  # Google
        ],
    )
    def test_no_secret_shaped_values(self, pattern: str, template_text: str) -> None:
        assert not re.search(pattern, template_text), f"Template contains a {pattern} secret"

    def test_credential_values_are_blank(self, template_text: str) -> None:
        for line in template_text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or "=" not in stripped:
                continue
            name, _, value = stripped.partition("=")
            if name.endswith(("_API_KEY", "_TOKEN", "_TOKEN_BEDROCK")):
                assert value == "", f"{name} must be blank in the template, got {value!r}"

    def test_real_env_file_is_not_committed(self) -> None:
        # `.gitignore` covers `.env` by exact name; this guards against a rename
        # that would start tracking real credentials.
        gitignore = (_TEMPLATE.parent / ".gitignore").read_text(encoding="utf-8")
        assert any(line.strip() == ".env" for line in gitignore.splitlines())
