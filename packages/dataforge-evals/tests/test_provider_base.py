"""Provider adapter contract tests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from dataforge_evals.agents.provider_base import ChatProviderAgent, ProviderError
from dataforge_evals.tasks import load_synthetic_task


class ExampleProvider(ChatProviderAgent):
    """Concrete provider used to exercise the shared adapter behavior."""

    name = "example"
    provider = "example"

    @property
    def endpoint(self) -> str:
        """Return a placeholder OpenAI-compatible endpoint."""
        return "https://example.invalid/v1/chat/completions"

    def headers(self) -> dict[str, str]:
        """Return placeholder authorization headers."""
        return {"Authorization": "Bearer test", "Content-Type": "application/json"}


class TestProviderPayload:
    """Provider prompts must be strict without leaking answers."""

    def test_payload_includes_repair_contract_records_and_metadata(self) -> None:
        task = load_synthetic_task()
        payload = ExampleProvider(api_key="test", model="example-model").payload(task)

        messages = payload["messages"]
        assert isinstance(messages, list)
        system_prompt = str(messages[0]["content"])
        user_payload = json.loads(str(messages[1]["content"]))

        assert "Rows are zero-based" in system_prompt
        assert "columns must exactly match" in system_prompt
        assert "Return strict JSON only" in system_prompt
        assert '"action":"submit_repairs"' in system_prompt
        assert "Use finish with an empty repairs list" in system_prompt
        assert user_payload["dataset"] == "synthetic"
        assert user_payload["metadata"]["source"] == "built-in synthetic"
        assert user_payload["records"][0]["HospitalName"] == "Mercy Hosp"
        assert user_payload["columns"] == ["HospitalName", "Phone", "Score"]

    def test_payload_does_not_include_ground_truth_clean_values(self) -> None:
        task = load_synthetic_task()
        payload = ExampleProvider(api_key="test", model="example-model").payload(task)

        serialized = json.dumps(payload)

        assert "ground_truth" not in serialized
        assert "Mercy Hospital" not in serialized
        assert "217-555-0101" not in serialized
        assert '"4.5"' not in serialized


class TestProviderErrors:
    """Provider failures should be clear and bounded."""

    def test_parse_accepts_submit_repairs_object(self) -> None:
        fixes = ExampleProvider(api_key="test", model="example-model")._parse_fixes(
            '{"action":"submit_repairs","repairs":[{"row":0,"column":"Score","new_value":"4.5"}]}'
        )

        assert len(fixes) == 1
        assert fixes[0].column == "Score"
        assert fixes[0].new_value == "4.5"

    def test_post_with_backoff_wraps_http_timeout(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        mock_client.post.side_effect = httpx.TimeoutException("slow")

        with (
            patch("dataforge_evals.agents.provider_base.httpx.Client", return_value=mock_client),
            pytest.raises(ProviderError, match=r"\[example\] request timed out after 3.0 seconds"),
        ):
            ExampleProvider(
                api_key="test", model="example-model", http_timeout_s=3.0
            )._post_with_backoff({"messages": []})

        assert mock_client.post.call_count == 1
