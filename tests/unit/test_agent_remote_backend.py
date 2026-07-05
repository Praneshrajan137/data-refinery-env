"""Tests for the torch-free remote model backend.

The transport speaks Gradio's REST protocol (submit -> read the SSE stream).
These tests mock ``httpx.Client`` so they exercise the full submit/parse path
with no network, no ``torch``, and no running Space.
"""

from __future__ import annotations

from typing import Any

import pytest

from dataforge.agent.backends.remote import (
    RemoteBackendUnavailableError,
    RemoteCompletionError,
    _parse_sse,
    _submit,
    build_remote_completion,
)


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, json_body: Any = None, text: str = "") -> None:
        self.status_code = status_code
        self._json_body = json_body
        self.text = text

    def json(self) -> Any:
        return self._json_body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import httpx

            raise httpx.HTTPStatusError("error", request=None, response=None)  # type: ignore[arg-type]


class _FakeClient:
    """Records POST/GET calls and returns scripted responses."""

    def __init__(self, *, post_map: dict[str, _FakeResponse], get_response: _FakeResponse) -> None:
        self._post_map = post_map
        self._get_response = get_response
        self.posted: list[str] = []
        self.got: list[str] = []

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def post(self, url: str, json: Any = None) -> _FakeResponse:  # noqa: A002
        self.posted.append(url)
        for suffix, response in self._post_map.items():
            if url.endswith(suffix):
                return response
        return _FakeResponse(status_code=404)

    def get(self, url: str) -> _FakeResponse:
        self.got.append(url)
        return self._get_response


class TestParseSse:
    def test_extracts_completion_from_complete_event(self) -> None:
        body = 'event: generating\ndata: null\n\nevent: complete\ndata: ["hello world"]\n\n'
        assert _parse_sse(body) == "hello world"

    def test_raises_on_error_event(self) -> None:
        body = "event: error\ndata: boom\n\n"
        with pytest.raises(RemoteCompletionError, match="reported an error"):
            _parse_sse(body)

    def test_raises_when_no_data(self) -> None:
        with pytest.raises(RemoteCompletionError, match="no data"):
            _parse_sse("event: heartbeat\n\n")

    def test_raises_on_unparseable_data(self) -> None:
        with pytest.raises(RemoteCompletionError, match="unparseable"):
            _parse_sse("event: complete\ndata: not-json\n\n")


class TestSubmit:
    def test_falls_back_to_legacy_prefix_on_404(self) -> None:
        post_map = {
            "/gradio_api/call/generate": _FakeResponse(status_code=404),
            "/call/generate": _FakeResponse(json_body={"event_id": "abc"}),
        }
        client = _FakeClient(post_map=post_map, get_response=_FakeResponse())
        url = _submit(client, "https://space", "generate", ["[]", 0.0, 8])
        assert url == "https://space/call/generate/abc"
        assert client.posted == [
            "https://space/gradio_api/call/generate",
            "https://space/call/generate",
        ]

    def test_raises_when_no_event_id(self) -> None:
        post_map = {"/gradio_api/call/generate": _FakeResponse(json_body={})}
        client = _FakeClient(post_map=post_map, get_response=_FakeResponse())
        with pytest.raises(RemoteCompletionError, match="no event id"):
            _submit(client, "https://space", "generate", [])


class TestBuildRemoteCompletion:
    def test_missing_url_raises(self, monkeypatch) -> None:  # noqa: ANN001
        monkeypatch.delenv("DATAFORGE_REMOTE_MODEL_URL", raising=False)
        with pytest.raises(RemoteBackendUnavailableError):
            build_remote_completion()

    def test_end_to_end_completion(self, monkeypatch) -> None:  # noqa: ANN001
        monkeypatch.setenv("DATAFORGE_REMOTE_MODEL_URL", "https://space/")
        post_map = {"/gradio_api/call/generate": _FakeResponse(json_body={"event_id": "e1"})}
        get_response = _FakeResponse(
            text='event: complete\ndata: ["{\\"action_type\\":\\"FINALIZE\\"}"]\n\n'
        )
        fake_client = _FakeClient(post_map=post_map, get_response=get_response)

        import httpx

        monkeypatch.setattr(httpx, "Client", lambda *a, **k: fake_client)

        complete = build_remote_completion()
        result = complete([{"role": "user", "content": "hi"}], None, 0.0)
        assert result == '{"action_type":"FINALIZE"}'
        assert fake_client.posted == ["https://space/gradio_api/call/generate"]
        assert fake_client.got == ["https://space/gradio_api/call/generate/e1"]

    def test_http_error_becomes_remote_error(self, monkeypatch) -> None:  # noqa: ANN001
        monkeypatch.setenv("DATAFORGE_REMOTE_MODEL_URL", "https://space")

        import httpx

        def _raise(*_a: object, **_k: object) -> None:
            raise httpx.ConnectError("refused")

        monkeypatch.setattr(httpx, "Client", _raise)
        complete = build_remote_completion()
        with pytest.raises(RemoteCompletionError, match="request failed"):
            complete([{"role": "user", "content": "hi"}], None, 0.0)
