"""Tests for Kaggle OAuth preflight isolation."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from scripts.preflight import check_kaggle_auth


def _write_oauth_credentials(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "refresh_token": "masked-refresh-token",
                "access_token": "masked-access-token",
                "access_token_expiration": "2099-01-01T00:00:00Z",
                "username": "dataforge-maintainer",
                "scopes": ["read"],
            }
        ),
        encoding="utf-8",
    )


def test_kaggle_cli_preflight_uses_clean_config_and_oauth_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI proof must not inherit stale legacy API-key state."""
    monkeypatch.setattr(check_kaggle_auth.metadata, "version", lambda _name: "2.1.2")
    credentials = tmp_path / "credentials.json"
    _write_oauth_credentials(credentials)
    captured: dict[str, Any] = {}

    def fake_runner(
        command: list[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(command, 0, stdout="dataset,count\n", stderr="")

    report = check_kaggle_auth.check_kaggle_auth(
        kaggle_json=credentials,
        kaggle_cli=Path("kaggle.exe"),
        run_cli=True,
        runner=fake_runner,
    )

    env = captured["env"]
    assert captured["command"][0] == "kaggle.exe"
    assert env["KAGGLE_CONFIG_DIR"] != str(credentials.parent)
    assert Path(env["KAGGLE_CONFIG_DIR"]).name.startswith("dataforge-kaggle-config-")
    assert "KAGGLE_USERNAME" not in env
    assert "KAGGLE_KEY" not in env
    assert "KAGGLE_API_TOKEN" not in env
    assert "KAGGLE_API_V1_TOKEN" not in env
    assert env["KAGGLE_CREDENTIALS_FILE"] == str(credentials)
    assert report["cli_checked"] is True
    assert report["legacy_kaggle_json_used"] is False
    assert report["tokens_printed"] is False


def test_kaggle_preflight_refuses_stale_kaggle_json(tmp_path: Path) -> None:
    stale_path = tmp_path / "kaggle.json"
    stale_path.write_text(json.dumps({"username": "old", "key": "stale"}), encoding="utf-8")

    try:
        check_kaggle_auth.check_kaggle_auth(kaggle_json=stale_path)
    except RuntimeError as exc:
        assert "Refusing to read stale legacy Kaggle API key file" in str(exc)
    else:  # pragma: no cover - assertion path only
        raise AssertionError("stale kaggle.json was accepted")


def test_kaggle_preflight_reports_near_expiry_oauth_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(check_kaggle_auth.metadata, "version", lambda _name: "2.1.2")
    credentials = tmp_path / "credentials.json"
    credentials.write_text(
        json.dumps(
            {
                "refresh_token": "masked-refresh-token",
                "access_token": "masked-access-token",
                "access_token_expiration": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
                "username": "dataforge-maintainer",
                "scopes": ["read"],
            }
        ),
        encoding="utf-8",
    )

    report = check_kaggle_auth.check_kaggle_auth(kaggle_json=credentials)

    assert report["access_token_refresh_recommended"] is True
    assert report["tokens_printed"] is False


def test_kaggle_cli_preflight_refreshes_near_expiry_before_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(check_kaggle_auth.metadata, "version", lambda _name: "2.1.2")
    credentials = tmp_path / "credentials.json"
    credentials.write_text(
        json.dumps(
            {
                "refresh_token": "masked-refresh-token",
                "access_token": "masked-access-token",
                "access_token_expiration": "2000-01-01T00:00:00Z",
                "username": "dataforge-maintainer",
                "scopes": ["read"],
            }
        ),
        encoding="utf-8",
    )
    refreshed: dict[str, Path] = {}

    def fake_refresh(path: Path) -> dict[str, Any]:
        refreshed["path"] = path
        return {
            "oauth_access_token_refreshed": True,
            "access_token_expires_at": "2099-01-01T00:00:00+00:00",
        }

    def fake_runner(
        command: list[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, stdout="dataset,count\n", stderr="")

    monkeypatch.setattr(check_kaggle_auth, "_refresh_oauth_access_token_if_needed", fake_refresh)

    report = check_kaggle_auth.check_kaggle_auth(
        kaggle_json=credentials,
        kaggle_cli=Path("kaggle.exe"),
        run_cli=True,
        runner=fake_runner,
    )

    assert refreshed["path"] == credentials
    assert report["oauth_access_token_refreshed"] is True
    assert report["access_token_expires_at"] == "2099-01-01T00:00:00+00:00"
    assert report["tokens_printed"] is False
