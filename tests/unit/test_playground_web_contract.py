"""Static contract tests for the playground frontend."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INDEX_PATH = PROJECT_ROOT / "playground" / "web" / "index.html"
CONFIG_PATH = PROJECT_ROOT / "playground" / "web" / "config.js"
APP_PATH = PROJECT_ROOT / "playground" / "web" / "app.js"


def test_index_uses_relative_asset_paths_and_config_contract() -> None:
    """The static frontend must be deployable on Pages without HF static assumptions."""
    body = INDEX_PATH.read_text(encoding="utf-8")
    assert "/static/" not in body
    assert 'src="./config.js"' in body
    assert 'src="./app.js"' in body
    assert 'href="./style.css"' in body


def test_config_js_exposes_backend_url_contract() -> None:
    """config.js defines the committed runtime contract for the backend URL."""
    body = CONFIG_PATH.read_text(encoding="utf-8")
    assert "window.__DATAFORGE_CONFIG__" in body
    assert "BACKEND_URL" in body


def test_frontend_stays_storage_free_and_capability_aware() -> None:
    """The frontend remains storage-free and consumes health capability metadata."""
    body = APP_PATH.read_text(encoding="utf-8")
    assert "localStorage" not in body
    assert "sessionStorage" not in body
    assert "advanced_available" in body
    assert "requestInFlight" in body
    assert "ArrowRight" in body
    assert "ArrowLeft" in body
