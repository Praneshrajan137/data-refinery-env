"""Deployment contract tests for the Cloudflare-hosted frontend."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

from playground.api.app import _build_cors_origin_regex, _build_cors_origins

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WRANGLER_PATH = PROJECT_ROOT / "wrangler.toml"
ASSETSIGNORE_PATH = PROJECT_ROOT / "playground" / "web" / "public" / ".assetsignore"
WEB_PACKAGE_PATH = PROJECT_ROOT / "playground" / "web" / "package.json"
WEB_CONFIG_PATH = PROJECT_ROOT / "playground" / "web" / "config.js"
PUBLIC_CONFIG_PATH = PROJECT_ROOT / "playground" / "web" / "public" / "config.js"
SYNC_SCRIPT_PATH = PROJECT_ROOT / "playground" / "web" / "scripts" / "sync_runtime_config.mjs"
HEADERS_PATH = PROJECT_ROOT / "playground" / "web" / "public" / "_headers"
RENDERER_PATH = PROJECT_ROOT / "scripts" / "playground" / "render_web_config.py"
DEPLOY_SPACE_PATH = PROJECT_ROOT / "scripts" / "playground" / "deploy_space.py"
VERIFIER_PATH = PROJECT_ROOT / "scripts" / "playground" / "verify_frontend_deploy.py"
HF_SYNC_WORKFLOW_PATH = PROJECT_ROOT / ".github" / "workflows" / "sync-to-hf.yml"


def _load_renderer_module():
    """Load the config renderer without requiring package imports."""
    spec = importlib.util.spec_from_file_location("render_web_config", RENDERER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_wrangler_config_declares_subpath_worker_assets() -> None:
    """The frontend deploy contract names the Worker and assets directory explicitly."""
    body = WRANGLER_PATH.read_text(encoding="utf-8")
    assert 'name = "dataforge"' in body
    assert 'main = "./playground/web/worker.js"' in body
    assert 'compatibility_date = "2026-04-27"' in body
    assert "workers_dev = true" in body
    assert "[build]" in body
    assert (
        'command = "npm --prefix playground/web install && npm --prefix playground/web run build"'
        in body
    )
    assert 'directory = "./playground/web/dist"' in body
    assert 'binding = "ASSETS"' in body
    assert 'not_found_handling = "single-page-application"' in body


def test_assetsignore_and_headers_protect_runtime_config() -> None:
    """Non-public docs stay out of the asset upload and config.js stays uncached."""
    assert "DEPLOY.md" in ASSETSIGNORE_PATH.read_text(encoding="utf-8")
    headers = HEADERS_PATH.read_text(encoding="utf-8")
    assert "/playground/config.js" in headers
    assert "Cache-Control: no-store" in headers
    assert "/playground/assets/*" in headers
    assert "immutable" in headers
    assert VERIFIER_PATH.exists()


def test_cloudflare_dashboard_config_path_is_supported() -> None:
    """Cloudflare's sed-based dashboard build command edits playground/web/config.js."""
    package = WEB_PACKAGE_PATH.read_text(encoding="utf-8")
    web_config = WEB_CONFIG_PATH.read_text(encoding="utf-8")
    public_config = PUBLIC_CONFIG_PATH.read_text(encoding="utf-8")

    assert "config:sync" in package
    assert "scripts/sync_runtime_config.mjs" in package
    assert 'BACKEND_URL: ""' in web_config
    # Derived from the generator, not duplicated: this assertion previously hardcoded a
    # retired HF Space host and failed after the backend moved to Azure Container Apps,
    # for a reason unrelated to the Cloudflare config contract it exists to check.
    sync_source = SYNC_SCRIPT_PATH.read_text(encoding="utf-8")
    match = re.search(r'DEFAULT_BACKEND_URL\s*=\s*"([^"]+)"', sync_source)
    assert match is not None, "sync_runtime_config.mjs no longer declares DEFAULT_BACKEND_URL"
    assert match.group(1) in public_config


def test_hf_sync_workflow_targets_dataforge_playground_space() -> None:
    """The manual HF deployment workflow pushes to the product-named Space."""
    body = HF_SYNC_WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "HF_SPACE_ID: Praneshrajan15/dataforge-playground" in body
    assert "HF_SPACE_ID: Praneshrajan15/data-quality-env" not in body


def test_workers_dev_is_the_public_playground_url() -> None:
    """Deploy scripts default to the production Workers URL."""
    deploy_space = DEPLOY_SPACE_PATH.read_text(encoding="utf-8")
    verifier = VERIFIER_PATH.read_text(encoding="utf-8")

    assert (
        'DEFAULT_FRONTEND_ORIGIN = "https://dataforge.praneshrajan15.workers.dev"' in deploy_space
    )
    assert (
        'DEFAULT_FRONTEND_URL = "https://dataforge.praneshrajan15.workers.dev/playground"'
        in verifier
    )


def test_renderer_writes_normalized_backend_url(tmp_path: Path) -> None:
    """The config renderer strips trailing slashes and writes valid JS."""
    module = _load_renderer_module()
    output_path = tmp_path / "config.js"

    rendered = module.render_config(
        "https://Praneshrajan15-dataforge-playground.hf.space/",
        output_path=output_path,
    )

    body = rendered.read_text(encoding="utf-8")
    assert '"https://Praneshrajan15-dataforge-playground.hf.space"' in body
    assert 'https://Praneshrajan15-dataforge-playground.hf.space/"' not in body


@pytest.mark.parametrize(
    "value",
    [
        "",
        "http://Praneshrajan15-dataforge-playground.hf.space",
        "https://Praneshrajan15-dataforge-playground.hf.space?preview=true",
    ],
)
def test_renderer_rejects_invalid_backend_urls(value: str) -> None:
    """The config renderer fails closed on missing or unsafe backend URLs."""
    module = _load_renderer_module()
    with pytest.raises(ValueError):
        module.normalize_backend_url(value)


def test_cors_helpers_allow_only_explicit_production_origins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production CORS uses explicit origins and no wildcard Cloudflare host regex."""
    monkeypatch.setenv(
        "DATAFORGE_PLAYGROUND_ORIGINS",
        "https://demo.example.com, https://dataforge.example.com",
    )
    monkeypatch.delenv("DATAFORGE_PLAYGROUND_DEV", raising=False)
    explicit = _build_cors_origins()
    regex = _build_cors_origin_regex()

    assert explicit == ["https://demo.example.com", "https://dataforge.example.com"]
    assert regex is None
    assert "https://dataforge.account-subdomain.workers.dev" not in explicit


def test_cors_helper_allows_localhost_only_in_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local browser development is regex-allowed only behind the dev flag."""
    monkeypatch.setenv("DATAFORGE_PLAYGROUND_DEV", "1")
    regex = _build_cors_origin_regex()

    assert regex is not None
    assert re.fullmatch(regex, "http://localhost:8788") is not None
    assert re.fullmatch(regex, "http://127.0.0.1:7860") is not None
    assert re.fullmatch(regex, "https://dataforge.account-subdomain.workers.dev") is None
