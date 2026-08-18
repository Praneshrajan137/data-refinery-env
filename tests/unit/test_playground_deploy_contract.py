"""Deployment contract tests for the single-origin Azure Container App.

TOPOLOGY CHANGE. These tests previously pinned a two-host deployment: a Cloudflare Workers
frontend plus a separately hosted backend. The playground is now served by ONE Azure Container
App -- the API and the SPA from the same origin -- so the assertions that encoded the old shape
were not merely stale, they described something the project no longer does.

What is asserted here is the property that made the old shape fragile: a hostname must never be a
build DEFAULT. The previous default was a hardcoded Azure Container Apps host from an expired
subscription, and because it lived in the build rather than in the deployment, the next frontend
build would have pointed a working site at a dead backend. Same-origin has no hostname to rot.
"""

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
VERIFIER_PATH = PROJECT_ROOT / "scripts" / "playground" / "verify_frontend_deploy.py"
API_APP_PATH = PROJECT_ROOT / "playground" / "api" / "app.py"
API_DOCKERFILE_PATH = PROJECT_ROOT / "playground" / "api" / "Dockerfile"
API_DOCKERIGNORE_PATH = PROJECT_ROOT / "playground" / "api" / "Dockerfile.dockerignore"


def _load_renderer_module():
    """Load the config renderer without requiring package imports."""
    spec = importlib.util.spec_from_file_location("render_web_config", RENDERER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_backend_url_defaults_to_same_origin() -> None:
    """No hostname may be a build default.

    The regex here deliberately accepts an EMPTY value. The previous version required one or more
    characters, which encoded the assumption that a backend is always somewhere else.
    """
    package = WEB_PACKAGE_PATH.read_text(encoding="utf-8")
    web_config = WEB_CONFIG_PATH.read_text(encoding="utf-8")
    public_config = PUBLIC_CONFIG_PATH.read_text(encoding="utf-8")
    sync_source = SYNC_SCRIPT_PATH.read_text(encoding="utf-8")

    assert "config:sync" in package
    assert "scripts/sync_runtime_config.mjs" in package
    assert 'BACKEND_URL: ""' in web_config

    match = re.search(r'DEFAULT_BACKEND_URL\s*=\s*"([^"]*)"', sync_source)
    assert match is not None, "sync_runtime_config.mjs no longer declares DEFAULT_BACKEND_URL"
    assert match.group(1) == "", "the build default must be same-origin, not a hostname"
    assert 'BACKEND_URL: ""' in public_config

    # The specific host that expired, and any successor, must not be reintroduced as a default.
    assert "azurecontainerapps.io" not in sync_source
    assert "hf.space" not in sync_source


def test_api_serves_the_spa_from_one_origin() -> None:
    """The API owns the frontend, under the prefix the router requires."""
    body = API_APP_PATH.read_text(encoding="utf-8")

    assert 'SPA_PREFIX = "/playground"' in body
    # routes.ts hardcodes /playground/* hrefs, so the mount point is a contract, not a preference.
    assert 'WEB_DIST = Path(__file__).resolve().parent / "web"' in body
    # Deep links must survive a hard refresh; a bare StaticFiles mount 404s them.
    assert "spa_catch_all" in body
    assert "SecurityHeadersMiddleware" in body
    # Serving is conditional, so an API-only image still works and says so.
    assert "if WEB_DIST.is_dir() and WEB_INDEX.is_file():" in body
    assert '"api_only"' in body


def test_spa_cache_policy_matches_asset_class() -> None:
    """config.js must never be cached; hashed assets always should be."""
    body = API_APP_PATH.read_text(encoding="utf-8")
    assert '"no-store"' in body
    assert '"public, max-age=31536000, immutable"' in body

    # The edge file is no longer the enforcement point, but it must not contradict the app.
    headers = HEADERS_PATH.read_text(encoding="utf-8")
    assert "/playground/config.js" in headers
    assert "Cache-Control: no-store" in headers
    assert "/playground/assets/*" in headers
    assert "immutable" in headers
    assert "DEPLOY.md" in ASSETSIGNORE_PATH.read_text(encoding="utf-8")
    assert VERIFIER_PATH.exists()


def test_csp_permits_only_the_serving_origin() -> None:
    """A same-origin deployment must not carry a foreign connect-src.

    The CSP named `https://*.hf.space` while the backend lived there. When the backend moved, the
    policy silently stopped describing reality -- nothing failed. Asserting both copies agree is
    what makes that failure loud.
    """
    headers = HEADERS_PATH.read_text(encoding="utf-8")
    app_body = API_APP_PATH.read_text(encoding="utf-8")

    assert "connect-src 'self';" in headers
    assert "connect-src 'self';" in app_body
    for stale_host in ("*.hf.space", "azurecontainerapps.io"):
        assert stale_host not in headers.split("Content-Security-Policy")[1]


def test_image_builds_its_own_bundle() -> None:
    """The image must not depend on a host-built bundle it cannot verify."""
    dockerfile = API_DOCKERFILE_PATH.read_text(encoding="utf-8")

    assert "FROM node:22-slim AS web" in dockerfile
    assert "npx vite build" in dockerfile
    assert "COPY --from=web /web/dist /home/user/app/web" in dockerfile
    # Container Apps target port.
    assert "EXPOSE 7860" in dockerfile
    # Empty on purpose: it exists to stop SlowAPI reading a repository .env.
    assert "COPY playground/api/slowapi.env" in dockerfile


def test_build_context_is_an_allowlist() -> None:
    """The context must fail closed.

    A denylist let the context reach 4 GiB -- model weights, datasets and notebooks that the
    Dockerfile never copies. Starting from `*` means a new heavy directory is excluded until
    someone names it.
    """
    body = API_DOCKERIGNORE_PATH.read_text(encoding="utf-8")
    lines = [
        line.strip() for line in body.splitlines() if line.strip() and not line.startswith("#")
    ]

    assert lines[0] == "*", "the scoped dockerignore must start by excluding everything"
    for required in ("!dataforge/", "!playground/web/src/", "!playground/api/app.py"):
        assert required in lines, f"{required} is copied by the Dockerfile but not allowed in"


def test_edge_deployment_cannot_silently_ship_a_same_origin_bundle() -> None:
    """The retired Cloudflare path must set BACKEND_URL explicitly.

    Same-origin is correct for the container and wrong for an edge deployment, which has no /api
    routes. Without this, `wrangler deploy` would produce a site whose API calls return the SPA
    shell instead of JSON -- a landmine created BY the move to same-origin defaults.
    """
    body = WRANGLER_PATH.read_text(encoding="utf-8")
    assert "[build.environment]" in body
    match = re.search(r'^BACKEND_URL = "(https://[^"]+)"', body, flags=re.MULTILINE)
    assert match is not None, "wrangler.toml must pin an absolute BACKEND_URL or not build at all"


def test_retired_edge_config_stays_structurally_valid() -> None:
    """The Workers site is still live, so its wiring must not rot unnoticed.

    Retired is not the same as deleted. These assertions were dropped in the first draft of this
    retopologisation, which would have let the still-serving fallback drift with nothing to catch
    it -- the exact failure mode being corrected elsewhere in this file.
    """
    body = WRANGLER_PATH.read_text(encoding="utf-8")
    assert 'name = "dataforge"' in body
    assert 'main = "./playground/web/worker.js"' in body
    assert 'directory = "./playground/web/dist"' in body
    assert 'binding = "ASSETS"' in body
    assert 'not_found_handling = "single-page-application"' in body
    assert "RETIRED" in body, "the retired status must be stated in the file itself"


def test_hf_sync_workflow_targets_dataforge_playground_space() -> None:
    """The HF Space remains a working backend fallback; its id must not drift.

    Worth keeping precisely because the Azure subscription is a Free Trial: when it expires, this
    is what is left.
    """
    workflow = PROJECT_ROOT / ".github" / "workflows" / "sync-to-hf.yml"
    body = workflow.read_text(encoding="utf-8")
    assert "HF_SPACE_ID: Praneshrajan15/dataforge-playground" in body
    assert "HF_SPACE_ID: Praneshrajan15/data-quality-env" not in body


def test_renderer_writes_normalized_backend_url(tmp_path: Path) -> None:
    """The config renderer strips trailing slashes and writes valid JS."""
    module = _load_renderer_module()
    output_path = tmp_path / "config.js"

    rendered = module.render_config(
        "https://dataforge-playground.example.eastus2.azurecontainerapps.io/",
        output_path=output_path,
    )

    body = rendered.read_text(encoding="utf-8")
    assert '"https://dataforge-playground.example.eastus2.azurecontainerapps.io"' in body
    assert 'azurecontainerapps.io/"' not in body


@pytest.mark.parametrize(
    "value",
    [
        "",
        "http://dataforge-playground.example.eastus2.azurecontainerapps.io",
        "https://dataforge-playground.example.eastus2.azurecontainerapps.io?preview=true",
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
    """Production CORS uses explicit origins and no wildcard host regex."""
    monkeypatch.setenv(
        "DATAFORGE_PLAYGROUND_ORIGINS",
        "https://demo.example.com, https://dataforge.example.com",
    )
    monkeypatch.delenv("DATAFORGE_PLAYGROUND_DEV", raising=False)
    explicit = _build_cors_origins()
    regex = _build_cors_origin_regex()

    assert explicit == ["https://demo.example.com", "https://dataforge.example.com"]
    assert regex is None


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
