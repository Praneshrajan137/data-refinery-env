"""Tests for Playground production monitor and release checklist."""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from dataforge.release.playground_check import (
    DEFAULT_BACKEND_URL,
    DEFAULT_FRONTEND_URL,
    NEGATIVE_CORS_ORIGIN,
    report_to_json,
    resolve_declared_backend_url,
    run_playground_check,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Derived from the defaults under test, never duplicated.
#
# These were hardcoded as the retired HF Space host and the Cloudflare origin, so the mock stopped
# matching the moment the real defaults moved to the Azure Container App -- the test failed for the
# host change, not for the behaviour it checks. Deriving them means the next migration needs no
# edit here. The frontend and backend now share one origin, which is the point.
BACKEND_HOST = httpx.URL(DEFAULT_BACKEND_URL).host
FRONTEND_ORIGIN = DEFAULT_BACKEND_URL


def _mock_transport(
    *,
    include_analyze: bool = True,
    config_backend_url: str | None = None,
    config_cache_control: str = "no-store",
) -> httpx.MockTransport:
    """Build a mock playground.

    ``config_backend_url`` is what config.js DECLARES. None means the same-origin form
    (``BACKEND_URL: ""``), which is what the real deployment ships -- the mock only ever served an
    absolute URL, so the check could regress on the production shape with every test still green.
    """
    frontend_origin = FRONTEND_ORIGIN

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url).rstrip("/")
        path = request.url.path.rstrip("/") or "/"
        if url == DEFAULT_FRONTEND_URL:
            return httpx.Response(
                200,
                text='<!doctype html><div id="root"></div><script src="/playground/config.js"></script><script src="/playground/assets/index.js"></script>',
                headers={"content-type": "text/html"},
            )
        if url == f"{DEFAULT_FRONTEND_URL}/config.js":
            return httpx.Response(
                200,
                text=(
                    "window.__DATAFORGE_CONFIG__={BACKEND_URL:"
                    f'"{"" if config_backend_url is None else config_backend_url}"}};'
                ),
                headers={"cache-control": config_cache_control},
            )
        if request.url.host == BACKEND_HOST and path == "/":
            return httpx.Response(200, json={"service": "DataForge Playground API", "status": "ok"})
        if request.url.host == BACKEND_HOST and path == "/api/health":
            headers = {}
            origin = request.headers.get("origin")
            if origin == frontend_origin:
                headers["access-control-allow-origin"] = frontend_origin
            if request.method == "OPTIONS" and origin == NEGATIVE_CORS_ORIGIN:
                return httpx.Response(
                    200,
                    headers={
                        "access-control-allow-origin": NEGATIVE_CORS_ORIGIN,
                        "access-control-allow-credentials": "true",
                    },
                )
            if request.method == "GET" and origin == NEGATIVE_CORS_ORIGIN:
                return httpx.Response(
                    403,
                    json={"error": "origin_not_allowed", "status": 403},
                    headers={
                        "access-control-allow-origin": NEGATIVE_CORS_ORIGIN,
                        "access-control-allow-credentials": "true",
                    },
                )
            return httpx.Response(
                200,
                json={
                    "service": "DataForge Playground API",
                    "status": "ok",
                    "advanced_available": False,
                    "max_upload_bytes": 1_048_576,
                    "api_version": "0.1.0",
                    "contract_version": "repair_contract_v2",
                    "build_sha": "test",
                    "server_time_utc": "2026-05-25T00:00:00+00:00",
                    "environment": "production",
                    "limits": {},
                    "cors_configured": True,
                    "otel_enabled": False,
                    "metrics": {"requests_total": 1},
                },
                headers=headers,
            )
        if request.url.host == BACKEND_HOST and path == "/api/samples/hospital_10rows":
            return httpx.Response(
                200, content=b"id,amount\n1,100\n", headers={"content-type": "text/csv"}
            )
        if request.url.host == BACKEND_HOST and path == "/api/profile":
            return httpx.Response(200, json={"issues": [], "meta": {"rows": 1}})
        if include_analyze and request.url.host == BACKEND_HOST and path == "/api/analyze":
            return httpx.Response(
                200,
                json={
                    "source": {"name": "hospital_10rows.csv", "rows": 1},
                    "risk_summary": {"repair_readiness": "verified"},
                    "repairs": [],
                    "verification": {"safety_verdict": "allow"},
                    "receipt": {"contract_version": "repair_contract_v2"},
                    "apply_handoff": {"dry_run_command": "dataforge repair path --dry-run"},
                    "meta": {"contract_version": "repair_contract_v2"},
                },
            )
        if request.url.host == BACKEND_HOST and path == "/api/repair":
            return httpx.Response(200, json={"fixes": [], "txn_journal": {}, "meta": {}})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def test_playground_check_answers_release_checklist() -> None:
    with httpx.Client(transport=_mock_transport(), follow_redirects=True) as client:
        report = run_playground_check(include_doctor=False, include_smoke=True, client=client)

    assert report.ok is True
    assert {check.name for check in report.checks} == {
        "frontend_deployed",
        "config_js_correct",
        "backend_deployed",
        "cors_correct",
        "smoke_flow_passing",
    }
    payload = json.loads(report_to_json(report))
    assert payload["ok"] is True
    smoke = next(check for check in payload["checks"] if check["name"] == "smoke_flow_passing")
    assert smoke["metadata"]["analyze_status_code"] == 200
    assert smoke["metadata"]["analyze_missing"] == []


def test_playground_check_fails_when_primary_analyze_route_is_missing() -> None:
    """A stale backend with only legacy endpoints must not pass release checks."""
    with httpx.Client(
        transport=_mock_transport(include_analyze=False), follow_redirects=True
    ) as client:
        report = run_playground_check(include_doctor=False, include_smoke=True, client=client)

    assert report.ok is False
    smoke = next(check for check in report.checks if check.name == "smoke_flow_passing")
    assert smoke.ok is False
    assert smoke.metadata["analyze_status_code"] == 404


def test_playground_monitor_workflow_is_scheduled() -> None:
    workflow = (PROJECT_ROOT / ".github" / "workflows" / "playground-monitor.yml").read_text(
        encoding="utf-8"
    )
    assert "workflow_dispatch" in workflow
    assert 'cron: "*/15 * * * *"' in workflow
    assert "monitor_playground.py --json" in workflow


def test_makefile_exposes_playground_release_check() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "playground-release-check" in makefile
    assert "release playground-check --json" in makefile


def test_live_browser_audit_script_covers_expected_flow() -> None:
    script = (PROJECT_ROOT / "scripts" / "playground" / "audit_live_playground.mjs").read_text(
        encoding="utf-8"
    )
    for marker in [
        "desktop_upload_profile_repair_copy_export_error",
        "mobile_sample_profile_repair_layout",
        "transaction_journal",
        "Mobile body overflow",
    ]:
        assert marker in script


def _config_check(**kwargs: object) -> object:
    """Run the checks against a mock and return the config.js verdict."""
    with httpx.Client(transport=_mock_transport(**kwargs), follow_redirects=True) as client:  # type: ignore[arg-type]
        report = run_playground_check(include_doctor=False, include_smoke=False, client=client)
    return next(check for check in report.checks if check.name == "config_js_correct")


def test_config_js_check_accepts_the_same_origin_form() -> None:
    """An empty BACKEND_URL is the deployed value, and must pass.

    The check previously required the absolute backend URL to appear verbatim in config.js, so this
    exact configuration -- the one actually serving production -- failed every scheduled run while
    the deployment was completely healthy.
    """
    check = _config_check(config_backend_url=None)
    assert check.ok is True
    assert check.metadata["same_origin"] is True
    assert check.metadata["declared_backend_url"] == ""
    assert check.metadata["effective_backend_url"] == DEFAULT_BACKEND_URL


def test_config_js_check_accepts_an_explicit_absolute_backend() -> None:
    """A split deployment naming the expected backend is still valid."""
    check = _config_check(config_backend_url=DEFAULT_BACKEND_URL)
    assert check.ok is True
    assert check.metadata["same_origin"] is False


def test_config_js_check_rejects_a_backend_that_is_not_the_expected_one() -> None:
    """The real error this check exists for: a config pointing somewhere else."""
    check = _config_check(config_backend_url="https://some-other-backend.example")
    assert check.ok is False
    assert "expected" in check.detail
    assert check.metadata["effective_backend_url"] == "https://some-other-backend.example"


def test_config_js_check_rejects_a_cacheable_config() -> None:
    """A cacheable config.js can pin a browser to a stale backend across a redeploy."""
    check = _config_check(config_cache_control="public, max-age=3600")
    assert check.ok is False
    assert "cacheable" in check.detail


def test_resolver_accepts_either_quote_style_and_reports_absence() -> None:
    """The resolver is the one place the rule lives, so its edges are tested directly.

    Single quotes are valid JavaScript and appear in existing test fixtures; a resolver that only
    understood double quotes reported "declares no BACKEND_URL" about a file that declares it.
    """
    frontend = "https://example.test/playground"

    # Empty means same-origin: resolves to the origin that served config.js.
    assert (
        resolve_declared_backend_url('{BACKEND_URL: ""}', frontend_url=frontend)
        == "https://example.test"
    )
    assert (
        resolve_declared_backend_url("{BACKEND_URL: ''}", frontend_url=frontend)
        == "https://example.test"
    )
    # Absolute resolves to itself, in either quote style.
    assert (
        resolve_declared_backend_url('{BACKEND_URL: "https://api.test"}', frontend_url=frontend)
        == "https://api.test"
    )
    assert (
        resolve_declared_backend_url("{BACKEND_URL: 'https://api.test'}", frontend_url=frontend)
        == "https://api.test"
    )
    # Absent is None, which callers report differently from "wrong host".
    assert resolve_declared_backend_url("{NOTHING: 1}", frontend_url=frontend) is None
