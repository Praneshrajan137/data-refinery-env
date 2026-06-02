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
    run_playground_check,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _mock_transport(*, include_analyze: bool = True) -> httpx.MockTransport:
    frontend_origin = "https://dataforge.praneshrajan15.workers.dev"

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
                text=f'window.__DATAFORGE_CONFIG__={{BACKEND_URL:"{DEFAULT_BACKEND_URL}"}};',
                headers={"cache-control": "no-store"},
            )
        if request.url.host == "praneshrajan15-dataforge-playground.hf.space" and path == "/":
            return httpx.Response(200, json={"service": "DataForge Playground API", "status": "ok"})
        if (
            request.url.host == "praneshrajan15-dataforge-playground.hf.space"
            and path == "/api/health"
        ):
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
        if (
            request.url.host == "praneshrajan15-dataforge-playground.hf.space"
            and path == "/api/samples/hospital_10rows"
        ):
            return httpx.Response(
                200, content=b"id,amount\n1,100\n", headers={"content-type": "text/csv"}
            )
        if (
            request.url.host == "praneshrajan15-dataforge-playground.hf.space"
            and path == "/api/profile"
        ):
            return httpx.Response(200, json={"issues": [], "meta": {"rows": 1}})
        if (
            include_analyze
            and request.url.host == "praneshrajan15-dataforge-playground.hf.space"
            and path == "/api/analyze"
        ):
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
        if (
            request.url.host == "praneshrajan15-dataforge-playground.hf.space"
            and path == "/api/repair"
        ):
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
