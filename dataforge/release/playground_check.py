"""Live Playground release and monitoring checks."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from dataforge.release.doctor import run_doctor

DEFAULT_BACKEND_URL = "https://Praneshrajan15-dataforge-playground.hf.space"
DEFAULT_FRONTEND_URL = "https://dataforge.dev/playground"
NEGATIVE_CORS_ORIGIN = "https://untrusted-dataforge.example"
REQUIRED_HEALTH_KEYS = {"status", "advanced_available", "max_upload_bytes"}
ENHANCED_HEALTH_KEYS = {
    "service",
    "api_version",
    "contract_version",
    "build_sha",
    "server_time_utc",
    "environment",
    "limits",
    "cors_configured",
    "otel_enabled",
    "metrics",
}


@dataclass(frozen=True)
class PlaygroundCheck:
    """One Playground check result."""

    name: str
    ok: bool
    detail: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class PlaygroundCheckReport:
    """Machine-readable Playground release report."""

    ok: bool
    frontend_url: str
    backend_url: str
    checks: list[PlaygroundCheck]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report."""
        return asdict(self)


def normalize_url(value: str) -> str:
    """Strip whitespace and trailing slashes from a URL."""
    return value.strip().rstrip("/")


def frontend_origin(frontend_url: str) -> str:
    """Return the origin portion of a frontend URL."""
    parts = urlsplit(frontend_url)
    return f"{parts.scheme}://{parts.netloc}"


def join_url(base_url: str, path: str) -> str:
    """Join a normalized base URL and absolute path fragment."""
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _timed_request(
    client: httpx.Client, method: str, url: str, **kwargs: Any
) -> tuple[httpx.Response, float]:
    """Run one HTTP request and return response plus elapsed milliseconds."""
    started = time.perf_counter()
    response = client.request(method, url, **kwargs)
    return response, (time.perf_counter() - started) * 1000


def _check_frontend_deployed(
    client: httpx.Client,
    *,
    frontend_url: str,
) -> PlaygroundCheck:
    try:
        response, latency_ms = _timed_request(client, "GET", frontend_url)
        body = response.text
        ok = (
            response.status_code == 200
            and "<!doctype html>" in body.lower()
            and 'id="root"' in body
            and "config.js" in body
            and "/playground/assets/" in body
        )
        detail = (
            "Frontend serves the React shell."
            if ok
            else "Frontend shell is missing required markers."
        )
        return PlaygroundCheck(
            "frontend_deployed",
            ok,
            detail,
            {
                "status_code": response.status_code,
                "latency_ms": round(latency_ms, 2),
                "content_type": response.headers.get("content-type", ""),
            },
        )
    except Exception as exc:
        return PlaygroundCheck("frontend_deployed", False, str(exc), {})


def _check_config_js(
    client: httpx.Client,
    *,
    frontend_url: str,
    backend_url: str,
) -> PlaygroundCheck:
    try:
        response, latency_ms = _timed_request(client, "GET", join_url(frontend_url, "config.js"))
        cache_control = response.headers.get("cache-control", "")
        ok = (
            response.status_code == 200
            and backend_url in response.text
            and "no-store" in cache_control.lower()
        )
        detail = (
            "config.js points at the expected backend and is uncached."
            if ok
            else "config.js is stale or cacheable."
        )
        return PlaygroundCheck(
            "config_js_correct",
            ok,
            detail,
            {
                "status_code": response.status_code,
                "latency_ms": round(latency_ms, 2),
                "cache_control": cache_control,
            },
        )
    except Exception as exc:
        return PlaygroundCheck("config_js_correct", False, str(exc), {})


def _check_backend_deployed(
    client: httpx.Client,
    *,
    backend_url: str,
    latency_threshold_ms: float,
) -> PlaygroundCheck:
    try:
        root, root_latency_ms = _timed_request(client, "GET", backend_url)
        health, health_latency_ms = _timed_request(client, "GET", f"{backend_url}/api/health")
        payload = health.json() if health.status_code == 200 else {}
        missing = sorted(REQUIRED_HEALTH_KEYS - set(payload))
        enhanced_missing = sorted(ENHANCED_HEALTH_KEYS - set(payload))
        ok = (
            root.status_code == 200
            and health.status_code == 200
            and payload.get("status") == "ok"
            and not missing
            and health_latency_ms <= latency_threshold_ms
        )
        detail = (
            "Backend root and health are reachable."
            if ok
            else "Backend root or health check failed."
        )
        return PlaygroundCheck(
            "backend_deployed",
            ok,
            detail,
            {
                "root_status_code": root.status_code,
                "health_status_code": health.status_code,
                "root_latency_ms": round(root_latency_ms, 2),
                "health_latency_ms": round(health_latency_ms, 2),
                "latency_threshold_ms": latency_threshold_ms,
                "required_missing": missing,
                "enhanced_missing": enhanced_missing,
                "metrics": payload.get("metrics", {}),
            },
        )
    except Exception as exc:
        return PlaygroundCheck("backend_deployed", False, str(exc), {})


def _check_cors(
    client: httpx.Client,
    *,
    frontend_url: str,
    backend_url: str,
) -> PlaygroundCheck:
    origin = frontend_origin(frontend_url)
    try:
        positive, positive_latency_ms = _timed_request(
            client,
            "GET",
            f"{backend_url}/api/health",
            headers={"Origin": origin},
        )
        negative, negative_latency_ms = _timed_request(
            client,
            "GET",
            f"{backend_url}/api/health",
            headers={"Origin": NEGATIVE_CORS_ORIGIN},
        )
        preflight, preflight_latency_ms = _timed_request(
            client,
            "OPTIONS",
            f"{backend_url}/api/health",
            headers={
                "Origin": NEGATIVE_CORS_ORIGIN,
                "Access-Control-Request-Method": "GET",
            },
        )
        allowed_origin = positive.headers.get("access-control-allow-origin")
        negative_allowed_origin = negative.headers.get("access-control-allow-origin")
        preflight_allowed_origin = preflight.headers.get("access-control-allow-origin")
        negative_error = ""
        try:
            negative_error = str(negative.json().get("error", ""))
        except ValueError:
            negative_error = ""
        negative_denied = negative.status_code == 403 and negative_error == "origin_not_allowed"
        ok = allowed_origin == origin and positive.status_code == 200 and negative_denied
        detail = (
            "Configured origin is allowed and disallowed origins cannot read API data."
            if ok
            else "CORS origin enforcement is incorrect."
        )
        return PlaygroundCheck(
            "cors_correct",
            ok,
            detail,
            {
                "frontend_origin": origin,
                "positive_status_code": positive.status_code,
                "negative_status_code": negative.status_code,
                "allowed_origin": allowed_origin,
                "negative_allowed_origin": negative_allowed_origin,
                "negative_error": negative_error,
                "negative_preflight_status_code": preflight.status_code,
                "negative_preflight_allowed_origin": preflight_allowed_origin,
                "positive_latency_ms": round(positive_latency_ms, 2),
                "negative_latency_ms": round(negative_latency_ms, 2),
                "negative_preflight_latency_ms": round(preflight_latency_ms, 2),
            },
        )
    except Exception as exc:
        return PlaygroundCheck("cors_correct", False, str(exc), {})


def _check_doctor() -> PlaygroundCheck:
    report = run_doctor(core=True, maintainer_deploy=False)
    return PlaygroundCheck(
        "doctor_passing",
        report.ok,
        "Core release doctor passed." if report.ok else "Core release doctor failed.",
        {"doctor": report.to_dict()},
    )


def _check_smoke_flow(client: httpx.Client, *, backend_url: str) -> PlaygroundCheck:
    try:
        sample, sample_latency_ms = _timed_request(
            client,
            "GET",
            f"{backend_url}/api/samples/hospital_10rows",
        )
        if sample.status_code != 200:
            return PlaygroundCheck(
                "smoke_flow_passing",
                False,
                "Sample endpoint failed.",
                {"sample_status_code": sample.status_code},
            )

        files = {"file": ("hospital_10rows.csv", sample.content, "text/csv")}
        analyze, analyze_latency_ms = _timed_request(
            client,
            "POST",
            f"{backend_url}/api/analyze",
            files=files,
        )
        files = {"file": ("hospital_10rows.csv", sample.content, "text/csv")}
        profile, profile_latency_ms = _timed_request(
            client,
            "POST",
            f"{backend_url}/api/profile",
            files=files,
        )
        files = {"file": ("hospital_10rows.csv", sample.content, "text/csv")}
        repair, repair_latency_ms = _timed_request(
            client,
            "POST",
            f"{backend_url}/api/repair",
            files=files,
        )
        analyze_payload = analyze.json() if analyze.status_code == 200 else {}
        profile_payload = profile.json() if profile.status_code == 200 else {}
        repair_payload = repair.json() if repair.status_code == 200 else {}
        analyze_required_keys = {
            "source",
            "risk_summary",
            "repairs",
            "verification",
            "receipt",
            "apply_handoff",
        }
        analyze_missing = sorted(analyze_required_keys - set(analyze_payload))
        ok = (
            analyze.status_code == 200
            and profile.status_code == 200
            and repair.status_code == 200
            and not analyze_missing
            and "issues" in profile_payload
            and "fixes" in repair_payload
            and "txn_journal" in repair_payload
        )
        return PlaygroundCheck(
            "smoke_flow_passing",
            ok,
            (
                "Sample analyze, profile, and repair dry-run passed."
                if ok
                else "Sample smoke flow failed."
            ),
            {
                "sample_latency_ms": round(sample_latency_ms, 2),
                "analyze_status_code": analyze.status_code,
                "profile_status_code": profile.status_code,
                "repair_status_code": repair.status_code,
                "analyze_latency_ms": round(analyze_latency_ms, 2),
                "profile_latency_ms": round(profile_latency_ms, 2),
                "repair_latency_ms": round(repair_latency_ms, 2),
                "analyze_missing": analyze_missing,
                "issue_count": len(profile_payload.get("issues", []))
                if isinstance(profile_payload, dict)
                else None,
                "repair_count": len(analyze_payload.get("repairs", []))
                if isinstance(analyze_payload, dict)
                else None,
                "fix_count": len(repair_payload.get("fixes", []))
                if isinstance(repair_payload, dict)
                else None,
            },
        )
    except Exception as exc:
        return PlaygroundCheck("smoke_flow_passing", False, str(exc), {})


def run_playground_check(
    *,
    frontend_url: str = DEFAULT_FRONTEND_URL,
    backend_url: str = DEFAULT_BACKEND_URL,
    latency_threshold_ms: float = 5_000.0,
    include_doctor: bool = True,
    include_smoke: bool = True,
    client: httpx.Client | None = None,
) -> PlaygroundCheckReport:
    """Run the public Playground release checklist."""
    normalized_frontend_url = normalize_url(frontend_url)
    normalized_backend_url = normalize_url(backend_url)

    def collect(active_client: httpx.Client) -> list[PlaygroundCheck]:
        checks = [
            _check_frontend_deployed(active_client, frontend_url=normalized_frontend_url),
            _check_config_js(
                active_client,
                frontend_url=normalized_frontend_url,
                backend_url=normalized_backend_url,
            ),
            _check_backend_deployed(
                active_client,
                backend_url=normalized_backend_url,
                latency_threshold_ms=latency_threshold_ms,
            ),
            _check_cors(
                active_client,
                frontend_url=normalized_frontend_url,
                backend_url=normalized_backend_url,
            ),
        ]
        if include_doctor:
            checks.append(_check_doctor())
        if include_smoke:
            checks.append(_check_smoke_flow(active_client, backend_url=normalized_backend_url))
        return checks

    if client is not None:
        checks = collect(client)
    else:
        with httpx.Client(follow_redirects=True, timeout=30.0) as owned_client:
            checks = collect(owned_client)

    return PlaygroundCheckReport(
        ok=all(check.ok for check in checks),
        frontend_url=normalized_frontend_url,
        backend_url=normalized_backend_url,
        checks=checks,
    )


def report_to_json(report: PlaygroundCheckReport) -> str:
    """Render a stable JSON report."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)
