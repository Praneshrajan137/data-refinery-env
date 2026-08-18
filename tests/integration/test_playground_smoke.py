"""Playground API smoke tests.

Tests the FastAPI playground backend using TestClient. These tests exercise
the contract defined in specs/SPEC_playground.md Appendix A.

All tests use the FastAPI TestClient (synchronous) and do not require a
running server or network access.
"""

from __future__ import annotations

import io
import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import playground.api.app as playground_api
from playground.api.app import (
    MAX_UPLOAD_BYTES,
    MAX_UPLOAD_CELLS,
    MAX_UPLOAD_COLUMNS,
    MAX_UPLOAD_ROWS,
    WORKFLOW_CONTRACT_VERSION,
    app,
    limiter,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a fresh TestClient for each test."""
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("AZURE_API_KEY", raising=False)
    monkeypatch.delenv("DATAFORGE_REMOTE_MODEL_URL", raising=False)
    monkeypatch.delenv("DATAFORGE_PLAYGROUND_AGENT_POLICY", raising=False)
    limiter._storage.reset()
    return TestClient(app)


def _hospital_csv_bytes() -> bytes:
    """Load the hospital_10rows fixture as raw bytes."""
    return (FIXTURES_DIR / "hospital_10rows.csv").read_bytes()


def _ndjson_events(text: str) -> list[dict[str, object]]:
    """Parse a complete NDJSON response body into workflow events."""
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _assert_problem_response(response, *, status: int, error: str) -> dict[str, object]:
    """Assert a stable RFC 9457 problem response and return its body."""
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"].endswith(f"/{error}")
    assert body["title"]
    assert body["status"] == status
    assert body["detail"]
    assert body["error"] == error
    assert "id,amount" not in json.dumps(body)
    return body


def _fake_agent_result() -> object:
    """Build a verified-agent result with one floor fix and one agent fix."""
    from dataforge import VerifiedFix
    from dataforge.agent.controller import AgentActionRecord, AgentRepairResult

    return AgentRepairResult(
        mode="dry_run",
        applied=False,
        source_path="upload.csv",
        source_sha256="a" * 64,
        policy_name="remote",
        steps_used=2,
        max_steps=8,
        floor_fix_count=1,
        agent_fix_count=1,
        fixes_count=2,
        residual_count=0,
        issues_count=3,
        safety_verdict="allow",
        reason="Agent finalized after resolving residual issues.",
        fixes=[
            VerifiedFix(
                row=0,
                column="amount",
                old_value="1020",
                new_value="102.0",
                detector_id="decimal_shift",
                operation="update",
                reason="decimal shift",
                confidence=0.9,
                provenance="deterministic",
                verifier_reason="smt: sat",
            ),
            VerifiedFix(
                row=1,
                column="ward",
                old_value="",
                new_value="north",
                detector_id="fd_violation",
                operation="update",
                reason="fd repair",
                confidence=0.7,
                provenance="llm_live",
                verifier_reason="smt: sat",
            ),
        ],
        trace=[
            AgentActionRecord(step=1, action_type="INSPECT_ROWS", accepted=None, detail="rows 0-2"),
            AgentActionRecord(step=2, action_type="FIX", accepted=True, detail="verified ward fix"),
        ],
    )


def _stable_analyze_payload(payload: dict[str, object]) -> dict[str, object]:
    """Normalize request-specific transaction fields before comparing endpoint parity."""
    stable = json.loads(json.dumps(payload))
    stable["txn_journal"]["txn_id"] = "<txn_id>"
    stable["txn_journal"]["created_at"] = "<created_at>"
    stable["receipt"]["txn_id"] = "<txn_id>"
    stable["receipt"]["revert_command"] = "<revert_command>"
    stable["receipt"]["constraints_artifact_sha256"] = "<constraints_artifact_sha256>"
    stable["apply_handoff"]["audit_command"] = "<audit_command>"
    stable["apply_handoff"]["revert_command"] = "<revert_command>"
    return stable


# ---------------------------------------------------------------------------
# API service root
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_root_returns_api_service_metadata(client: TestClient) -> None:
    """GET / returns stable service metadata instead of crashing."""
    response = client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert body["service"] == "DataForge Playground API"
    assert body["docs_url"] == "/api/docs"
    # Derived from the filesystem, not pinned to a literal.
    #
    # `frontend_hosting` now reports whether this process is actually serving the SPA, which
    # depends on whether a bundle was baked into the image. Pinning either value would make the
    # test environment-dependent: it is present after a local `docker build` or a manual copy, and
    # absent in a source checkout. Asserting the REPORT MATCHES REALITY is the invariant -- the
    # field is worthless if it can claim to serve a frontend that is not there.
    from playground.api.app import WEB_INDEX

    expected_hosting = "azure_container_app_same_origin" if WEB_INDEX.is_file() else "api_only"
    assert body["frontend_hosting"] == expected_hosting


# ---------------------------------------------------------------------------
# Case A.5: Health endpoint
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_health(client: TestClient) -> None:
    """GET /api/health returns the backend readiness and UI capability contract."""
    response = client.get("/api/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "DataForge Playground API"
    assert body["advanced_available"] is False
    assert body["max_upload_bytes"] == MAX_UPLOAD_BYTES
    assert body["streaming_available"] is True
    assert body["workflow_contract_version"] == WORKFLOW_CONTRACT_VERSION
    assert body["limits"] == {
        "max_upload_bytes": MAX_UPLOAD_BYTES,
        "max_rows": MAX_UPLOAD_ROWS,
        "max_columns": MAX_UPLOAD_COLUMNS,
        "max_cells": MAX_UPLOAD_CELLS,
    }
    assert body["api_version"] == "0.1.0"
    assert body["contract_version"] == "repair_contract_v2"
    assert body["verify_available"] is True
    assert body["entity_consensus_available"] is True
    assert "server_time_utc" in body
    assert "metrics" in body
    assert "requests_total" in body["metrics"]


@pytest.mark.integration
def test_health_reports_advanced_capability_when_keyed(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /api/health exposes advanced mode availability when a provider key exists."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["advanced_available"] is True


@pytest.mark.integration
def test_health_reports_agent_capability(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /api/health reports agent availability gated on the remote model URL."""
    monkeypatch.delenv("DATAFORGE_REMOTE_MODEL_URL", raising=False)
    monkeypatch.delenv("AZURE_API_KEY", raising=False)
    body = client.get("/api/health").json()
    assert body["agent_available"] is False
    assert isinstance(body["agent_max_steps"], int)
    assert body["agent_max_steps"] >= 1

    monkeypatch.setenv("DATAFORGE_REMOTE_MODEL_URL", "https://example.hf.space")
    assert client.get("/api/health").json()["agent_available"] is True


@pytest.mark.integration
def test_health_reports_azure_agent_policy(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A first-party Azure deployment (gpt-5.6-sol) is surfaced as the proposer."""
    monkeypatch.setenv("AZURE_API_KEY", "test-key")
    body = client.get("/api/health").json()
    assert body["agent_available"] is True
    assert body["agent_policy"] == "hosted:azure"
    assert body["agent_provider"] == "azure"


@pytest.mark.integration
def test_health_explicit_agent_policy_override_wins(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit policy override beats autodetection even when Azure is set."""
    monkeypatch.setenv("AZURE_API_KEY", "test-key")
    monkeypatch.setenv("DATAFORGE_PLAYGROUND_AGENT_POLICY", "remote")
    body = client.get("/api/health").json()
    assert body["agent_policy"] == "remote"
    assert body["agent_provider"] is None


@pytest.mark.integration
def test_health_remote_wins_when_both_configured(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Least surprise: an existing remote deployment is not silently switched to
    Azure when AZURE_API_KEY is also present (e.g. added for the corrector)."""
    monkeypatch.setenv("AZURE_API_KEY", "test-key")
    monkeypatch.setenv("DATAFORGE_REMOTE_MODEL_URL", "https://example.hf.space")
    body = client.get("/api/health").json()
    assert body["agent_policy"] == "remote"
    assert body["agent_provider"] is None


@pytest.mark.integration
def test_agent_mode_uses_resolved_azure_policy(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Agent analyze drives the resolved hosted:azure proposer through the gate."""
    monkeypatch.setenv("AZURE_API_KEY", "test-key")
    captured: dict[str, object] = {}

    def _capture(request: object) -> object:
        captured["policy"] = getattr(request, "policy", None)
        captured["provider"] = getattr(request, "provider", None)
        return _fake_agent_result()

    monkeypatch.setattr(playground_api, "run_agent_repair", _capture)
    response = client.post(
        "/api/analyze",
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
        data={"repair_mode": "agent"},
    )
    assert response.status_code == 200
    assert captured["policy"] == "hosted"
    assert captured["provider"] == "azure"


@pytest.mark.integration
def test_analyze_agent_mode_unavailable_is_problem(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Agent mode without a configured remote model returns a stable problem."""
    monkeypatch.delenv("DATAFORGE_REMOTE_MODEL_URL", raising=False)
    monkeypatch.delenv("AZURE_API_KEY", raising=False)
    response = client.post(
        "/api/analyze",
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
        data={"repair_mode": "agent"},
    )
    _assert_problem_response(response, status=400, error="agent_mode_unavailable")


@pytest.mark.integration
def test_analyze_invalid_repair_mode_is_problem(client: TestClient) -> None:
    """An unknown repair_mode value is rejected as a stable problem."""
    response = client.post(
        "/api/analyze",
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
        data={"repair_mode": "banana"},
    )
    _assert_problem_response(response, status=400, error="invalid_repair_mode")


@pytest.mark.integration
def test_analyze_agent_mode_attaches_verified_agent_summary(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Agent mode runs the verified loop and attaches an agent summary + trace."""
    monkeypatch.setenv("DATAFORGE_REMOTE_MODEL_URL", "https://example.hf.space")
    monkeypatch.setattr(playground_api, "run_agent_repair", lambda request: _fake_agent_result())

    response = client.post(
        "/api/analyze",
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
        data={"repair_mode": "agent"},
    )
    assert response.status_code == 200
    body = response.json()
    agent = body["agent"]
    assert agent is not None
    assert agent["policy_name"] == "remote"
    assert agent["agent_fix_count"] == 1
    assert agent["floor_fix_count"] == 1
    assert len(agent["trace"]) == 2
    # agent_fixes exposes only residual (non-deterministic) verified fixes.
    assert len(agent["agent_fixes"]) == 1
    assert agent["agent_fixes"][0]["provenance"] == "llm_live"
    # The default deterministic response has no agent block.
    assert "Agent proposals come from the 'remote' policy" in " ".join(body["limitations"])


@pytest.mark.integration
def test_analyze_default_mode_has_no_agent_block(client: TestClient) -> None:
    """The default deterministic analyze response omits the agent summary."""
    response = client.post(
        "/api/analyze",
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
    )
    assert response.status_code == 200
    assert response.json()["agent"] is None


@pytest.mark.integration
def test_cors_rejects_unconfigured_workers_dev_origin(client: TestClient) -> None:
    """Workers-hosted frontends must be explicitly configured in production CORS."""
    origin = "https://dataforge.example-subdomain.workers.dev"
    response = client.get(
        "/api/health",
        headers={"Origin": origin},
    )
    assert response.status_code == 403
    assert response.json()["error"] == "origin_not_allowed"
    assert "access-control-allow-origin" not in response.headers

    preflight = client.options(
        "/api/health",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "GET",
        },
    )
    assert preflight.status_code == 403
    assert "access-control-allow-origin" not in preflight.headers


# ---------------------------------------------------------------------------
# Case A.1: Profile hospital_10rows
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_profile_hospital(client: TestClient) -> None:
    """POST /api/profile with hospital_10rows returns valid issue list."""
    csv_bytes = _hospital_csv_bytes()
    response = client.post(
        "/api/profile",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 200
    body = response.json()

    # Top-level keys
    assert "issues" in body
    assert "meta" in body

    # Meta section
    meta = body["meta"]
    assert meta["rows"] == 10
    assert meta["columns"] == 10
    assert meta["contract_version"] == "repair_contract_v2"

    # Issues are non-empty for the seeded fixture
    issues = body["issues"]
    assert len(issues) > 0

    # Each issue has required keys
    for issue in issues:
        assert "column" in issue
        assert "issue_type" in issue
        assert "severity" in issue
        assert "row_indices" in issue


@pytest.mark.integration
def test_analyze_hospital_returns_proof_loop_payload(client: TestClient) -> None:
    """POST /api/analyze returns risk, constraints, verified repairs, and apply handoff."""
    csv_bytes = _hospital_csv_bytes()
    response = client.post(
        "/api/analyze",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 200
    body = response.json()

    assert body["source"]["name"] == "hospital_10rows.csv"
    assert body["source"]["rows"] == 10
    assert body["risk_summary"]["dataset_level"] in {"medium", "high"}
    assert body["risk_summary"]["repair_readiness"] in {"verified", "partial", "blocked"}
    assert body["schema_inference"]["schema_version"] == "constraint_review_v1"
    assert body["schema_inference"]["source_sha256"] == body["source"]["sha256"]
    assert body["schema_inference"]["candidates"]
    assert all(
        candidate["decision"] == "pending" for candidate in body["schema_inference"]["candidates"]
    )
    assert "issues" in body
    assert "repairs" in body
    assert "verification" in body
    assert "txn_journal" in body
    assert body["txn_journal"]["applied"] is False
    assert body["receipt"]["receipt_version"] == "repair_receipt_v1"
    assert body["receipt"]["contract_version"] == "repair_contract_v2"
    assert body["receipt"]["source_sha256"] == body["source"]["sha256"]
    assert "root_causes" in body["receipt"]
    assert "candidate_repairs" in body["receipt"]
    assert "proof_obligations" in body["receipt"]
    assert "limitations" in body["receipt"]
    assert (
        body["apply_handoff"]["dry_run_command"]
        == "dataforge repair path/to/hospital_10rows.csv --dry-run"
    )
    assert body["limitations"]


@pytest.mark.integration
def test_analyze_surfaces_trust_certificate_and_verification_strength(
    client: TestClient,
) -> None:
    """The analysis exposes the engine trust signals the browser needs.

    Regression guard for the trust-legible surface: per-fix verification
    strength, the receipt's independent-verification status and applied-vs-
    suggested split, and an independently re-verifiable certificate block.
    """
    csv_bytes = _hospital_csv_bytes()
    response = client.post(
        "/api/analyze",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 200
    body = response.json()

    # Certificate: present, itemized, and (for this deterministic dry run) re-verifies.
    certificate = body["certificate"]
    assert certificate["ok"] is True
    check_names = {check["name"] for check in certificate["checks"]}
    assert {"schema_recognized", "data_identity", "auto_apply_is_proven_deterministic"} <= (
        check_names
    )
    assert all(check["ok"] for check in certificate["checks"])

    # Per-fix proof strength is honest: deterministic hospital fixes are proven.
    assert body["repairs"], "hospital sample should produce verified deterministic fixes"
    for fix in body["repairs"]:
        assert "verification_strength" in fix
        assert "review_reason" in fix
        assert fix["verification_strength"] == "proven"

    # Receipt trust fields: honest independent-verification + applied/suggested split.
    receipt = body["receipt"]
    assert receipt["independent_verification"] in {"agreed", "not_run"}
    assert "applied_fixes" in receipt
    assert "suggested_fixes" in receipt


@pytest.mark.integration
def test_analyze_stream_returns_ordered_workflow_events_and_final_payload(
    client: TestClient,
) -> None:
    """POST /api/analyze/stream streams the proof loop and ends with AnalyzeResponse parity."""
    csv_bytes = _hospital_csv_bytes()
    json_response = client.post(
        "/api/analyze",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    stream_response = client.post(
        "/api/analyze/stream",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )

    assert json_response.status_code == 200
    assert stream_response.status_code == 200
    assert stream_response.headers["x-dataforge-workflow-contract"] == WORKFLOW_CONTRACT_VERSION

    events = _ndjson_events(stream_response.text)
    stage_ids = [event["stage_id"] for event in events]
    statuses = [event["status"] for event in events]
    sequences = [event["sequence"] for event in events]
    assert sequences == sorted(sequences)
    assert stage_ids[:3] == ["intake", "intake", "schema_inference"]
    assert statuses[:3] == ["running", "completed", "running"]
    assert list(dict.fromkeys(stage_ids)) == [
        "intake",
        "schema_inference",
        "constraint_review",
        "detectors",
        "repair_candidates",
        "safety_gate",
        "smt_verifier",
        "dry_run_transaction",
        "receipt",
    ]
    assert all(event["schema_version"] == WORKFLOW_CONTRACT_VERSION for event in events)
    assert all(
        "run_id" in event and "summary" in event and "started_at" in event for event in events
    )
    assert events[-1]["stage_id"] == "receipt"
    assert events[-1]["status"] == "completed"
    assert _stable_analyze_payload(events[-1]["analysis"]) == _stable_analyze_payload(
        json_response.json()
    )


@pytest.mark.integration
def test_analyze_stream_reports_advanced_unavailable_as_problem_event(client: TestClient) -> None:
    """Advanced stream requests fail inside the event contract without returning mutation data."""
    response = client.post(
        "/api/analyze/stream",
        params={"advanced": "true"},
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
    )

    assert response.status_code == 200
    events = _ndjson_events(response.text)
    assert events[0]["stage_id"] == "intake"
    assert events[-1]["stage_id"] == "receipt"
    assert events[-1]["status"] == "failed"
    assert "analysis" not in events[-1]
    problem = events[-1]["problem"]
    assert isinstance(problem, dict)
    assert problem["error"] == "advanced_mode_unavailable"
    assert problem["status"] == 400


@pytest.mark.integration
def test_analyze_clean_csv_returns_no_action_with_pending_assumptions(client: TestClient) -> None:
    """Clean inputs still surface inferred assumptions without inventing repairs."""
    csv_bytes = b"id,name\n1,Ada\n2,Lin\n3,Grace\n"
    response = client.post(
        "/api/analyze",
        files={"file": ("clean.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["source"]["rows"] == 3
    assert body["receipt"]["issues_count"] == 0
    assert body["repairs"] == []
    assert body["risk_summary"]["repair_readiness"] == "no_action"
    assert body["schema_inference"]["candidates"]
    assert all(
        candidate["decision"] == "pending" for candidate in body["schema_inference"]["candidates"]
    )


@pytest.mark.integration
def test_analyze_malformed_csv_returns_problem_detail(client: TestClient) -> None:
    """Malformed analyze uploads return the same stable CSV problem detail."""
    response = client.post(
        "/api/analyze",
        files={"file": ("broken.csv", io.BytesIO(b'id,name\n1,"unterminated'), "text/csv")},
    )
    _assert_problem_response(response, status=400, error="invalid_csv")


def _multi_source_csv_bytes() -> bytes:
    """6 entities x 4 rows; A/B/C each have one wrong value (so no clean FD is
    inferred), each entity keeps a >=0.7 consensus -> entity_consensus fires."""
    lines = ["entity,val"]
    wrong = {"A", "B", "C"}
    for entity in "ABCDEF":
        for i in range(4):
            value = "WRONG" + entity if (entity in wrong and i == 0) else "VAL" + entity
            lines.append(f"{entity},{value}")
    return ("\n".join(lines) + "\n").encode("utf-8")


@pytest.mark.integration
def test_analyze_entity_consensus_flag_surfaces_review_suggestions(client: TestClient) -> None:
    """allow_entity_consensus surfaces cross-row consensus fixes as review suggestions."""
    csv = _multi_source_csv_bytes()

    off = client.post(
        "/api/analyze",
        files={"file": ("multi.csv", io.BytesIO(csv), "text/csv")},
    )
    assert off.status_code == 200
    off_consensus = [
        s
        for s in off.json()["receipt"]["suggested_fixes"]
        if s.get("review_reason") == "unverified_entity_consensus"
    ]
    assert off_consensus == []  # off by default

    on = client.post(
        "/api/analyze",
        files={"file": ("multi.csv", io.BytesIO(csv), "text/csv")},
        data={"allow_entity_consensus": "true"},
    )
    assert on.status_code == 200
    on_consensus = [
        s
        for s in on.json()["receipt"]["suggested_fixes"]
        if s.get("review_reason") == "unverified_entity_consensus"
    ]
    assert on_consensus, "entity_consensus suggestions should surface when the flag is set"
    # Held for review (never auto-applied), and the value is a sibling-row consensus.
    assert all(s["new_value"].startswith("VAL") for s in on_consensus)


@pytest.mark.integration
def test_health_reports_entity_consensus_capability(client: TestClient) -> None:
    """GET /api/health advertises the entity-consensus capability (always available)."""
    assert client.get("/api/health").json()["entity_consensus_available"] is True


@pytest.mark.integration
def test_analyze_accepts_reviewed_constraints(client: TestClient) -> None:
    """Accepted inferred repair-supported constraints feed the shared repair engine."""
    csv_bytes = (
        b"code,name\n"
        b"A,Alpha\n"
        b"A,Alpha\n"
        b"A,Alfa\n"
        b"B,Beta\n"
        b"B,Beta\n"
        b"C,Gamma\n"
        b"C,Gamma\n"
        b"D,Delta\n"
        b"D,Delta\n"
        b"E,Echo\n"
    )
    first = client.post(
        "/api/analyze",
        files={"file": ("fd.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert first.status_code == 200
    candidates = first.json()["schema_inference"]["candidates"]
    fd_candidate = next(
        candidate
        for candidate in candidates
        if candidate["kind"] == "functional_dependency"
        and candidate["columns"] == ["code"]
        and candidate["dependent"] == "name"
    )

    second = client.post(
        "/api/analyze",
        data={"accepted_constraint_ids": f'["{fd_candidate["candidate_id"]}"]'},
        files={"file": ("fd.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert second.status_code == 200
    body = second.json()
    assert fd_candidate["candidate_id"] in body["receipt"]["accepted_constraint_ids"]
    assert any(repair["detector_id"] == "fd_violation" for repair in body["repairs"])
    assert body["apply_handoff"]["apply_command"].endswith("--constraints constraints.json --apply")


@pytest.mark.integration
def test_analyze_rejects_unknown_constraint_id(client: TestClient) -> None:
    """Unknown accepted constraints fail as RFC 9457 problem details."""
    response = client.post(
        "/api/analyze",
        data={"accepted_constraint_ids": '["cnd-0000000000000000"]'},
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
    )
    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "unknown_constraint_id"
    assert body["type"].endswith("/unknown_constraint_id")
    assert body["unknown_ids"] == ["cnd-0000000000000000"]


@pytest.mark.integration
def test_analyze_advanced_unavailable_without_provider_key(client: TestClient) -> None:
    """POST /api/analyze?advanced=true returns 400 when no provider key is configured."""
    response = client.post(
        "/api/analyze",
        params={"advanced": "true"},
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
    )
    assert response.status_code == 400
    assert response.json()["error"] == "advanced_mode_unavailable"


@pytest.mark.integration
def test_profile_advanced_unavailable_without_provider_key(client: TestClient) -> None:
    """POST /api/profile?advanced=true returns 400 when no provider key is configured."""
    csv_bytes = _hospital_csv_bytes()
    response = client.post(
        "/api/profile",
        params={"advanced": "true"},
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 400
    body = response.json()
    assert body["type"].endswith("/advanced_mode_unavailable")
    assert body["status"] == 400
    assert body["error"] == "advanced_mode_unavailable"


@pytest.mark.integration
def test_profile_advanced_allowed_when_provider_key_is_present(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /api/profile?advanced=true is accepted when a provider key is configured."""
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    csv_bytes = _hospital_csv_bytes()
    response = client.post(
        "/api/profile",
        params={"advanced": "true"},
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Case A.3: Oversize upload rejected
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_near_limit_upload_is_accepted(client: TestClient) -> None:
    """A valid CSV file at the 1 MiB file cap is not rejected for multipart overhead."""
    payload_prefix = b"value\n"
    csv_bytes = payload_prefix + (b"x" * (MAX_UPLOAD_BYTES - len(payload_prefix)))
    response = client.post(
        "/api/profile",
        files={"file": ("near_limit.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 200
    assert response.json()["meta"]["rows"] == 1


@pytest.mark.integration
def test_oversize_body_rejected(client: TestClient) -> None:
    """POST /api/profile with > 1 MB body returns 413."""
    oversized = b"value\n" + (b"x" * MAX_UPLOAD_BYTES)
    response = client.post(
        "/api/profile",
        files={"file": ("big.csv", io.BytesIO(oversized), "text/csv")},
    )
    assert response.status_code == 413
    assert response.json()["error"] == "file_too_large"
    assert response.headers["x-dataforge-request-id"]


@pytest.mark.integration
def test_malformed_csv_returns_stable_problem_detail(client: TestClient) -> None:
    """Malformed CSV uploads are client errors, not profile pipeline failures."""
    response = client.post(
        "/api/profile",
        files={"file": ("broken.csv", io.BytesIO(b'id,name\n1,"unterminated'), "text/csv")},
    )

    body = _assert_problem_response(response, status=400, error="invalid_csv")
    assert body["request_id"] == response.headers["x-dataforge-request-id"]


@pytest.mark.integration
def test_empty_csv_returns_stable_problem_detail(client: TestClient) -> None:
    """Empty CSV uploads get a clear problem detail."""
    response = client.post(
        "/api/profile",
        files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
    )

    _assert_problem_response(response, status=400, error="empty_csv")


@pytest.mark.integration
def test_unsupported_file_type_returns_problem_detail(client: TestClient) -> None:
    """Non-CSV uploads fail with stable problem details."""
    response = client.post(
        "/api/profile",
        files={"file": ("payload.json", io.BytesIO(b'{"secret":"nope"}'), "application/json")},
    )

    _assert_problem_response(response, status=415, error="unsupported_file_type")


@pytest.mark.integration
def test_upload_row_and_column_limits_are_enforced(client: TestClient) -> None:
    """The backend rejects valid CSVs that exceed playground processing limits."""
    too_many_rows = "value\n" + "\n".join(str(index) for index in range(MAX_UPLOAD_ROWS + 1))
    row_response = client.post(
        "/api/profile",
        files={"file": ("rows.csv", io.BytesIO(too_many_rows.encode()), "text/csv")},
    )
    assert row_response.status_code == 413
    assert row_response.json()["error"] == "too_many_rows"

    too_many_columns = ",".join(f"c{index}" for index in range(MAX_UPLOAD_COLUMNS + 1))
    too_many_columns += "\n" + ",".join("x" for _ in range(MAX_UPLOAD_COLUMNS + 1))
    column_response = client.post(
        "/api/profile",
        files={"file": ("columns.csv", io.BytesIO(too_many_columns.encode()), "text/csv")},
    )
    assert column_response.status_code == 413
    assert column_response.json()["error"] == "too_many_columns"


# ---------------------------------------------------------------------------
# Case A.4: Missing file rejected
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_missing_file_rejected(client: TestClient) -> None:
    """POST /api/profile with no file field returns 422."""
    response = client.post("/api/profile")
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Case A.6: Sample download
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_samples_hospital(client: TestClient) -> None:
    """GET /api/samples/hospital_10rows returns CSV with content-disposition."""
    response = client.get("/api/samples/hospital_10rows")
    assert response.status_code == 200
    assert "text/csv" in response.headers.get("content-type", "")
    disposition = response.headers.get("content-disposition", "")
    assert "hospital_10rows.csv" in disposition
    # Body should contain CSV content with a header row
    text = response.text
    assert len(text.strip().splitlines()) > 1


# ---------------------------------------------------------------------------
# Guardrail: verify externally-proposed fixes
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_verify_scenario_returns_curated_batch(client: TestClient) -> None:
    """GET /api/verify-scenarios/{name} returns a curated batch + authoritative ids."""
    response = client.get("/api/verify-scenarios/hospital_10rows")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "hospital_10rows"
    assert body["proposer"]
    assert len(body["fixes"]) == 4
    assert body["accepted_constraint_ids"], "scenario must resolve authoritative constraint ids"
    assert body["note"]


@pytest.mark.integration
def test_verify_scenario_unknown_is_404(client: TestClient) -> None:
    """An unknown scenario name is a stable 404."""
    response = client.get("/api/verify-scenarios/not_a_sample")
    assert response.status_code == 404


@pytest.mark.integration
@pytest.mark.parametrize("name", ["hospital_10rows", "flights_10rows", "beers_10rows"])
def test_verify_fixes_curated_batch_yields_guardrail_split(client: TestClient, name: str) -> None:
    """The scripted untrusted-agent batch proves the correct edit and blocks the rest.

    Fetches the sample bytes via the API so the content-derived constraint ids in
    the scenario match the posted file, then asserts the full guardrail story:
    exactly one proven would-apply fix, the corrupting/stale/invalid proposals
    held or rejected with honest reasons, and a certificate that re-verifies.
    """
    scenario = client.get(f"/api/verify-scenarios/{name}").json()
    csv_bytes = client.get(f"/api/samples/{name}").content

    response = client.post(
        "/api/verify-fixes",
        files={"file": (f"{name}.csv", io.BytesIO(csv_bytes), "text/csv")},
        data={
            "fixes": json.dumps(scenario["fixes"]),
            "accepted_constraint_ids": json.dumps(scenario["accepted_constraint_ids"]),
            "proposer": scenario["proposer"],
        },
    )
    assert response.status_code == 200
    body = response.json()

    assert body["proposed_count"] == 4
    assert body["authoritative_schema"] is True
    assert body["proposer"] == scenario["proposer"]

    # Exactly the one correctly-typed edit is proven and would apply.
    assert len(body["would_apply"]) == 1
    assert body["would_apply"][0]["verification_strength"] == "proven"

    # The corrupting / stale / invalid proposals are each blocked with an honest reason.
    reasons = {held["review_reason"] for held in body["receipt"]["suggested_fixes"]}
    assert {"verifier_rejected", "stale_precondition", "invalid_target"} <= reasons

    # Nothing was written; the receipt re-verifies as a certificate.
    assert body["receipt"]["applied"] is False
    assert body["certificate"]["ok"] is True
    assert body["receipt"]["independent_verification"] == "agreed"


@pytest.mark.integration
def test_verify_fixes_without_schema_holds_everything(client: TestClient) -> None:
    """With no accepted constraints, no external value is proven (held, correctly)."""
    csv_bytes = client.get("/api/samples/hospital_10rows").content
    response = client.post(
        "/api/verify-fixes",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
        data={"fixes": json.dumps([{"row": 0, "column": "er_wait_time", "new_value": "30"}])},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["authoritative_schema"] is False
    assert body["would_apply"] == []
    assert body["receipt"]["suggested_fixes"], "unproven external fix must be held, not applied"


@pytest.mark.integration
def test_verify_fixes_rejects_empty_batch(client: TestClient) -> None:
    """An empty fix batch is a stable 400 problem."""
    csv_bytes = client.get("/api/samples/hospital_10rows").content
    response = client.post(
        "/api/verify-fixes",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
        data={"fixes": "[]"},
    )
    _assert_problem_response(response, status=400, error="empty_fixes")


@pytest.mark.integration
def test_verify_fixes_rejects_invalid_json(client: TestClient) -> None:
    """Malformed fixes JSON is a stable 400 problem."""
    csv_bytes = client.get("/api/samples/hospital_10rows").content
    response = client.post(
        "/api/verify-fixes",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
        data={"fixes": "{not json"},
    )
    _assert_problem_response(response, status=400, error="invalid_fixes")


# ---------------------------------------------------------------------------
# Repair dry-run
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_repair_dry_run(client: TestClient) -> None:
    """POST /api/repair?dry_run=true returns fixes + a real ephemeral txn journal view."""
    csv_bytes = _hospital_csv_bytes()
    response = client.post(
        "/api/repair",
        params={"dry_run": "true"},
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 200
    body = response.json()

    assert "fixes" in body
    assert "txn_journal" in body
    assert body["meta"]["contract_version"] == "repair_contract_v2"
    assert "receipt" in body
    assert body["receipt"]["contract_version"] == "repair_contract_v2"
    assert body["receipt"]["source_sha256"] == body["txn_journal"]["source_sha256"]

    journal = body["txn_journal"]
    assert "txn_id" in journal
    assert journal["txn_id"].startswith("txn-")
    assert journal["created_at"].startswith("20")
    assert journal["source_name"] == "hospital_10rows.csv"
    assert len(journal["source_sha256"]) == 64
    assert journal["applied"] is False
    assert journal["fixes_count"] == len(body["fixes"])
    assert journal["events"] == [{"event_type": "created"}]
    for fix in body["fixes"]:
        assert "verifier_reason" in fix


@pytest.mark.integration
def test_repair_advanced_unavailable_without_provider_key(client: TestClient) -> None:
    """POST /api/repair?advanced=true returns 400 when no provider key is configured."""
    csv_bytes = _hospital_csv_bytes()
    response = client.post(
        "/api/repair",
        params={"dry_run": "true", "advanced": "true"},
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 400
    assert response.json()["error"] == "advanced_mode_unavailable"


@pytest.mark.integration
def test_repair_apply_request_returns_problem_detail(client: TestClient) -> None:
    """Hosted playground apply remains unsupported and machine-readable."""
    response = client.post(
        "/api/repair",
        params={"dry_run": "false"},
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
    )

    _assert_problem_response(response, status=400, error="apply_not_supported")


@pytest.mark.integration
def test_request_timeout_returns_problem_detail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pipeline timeouts return RFC 9457 problem details without raw CSV data."""

    def slow_profile_upload(*_args: object, **_kwargs: object) -> object:
        time.sleep(0.05)
        return object()

    monkeypatch.setattr(playground_api, "REQUEST_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(playground_api, "_profile_upload", slow_profile_upload)

    response = client.post(
        "/api/profile",
        files={"file": ("hospital_10rows.csv", io.BytesIO(_hospital_csv_bytes()), "text/csv")},
    )

    _assert_problem_response(response, status=504, error="request_timeout")


@pytest.mark.integration
def test_rate_limit_returns_429_on_eleventh_post(client: TestClient) -> None:
    """The in-memory rate limiter rejects the eleventh POST within a minute."""
    csv_bytes = _hospital_csv_bytes()

    for _ in range(10):
        response = client.post(
            "/api/profile",
            files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
        )
        assert response.status_code == 200

    response = client.post(
        "/api/profile",
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    _assert_problem_response(response, status=429, error="rate_limit_exceeded")
    assert response.headers["retry-after"] == "60"
