"""Playground API smoke tests.

Tests the FastAPI playground backend using TestClient. These tests exercise
the contract defined in specs/SPEC_playground.md Appendix A.

All tests use the FastAPI TestClient (synchronous) and do not require a
running server or network access.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

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
    limiter._storage.reset()
    return TestClient(app)


def _hospital_csv_bytes() -> bytes:
    """Load the hospital_10rows fixture as raw bytes."""
    return (FIXTURES_DIR / "hospital_10rows.csv").read_bytes()


def _ndjson_events(text: str) -> list[dict[str, object]]:
    """Parse a complete NDJSON response body into workflow events."""
    return [json.loads(line) for line in text.splitlines() if line.strip()]


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
    assert body["frontend_hosting"] == "cloudflare_static_assets"


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
    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "invalid_csv"
    assert body["status"] == 400


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

    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "invalid_csv"
    assert body["status"] == 400
    assert body["request_id"] == response.headers["x-dataforge-request-id"]


@pytest.mark.integration
def test_empty_csv_returns_stable_problem_detail(client: TestClient) -> None:
    """Empty CSV uploads get a clear problem detail."""
    response = client.post(
        "/api/profile",
        files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "empty_csv"


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
    assert response.status_code == 429
    assert response.headers["retry-after"] == "60"
    assert response.json()["error"] == "rate_limit_exceeded"
