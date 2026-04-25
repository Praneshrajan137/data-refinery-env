"""Playground API smoke tests.

Tests the FastAPI playground backend using TestClient. These tests exercise
the contract defined in specs/SPEC_playground.md Appendix A.

All tests use the FastAPI TestClient (synchronous) and do not require a
running server or network access.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from playground.api.app import MAX_UPLOAD_BYTES, app, limiter

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
    assert body["frontend_hosting"] == "cloudflare_pages"


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
    assert body["advanced_available"] is False
    assert body["max_upload_bytes"] == MAX_UPLOAD_BYTES


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
def test_profile_advanced_unavailable_without_provider_key(client: TestClient) -> None:
    """POST /api/profile?advanced=true returns 400 when no provider key is configured."""
    csv_bytes = _hospital_csv_bytes()
    response = client.post(
        "/api/profile",
        params={"advanced": "true"},
        files={"file": ("hospital_10rows.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": {"error": "advanced_mode_unavailable"}}


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
def test_oversize_body_rejected(client: TestClient) -> None:
    """POST /api/profile with > 1 MB body returns 413."""
    oversized = b"x" * (1_048_576 + 1024)  # ~1.001 MB
    response = client.post(
        "/api/profile",
        files={"file": ("big.csv", io.BytesIO(oversized), "text/csv")},
        headers={"Content-Length": str(len(oversized) + 200)},
    )
    assert response.status_code == 413


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

    journal = body["txn_journal"]
    assert "txn_id" in journal
    assert journal["txn_id"].startswith("txn-")
    assert journal["created_at"].startswith("20")
    assert journal["source_name"] == "hospital_10rows.csv"
    assert len(journal["source_sha256"]) == 64
    assert journal["applied"] is False
    assert journal["fixes_count"] == len(body["fixes"])
    assert journal["events"] == [{"event_type": "created"}]


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
    assert response.json() == {"detail": {"error": "advanced_mode_unavailable"}}


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
    assert response.json()["error"] == "rate_limit_exceeded"
