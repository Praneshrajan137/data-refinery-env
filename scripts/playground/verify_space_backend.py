"""Verify the Hugging Face Space backend after cold start."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import httpx

# The Azure Container App now serves the API. This script previously defaulted to the retired HF
# Space, so it could report a healthy backend that nothing was using.
DEFAULT_BACKEND_URL = (
    "https://dataforge-playground.grayflower-1f6e0578.eastus2.azurecontainerapps.io"
)


def _normalize_url(value: str) -> str:
    """Strip whitespace and trailing slashes."""
    return value.strip().rstrip("/")


def _request_until_ready(
    client: httpx.Client,
    url: str,
    *,
    timeout_s: float,
    interval_s: float,
) -> httpx.Response:
    """Retry a GET until the Space wakes or the deadline expires."""
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = client.get(url)
            if response.status_code == 200:
                return response
            last_error = RuntimeError(f"{url} returned {response.status_code}")
        except httpx.HTTPError as exc:
            last_error = exc
        time.sleep(interval_s)
    if last_error is not None:
        raise RuntimeError(f"{url} did not become ready: {last_error}") from last_error
    raise RuntimeError(f"{url} did not become ready before timeout.")


def _post_sample_csv(
    client: httpx.Client, backend_url: str, sample_csv: Path, route: str
) -> httpx.Response:
    """POST the local sample CSV to a playground route."""
    with sample_csv.open("rb") as handle:
        files = {"file": ("hospital_10rows.csv", handle, "text/csv")}
        return client.post(f"{backend_url}{route}", files=files)


def verify_space_backend(
    *,
    backend_url: str = DEFAULT_BACKEND_URL,
    timeout_s: float = 180.0,
    interval_s: float = 10.0,
    sample_csv: Path = Path("playground/api/samples/hospital_10rows.csv"),
) -> None:
    """Verify health, sample download, analyze, profile, and repair endpoints."""
    backend_url = _normalize_url(backend_url)
    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        health = _request_until_ready(
            client,
            f"{backend_url}/api/health",
            timeout_s=timeout_s,
            interval_s=interval_s,
        )
        payload = health.json()
        if payload.get("status") != "ok":
            raise RuntimeError(f"Health payload is not ready: {payload}")

        sample = client.get(f"{backend_url}/api/samples/hospital_10rows")
        if sample.status_code != 200 or "text/csv" not in sample.headers.get("content-type", ""):
            raise RuntimeError(f"Sample endpoint failed: {sample.status_code}")

        if not sample_csv.exists():
            raise RuntimeError(f"Missing local sample CSV for POST smoke: {sample_csv}")
        analyze = _post_sample_csv(client, backend_url, sample_csv, "/api/analyze")
        if analyze.status_code != 200:
            raise RuntimeError(
                f"Analyze endpoint failed: {analyze.status_code} {analyze.text[:200]}"
            )
        analyze_payload = analyze.json()
        missing = {
            "source",
            "risk_summary",
            "repairs",
            "verification",
            "receipt",
            "apply_handoff",
        } - set(analyze_payload)
        if missing:
            raise RuntimeError(f"Analyze payload is missing required keys: {sorted(missing)}")

        profile = _post_sample_csv(client, backend_url, sample_csv, "/api/profile")
        if profile.status_code != 200:
            raise RuntimeError(
                f"Profile endpoint failed: {profile.status_code} {profile.text[:200]}"
            )

        repair = _post_sample_csv(client, backend_url, sample_csv, "/api/repair")
        if repair.status_code != 200:
            raise RuntimeError(f"Repair endpoint failed: {repair.status_code} {repair.text[:200]}")

        # The guardrail surface was NOT checked here at all until 2026-09-08, which is part of why
        # it could degrade to uniform refusal on a deployed Space without anyone noticing. The
        # assertion that matters is that something is actually PROVEN: a guardrail which can only
        # ever refuse demonstrates nothing, and refusal is indistinguishable from a broken premise.
        scenario = client.get(f"{backend_url}/api/verify-scenarios/hospital_10rows")
        if scenario.status_code != 200:
            raise RuntimeError(
                f"Verify-scenario endpoint failed: {scenario.status_code} {scenario.text[:200]}"
            )
        scenario_payload = scenario.json()
        if scenario_payload.get("declared_schema") != "hospital_10rows":
            raise RuntimeError(
                "The hospital scenario reports no declared premise, so it cannot prove anything: "
                f"declared_schema={scenario_payload.get('declared_schema')!r}"
            )

        with sample_csv.open("rb") as handle:
            verify = client.post(
                f"{backend_url}/api/verify-fixes",
                files={"file": ("hospital_10rows.csv", handle, "text/csv")},
                data={
                    "fixes": json.dumps(scenario_payload["fixes"]),
                    "accepted_constraint_ids": json.dumps(
                        scenario_payload["accepted_constraint_ids"]
                    ),
                    "proposer": scenario_payload["proposer"],
                    "declared_schema": scenario_payload["declared_schema"],
                },
            )
        if verify.status_code != 200:
            raise RuntimeError(
                f"Verify-fixes endpoint failed: {verify.status_code} {verify.text[:200]}"
            )
        verify_payload = verify.json()
        if not verify_payload.get("would_apply"):
            raise RuntimeError(
                "The guardrail scenario proved nothing on the deployed backend. Every proposal was "
                "held, so the surface is demonstrating uniform refusal rather than the "
                "proven / held / rejected split its own copy promises."
            )
        held_reasons = {
            str(item.get("review_reason"))
            for item in verify_payload.get("receipt", {}).get("suggested_fixes", [])
        }
        if not {"verifier_rejected", "stale_precondition", "invalid_target"} <= held_reasons:
            raise RuntimeError(
                f"The guardrail scenario did not block for all three distinct reasons: "
                f"{sorted(held_reasons)}"
            )

    print(f"Verified HF Space backend {backend_url}")


def _build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend-url", default=DEFAULT_BACKEND_URL)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    parser.add_argument("--interval-s", type=float, default=10.0)
    parser.add_argument(
        "--sample-csv", type=Path, default=Path("playground/api/samples/hospital_10rows.csv")
    )
    return parser


def main() -> None:
    """Run backend verification."""
    args = _build_parser().parse_args()
    verify_space_backend(
        backend_url=args.backend_url,
        timeout_s=args.timeout_s,
        interval_s=args.interval_s,
        sample_csv=args.sample_csv,
    )


if __name__ == "__main__":
    main()
