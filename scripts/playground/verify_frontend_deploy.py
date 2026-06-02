"""Verify the deployed Cloudflare frontend against the HF backend."""

from __future__ import annotations

import argparse
from urllib.parse import urlsplit

import httpx

DEFAULT_BACKEND_URL = "https://Praneshrajan15-dataforge-playground.hf.space"
DEFAULT_FRONTEND_URL = "https://dataforge.praneshrajan15.workers.dev/playground"


def normalize_url(value: str) -> str:
    """Strip whitespace and trailing slashes from a URL."""
    return value.strip().rstrip("/")


def _join_url(base_url: str, path: str) -> str:
    """Join a normalized base URL and an absolute path fragment."""
    return f"{base_url}/{path.lstrip('/')}"


def require(condition: bool, message: str) -> None:
    """Exit with a readable error when an assertion fails."""
    if not condition:
        raise SystemExit(message)


def verify(frontend_url: str, backend_url: str) -> None:
    """Run the end-to-end deployment checks."""
    frontend_url = normalize_url(frontend_url)
    backend_url = normalize_url(backend_url)
    parts = urlsplit(frontend_url)
    frontend_origin = f"{parts.scheme}://{parts.netloc}"

    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        frontend = client.get(frontend_url)
        require(frontend.status_code == 200, f"Frontend root returned {frontend.status_code}")
        require("<!doctype html>" in frontend.text.lower(), "Frontend root did not return HTML.")
        require("config.js" in frontend.text, "Frontend HTML is missing config.js.")
        require("/playground/assets/" in frontend.text, "Frontend HTML is missing built assets.")
        require('id="root"' in frontend.text, "Frontend HTML is missing the React mount node.")

        config = client.get(_join_url(frontend_url, "config.js"))
        require(config.status_code == 200, f"config.js returned {config.status_code}")
        require(
            backend_url in config.text,
            f"config.js does not contain backend URL {backend_url}",
        )
        cache_control = config.headers.get("cache-control", "")
        require("no-store" in cache_control.lower(), "config.js must be served with no-store.")

        backend_root = client.get(backend_url)
        require(
            backend_root.status_code == 200, f"Backend root returned {backend_root.status_code}"
        )
        root_payload = backend_root.json()
        require(
            root_payload.get("service") == "DataForge Playground API",
            "Backend root must return API metadata, not frontend HTML.",
        )

        health = client.get(f"{backend_url}/api/health")
        require(health.status_code == 200, f"Backend health returned {health.status_code}")
        health_payload = health.json()
        require(health_payload.get("status") == "ok", "Backend health is missing status=ok.")
        require(
            "advanced_available" in health_payload,
            "Backend health is missing advanced_available.",
        )
        require("max_upload_bytes" in health_payload, "Backend health is missing max_upload_bytes.")

        cors = client.get(
            f"{backend_url}/api/health",
            headers={"Origin": frontend_origin},
        )
        require(cors.status_code == 200, f"CORS health probe returned {cors.status_code}")
        require(
            cors.headers.get("access-control-allow-origin") == frontend_origin,
            "Backend CORS response does not allow the deployed frontend origin.",
        )

    print(f"Verified frontend {frontend_url}")
    print(f"Verified backend {backend_url}")


def _build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--frontend-url",
        default=DEFAULT_FRONTEND_URL,
        help="Cloudflare frontend URL.",
    )
    parser.add_argument(
        "--backend-url",
        default=DEFAULT_BACKEND_URL,
        help="Hugging Face backend base URL.",
    )
    return parser


def main() -> None:
    """Run the deployment verification checks."""
    args = _build_parser().parse_args()
    verify(args.frontend_url, args.backend_url)


if __name__ == "__main__":
    main()
