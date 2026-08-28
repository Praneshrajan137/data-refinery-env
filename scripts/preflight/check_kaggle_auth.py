"""Masked Kaggle authentication preflight for DataForge remote training."""

from __future__ import annotations

import argparse
import importlib.metadata as metadata
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_KAGGLE_CREDENTIALS = Path.home() / ".kaggle" / "credentials.json"
STALE_KAGGLE_JSON = Path.home() / ".kaggle" / "kaggle.json"
LEGACY_KAGGLE_ENV_VARS = (
    "KAGGLE_USERNAME",
    "KAGGLE_KEY",
    "KAGGLE_API_TOKEN",
    "KAGGLE_API_V1_TOKEN",
)
OAUTH_REFRESH_WINDOW = timedelta(hours=1)
Runner = Callable[..., subprocess.CompletedProcess[str]]


def _parse_expiration(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise RuntimeError("Kaggle OAuth credentials are missing access_token_expiration")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise RuntimeError("Kaggle OAuth access_token_expiration is not ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _refresh_needed(expires_at: datetime, *, now: datetime | None = None) -> bool:
    current = now or datetime.now(UTC)
    return expires_at <= current + OAUTH_REFRESH_WINDOW


def _load_credentials(path: Path) -> dict[str, Any]:
    """Load Kaggle OAuth credentials without returning tokens."""
    if path.name == "kaggle.json":
        raise RuntimeError(
            f"Refusing to read stale legacy Kaggle API key file: {path}. "
            f"Use OAuth credentials at {DEFAULT_KAGGLE_CREDENTIALS}."
        )
    if not path.exists():
        raise RuntimeError(f"Missing Kaggle credentials file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise RuntimeError("Kaggle credentials must contain a JSON object")
    required = {"refresh_token", "access_token", "access_token_expiration", "username", "scopes"}
    missing = sorted(required - set(payload))
    if missing:
        raise RuntimeError("Kaggle OAuth credentials missing fields: " + ", ".join(missing))
    username = payload.get("username")
    scopes = payload.get("scopes")
    if not isinstance(username, str) or not username:
        raise RuntimeError("Kaggle OAuth credentials are missing username")
    if not isinstance(scopes, list) or not scopes:
        raise RuntimeError("Kaggle OAuth credentials are missing scopes")
    expires_at = _parse_expiration(payload.get("access_token_expiration"))
    return {
        "credentials_present": True,
        "credential_path": str(path),
        "credential_type": "oauth",
        "username": username,
        "scopes_count": len(scopes),
        "access_token_expires_at": expires_at.isoformat(),
        "access_token_refresh_recommended": _refresh_needed(expires_at),
        "legacy_kaggle_json_exists": STALE_KAGGLE_JSON.exists(),
        "legacy_kaggle_json_used": False,
        "tokens_printed": False,
    }


def check_kaggle_auth(
    *,
    kaggle_json: Path = DEFAULT_KAGGLE_CREDENTIALS,
    kaggle_cli: Path | None = None,
    run_cli: bool = False,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    """Return masked Kaggle auth and CLI status without printing the API key."""
    report = _load_credentials(kaggle_json)
    try:
        kaggle_version = metadata.version("kaggle")
    except metadata.PackageNotFoundError as exc:
        raise RuntimeError("Python package 'kaggle' is not installed") from exc
    report.update(
        {
            "client": "kaggle_python_package",
            "client_version": kaggle_version,
        }
    )
    if run_cli:
        report.update(
            _run_cli_with_clean_config(
                kaggle_json=kaggle_json,
                kaggle_cli=kaggle_cli,
                runner=runner,
            )
        )
    else:
        report["cli_checked"] = False
    return report


def _resolve_kaggle_cli(kaggle_cli: Path | None) -> Path:
    if kaggle_cli is not None:
        return kaggle_cli
    local_cli = PROJECT_ROOT / ".venv" / "Scripts" / "kaggle.exe"
    if local_cli.exists():
        return local_cli
    discovered = shutil.which("kaggle") or shutil.which("kaggle.exe")
    if discovered is None:
        raise RuntimeError("Kaggle CLI executable not found.")
    return Path(discovered)


def _run_cli_with_clean_config(
    *,
    kaggle_json: Path,
    kaggle_cli: Path | None,
    runner: Runner,
) -> dict[str, Any]:
    """Run a read-only Kaggle CLI command with legacy config and env isolated."""
    resolved_cli = _resolve_kaggle_cli(kaggle_cli)
    refresh_report = _refresh_oauth_access_token_if_needed(kaggle_json)
    command = [
        str(resolved_cli),
        "datasets",
        "list",
        "--mine",
        "--page",
        "1",
        "--csv",
    ]
    with tempfile.TemporaryDirectory(prefix="dataforge-kaggle-config-") as clean_config:
        env = os.environ.copy()
        for key in LEGACY_KAGGLE_ENV_VARS:
            env.pop(key, None)
        env["KAGGLE_CONFIG_DIR"] = clean_config
        env["KAGGLE_CREDENTIALS_FILE"] = str(kaggle_json)
        result = runner(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=60,
            check=False,
            env=env,
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise RuntimeError(
                "Kaggle CLI clean-config OAuth preflight failed "
                f"with exit code {result.returncode}: {stderr[:500]}"
            )
        return {
            "cli_checked": True,
            "cli_command": " ".join(command),
            "clean_config_dir_used": True,
            "legacy_env_cleared": True,
            "oauth_credentials_file": str(kaggle_json),
            **refresh_report,
            "tokens_printed": False,
        }


def _refresh_oauth_access_token_if_needed(kaggle_json: Path) -> dict[str, Any]:
    payload = json.loads(kaggle_json.read_text(encoding="utf-8-sig"))
    expires_at = _parse_expiration(payload.get("access_token_expiration"))
    if not _refresh_needed(expires_at):
        return {"oauth_access_token_refreshed": False}
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        from kagglesdk.kaggle_creds import KaggleCredentials
    except ImportError as exc:
        raise RuntimeError(
            "Kaggle OAuth refresh requires kaggle>=2, which provides kagglesdk. "
            'Install it with: pip install -e ".[kaggle]"'
        ) from exc

    api = KaggleApi()
    with api.build_kaggle_client() as kaggle:
        creds = KaggleCredentials.load(client=kaggle, file_path=str(kaggle_json))
        if creds is None:
            raise RuntimeError("Kaggle OAuth credentials could not be loaded for refresh")
        creds.refresh_access_token()
        creds.save(file_path=str(kaggle_json))
    refreshed_payload = json.loads(kaggle_json.read_text(encoding="utf-8-sig"))
    refreshed_expiry = _parse_expiration(refreshed_payload.get("access_token_expiration"))
    return {
        "oauth_access_token_refreshed": True,
        "access_token_expires_at": refreshed_expiry.isoformat(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kaggle-json", type=Path, default=DEFAULT_KAGGLE_CREDENTIALS)
    parser.add_argument("--kaggle-cli", type=Path, default=None)
    parser.add_argument(
        "--check-cli",
        action="store_true",
        help="Run the Kaggle CLI with a clean KAGGLE_CONFIG_DIR and OAuth credentials.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        report = check_kaggle_auth(
            kaggle_json=args.kaggle_json,
            kaggle_cli=args.kaggle_cli,
            run_cli=args.check_cli,
        )
    except Exception as exc:
        print(f"Kaggle auth preflight failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
