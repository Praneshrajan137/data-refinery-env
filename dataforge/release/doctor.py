"""Release doctor checks for DataForge public-surface gates."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import asdict, dataclass
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any

EXPECTED_HF_USER = "Praneshrajan15"
DEFAULT_KAGGLE_CREDENTIALS = Path.home() / ".kaggle" / "credentials.json"
STALE_KAGGLE_JSON = Path.home() / ".kaggle" / "kaggle.json"
CORE_DISTRIBUTION = "dataforge_07"


@dataclass(frozen=True)
class DoctorCheck:
    """One release doctor check result."""

    name: str
    ok: bool
    detail: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class DoctorReport:
    """Machine-readable release doctor report."""

    ok: bool
    checks: list[DoctorCheck]
    scopes: list[str]
    secrets_printed: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report."""
        return asdict(self)


def _check_hugging_face() -> DoctorCheck:
    try:
        from huggingface_hub import HfApi, get_token
    except ImportError as exc:
        return DoctorCheck("hugging_face", False, f"huggingface_hub missing: {exc}", {})

    token_present = bool(get_token())
    if not token_present:
        return DoctorCheck(
            "hugging_face",
            False,
            "No cached Hugging Face token found.",
            {"expected_user": EXPECTED_HF_USER, "token_present": False},
        )
    try:
        info = HfApi(token=get_token()).whoami()
    except Exception as exc:
        return DoctorCheck(
            "hugging_face",
            False,
            f"Could not resolve Hugging Face identity: {exc}",
            {"expected_user": EXPECTED_HF_USER, "token_present": True},
        )
    user = str(info.get("name", ""))
    return DoctorCheck(
        "hugging_face",
        user == EXPECTED_HF_USER,
        "Authenticated with expected Hugging Face user."
        if user == EXPECTED_HF_USER
        else f"Authenticated as {user!r}, expected {EXPECTED_HF_USER!r}.",
        {"user": user, "expected_user": EXPECTED_HF_USER, "token_present": True},
    )


def _load_kaggle_oauth(path: Path) -> dict[str, Any]:
    if path.name == "kaggle.json":
        raise RuntimeError(
            f"Refusing to read stale legacy Kaggle API key file: {path}. "
            f"Use OAuth credentials at {DEFAULT_KAGGLE_CREDENTIALS}."
        )
    if not path.exists():
        raise RuntimeError(f"Missing Kaggle OAuth credentials: {path}")
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise RuntimeError("Kaggle OAuth credentials must be a JSON object.")
    required = {"refresh_token", "access_token", "access_token_expiration", "username", "scopes"}
    missing = sorted(required - set(payload))
    if missing:
        raise RuntimeError("Kaggle OAuth credentials missing fields: " + ", ".join(missing))
    if not isinstance(payload.get("username"), str) or not payload["username"]:
        raise RuntimeError("Kaggle OAuth credentials are missing username.")
    scopes = payload.get("scopes")
    if not isinstance(scopes, list) or not scopes:
        raise RuntimeError("Kaggle OAuth credentials are missing scopes.")
    return payload


def _check_kaggle_oauth(credentials_path: Path = DEFAULT_KAGGLE_CREDENTIALS) -> DoctorCheck:
    try:
        payload = _load_kaggle_oauth(credentials_path)
    except Exception as exc:
        return DoctorCheck(
            "kaggle_oauth", False, str(exc), {"credential_path": str(credentials_path)}
        )
    return DoctorCheck(
        "kaggle_oauth",
        True,
        "Kaggle OAuth credentials are present and legacy key is ignored.",
        {
            "credential_path": str(credentials_path),
            "username": payload["username"],
            "scopes_count": len(payload["scopes"]),
            "legacy_kaggle_json_exists": STALE_KAGGLE_JSON.exists(),
            "legacy_kaggle_json_used": False,
            "tokens_printed": False,
        },
    )


def _check_kaggle_cli_clean_config(
    credentials_path: Path = DEFAULT_KAGGLE_CREDENTIALS,
) -> DoctorCheck:
    """Verify Kaggle CLI auth through OAuth under a clean config directory."""
    script = _project_root() / "scripts" / "preflight" / "check_kaggle_auth.py"
    if not script.exists():
        return DoctorCheck(
            "kaggle_cli_clean_config",
            False,
            f"Kaggle auth preflight script not found: {script}",
            {"credential_path": str(credentials_path), "tokens_printed": False},
        )
    command = [
        sys.executable,
        str(script),
        "--kaggle-json",
        str(credentials_path),
        "--check-cli",
    ]
    try:
        result = subprocess.run(
            command,
            cwd=_project_root(),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=90,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return DoctorCheck(
            "kaggle_cli_clean_config",
            False,
            "Kaggle CLI clean-config OAuth preflight timed out.",
            {"credential_path": str(credentials_path), "tokens_printed": False},
        )
    ok = result.returncode == 0
    detail = (
        "Kaggle CLI read-only command succeeded with clean KAGGLE_CONFIG_DIR."
        if ok
        else "Kaggle CLI clean-config OAuth preflight failed."
    )
    return DoctorCheck(
        "kaggle_cli_clean_config",
        ok,
        detail,
        {
            "credential_path": str(credentials_path),
            "command": " ".join(command),
            "tokens_printed": False,
        },
    )


def _check_cloudflare() -> DoctorCheck:
    npx = shutil.which("npx") or shutil.which("npx.cmd")
    if npx is None:
        return DoctorCheck("cloudflare", False, "npx/wrangler not available on PATH.", {})
    env_file_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False) as env_file:
            env_file_path = env_file.name
        command = [npx, "wrangler", "whoami", "--env-file", env_file_path]
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=60,
                check=False,
            )
        finally:
            if env_file_path:
                Path(env_file_path).unlink(missing_ok=True)
    except FileNotFoundError:
        return DoctorCheck("cloudflare", False, "npx/wrangler not available on PATH.", {})
    except subprocess.TimeoutExpired:
        return DoctorCheck("cloudflare", False, "wrangler whoami timed out.", {})

    output = result.stdout + result.stderr
    ok = result.returncode == 0 and "logged in" in output.lower()
    has_route_scope = "workers_routes (write)" in output
    return DoctorCheck(
        "cloudflare",
        ok and has_route_scope,
        "Wrangler OAuth is logged in with Workers route scope."
        if ok and has_route_scope
        else "Wrangler is not logged in with the required Workers route scope.",
        {
            "wrangler_available": result.returncode == 0,
            "logged_in": ok,
            "workers_routes_write": has_route_scope,
            "command": f"{npx} wrangler whoami --env-file <empty-env-file>",
            "env_file_override": "empty",
        },
    )


def _project_root() -> Path:
    """Return the repository root for local core release checks."""
    return Path(__file__).resolve().parents[2]


def _check_package_boundary() -> DoctorCheck:
    """Verify the core wheel only includes the public DataForge namespace."""
    pyproject_path = _project_root() / "pyproject.toml"
    if not pyproject_path.exists():
        try:
            distribution = importlib_metadata.distribution(CORE_DISTRIBUTION)
            top_level = distribution.read_text("top_level.txt") or ""
        except importlib_metadata.PackageNotFoundError as exc:
            return DoctorCheck(
                "core_package_boundary",
                False,
                f"Could not read installed package metadata: {exc}",
                {"distribution": CORE_DISTRIBUTION},
            )
        top_level_packages = sorted(line.strip() for line in top_level.splitlines() if line.strip())
        expected_top_level = ["dataforge"]
        return DoctorCheck(
            "core_package_boundary",
            top_level_packages == expected_top_level,
            "Installed wheel exposes only the dataforge top-level package."
            if top_level_packages == expected_top_level
            else (
                "Installed wheel exposes top-level packages "
                f"{top_level_packages!r}, expected {expected_top_level!r}."
            ),
            {"top_level": top_level_packages, "expected": expected_top_level},
        )

    try:
        pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        include = pyproject["tool"]["setuptools"]["packages"]["find"]["include"]
    except Exception as exc:
        return DoctorCheck(
            "core_package_boundary",
            False,
            f"Could not read package include contract: {exc}",
            {"pyproject": str(pyproject_path)},
        )
    expected = ["dataforge", "dataforge.*"]
    return DoctorCheck(
        "core_package_boundary",
        include == expected,
        "Core wheel includes only the dataforge namespace."
        if include == expected
        else f"Core wheel package include list is {include!r}, expected {expected!r}.",
        {"include": include, "expected": expected},
    )


def _check_packaged_core_files() -> DoctorCheck:
    """Verify files required by source-install and wheel smoke commands exist."""
    root = _project_root()
    required = [
        root / "dataforge" / "py.typed",
        root / "dataforge" / "fixtures" / "hospital_10rows.csv",
        root / "dataforge" / "fixtures" / "hospital_schema.yaml",
        root / "dataforge" / "safety" / "constitutions" / "default.yaml",
    ]
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    return DoctorCheck(
        "core_packaged_files",
        not missing,
        "Core package data files required by smoke commands are present."
        if not missing
        else "Missing core package data files: " + ", ".join(missing),
        {"missing": missing},
    )


def _core_checks() -> list[DoctorCheck]:
    """Return local OSS release checks that do not require personal accounts."""
    return [_check_package_boundary(), _check_packaged_core_files()]


def _maintainer_deploy_checks(kaggle_credentials: Path) -> list[DoctorCheck]:
    """Return maintainer-specific deploy/auth checks."""
    return [
        _check_hugging_face(),
        _check_kaggle_oauth(kaggle_credentials),
        _check_kaggle_cli_clean_config(kaggle_credentials),
        _check_cloudflare(),
    ]


def run_doctor(
    *,
    kaggle_credentials: Path = DEFAULT_KAGGLE_CREDENTIALS,
    core: bool = True,
    maintainer_deploy: bool = False,
) -> DoctorReport:
    """Run selected release doctor checks."""
    checks: list[DoctorCheck] = []
    scopes: list[str] = []
    if core:
        checks.extend(_core_checks())
        scopes.append("core")
    if maintainer_deploy:
        checks.extend(_maintainer_deploy_checks(kaggle_credentials))
        scopes.append("maintainer_deploy")
    return DoctorReport(
        ok=all(check.ok for check in checks),
        checks=checks,
        scopes=scopes,
    )


def main(argv: list[str] | None = None) -> int:
    """Script entrypoint used by CI and local release work."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text.")
    parser.add_argument("--kaggle-credentials", type=Path, default=DEFAULT_KAGGLE_CREDENTIALS)
    parser.add_argument("--core", action="store_true", help="Run OSS core release checks.")
    parser.add_argument(
        "--maintainer-deploy",
        action="store_true",
        help="Run maintainer-specific deploy/auth checks.",
    )
    args = parser.parse_args(argv)
    core = args.core or not args.maintainer_deploy
    report = run_doctor(
        kaggle_credentials=args.kaggle_credentials,
        core=core,
        maintainer_deploy=args.maintainer_deploy,
    )
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        for check in report.checks:
            status = "ok" if check.ok else "fail"
            print(f"{status:4} {check.name}: {check.detail}")
    return 0 if report.ok else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
