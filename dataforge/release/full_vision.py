"""External completion gate for the full original DataForge vision.

This gate is intentionally stricter than the local release gate. It checks
public package names, the production Workers playground, Hugging Face artifacts,
and evidence files that can only be produced by real external work.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "dataforge_full_vision_gate_v1"
EXPECTED_VERSION = "0.1.0"
EXPECTED_PACKAGES = (
    "dataforge_07",
    "dataforge_07_mcp",
    "dataforge_07_evals",
    "dataforge_07_dbt",
    "dataforge_07_agent_patterns",
)
EXPECTED_MODEL_SIZES = ("0.5B", "1.5B", "3B", "7B")
EXPECTED_MODEL_STAGES = ("SFT", "GRPO", "GiGPO")
EXPECTED_DESIGN_PERSONAS = ("marcus", "priya", "shreya", "agent")
DEFAULT_FRONTEND_URL = "https://dataforge.praneshrajan15.workers.dev/playground"
DEFAULT_BACKEND_URL = "https://Praneshrajan15-dataforge-playground.hf.space"
DEFAULT_HF_OWNER = "Praneshrajan15"
REQUIRED_PACKAGE_EVIDENCE_FIELDS = (
    "version",
    "workflow_run_url",
    "testpypi_smoke_log_path",
    "pypi_smoke_log_path",
)
EXPECTED_PUBLISHER_REPOSITORY = "Aegis15/dataforge"
EXPECTED_OIDC_ISSUER = "https://token.actions.githubusercontent.com"
PUBLISH_ATTESTATION_PREDICATE = "https://docs.pypi.org/attestations/publish/v1"
PYPI_WORKFLOWS = {
    "dataforge_07": "publish-dataforge.yml",
    "dataforge_07_mcp": "publish-dataforge-mcp.yml",
    "dataforge_07_evals": "publish-dataforge-evals.yml",
    "dataforge_07_dbt": "publish-dataforge-dbt.yml",
    "dataforge_07_agent_patterns": "publish-dataforge-agent-patterns.yml",
}
TESTPYPI_WORKFLOWS = {
    "dataforge_07": "publish-testpypi.yml",
    "dataforge_07_mcp": "publish-dataforge-mcp-testpypi.yml",
    "dataforge_07_evals": "publish-dataforge-evals-testpypi.yml",
    "dataforge_07_dbt": "publish-dataforge-dbt-testpypi.yml",
    "dataforge_07_agent_patterns": "publish-dataforge-agent-patterns-testpypi.yml",
}


@dataclass(frozen=True)
class FullVisionCheck:
    """One full-vision completion check."""

    name: str
    ok: bool
    detail: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class FullVisionReport:
    """Machine-readable full-vision completion report."""

    ok: bool
    checks: list[FullVisionCheck]
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report."""
        return asdict(self)


def _project_root() -> Path:
    """Return the repository root."""
    return Path(__file__).resolve().parents[2]


def _evidence_root(root: Path | None = None) -> Path:
    """Return the release evidence directory."""
    return root if root is not None else _project_root() / "docs" / "evidence"


def _fetch_json(url: str, *, timeout_s: float = 20.0) -> tuple[dict[str, Any] | None, str]:
    """Fetch JSON from a public endpoint."""
    request = urllib.request.Request(url, headers={"User-Agent": "dataforge-full-vision-gate"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            return json.loads(response.read().decode("utf-8")), ""
    except urllib.error.HTTPError as exc:
        return None, f"HTTP {exc.code}"
    except Exception as exc:
        return None, str(exc)


def _fetch_text(
    url: str,
    *,
    timeout_s: float = 20.0,
    headers: dict[str, str] | None = None,
) -> tuple[int | None, str, dict[str, str], str]:
    """Fetch text and return status, body, response headers, and error."""
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "dataforge-full-vision-gate", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            response_headers = {key.lower(): value for key, value in response.headers.items()}
            return (
                int(response.status),
                response.read().decode("utf-8", errors="replace"),
                response_headers,
                "",
            )
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read().decode("utf-8", errors="replace"), {}, f"HTTP {exc.code}"
    except Exception as exc:
        return None, "", {}, str(exc)


def _load_json_file(path: Path) -> tuple[dict[str, Any] | None, str]:
    """Load a JSON evidence file."""
    if not path.exists():
        return None, f"Missing evidence file: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, f"Invalid JSON evidence file {path}: {exc}"
    if not isinstance(payload, dict):
        return None, f"Evidence file {path} must contain a JSON object."
    return payload, ""


def _resolve_evidence_file(evidence_root: Path, raw_path: Any) -> Path | None:
    """Resolve a manifest evidence path and require it to point to a file."""
    text = str(raw_path or "").strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = evidence_root / path
    if not path.is_file():
        return None
    return path


def _is_https_url(value: Any) -> bool:
    """Return whether a manifest field is a concrete HTTPS URL."""
    return str(value or "").strip().startswith("https://")


def _is_sha256(value: Any) -> bool:
    """Return whether a manifest field looks like a SHA-256 hex digest."""
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _check_distribution_evidence(
    *,
    package: str,
    index: str,
    kind: str,
    payload: Any,
    expected_workflow: str,
) -> list[str]:
    """Return errors for one wheel or sdist evidence object."""
    errors: list[str] = []
    label = f"{package}.{index}.{kind}"
    if not isinstance(payload, dict):
        return [f"{label}: evidence must be an object"]
    for field in (
        "filename",
        "package_type",
        "download_url",
        "sha256",
        "upload_time_iso_8601",
        "provenance_url",
        "integrity_predicate_type",
        "integrity_subject_sha256",
    ):
        if not str(payload.get(field, "")).strip():
            errors.append(f"{label}: {field} is required")
    for field in ("download_url", "provenance_url"):
        if payload.get(field) and not _is_https_url(payload.get(field)):
            errors.append(f"{label}: {field} must be an HTTPS URL")
    sha256 = str(payload.get("sha256", "")).lower()
    subject_sha256 = str(payload.get("integrity_subject_sha256", "")).lower()
    if sha256 and not _is_sha256(sha256):
        errors.append(f"{label}: sha256 must be a SHA-256 hex digest")
    if subject_sha256 and not _is_sha256(subject_sha256):
        errors.append(f"{label}: integrity_subject_sha256 must be a SHA-256 hex digest")
    if sha256 and subject_sha256 and sha256 != subject_sha256:
        errors.append(f"{label}: integrity_subject_sha256 must match sha256")
    if payload.get("integrity_predicate_type") != PUBLISH_ATTESTATION_PREDICATE:
        errors.append(f"{label}: integrity_predicate_type must be {PUBLISH_ATTESTATION_PREDICATE}")
    publisher = payload.get("trusted_publisher")
    if not isinstance(publisher, dict):
        errors.append(f"{label}: trusted_publisher must be an object")
        return errors
    if publisher.get("repository") != EXPECTED_PUBLISHER_REPOSITORY:
        errors.append(
            f"{label}: trusted_publisher.repository must be {EXPECTED_PUBLISHER_REPOSITORY}"
        )
    if publisher.get("workflow") != expected_workflow:
        errors.append(f"{label}: trusted_publisher.workflow must be {expected_workflow}")
    if publisher.get("oidc_issuer") != EXPECTED_OIDC_ISSUER:
        errors.append(f"{label}: trusted_publisher.oidc_issuer must be {EXPECTED_OIDC_ISSUER}")
    identity = str(publisher.get("identity", ""))
    if identity and expected_workflow not in identity:
        errors.append(f"{label}: trusted_publisher.identity must reference {expected_workflow}")
    return errors


def _check_index_evidence(
    *,
    package: str,
    index: str,
    payload: Any,
    expected_workflow: str,
) -> list[str]:
    """Return errors for one package-index evidence object."""
    errors: list[str] = []
    label = f"{package}.{index}"
    if not isinstance(payload, dict):
        return [f"{label}: evidence must be an object"]
    if payload.get("index") != index:
        errors.append(f"{label}: index must be {index}")
    if not _is_https_url(payload.get("project_url")):
        errors.append(f"{label}: project_url must be an HTTPS URL")
    for kind in ("wheel", "sdist"):
        errors.extend(
            _check_distribution_evidence(
                package=package,
                index=index,
                kind=kind,
                payload=payload.get(kind),
                expected_workflow=expected_workflow,
            )
        )
    return errors


def _version_tuple(value: str) -> tuple[int, ...]:
    """Parse a simple dotted numeric version for release-gate comparisons."""
    parts: list[int] = []
    for raw_part in value.split("."):
        digits = "".join(ch for ch in raw_part if ch.isdigit())
        parts.append(int(digits or "0"))
    return tuple(parts)


def _check_package_index(index_name: str, base_url: str) -> FullVisionCheck:
    """Verify all final package names exist on one package index."""
    package_results: dict[str, Any] = {}
    errors: list[str] = []
    for package in EXPECTED_PACKAGES:
        url = f"{base_url.rstrip('/')}/{package}/json"
        payload, error = _fetch_json(url)
        if payload is None:
            package_results[package] = {"ok": False, "error": error}
            errors.append(f"{package}: {error}")
            continue
        info = payload.get("info", {}) if isinstance(payload.get("info"), dict) else {}
        version = str(info.get("version", ""))
        summary = str(info.get("summary", ""))
        home_page = str(info.get("home_page", ""))
        project_urls = info.get("project_urls", {})
        project_urls_text = (
            json.dumps(project_urls, sort_keys=True) if isinstance(project_urls, dict) else ""
        )
        ok = _version_tuple(version) >= _version_tuple(EXPECTED_VERSION)
        if package == "dataforge":
            metadata_text = f"{summary} {home_page} {project_urls_text}".lower()
            ok = (
                ok
                and ("dataforge" in metadata_text)
                and ("aegis15" in metadata_text or "data-quality" in metadata_text)
            )
        package_results[package] = {
            "ok": ok,
            "version": version,
            "summary": summary,
            "home_page": home_page,
            "project_urls": project_urls,
            "url": url,
        }
        if not ok:
            errors.append(
                f"{package}: found version={version!r}, summary={summary!r}, home_page={home_page!r}"
            )
    return FullVisionCheck(
        name=f"{index_name}_packages",
        ok=not errors,
        detail=f"All final DataForge packages are published on {index_name}."
        if not errors
        else f"{index_name} package publication is incomplete or not owned by this project.",
        metadata={"packages": package_results, "errors": errors},
    )


def _check_publish_evidence(evidence_root: Path) -> FullVisionCheck:
    """Verify local evidence for trusted publishing and attestations."""
    path = evidence_root / "pypi" / "publish_report.json"
    payload, error = _load_json_file(path)
    if payload is None:
        return FullVisionCheck("pypi_publish_evidence", False, error, {"path": str(path)})
    packages = payload.get("packages")
    errors: list[str] = []
    if payload.get("schema_version") != "dataforge_pypi_publish_report_v2":
        errors.append("schema_version must be dataforge_pypi_publish_report_v2")
    if not isinstance(packages, list):
        errors.append("packages must be a list")
        packages = []
    seen = {str(item.get("name", "")) for item in packages if isinstance(item, dict)}
    missing = sorted(set(EXPECTED_PACKAGES) - seen)
    errors.extend(f"missing package evidence: {name}" for name in missing)
    for item in packages:
        if not isinstance(item, dict):
            errors.append("package evidence entries must be objects")
            continue
        name = str(item.get("name", ""))
        for field in REQUIRED_PACKAGE_EVIDENCE_FIELDS:
            if not str(item.get(field, "")).strip():
                errors.append(f"{name}: {field} is required")
        if item.get("version") and str(item.get("version")) != EXPECTED_VERSION:
            errors.append(f"{name}: version must be {EXPECTED_VERSION}")
        if item.get("trusted_publishing") is not True:
            errors.append(f"{name}: trusted_publishing must be true")
        if item.get("attestations") is not True:
            errors.append(f"{name}: attestations must be true")
        if item.get("testpypi_fresh_install") is not True:
            errors.append(f"{name}: TestPyPI fresh-install proof is missing")
        if item.get("pypi_fresh_install") is not True:
            errors.append(f"{name}: PyPI fresh-install proof is missing")
        for field in ("workflow_run_url",):
            if item.get(field) and not _is_https_url(item.get(field)):
                errors.append(f"{name}: {field} must be an HTTPS URL")
        for field in ("testpypi_smoke_log_path", "pypi_smoke_log_path"):
            if item.get(field) and _resolve_evidence_file(evidence_root, item.get(field)) is None:
                errors.append(f"{name}: {field} must point to an evidence file")
        if name in PYPI_WORKFLOWS:
            errors.extend(
                _check_index_evidence(
                    package=name,
                    index="pypi",
                    payload=item.get("pypi"),
                    expected_workflow=PYPI_WORKFLOWS[name],
                )
            )
            errors.extend(
                _check_index_evidence(
                    package=name,
                    index="testpypi",
                    payload=item.get("testpypi"),
                    expected_workflow=TESTPYPI_WORKFLOWS[name],
                )
            )
    return FullVisionCheck(
        name="pypi_publish_evidence",
        ok=not errors,
        detail="Trusted publishing, attestations, provenance, and fresh-install evidence exist."
        if not errors
        else "Publishing evidence is incomplete.",
        metadata={"path": str(path), "errors": errors},
    )


def _check_workers_playground(frontend_url: str, backend_url: str) -> FullVisionCheck:
    """Verify the production Workers playground serves the frontend shell."""
    errors: list[str] = []
    metadata: dict[str, Any] = {"frontend_url": frontend_url, "backend_url": backend_url}

    status, body, headers, error = _fetch_text(frontend_url)
    metadata["status_code"] = status
    metadata["content_type"] = headers.get("content-type", "")
    if error:
        errors.append(f"frontend fetch failed: {error}")
    if status != 200:
        errors.append(f"frontend returned {status}, expected 200")
    lowered = body.lower()
    for marker in ("<!doctype html>", 'id="root"', "config.js"):
        if marker not in lowered:
            errors.append(f"frontend HTML missing marker {marker!r}")
    config_url = f"{frontend_url.rstrip('/')}/config.js"
    config_status, config_body, config_headers, config_error = _fetch_text(config_url)
    metadata["config_status_code"] = config_status
    metadata["config_cache_control"] = config_headers.get("cache-control", "")
    if config_error:
        errors.append(f"config.js fetch failed: {config_error}")
    if backend_url not in config_body:
        errors.append("config.js does not point at the expected HF backend")
    if "no-store" not in metadata["config_cache_control"].lower():
        errors.append("config.js is not served with Cache-Control: no-store")
    return FullVisionCheck(
        name="workers_dev_playground",
        ok=not errors,
        detail="workers.dev playground serves the production frontend."
        if not errors
        else "workers.dev playground is not production-ready.",
        metadata={**metadata, "errors": errors},
    )


def _check_hf_backend(
    frontend_url: str, backend_url: str, expected_git_sha: str | None
) -> FullVisionCheck:
    """Verify the deployed HF Space backend and CORS."""
    errors: list[str] = []
    health_url = f"{backend_url.rstrip('/')}/api/health"
    status, body, _headers, error = _fetch_text(health_url)
    metadata: dict[str, Any] = {
        "backend_url": backend_url,
        "health_url": health_url,
        "status_code": status,
    }
    if error:
        errors.append(f"health fetch failed: {error}")
    payload: dict[str, Any] = {}
    if status == 200:
        try:
            decoded = json.loads(body)
            if isinstance(decoded, dict):
                payload = decoded
        except json.JSONDecodeError as exc:
            errors.append(f"health JSON decode failed: {exc}")
    else:
        errors.append(f"health returned {status}, expected 200")
    metadata["health"] = payload
    if payload.get("status") != "ok":
        errors.append("health status is not ok")
    if payload.get("environment") != "production":
        errors.append("health environment is not production")
    if expected_git_sha:
        build_sha = str(payload.get("build_sha", ""))
        if not build_sha.startswith(expected_git_sha[:12]):
            errors.append(
                f"build_sha {build_sha!r} does not match release SHA {expected_git_sha!r}"
            )

    origin = frontend_url.split("/playground", 1)[0]
    cors_status, _cors_body, cors_headers, cors_error = _fetch_text(
        health_url,
        headers={"Origin": origin},
    )
    metadata["cors_status_code"] = cors_status
    metadata["cors_allow_origin"] = cors_headers.get("access-control-allow-origin", "")
    if cors_error:
        errors.append(f"CORS probe failed: {cors_error}")
    if metadata["cors_allow_origin"] != origin:
        errors.append(f"CORS does not allow frontend origin {origin}")
    return FullVisionCheck(
        name="hf_space_backend",
        ok=not errors,
        detail="HF Space backend is live, production, and CORS-compatible."
        if not errors
        else "HF Space backend verification failed.",
        metadata={**metadata, "errors": errors},
    )


def _check_dbt_evidence(evidence_root: Path) -> FullVisionCheck:
    """Verify the dbt-duckdb fresh environment proof."""
    path = evidence_root / "dbt_duckdb" / "fresh_env_report.json"
    payload, error = _load_json_file(path)
    if payload is None:
        return FullVisionCheck("dbt_duckdb_fresh_env", False, error, {"path": str(path)})
    errors: list[str] = []
    if payload.get("schema_version") != "dataforge_dbt_fresh_env_proof_v1":
        errors.append("schema_version must be dataforge_dbt_fresh_env_proof_v1")
    if payload.get("install_source") != "pypi":
        errors.append("install_source must be pypi")
    if payload.get("package") != "dataforge_07_dbt":
        errors.append("package must be dataforge_07_dbt")
    if not str(payload.get("python_version", "")).startswith("3.12"):
        errors.append("python_version must be 3.12.x")
    if not str(payload.get("dbt_core_version", "")).strip():
        errors.append("dbt_core_version is required")
    if not str(payload.get("dbt_duckdb_version", "")).strip():
        errors.append("dbt_duckdb_version is required")
    for field in ("dbt_seed_passed", "dbt_run_passed", "dbt_test_passed"):
        if payload.get(field) is not True:
            errors.append(f"{field} must be true")
    for field in (
        "dataforge_dbt_dry_run_passed",
        "dataforge_dbt_refuse_passed",
        "dataforge_dbt_apply_passed",
        "dataforge_table_store_audit_passed",
        "dataforge_table_store_revert_passed",
    ):
        if payload.get(field) is not True:
            errors.append(f"{field} must be true")
    if payload.get("dbt_duckdb_e2e_passed") is not True:
        errors.append("dbt_duckdb_e2e_passed must be true")
    if int(payload.get("skipped_tests", -1)) != 0:
        errors.append("skipped_tests must be 0")
    if payload.get("audit_artifact_written") is not True:
        errors.append("audit_artifact_written must be true")
    for field in ("artifact_path", "command_log_path"):
        if _resolve_evidence_file(evidence_root, payload.get(field)) is None:
            errors.append(f"{field} must point to an evidence file")
    return FullVisionCheck(
        name="dbt_duckdb_fresh_env",
        ok=not errors,
        detail="dbt-duckdb fresh-env proof is complete."
        if not errors
        else "dbt-duckdb fresh-env proof is incomplete.",
        metadata={"path": str(path), "errors": errors},
    )


def _check_design_partner_evidence(evidence_root: Path) -> FullVisionCheck:
    """Verify real design-partner evidence exists for every required path."""
    path = evidence_root / "design_partners" / "manifest.json"
    payload, error = _load_json_file(path)
    if payload is None:
        return FullVisionCheck("design_partner_evidence", False, error, {"path": str(path)})
    errors: list[str] = []
    if payload.get("schema_version") != "dataforge_design_partner_manifest_v1":
        errors.append("schema_version must be dataforge_design_partner_manifest_v1")
    entries = payload.get("validations")
    if not isinstance(entries, list):
        errors.append("validations must be a list")
        entries = []
    by_persona = {
        str(entry.get("persona", "")).lower(): entry for entry in entries if isinstance(entry, dict)
    }
    for persona in EXPECTED_DESIGN_PERSONAS:
        entry = by_persona.get(persona)
        if entry is None:
            errors.append(f"missing design-partner validation: {persona}")
            continue
        for field in ("role", "session_date", "production_surface", "task_completed"):
            if not str(entry.get(field, "")).strip():
                errors.append(f"{persona}: {field} is required")
        if entry.get("validated") is not True:
            errors.append(f"{persona}: validated must be true")
        if entry.get("trust_confirmed") is not True:
            errors.append(f"{persona}: trust_confirmed must be true")
        if not entry.get("evidence_path"):
            errors.append(f"{persona}: evidence_path is required")
        elif _resolve_evidence_file(evidence_root, entry.get("evidence_path")) is None:
            errors.append(f"{persona}: evidence_path must point to an evidence file")
        if not entry.get("consent_record"):
            errors.append(f"{persona}: consent_record is required")
        elif _resolve_evidence_file(evidence_root, entry.get("consent_record")) is None:
            errors.append(f"{persona}: consent_record must point to an evidence file")
        if entry.get("blocking_findings_closed") is not True:
            errors.append(f"{persona}: blocking_findings_closed must be true")
        timing = entry.get("timing_seconds")
        if not isinstance(timing, int | float) or timing <= 0:
            errors.append(f"{persona}: timing_seconds must be positive")
    return FullVisionCheck(
        name="design_partner_evidence",
        ok=not errors,
        detail="Design-partner evidence covers Marcus, Priya, Shreya, and agent paths."
        if not errors
        else "Design-partner evidence is incomplete.",
        metadata={"path": str(path), "errors": errors},
    )


def _check_hf_model_family(evidence_root: Path, hf_owner: str) -> FullVisionCheck:
    """Verify public HF model repos and local quality evidence for the full family."""
    errors: list[str] = []
    repo_results: dict[str, Any] = {}
    for size in EXPECTED_MODEL_SIZES:
        for stage in EXPECTED_MODEL_STAGES:
            repo_id = f"{hf_owner}/DataForge-{size}-{stage}"
            payload, error = _fetch_json(f"https://huggingface.co/api/models/{repo_id}")
            if payload is None:
                repo_results[repo_id] = {"ok": False, "error": error}
                errors.append(f"{repo_id}: {error}")
                continue
            card = payload.get("cardData", {})
            card = card if isinstance(card, dict) else {}
            missing_card_fields = [
                field for field in ("license", "datasets", "base_model") if not card.get(field)
            ]
            ok = not missing_card_fields
            repo_results[repo_id] = {
                "ok": ok,
                "sha": payload.get("sha"),
                "lastModified": payload.get("lastModified"),
                "missing_card_fields": missing_card_fields,
            }
            if not ok:
                errors.append(f"{repo_id}: missing model-card fields {missing_card_fields}")

    path = evidence_root / "models" / "model_family_report.json"
    payload, error = _load_json_file(path)
    if payload is None:
        errors.append(error)
        evidence_metadata: dict[str, Any] = {"path": str(path), "loaded": False}
    else:
        evidence_metadata = {"path": str(path), "loaded": True}
        if payload.get("schema_version") != "dataforge_model_family_report_v1":
            errors.append(
                "model_family_report schema_version must be dataforge_model_family_report_v1"
            )
        models = payload.get("models")
        if not isinstance(models, list):
            errors.append("model_family_report models must be a list")
            models = []
        entries_by_repo = {
            str(item.get("repo_id", "")): item for item in models if isinstance(item, dict)
        }
        expected_repos = {
            f"{hf_owner}/DataForge-{size}-{stage}"
            for size in EXPECTED_MODEL_SIZES
            for stage in EXPECTED_MODEL_STAGES
        }
        missing = sorted(expected_repos - set(entries_by_repo))
        errors.extend(f"missing model evidence: {repo_id}" for repo_id in missing)
        for repo_id in sorted(expected_repos & set(entries_by_repo)):
            item = entries_by_repo[repo_id]
            if item.get("verifier_passed") is not True:
                errors.append(f"{repo_id}: verifier_passed must be true")
            if item.get("limitations_documented") is not True:
                errors.append(f"{repo_id}: limitations_documented must be true")
            if not str(item.get("dataset_repo", "")).strip():
                errors.append(f"{repo_id}: dataset_repo is required")
            for field in ("training_run_url", "model_card_url"):
                if not _is_https_url(item.get(field)):
                    errors.append(f"{repo_id}: {field} must be an HTTPS URL")
            for field in ("eval_report_path", "verification_report_path"):
                if _resolve_evidence_file(evidence_root, item.get(field)) is None:
                    errors.append(f"{repo_id}: {field} must point to an evidence file")
            metrics = item.get("eval_metrics")
            if not isinstance(metrics, dict) or not metrics:
                errors.append(f"{repo_id}: eval_metrics must be a non-empty object")
    return FullVisionCheck(
        name="hf_model_family",
        ok=not errors,
        detail="Full HF model family exists with verifier-passed quality evidence."
        if not errors
        else "Full HF model family is incomplete.",
        metadata={"repos": repo_results, "evidence": evidence_metadata, "errors": errors},
    )


def run_full_vision_gate(
    *,
    evidence_root: Path | None = None,
    frontend_url: str = DEFAULT_FRONTEND_URL,
    backend_url: str = DEFAULT_BACKEND_URL,
    expected_git_sha: str | None = None,
    hf_owner: str = DEFAULT_HF_OWNER,
) -> FullVisionReport:
    """Run the external full-original-vision completion gate."""
    root = _evidence_root(evidence_root)
    checks = [
        _check_package_index("pypi", "https://pypi.org/pypi"),
        _check_package_index("testpypi", "https://test.pypi.org/pypi"),
        _check_publish_evidence(root),
        _check_workers_playground(frontend_url, backend_url),
        _check_hf_backend(frontend_url, backend_url, expected_git_sha),
        _check_dbt_evidence(root),
        _check_design_partner_evidence(root),
        _check_hf_model_family(root, hf_owner),
    ]
    return FullVisionReport(ok=all(check.ok for check in checks), checks=checks)


def main(argv: list[str] | None = None) -> int:
    """Run the full-vision gate as a standalone script."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text.")
    parser.add_argument("--evidence-root", type=Path, default=None)
    parser.add_argument("--frontend-url", default=DEFAULT_FRONTEND_URL)
    parser.add_argument("--backend-url", default=DEFAULT_BACKEND_URL)
    parser.add_argument("--expected-git-sha", default=None)
    parser.add_argument("--hf-owner", default=DEFAULT_HF_OWNER)
    args = parser.parse_args(argv)

    report = run_full_vision_gate(
        evidence_root=args.evidence_root,
        frontend_url=args.frontend_url,
        backend_url=args.backend_url,
        expected_git_sha=args.expected_git_sha,
        hf_owner=args.hf_owner,
    )
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        for check in report.checks:
            status = "ok" if check.ok else "fail"
            print(f"{status:4} {check.name}: {check.detail}")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
