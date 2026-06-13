"""Tests for the full original DataForge vision external gate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dataforge.release import full_vision


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_text(path: Path, text: str = "evidence\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _publisher(workflow: str) -> dict[str, str]:
    return {
        "repository": full_vision.EXPECTED_PUBLISHER_REPOSITORY,
        "workflow": workflow,
        "ref": "refs/tags/v0.1.0",
        "identity": (
            f"https://github.com/Aegis15/dataforge/.github/workflows/{workflow}@refs/tags/v0.1.0"
        ),
        "oidc_issuer": full_vision.EXPECTED_OIDC_ISSUER,
    }


def _dist(package: str, index: str, kind: str, workflow: str) -> dict[str, Any]:
    extension = "py3-none-any.whl" if kind == "wheel" else "tar.gz"
    package_type = "bdist_wheel" if kind == "wheel" else "sdist"
    digest = ("a" if kind == "wheel" else "b") * 64
    filename = f"{package}-0.1.0-{extension}" if kind == "wheel" else f"{package}-0.1.0.tar.gz"
    return {
        "filename": filename,
        "package_type": package_type,
        "download_url": f"https://files.example/{index}/{filename}",
        "sha256": digest,
        "upload_time_iso_8601": "2026-06-03T00:00:00.000000Z",
        "provenance_url": f"https://{index}.example/integrity/{package}/0.1.0/{filename}",
        "integrity_predicate_type": full_vision.PUBLISH_ATTESTATION_PREDICATE,
        "integrity_subject_sha256": digest,
        "trusted_publisher": _publisher(workflow),
    }


def _index(package: str, index: str, workflow: str) -> dict[str, Any]:
    host = "pypi.org" if index == "pypi" else "test.pypi.org"
    return {
        "index": index,
        "project_url": f"https://{host}/project/{package}/",
        "wheel": _dist(package, index, "wheel", workflow),
        "sdist": _dist(package, index, "sdist", workflow),
    }


def test_full_vision_gate_can_pass_with_external_evidence(tmp_path: Path, monkeypatch: Any) -> None:
    """A fully evidenced external state satisfies every full-vision check."""
    for name in full_vision.EXPECTED_PACKAGES:
        _write_text(tmp_path / "pypi" / f"{name}-testpypi-smoke.log")
        _write_text(tmp_path / "pypi" / f"{name}-pypi-smoke.log")
    _write_json(
        tmp_path / "pypi" / "publish_report.json",
        {
            "schema_version": "dataforge_pypi_publish_report_v2",
            "packages": [
                {
                    "name": name,
                    "version": "0.1.0",
                    "trusted_publishing": True,
                    "attestations": True,
                    "testpypi_fresh_install": True,
                    "pypi_fresh_install": True,
                    "workflow_run_url": f"https://github.com/Aegis15/dataforge/actions/runs/{name}",
                    "testpypi_smoke_log_path": f"pypi/{name}-testpypi-smoke.log",
                    "pypi_smoke_log_path": f"pypi/{name}-pypi-smoke.log",
                    "testpypi": _index(name, "testpypi", full_vision.TESTPYPI_WORKFLOWS[name]),
                    "pypi": _index(name, "pypi", full_vision.PYPI_WORKFLOWS[name]),
                }
                for name in full_vision.EXPECTED_PACKAGES
            ],
        },
    )
    _write_text(tmp_path / "dbt_duckdb" / "txn.jsonl")
    _write_text(tmp_path / "dbt_duckdb" / "commands.log")
    _write_json(
        tmp_path / "dbt_duckdb" / "fresh_env_report.json",
        {
            "schema_version": "dataforge_dbt_fresh_env_proof_v1",
            "install_source": "pypi",
            "package": "dataforge_07_dbt",
            "python_version": "3.12.10",
            "dbt_core_version": "1.10.0",
            "dbt_duckdb_version": "1.10.0",
            "dbt_seed_passed": True,
            "dbt_run_passed": True,
            "dbt_test_passed": True,
            "dataforge_dbt_dry_run_passed": True,
            "dataforge_dbt_refuse_passed": True,
            "dataforge_dbt_apply_passed": True,
            "dataforge_table_store_audit_passed": True,
            "dataforge_table_store_revert_passed": True,
            "dbt_duckdb_e2e_passed": True,
            "skipped_tests": 0,
            "audit_artifact_written": True,
            "artifact_path": "dbt_duckdb/txn.jsonl",
            "command_log_path": "dbt_duckdb/commands.log",
        },
    )
    for persona in full_vision.EXPECTED_DESIGN_PERSONAS:
        _write_text(tmp_path / "design_partners" / f"{persona}.md")
        _write_text(tmp_path / "design_partners" / f"{persona}.consent.json", "{}\n")
    _write_json(
        tmp_path / "design_partners" / "manifest.json",
        {
            "schema_version": "dataforge_design_partner_manifest_v1",
            "validations": [
                {
                    "persona": persona,
                    "role": "external practitioner",
                    "session_date": "2026-06-03",
                    "production_surface": "workers playground",
                    "task_completed": "analyze sample and export receipt",
                    "validated": True,
                    "trust_confirmed": True,
                    "evidence_path": f"design_partners/{persona}.md",
                    "consent_record": f"design_partners/{persona}.consent.json",
                    "timing_seconds": 120,
                    "blocking_findings_closed": True,
                }
                for persona in full_vision.EXPECTED_DESIGN_PERSONAS
            ],
        },
    )
    manifest = full_vision.load_model_family_manifest()
    expected_model_repos = list(manifest.repo_ids())
    for repo_id in expected_model_repos:
        slug = repo_id.rsplit("/", 1)[1]
        _write_text(tmp_path / "models" / f"{slug}.eval.json")
        _write_text(tmp_path / "models" / f"{slug}.verification.json")
    _write_json(
        tmp_path / "models" / "model_family_report.json",
        {
            "schema_version": "dataforge_model_family_report_v2",
            "hf_owner": "Praneshrajan15",
            "dataset_repo": manifest.dataset_repo,
            "manifest_schema_version": manifest.schema_version,
            "manifest_sha256": manifest.manifest_hash,
            "models": [
                {
                    "size": entry.size,
                    "stage": entry.stage,
                    "repo_id": entry.repo_id,
                    "artifact_status": "public",
                    "quality_status": "quality_improved_verified",
                    "verifier_passed": True,
                    "upstream_license": entry.upstream_license,
                    "hub_license": entry.hub_license,
                    "license_name": entry.license_name,
                    "base_model": entry.base_model,
                    "predecessor_repo": entry.predecessor_repo,
                    "dataset_repo": manifest.dataset_repo,
                    "training_backend": entry.training_backend,
                    "training_run_url": "https://huggingface.co/jobs/Praneshrajan15/example",
                    "source_git_commit": "abc123def456",
                    "dataset_sha": "dataset-sha",
                    "model_sha": "model-sha",
                    "model_card_url": f"https://huggingface.co/{entry.repo_id}",
                    "eval_report_path": f"models/{entry.slug}.eval.json",
                    "verification_report_path": (f"models/{entry.slug}.verification.json"),
                    "eval_metrics": {"macro_f1": 0.5, "parse_success_rate": 1.0},
                    "limitations": ["verified external release"],
                }
                for entry in manifest.entries
            ],
        },
    )
    entries_by_repo = {entry.repo_id: entry for entry in manifest.entries}

    def fake_fetch_json(url: str, *, timeout_s: float = 20.0) -> tuple[dict[str, Any] | None, str]:
        if "pypi.org/pypi/" in url:
            return (
                {
                    "info": {
                        "version": "0.1.0",
                        "summary": "DataForge data-quality repair",
                        "home_page": "https://github.com/Aegis15/dataforge",
                    }
                },
                "",
            )
        if "huggingface.co/api/models/" in url:
            repo_id = url.rsplit("/api/models/", 1)[1]
            entry = entries_by_repo[repo_id]
            card = {
                "license": entry.hub_license,
                "datasets": [manifest.dataset_repo],
                "base_model": entry.base_model,
            }
            if entry.license_name:
                card["license_name"] = entry.license_name
            return (
                {
                    "sha": "abc123",
                    "lastModified": "2026-06-01T00:00:00Z",
                    "cardData": card,
                },
                "",
            )
        raise AssertionError(url)

    def fake_fetch_text(
        url: str,
        *,
        timeout_s: float = 20.0,
        headers: dict[str, str] | None = None,
    ) -> tuple[int | None, str, dict[str, str], str]:
        if url == "https://dataforge.praneshrajan15.workers.dev/playground":
            return (
                200,
                '<!doctype html><div id="root"></div><script src="config.js"></script>',
                {},
                "",
            )
        if url == "https://dataforge.praneshrajan15.workers.dev/playground/config.js":
            return (
                200,
                "window.__DATAFORGE_CONFIG__={BACKEND_URL:'https://backend.example'}",
                {"cache-control": "no-store"},
                "",
            )
        if url == "https://backend.example/api/health":
            response_headers = {}
            if headers and headers.get("Origin") == "https://dataforge.praneshrajan15.workers.dev":
                response_headers["access-control-allow-origin"] = (
                    "https://dataforge.praneshrajan15.workers.dev"
                )
            return (
                200,
                json.dumps(
                    {
                        "status": "ok",
                        "environment": "production",
                        "build_sha": "abc123def456",
                    }
                ),
                response_headers,
                "",
            )
        raise AssertionError(url)

    monkeypatch.setattr(full_vision, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(full_vision, "_fetch_text", fake_fetch_text)

    report = full_vision.run_full_vision_gate(
        evidence_root=tmp_path,
        frontend_url="https://dataforge.praneshrajan15.workers.dev/playground",
        backend_url="https://backend.example",
        expected_git_sha="abc123def456",
    )

    assert report.ok is True
    assert {check.name for check in report.checks} == {
        "pypi_packages",
        "testpypi_packages",
        "pypi_publish_evidence",
        "workers_dev_playground",
        "hf_space_backend",
        "dbt_duckdb_fresh_env",
        "design_partner_evidence",
        "hf_model_family",
    }
