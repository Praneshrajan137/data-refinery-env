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


def test_full_vision_gate_can_pass_with_external_evidence(tmp_path: Path, monkeypatch: Any) -> None:
    """A fully evidenced external state satisfies every full-vision check."""
    for name in full_vision.EXPECTED_PACKAGES:
        _write_text(tmp_path / "pypi" / f"{name}-testpypi-smoke.log")
        _write_text(tmp_path / "pypi" / f"{name}-pypi-smoke.log")
    _write_json(
        tmp_path / "pypi" / "publish_report.json",
        {
            "schema_version": "dataforge_pypi_publish_report_v1",
            "packages": [
                {
                    "name": name,
                    "trusted_publishing": True,
                    "attestations": True,
                    "testpypi_fresh_install": True,
                    "pypi_fresh_install": True,
                    "testpypi_url": f"https://test.pypi.org/project/{name}/",
                    "pypi_url": f"https://pypi.org/project/{name}/",
                    "workflow_run_url": f"https://github.com/Aegis15/dataforge/actions/runs/{name}",
                    "attestation_url": f"https://pypi.org/project/{name}/0.1.0/#data",
                    "wheel_sha256": "a" * 64,
                    "sdist_sha256": "b" * 64,
                    "testpypi_smoke_log_path": f"pypi/{name}-testpypi-smoke.log",
                    "pypi_smoke_log_path": f"pypi/{name}-pypi-smoke.log",
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
                    "validated": True,
                    "evidence_path": f"design_partners/{persona}.md",
                    "consent_record": f"design_partners/{persona}.consent.json",
                    "timing_seconds": 120,
                    "blocking_findings_closed": True,
                }
                for persona in full_vision.EXPECTED_DESIGN_PERSONAS
            ],
        },
    )
    expected_model_repos = [
        f"Praneshrajan15/DataForge-{size}-{stage}"
        for size in full_vision.EXPECTED_MODEL_SIZES
        for stage in full_vision.EXPECTED_MODEL_STAGES
    ]
    for repo_id in expected_model_repos:
        slug = repo_id.rsplit("/", 1)[1]
        _write_text(tmp_path / "models" / f"{slug}.eval.json")
        _write_text(tmp_path / "models" / f"{slug}.verification.json")
    _write_json(
        tmp_path / "models" / "model_family_report.json",
        {
            "schema_version": "dataforge_model_family_report_v1",
            "models": [
                {
                    "repo_id": repo_id,
                    "verifier_passed": True,
                    "dataset_repo": "Praneshrajan15/dataforge-sft-trajectories",
                    "training_run_url": "https://huggingface.co/jobs/Praneshrajan15/example",
                    "model_card_url": f"https://huggingface.co/{repo_id}",
                    "eval_report_path": f"models/{repo_id.rsplit('/', 1)[1]}.eval.json",
                    "verification_report_path": (
                        f"models/{repo_id.rsplit('/', 1)[1]}.verification.json"
                    ),
                    "limitations_documented": True,
                    "eval_metrics": {"macro_f1": 0.5, "parse_success_rate": 1.0},
                }
                for repo_id in expected_model_repos
            ],
        },
    )

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
            return (
                {
                    "sha": "abc123",
                    "lastModified": "2026-06-01T00:00:00Z",
                    "cardData": {
                        "license": "apache-2.0",
                        "datasets": ["Praneshrajan15/dataforge-sft-trajectories"],
                        "base_model": "Qwen/Qwen2.5-0.5B-Instruct",
                    },
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
