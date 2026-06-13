"""Build or verify the DataForge Hugging Face model-family evidence report."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.release.model_family import (  # noqa: E402
    FAMILY_REPORT_SCHEMA_VERSION,
    MODEL_FAMILY_STAGES,
    PASSING_QUALITY_STATUSES,
    PUBLIC_ARTIFACT_STATUSES,
    ModelFamilyEntry,
    ModelFamilyManifest,
    build_hub_upload_manifest,
    load_model_family_manifest,
)
from scripts.model.verify_gigpo_release import verify_gigpo_release  # noqa: E402
from scripts.model.verify_grpo_release import verify_grpo_release  # noqa: E402
from scripts.model.verify_sft_release import verify_sft_release  # noqa: E402


class ModelFamilyVerificationError(RuntimeError):
    """Raised when model-family evidence is incomplete or inconsistent."""


def build_policy_report(
    manifest: ModelFamilyManifest,
    *,
    source_git_commit: str = "",
) -> dict[str, Any]:
    """Return an honest v2 family report from manifest policy alone."""
    return {
        "schema_version": FAMILY_REPORT_SCHEMA_VERSION,
        "hf_owner": manifest.hf_owner,
        "dataset_repo": manifest.dataset_repo,
        "manifest_schema_version": manifest.schema_version,
        "manifest_sha256": manifest.manifest_hash,
        "models": [
            build_hub_upload_manifest(
                entry,
                dataset_repo=manifest.dataset_repo,
                dataset_sha="",
                source_git_commit=source_git_commit,
                eval_metrics={},
            )
            for entry in manifest.entries
        ],
    }


def verify_public_manifest_rows(
    manifest: ModelFamilyManifest,
    *,
    token: str | None,
    source_git_commit: str = "",
) -> dict[str, Any]:
    """Verify rows marked public in the manifest and return a v2 family report."""
    report = build_policy_report(manifest, source_git_commit=source_git_commit)
    by_repo = {row["repo_id"]: row for row in report["models"] if isinstance(row, dict)}
    for entry in manifest.entries:
        if entry.artifact_status not in PUBLIC_ARTIFACT_STATUSES:
            continue
        by_repo[entry.repo_id].update(_verify_entry(entry, manifest=manifest, token=token))
    return report


def validate_family_report(report: dict[str, Any], manifest: ModelFamilyManifest) -> list[str]:
    """Return errors that prevent a family report from satisfying the quality gate."""
    errors: list[str] = []
    if report.get("schema_version") != FAMILY_REPORT_SCHEMA_VERSION:
        errors.append(f"schema_version must be {FAMILY_REPORT_SCHEMA_VERSION}")
    rows = report.get("models")
    if not isinstance(rows, list):
        return [*errors, "models must be a list"]
    by_repo = {str(row.get("repo_id", "")): row for row in rows if isinstance(row, dict)}
    missing = sorted(set(manifest.repo_ids()) - set(by_repo))
    errors.extend(f"missing model evidence: {repo_id}" for repo_id in missing)
    for entry in manifest.entries:
        row = by_repo.get(entry.repo_id)
        if row is None:
            continue
        label = entry.repo_id
        if row.get("size") != entry.size:
            errors.append(f"{label}: size must be {entry.size}")
        if row.get("stage") != entry.stage:
            errors.append(f"{label}: stage must be {entry.stage}")
        if row.get("base_model") != entry.base_model:
            errors.append(f"{label}: base_model must be {entry.base_model}")
        if row.get("upstream_license") != entry.upstream_license:
            errors.append(f"{label}: upstream_license must be {entry.upstream_license}")
        if row.get("artifact_status") not in PUBLIC_ARTIFACT_STATUSES:
            errors.append(f"{label}: artifact_status must be public")
        if row.get("quality_status") not in PASSING_QUALITY_STATUSES:
            errors.append(f"{label}: quality_status must be quality-verified")
        if row.get("verifier_passed") is not True:
            errors.append(f"{label}: verifier_passed must be true")
        if entry.predecessor_repo and row.get("predecessor_repo") != entry.predecessor_repo:
            errors.append(f"{label}: predecessor_repo must be {entry.predecessor_repo}")
        for field in (
            "dataset_repo",
            "training_backend",
            "training_run_url",
            "source_git_commit",
            "dataset_sha",
            "model_sha",
            "model_card_url",
            "eval_report_path",
            "verification_report_path",
        ):
            if not str(row.get(field, "")).strip():
                errors.append(f"{label}: {field} is required")
        if not isinstance(row.get("eval_metrics"), dict) or not row.get("eval_metrics"):
            errors.append(f"{label}: eval_metrics must be a non-empty object")
        limitations = row.get("limitations")
        if not isinstance(limitations, list) or not limitations:
            errors.append(f"{label}: limitations must be a non-empty list")
    errors.extend(_report_dependency_errors(by_repo, manifest))
    return errors


def _verify_entry(
    entry: ModelFamilyEntry,
    *,
    manifest: ModelFamilyManifest,
    token: str | None,
) -> dict[str, Any]:
    if entry.stage == "SFT":
        evidence = verify_sft_release(
            model_repo=entry.repo_id,
            dataset_repo=manifest.dataset_repo,
            token=token,
            require_eval_diagnostics=True,
            expected_model_license=entry.upstream_license,
        )
        return {
            "artifact_status": "public",
            "quality_status": evidence.release_status,
            "verifier_passed": evidence.quality_milestone,
            "dataset_sha": evidence.dataset.sha,
            "model_sha": evidence.model.sha,
            "eval_metrics": {
                "base_f1": evidence.metrics["base_f1"],
                "sft_f1": evidence.metrics["sft_f1"],
                "parse_success_rate": evidence.metrics.get("parse_success_rate"),
                "schema_case_error_count": evidence.metrics.get("schema_case_error_count"),
            },
        }
    if entry.stage == "GRPO":
        evidence = verify_grpo_release(
            model_repo=entry.repo_id,
            token=token,
            expected_model_license=entry.upstream_license,
            expected_sft_model=entry.predecessor_repo,
        )
        return {
            "artifact_status": "public",
            "quality_status": evidence.release_status,
            "verifier_passed": True,
            "dataset_sha": str(evidence.metrics["dataset_sha"]),
            "model_sha": evidence.model.sha,
            "eval_metrics": {
                "sft_f1": evidence.metrics["sft_f1"],
                "grpo_f1": evidence.metrics["grpo_f1"],
                "f1_delta": evidence.metrics["f1_delta"],
                "parse_success_rate": evidence.metrics["parse_success_rate"],
                "schema_case_error_count": evidence.metrics["schema_case_error_count"],
            },
        }
    if entry.stage == "GiGPO":
        evidence = verify_gigpo_release(
            model_repo=entry.repo_id,
            token=token,
            expected_model_license=entry.upstream_license,
            expected_grpo_model=entry.predecessor_repo,
        )
        return {
            "artifact_status": "public",
            "quality_status": evidence.release_status,
            "verifier_passed": True,
            "dataset_sha": str(evidence.metrics["dataset_sha"]),
            "model_sha": evidence.model.sha,
            "eval_metrics": {
                "grpo_f1": evidence.metrics["grpo_f1"],
                "gigpo_f1": evidence.metrics["gigpo_f1"],
                "f1_delta": evidence.metrics["f1_delta"],
                "parse_success_rate": evidence.metrics["parse_success_rate"],
                "schema_case_error_count": evidence.metrics["schema_case_error_count"],
            },
        }
    raise ModelFamilyVerificationError(f"Unknown model-family stage: {entry.stage}")


def _report_dependency_errors(
    by_repo: dict[str, dict[str, Any]],
    manifest: ModelFamilyManifest,
) -> list[str]:
    errors: list[str] = []
    for entry in manifest.entries:
        if entry.stage == "SFT" or not entry.predecessor_repo:
            continue
        row = by_repo.get(entry.repo_id, {})
        predecessor = by_repo.get(entry.predecessor_repo, {})
        row_verified = (
            row.get("artifact_status") in PUBLIC_ARTIFACT_STATUSES
            and row.get("quality_status") in PASSING_QUALITY_STATUSES
        )
        predecessor_verified = (
            predecessor.get("artifact_status") in PUBLIC_ARTIFACT_STATUSES
            and predecessor.get("quality_status") in PASSING_QUALITY_STATUSES
        )
        if row_verified and not predecessor_verified:
            errors.append(
                f"{entry.repo_id}: cannot be quality-verified before {entry.predecessor_repo}."
            )
    return errors


def _write_report(report: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def _print_report_summary(report: dict[str, Any]) -> None:
    table = Table(title="DataForge Model Family Evidence")
    table.add_column("Stage")
    table.add_column("Public")
    table.add_column("Quality")
    rows = report.get("models", [])
    if isinstance(rows, list):
        stage_order = {stage: index for index, stage in enumerate(MODEL_FAMILY_STAGES)}
        sorted_rows = sorted(
            [row for row in rows if isinstance(row, dict)],
            key=lambda row: (str(row.get("size", "")), stage_order.get(str(row.get("stage")), 99)),
        )
        for row in sorted_rows:
            table.add_row(
                str(row.get("repo_id", "")),
                str(row.get("artifact_status", "")),
                str(row.get("quality_status", "")),
            )
    Console().print(table)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--source-git-commit", default="")
    parser.add_argument(
        "--verify-public",
        action="store_true",
        help="Run leaf verifiers for rows marked public in the manifest.",
    )
    parser.add_argument(
        "--require-complete-quality-family",
        action="store_true",
        help="Fail unless every expected row is public and quality-verified.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the family verifier CLI."""
    load_dotenv()
    args = _build_parser().parse_args(argv)
    token = (os.environ.get("HF_TOKEN") or "").strip() or None
    manifest = load_model_family_manifest(args.manifest)
    try:
        report = (
            verify_public_manifest_rows(
                manifest,
                token=token,
                source_git_commit=args.source_git_commit,
            )
            if args.verify_public
            else build_policy_report(manifest, source_git_commit=args.source_git_commit)
        )
    except Exception as exc:
        print(f"model-family verification failed: {exc}", file=sys.stderr)
        return 2
    _print_report_summary(report)
    if args.require_complete_quality_family:
        errors = validate_family_report(report, manifest)
        if errors:
            for error in errors:
                print(f"model-family gate: {error}", file=sys.stderr)
            return 2
    if args.output is not None:
        _write_report(report, args.output)
        Console().print(f"Wrote model-family report to {args.output}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
