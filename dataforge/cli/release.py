"""CLI group for local release verification."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from dataforge.release.doctor import DEFAULT_KAGGLE_CREDENTIALS, run_doctor
from dataforge.release.full_vision import (
    DEFAULT_BACKEND_URL as FULL_VISION_BACKEND_URL,
)
from dataforge.release.full_vision import (
    DEFAULT_FRONTEND_URL as FULL_VISION_FRONTEND_URL,
)
from dataforge.release.full_vision import (
    run_full_vision_gate,
)
from dataforge.release.gate import run_release_gate

release_app = typer.Typer(help="Release verification utilities.", no_args_is_help=True)


@release_app.command(name="doctor")
def doctor(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
    core: Annotated[
        bool,
        typer.Option("--core", help="Run OSS core release checks."),
    ] = False,
    maintainer_deploy: Annotated[
        bool,
        typer.Option(
            "--maintainer-deploy",
            help="Run maintainer-specific deploy/auth checks.",
        ),
    ] = False,
    kaggle_credentials: Annotated[
        Path,
        typer.Option(
            "--kaggle-credentials",
            help="Path to Kaggle OAuth credentials.json. Legacy kaggle.json is never read.",
        ),
    ] = DEFAULT_KAGGLE_CREDENTIALS,
) -> None:
    """Verify local release/deploy auth without printing secrets."""
    run_core = core or not maintainer_deploy
    report = run_doctor(
        kaggle_credentials=kaggle_credentials,
        core=run_core,
        maintainer_deploy=maintainer_deploy,
    )
    if json_output:
        typer.echo(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        for check in report.checks:
            status = "ok" if check.ok else "fail"
            typer.echo(f"{status:4} {check.name}: {check.detail}")
    raise typer.Exit(code=0 if report.ok else 2)


@release_app.command(name="gate")
def gate(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
    keep_artifacts: Annotated[
        bool,
        typer.Option(
            "--keep-artifacts",
            help="Copy the temporary gate workspace to dist/release-gate-latest.",
        ),
    ] = False,
) -> None:
    """Build, audit, offline-install, and smoke-test the release wheel."""
    report = run_release_gate(keep_artifacts=keep_artifacts)
    if json_output:
        typer.echo(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        for step in report.steps:
            status = "ok" if step.ok else "fail"
            typer.echo(f"{status:4} {step.name}: {step.detail}")
    raise typer.Exit(code=0 if report.ok else 1)


@release_app.command(name="full-vision")
def full_vision(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
    evidence_root: Annotated[
        Path | None,
        typer.Option("--evidence-root", help="Directory containing external proof manifests."),
    ] = None,
    frontend_url: Annotated[
        str,
        typer.Option("--frontend-url", help="Mandatory dataforge.dev Playground URL."),
    ] = FULL_VISION_FRONTEND_URL,
    backend_url: Annotated[
        str,
        typer.Option("--backend-url", help="Hugging Face Playground backend URL."),
    ] = FULL_VISION_BACKEND_URL,
    expected_git_sha: Annotated[
        str | None,
        typer.Option("--expected-git-sha", help="Release git SHA expected in backend health."),
    ] = None,
) -> None:
    """Verify the external gates for the full original DataForge vision."""
    report = run_full_vision_gate(
        evidence_root=evidence_root,
        frontend_url=frontend_url,
        backend_url=backend_url,
        expected_git_sha=expected_git_sha,
    )
    if json_output:
        typer.echo(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        for check in report.checks:
            status = "ok" if check.ok else "fail"
            typer.echo(f"{status:4} {check.name}: {check.detail}")
    raise typer.Exit(code=0 if report.ok else 1)


@release_app.command(name="playground-check")
def playground_check(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
    frontend_url: Annotated[
        str,
        typer.Option("--frontend-url", help="Cloudflare Playground frontend URL."),
    ] = "https://dataforge.dev/playground",
    backend_url: Annotated[
        str,
        typer.Option("--backend-url", help="Hugging Face Playground backend URL."),
    ] = "https://Praneshrajan15-dataforge-playground.hf.space",
    latency_threshold_ms: Annotated[
        float,
        typer.Option("--latency-threshold-ms", help="Warm health latency threshold."),
    ] = 5_000.0,
) -> None:
    """Verify the deployed Playground release checklist."""
    from dataforge.release.playground_check import report_to_json, run_playground_check

    report = run_playground_check(
        frontend_url=frontend_url,
        backend_url=backend_url,
        latency_threshold_ms=latency_threshold_ms,
        include_doctor=True,
        include_smoke=True,
    )
    if json_output:
        typer.echo(report_to_json(report))
    else:
        for check in report.checks:
            status = "ok" if check.ok else "fail"
            typer.echo(f"{status:4} {check.name}: {check.detail}")
    raise typer.Exit(code=0 if report.ok else 1)
