"""Verify a gated DataForge GiGPO model release on Hugging Face."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

DEFAULT_MODEL_REPO = "Praneshrajan15/DataForge-0.5B-GiGPO"
REQUIRED_MODEL_FILES = frozenset(
    {
        "README.md",
        "config.json",
        "model.safetensors",
        "tokenizer.json",
        "tokenizer_config.json",
        "training_metrics.json",
        "eval_diagnostics.json",
    }
)
REQUIRED_METRIC_FIELDS = frozenset(
    {
        "model_name",
        "model_license",
        "base_model",
        "grpo_model",
        "dataset_repo",
        "dataset_sha",
        "source_git_commit",
        "benchmark_name",
        "benchmark_seeds",
        "gpu_hours",
        "attempted_steps",
        "grpo_f1",
        "gigpo_f1",
        "f1_delta",
        "parse_success_rate",
        "schema_case_error_count",
        "failure_samples",
        "acceptance_gate_passed",
    }
)


class HubSibling(Protocol):
    """Minimal Hugging Face sibling shape."""

    rfilename: str


class HubRepoInfo(Protocol):
    """Minimal Hugging Face repo-info shape."""

    siblings: list[HubSibling]
    sha: str | None


class HubApi(Protocol):
    """Subset of HfApi used by the verifier."""

    def repo_info(
        self,
        repo_id: str,
        *,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> HubRepoInfo:
        """Return repository metadata."""


class DownloadFile(Protocol):
    """Callable shape for downloading one Hub file."""

    def __call__(
        self,
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        token: str | None = None,
    ) -> str:
        """Download a repo file and return a local path."""


class GigpoReleaseVerificationError(RuntimeError):
    """Raised when a GiGPO release is incomplete or below the public gate."""


@dataclass(frozen=True, slots=True)
class GigpoRepoEvidence:
    """Verified file evidence for a model repository."""

    repo_id: str
    sha: str
    files: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GigpoReleaseEvidence:
    """Serializable GiGPO release verification report."""

    model: GigpoRepoEvidence
    metrics: dict[str, Any]
    release_status: str
    quality_gate_checked: bool
    diagnostics_checked: bool


def _repo_files(info: HubRepoInfo) -> tuple[str, ...]:
    """Return sorted repository file paths."""
    return tuple(sorted(sibling.rfilename for sibling in info.siblings))


def _download_text(
    repo_id: str,
    *,
    filename: str,
    token: str | None,
    downloader: DownloadFile,
) -> str:
    """Download and read a UTF-8 model file."""
    path = Path(downloader(repo_id, filename=filename, repo_type="model", token=token))
    return path.read_text(encoding="utf-8")


def _load_json(
    repo_id: str,
    *,
    filename: str,
    token: str | None,
    downloader: DownloadFile,
) -> dict[str, Any]:
    """Download and parse a JSON object from the model repo."""
    payload = json.loads(
        _download_text(repo_id, filename=filename, token=token, downloader=downloader)
    )
    if not isinstance(payload, dict):
        raise GigpoReleaseVerificationError(f"{repo_id}/{filename} must contain a JSON object.")
    return cast(dict[str, Any], payload)


def _validate_metrics(metrics: dict[str, Any]) -> None:
    """Validate GiGPO release metrics and public gate evidence."""
    missing = sorted(REQUIRED_METRIC_FIELDS - set(metrics))
    if missing:
        raise GigpoReleaseVerificationError(
            "training_metrics.json missing required fields: " + ", ".join(missing)
        )
    if str(metrics["model_license"]).lower() != "apache-2.0":
        raise GigpoReleaseVerificationError("model_license must be apache-2.0.")
    if metrics["benchmark_name"] != "DataForge-Bench-light-verified":
        raise GigpoReleaseVerificationError("benchmark_name must be DataForge-Bench-light-verified.")
    if metrics["benchmark_seeds"] != [0, 1, 2]:
        raise GigpoReleaseVerificationError("benchmark_seeds must be [0, 1, 2].")
    for field in ("gpu_hours", "grpo_f1", "gigpo_f1", "f1_delta", "parse_success_rate"):
        if not isinstance(metrics[field], int | float):
            raise GigpoReleaseVerificationError(f"{field} must be numeric.")
    if int(metrics["attempted_steps"]) < 1:
        raise GigpoReleaseVerificationError("attempted_steps must be positive.")
    if metrics["acceptance_gate_passed"] is not True:
        raise GigpoReleaseVerificationError("GiGPO acceptance gate did not pass.")
    if float(metrics["f1_delta"]) < 0.02:
        raise GigpoReleaseVerificationError("GiGPO F1 delta is below the +0.02 acceptance gate.")
    if float(metrics["gigpo_f1"]) - float(metrics["grpo_f1"]) < 0.02:
        raise GigpoReleaseVerificationError("gigpo_f1 must exceed grpo_f1 by at least 0.02.")
    if float(metrics["parse_success_rate"]) < 0.99:
        raise GigpoReleaseVerificationError("parse_success_rate must be >= 0.99.")
    if int(metrics["schema_case_error_count"]) != 0:
        raise GigpoReleaseVerificationError("schema_case_error_count must be 0.")
    if not isinstance(metrics["failure_samples"], list):
        raise GigpoReleaseVerificationError("failure_samples must be a list.")


def _validate_diagnostics(diagnostics: dict[str, Any]) -> None:
    """Validate bounded GiGPO evaluation diagnostics."""
    if diagnostics.get("schema_version") != "dataforge_gigpo_eval_diagnostics_v1":
        raise GigpoReleaseVerificationError("eval_diagnostics.json has an unknown schema_version.")
    samples = diagnostics.get("failure_samples", [])
    if not isinstance(samples, list):
        raise GigpoReleaseVerificationError("eval_diagnostics.failure_samples must be a list.")
    if len(samples) > 25:
        raise GigpoReleaseVerificationError("eval_diagnostics.failure_samples must be bounded.")


def verify_gigpo_release(
    *,
    model_repo: str = DEFAULT_MODEL_REPO,
    api: HubApi | None = None,
    downloader: DownloadFile | None = None,
    token: str | None = None,
) -> GigpoReleaseEvidence:
    """Verify a public GiGPO checkpoint before citing it in docs."""
    resolved_api: HubApi
    if api is None:
        from huggingface_hub import HfApi

        resolved_api = cast(HubApi, HfApi(token=token))
    else:
        resolved_api = api
    if downloader is None:
        from huggingface_hub import hf_hub_download

        downloader = hf_hub_download

    info = resolved_api.repo_info(model_repo, repo_type="model", token=token)
    files = _repo_files(info)
    missing = sorted(REQUIRED_MODEL_FILES - set(files))
    if missing:
        raise GigpoReleaseVerificationError(
            f"{model_repo} missing required files: {', '.join(missing)}"
        )
    readme = _download_text(model_repo, filename="README.md", token=token, downloader=downloader)
    if "DataForge" not in readme or "GiGPO" not in readme:
        raise GigpoReleaseVerificationError("README.md must identify the DataForge GiGPO release.")
    metrics = _load_json(
        model_repo,
        filename="training_metrics.json",
        token=token,
        downloader=downloader,
    )
    _validate_metrics(metrics)
    diagnostics = _load_json(
        model_repo,
        filename="eval_diagnostics.json",
        token=token,
        downloader=downloader,
    )
    _validate_diagnostics(diagnostics)
    return GigpoReleaseEvidence(
        model=GigpoRepoEvidence(repo_id=model_repo, sha=info.sha or "unknown", files=files),
        metrics=metrics,
        release_status="quality_improved_verified",
        quality_gate_checked=True,
        diagnostics_checked=True,
    )


def write_report(evidence: GigpoReleaseEvidence, output: Path) -> None:
    """Write stable JSON verification evidence."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(evidence), indent=2, sort_keys=True), encoding="utf-8")


def _print_summary(evidence: GigpoReleaseEvidence) -> None:
    """Render a compact terminal summary."""
    table = Table(title="DataForge GiGPO Release Verification")
    table.add_column("Check")
    table.add_column("Value")
    table.add_row("Model repo", evidence.model.repo_id)
    table.add_row("Model SHA", evidence.model.sha)
    table.add_row("GRPO F1", str(evidence.metrics["grpo_f1"]))
    table.add_row("GiGPO F1", str(evidence.metrics["gigpo_f1"]))
    table.add_row("F1 delta", str(evidence.metrics["f1_delta"]))
    table.add_row("GPU hours", str(evidence.metrics["gpu_hours"]))
    table.add_row("Release status", evidence.release_status)
    Console().print(table)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-repo", default=DEFAULT_MODEL_REPO)
    parser.add_argument("--output", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the GiGPO release verifier CLI."""
    load_dotenv()
    args = _build_parser().parse_args(argv)
    token = (os.environ.get("HF_TOKEN") or "").strip() or None
    try:
        evidence = verify_gigpo_release(model_repo=args.model_repo, token=token)
    except GigpoReleaseVerificationError as exc:
        print(f"GiGPO release verification failed: {exc}", file=sys.stderr)
        return 2
    _print_summary(evidence)
    if args.output is not None:
        write_report(evidence, args.output)
        Console().print(f"Wrote verification report to {args.output}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
