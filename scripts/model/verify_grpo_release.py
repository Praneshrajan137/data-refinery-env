"""Verify a gated DataForge GRPO model release on Hugging Face."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.release.model_family import (  # noqa: E402
    expected_license_for_repo,
    expected_predecessor_for_repo,
    license_matches,
)

DEFAULT_MODEL_REPO = "Praneshrajan15/DataForge-0.5B-GRPO"
REQUIRED_MODEL_FILES = frozenset(
    {
        "README.md",
        "config.json",
        "model.safetensors",
        "tokenizer.json",
        "tokenizer_config.json",
        "training_metrics.json",
        "eval_diagnostics.json",
        "eval_task_manifest.json",
    }
)
REQUIRED_METRIC_FIELDS = frozenset(
    {
        "model_name",
        "model_license",
        "base_model",
        "sft_model",
        "dataset_repo",
        "dataset_sha",
        "source_git_commit",
        "benchmark_name",
        "benchmark_seeds",
        "gpu_hours",
        "attempted_steps",
        "sft_f1",
        "grpo_f1",
        "f1_delta",
        "parse_success_rate",
        "schema_case_error_count",
        "failure_samples",
        "acceptance_gate_passed",
        "training_stage",
        "smoke_report_sha256",
        "eval_task_manifest_sha256",
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


class GrpoReleaseVerificationError(RuntimeError):
    """Raised when a GRPO release is incomplete or below the public gate."""


@dataclass(frozen=True, slots=True)
class GrpoRepoEvidence:
    """Verified file evidence for a model repository."""

    repo_id: str
    sha: str
    files: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GrpoReleaseEvidence:
    """Serializable GRPO release verification report."""

    model: GrpoRepoEvidence
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
        raise GrpoReleaseVerificationError(f"{repo_id}/{filename} must be a JSON object.")
    return cast(dict[str, Any], payload)


def _validate_metrics(
    metrics: dict[str, Any],
    *,
    model_repo: str,
    expected_model_license: str,
    expected_sft_model: str | None,
) -> None:
    """Validate GRPO release metrics and public gate evidence."""
    missing = sorted(REQUIRED_METRIC_FIELDS - set(metrics))
    if missing:
        raise GrpoReleaseVerificationError(
            "training_metrics.json missing required fields: " + ", ".join(missing)
        )
    if not license_matches(metrics["model_license"], expected_model_license):
        raise GrpoReleaseVerificationError(
            f"model_license must be {expected_model_license} for {model_repo}."
        )
    if expected_sft_model is not None and metrics["sft_model"] != expected_sft_model:
        raise GrpoReleaseVerificationError(
            f"sft_model must point to verified predecessor {expected_sft_model}."
        )
    if metrics["benchmark_name"] != "DataForge-Bench-light-verified":
        raise GrpoReleaseVerificationError("benchmark_name must be DataForge-Bench-light-verified.")
    if metrics["benchmark_seeds"] != [0, 1, 2]:
        raise GrpoReleaseVerificationError("benchmark_seeds must be [0, 1, 2].")
    for field in ("gpu_hours", "sft_f1", "grpo_f1", "f1_delta", "parse_success_rate"):
        if not isinstance(metrics[field], int | float):
            raise GrpoReleaseVerificationError(f"{field} must be numeric.")
    if int(metrics["attempted_steps"]) < 1:
        raise GrpoReleaseVerificationError("attempted_steps must be positive.")
    if metrics["acceptance_gate_passed"] is not True:
        raise GrpoReleaseVerificationError("GRPO acceptance gate did not pass.")
    if metrics["training_stage"] != "candidate":
        raise GrpoReleaseVerificationError("training_stage must be candidate.")
    if int(metrics["attempted_steps"]) != 500:
        raise GrpoReleaseVerificationError("0.5B GRPO candidate must record attempted_steps=500.")
    for digest_field in ("smoke_report_sha256", "eval_task_manifest_sha256"):
        value = metrics.get(digest_field)
        if not isinstance(value, str) or len(value) != 64:
            raise GrpoReleaseVerificationError(f"{digest_field} must be a SHA-256 digest.")
    if float(metrics["f1_delta"]) < 0.03:
        raise GrpoReleaseVerificationError("GRPO F1 delta is below the +0.03 acceptance gate.")
    if float(metrics["grpo_f1"]) - float(metrics["sft_f1"]) < 0.03:
        raise GrpoReleaseVerificationError("grpo_f1 must exceed sft_f1 by at least 0.03.")
    if float(metrics["parse_success_rate"]) < 0.99:
        raise GrpoReleaseVerificationError("parse_success_rate must be >= 0.99.")
    if int(metrics["schema_case_error_count"]) != 0:
        raise GrpoReleaseVerificationError("schema_case_error_count must be 0.")
    if not isinstance(metrics["failure_samples"], list):
        raise GrpoReleaseVerificationError("failure_samples must be a list.")
    if len(metrics["failure_samples"]) > 25:
        raise GrpoReleaseVerificationError("failure_samples must be bounded.")


def _validate_diagnostics(diagnostics: dict[str, Any]) -> None:
    """Validate bounded GRPO evaluation diagnostics."""
    if diagnostics.get("schema_version") != "dataforge_grpo_eval_diagnostics_v1":
        raise GrpoReleaseVerificationError("eval_diagnostics.json has an unknown schema_version.")
    for section in ("sft_eval", "grpo_eval"):
        value = diagnostics.get(section)
        if not isinstance(value, dict):
            raise GrpoReleaseVerificationError(f"eval_diagnostics.json must include {section}.")
        if not isinstance(value.get("dataset_f1"), dict):
            raise GrpoReleaseVerificationError(f"{section}.dataset_f1 must be present.")
    samples = diagnostics.get("failure_samples", [])
    if not isinstance(samples, list):
        raise GrpoReleaseVerificationError("eval_diagnostics.failure_samples must be a list.")
    if len(samples) > 25:
        raise GrpoReleaseVerificationError("eval_diagnostics.failure_samples must be bounded.")


def _validate_task_manifest(manifest: dict[str, Any]) -> None:
    """Validate the public held-out eval task manifest without hidden labels."""
    if manifest.get("schema_version") != "dataforge_grpo_eval_task_manifest_v1":
        raise GrpoReleaseVerificationError("eval_task_manifest.json has an unknown schema_version.")
    if manifest.get("benchmark_name") != "DataForge-Bench-light-verified":
        raise GrpoReleaseVerificationError("eval_task_manifest benchmark_name is wrong.")
    if manifest.get("benchmark_seeds") != [0, 1, 2]:
        raise GrpoReleaseVerificationError("eval_task_manifest benchmark_seeds must be [0, 1, 2].")
    source_audit = manifest.get("source_audit")
    if not isinstance(source_audit, dict) or source_audit.get("ok") is not True:
        raise GrpoReleaseVerificationError("eval_task_manifest source_audit must pass.")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise GrpoReleaseVerificationError("eval_task_manifest must include held-out tasks.")
    if int(manifest.get("heldout_tasks", 0)) != len(tasks):
        raise GrpoReleaseVerificationError("eval_task_manifest heldout_tasks mismatch.")
    for task in tasks:
        if not isinstance(task, dict):
            raise GrpoReleaseVerificationError("eval_task_manifest tasks must be objects.")
        missing = {
            "task_id",
            "dataset",
            "prompt_hash",
            "allowed_columns",
            "valid_rows",
            "truth_cell_count",
            "truth_hash",
            "source",
        } - set(task)
        if missing:
            raise GrpoReleaseVerificationError(
                "eval_task_manifest task missing fields: " + ", ".join(sorted(missing))
            )
        if "ground_truth" in task or "hidden_ground_truth" in task:
            raise GrpoReleaseVerificationError("eval_task_manifest must not expose hidden labels.")
        if not isinstance(task["prompt_hash"], str) or len(task["prompt_hash"]) != 64:
            raise GrpoReleaseVerificationError("eval_task_manifest prompt_hash must be SHA-256.")
        if not isinstance(task["truth_hash"], str) or len(task["truth_hash"]) != 64:
            raise GrpoReleaseVerificationError("eval_task_manifest truth_hash must be SHA-256.")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_local_grpo_artifact_dir(
    model_dir: Path,
    *,
    model_repo: str = DEFAULT_MODEL_REPO,
    expected_model_license: str | None = None,
    expected_sft_model: str | None = None,
) -> GrpoReleaseEvidence:
    """Verify merged GRPO artifacts before a Kaggle upload touches the Hub."""
    files = tuple(
        sorted(
            path.relative_to(model_dir).as_posix()
            for path in model_dir.rglob("*")
            if path.is_file()
        )
    )
    missing = sorted(REQUIRED_MODEL_FILES - set(files))
    if missing:
        raise GrpoReleaseVerificationError(
            f"{model_dir} missing required files: {', '.join(missing)}"
        )
    readme = (model_dir / "README.md").read_text(encoding="utf-8")
    if "DataForge" not in readme or "GRPO" not in readme:
        raise GrpoReleaseVerificationError("README.md must identify the DataForge GRPO release.")
    metrics = json.loads((model_dir / "training_metrics.json").read_text(encoding="utf-8"))
    diagnostics = json.loads((model_dir / "eval_diagnostics.json").read_text(encoding="utf-8"))
    manifest = json.loads((model_dir / "eval_task_manifest.json").read_text(encoding="utf-8"))
    if (
        not isinstance(metrics, dict)
        or not isinstance(diagnostics, dict)
        or not isinstance(manifest, dict)
    ):
        raise GrpoReleaseVerificationError("local GRPO artifacts must be JSON objects.")
    if metrics.get("eval_task_manifest_sha256") != _sha256_file(
        model_dir / "eval_task_manifest.json"
    ):
        raise GrpoReleaseVerificationError(
            "eval_task_manifest_sha256 does not match artifact bytes."
        )
    resolved_expected_license = expected_model_license or expected_license_for_repo(model_repo)
    resolved_expected_sft = expected_sft_model
    if resolved_expected_sft is None:
        resolved_expected_sft = expected_predecessor_for_repo(model_repo)
    _validate_metrics(
        metrics,
        model_repo=model_repo,
        expected_model_license=resolved_expected_license,
        expected_sft_model=resolved_expected_sft,
    )
    _validate_diagnostics(diagnostics)
    _validate_task_manifest(manifest)
    return GrpoReleaseEvidence(
        model=GrpoRepoEvidence(repo_id=model_repo, sha="local", files=files),
        metrics=metrics,
        release_status="quality_improved_verified",
        quality_gate_checked=True,
        diagnostics_checked=True,
    )


def verify_grpo_release(
    *,
    model_repo: str = DEFAULT_MODEL_REPO,
    api: HubApi | None = None,
    downloader: DownloadFile | None = None,
    token: str | None = None,
    expected_model_license: str | None = None,
    expected_sft_model: str | None = None,
) -> GrpoReleaseEvidence:
    """Verify a public GRPO checkpoint before citing it in docs."""
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
        raise GrpoReleaseVerificationError(
            f"{model_repo} missing required files: {', '.join(missing)}"
        )
    readme = _download_text(model_repo, filename="README.md", token=token, downloader=downloader)
    if "DataForge" not in readme or "GRPO" not in readme:
        raise GrpoReleaseVerificationError("README.md must identify the DataForge GRPO release.")
    metrics = _load_json(
        model_repo,
        filename="training_metrics.json",
        token=token,
        downloader=downloader,
    )
    resolved_expected_license = expected_model_license or expected_license_for_repo(model_repo)
    resolved_expected_sft = expected_sft_model
    if resolved_expected_sft is None:
        resolved_expected_sft = expected_predecessor_for_repo(model_repo)
    _validate_metrics(
        metrics,
        model_repo=model_repo,
        expected_model_license=resolved_expected_license,
        expected_sft_model=resolved_expected_sft,
    )
    diagnostics = _load_json(
        model_repo,
        filename="eval_diagnostics.json",
        token=token,
        downloader=downloader,
    )
    _validate_diagnostics(diagnostics)
    manifest = _load_json(
        model_repo,
        filename="eval_task_manifest.json",
        token=token,
        downloader=downloader,
    )
    _validate_task_manifest(manifest)
    return GrpoReleaseEvidence(
        model=GrpoRepoEvidence(repo_id=model_repo, sha=info.sha or "unknown", files=files),
        metrics=metrics,
        release_status="quality_improved_verified",
        quality_gate_checked=True,
        diagnostics_checked=True,
    )


def write_report(evidence: GrpoReleaseEvidence, output: Path) -> None:
    """Write stable JSON verification evidence."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(evidence), indent=2, sort_keys=True), encoding="utf-8")


def _print_summary(evidence: GrpoReleaseEvidence) -> None:
    """Render a compact terminal summary."""
    table = Table(title="DataForge GRPO Release Verification")
    table.add_column("Check")
    table.add_column("Value")
    table.add_row("Model repo", evidence.model.repo_id)
    table.add_row("Model SHA", evidence.model.sha)
    table.add_row("SFT F1", str(evidence.metrics["sft_f1"]))
    table.add_row("GRPO F1", str(evidence.metrics["grpo_f1"]))
    table.add_row("F1 delta", str(evidence.metrics["f1_delta"]))
    table.add_row("GPU hours", str(evidence.metrics["gpu_hours"]))
    table.add_row("Release status", evidence.release_status)
    Console().print(table)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-repo", default=DEFAULT_MODEL_REPO)
    parser.add_argument("--local-dir", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the GRPO release verifier CLI."""
    load_dotenv()
    args = _build_parser().parse_args(argv)
    token = (os.environ.get("HF_TOKEN") or "").strip() or None
    try:
        if args.local_dir is not None:
            evidence = verify_local_grpo_artifact_dir(args.local_dir, model_repo=args.model_repo)
        else:
            evidence = verify_grpo_release(model_repo=args.model_repo, token=token)
    except GrpoReleaseVerificationError as exc:
        print(f"GRPO release verification failed: {exc}", file=sys.stderr)
        return 2
    _print_summary(evidence)
    if args.output is not None:
        write_report(evidence, args.output)
        Console().print(f"Wrote verification report to {args.output}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
