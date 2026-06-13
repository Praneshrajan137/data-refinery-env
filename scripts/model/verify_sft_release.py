"""Verify the published DataForge SFT model and trajectory dataset on Hugging Face."""

from __future__ import annotations

import argparse
import json
import os
import re
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

from dataforge.release.model_family import expected_license_for_repo, license_matches  # noqa: E402

DEFAULT_MODEL_REPO = "Praneshrajan15/DataForge-0.5B-SFT"
DEFAULT_DATASET_REPO = "Praneshrajan15/dataforge-sft-trajectories"
DEFAULT_MIN_DATASET_RECORDS = 32

REQUIRED_MODEL_FILES = frozenset(
    {
        "README.md",
        "config.json",
        "model.safetensors",
        "tokenizer.json",
        "tokenizer_config.json",
        "training_metrics.json",
    }
)
REQUIRED_DATASET_FILES = frozenset(
    {
        "README.md",
        "expert_v1.jsonl",
        "split_manifest.json",
        "sft_05b.yaml",
        "MODEL_CARD_TEMPLATE.md",
    }
)
REQUIRED_DATASET_FILES_V2 = frozenset(
    {
        "README.md",
        "expert_v2.jsonl",
        "split_manifest_v2.json",
        "sft_05b_v2.yaml",
        "MODEL_CARD_TEMPLATE.md",
    }
)
REQUIRED_DATASET_FILES_V3 = frozenset(
    {
        "README.md",
        "expert_v3.jsonl",
        "split_manifest_v3.json",
        "sft_05b_v3.yaml",
        "MODEL_CARD_TEMPLATE.md",
    }
)
REQUIRED_DATASET_FILES_V4 = frozenset(
    {
        "README.md",
        "expert_v4.jsonl",
        "split_manifest_v4.json",
        "sft_05b_v4.yaml",
        "MODEL_CARD_TEMPLATE.md",
    }
)
REQUIRED_METRIC_FIELDS = frozenset(
    {
        "model_name",
        "model_license",
        "base_model",
        "teacher_model",
        "dataset_repo",
        "training_examples",
        "kaggle_hours",
        "base_f1",
        "sft_f1",
        "repo_id",
    }
)
PLACEHOLDER_RE = re.compile(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}|<you>|TBD|pending", re.IGNORECASE)


class HubSibling(Protocol):
    """Minimal Hugging Face sibling shape used by the verifier."""

    rfilename: str


class HubRepoInfo(Protocol):
    """Minimal Hugging Face repo-info shape used by the verifier."""

    siblings: list[HubSibling]
    sha: str | None


class HubApi(Protocol):
    """Protocol for the subset of HfApi used by this verifier."""

    def repo_info(
        self,
        repo_id: str,
        *,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> HubRepoInfo:
        """Return repository metadata."""


class DownloadFile(Protocol):
    """Callable shape for downloading one file from a Hub repo."""

    def __call__(
        self,
        repo_id: str,
        *,
        filename: str,
        repo_type: str | None = None,
        revision: str | None = None,
        token: str | None = None,
    ) -> str:
        """Download a repo file and return a local path."""


class ReleaseVerificationError(RuntimeError):
    """Raised when a published SFT release is incomplete or dishonest."""


@dataclass(frozen=True, slots=True)
class RepoEvidence:
    """Verified evidence for one Hub repository."""

    repo_id: str
    repo_type: str
    sha: str
    files: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReleaseEvidence:
    """Serializable verification report for a DataForge SFT release."""

    model: RepoEvidence
    dataset: RepoEvidence
    metrics: dict[str, Any]
    dataset_records: int
    quality_milestone: bool
    release_status: str
    quality_gate_failures: tuple[str, ...]
    dataset_sha_metric_checked: bool
    eval_diagnostics_checked: bool
    model_readme_checked: bool
    dataset_readme_checked: bool
    trajectory_filename: str = "expert_v1.jsonl"
    split_manifest_filename: str = "split_manifest.json"


def _repo_files(info: HubRepoInfo) -> tuple[str, ...]:
    """Return sorted repository file paths from Hugging Face metadata."""
    return tuple(sorted(sibling.rfilename for sibling in info.siblings))


def _missing(required: frozenset[str], files: tuple[str, ...]) -> list[str]:
    """Return required files missing from a repository file manifest."""
    file_set = set(files)
    return sorted(required - file_set)


def _dataset_contract_files(
    files: tuple[str, ...],
    *,
    trajectory_filename: str | None = None,
) -> tuple[frozenset[str], str, str]:
    """Return the dataset artifact contract to verify."""
    contracts_by_trajectory = {
        "expert_v1.jsonl": (REQUIRED_DATASET_FILES, "expert_v1.jsonl", "split_manifest.json"),
        "expert_v2.jsonl": (
            REQUIRED_DATASET_FILES_V2,
            "expert_v2.jsonl",
            "split_manifest_v2.json",
        ),
        "expert_v3.jsonl": (
            REQUIRED_DATASET_FILES_V3,
            "expert_v3.jsonl",
            "split_manifest_v3.json",
        ),
        "expert_v4.jsonl": (
            REQUIRED_DATASET_FILES_V4,
            "expert_v4.jsonl",
            "split_manifest_v4.json",
        ),
    }
    if trajectory_filename:
        try:
            return contracts_by_trajectory[trajectory_filename]
        except KeyError as exc:
            raise ReleaseVerificationError(
                f"Unsupported trajectory filename in training metrics: {trajectory_filename!r}."
            ) from exc
    file_set = set(files)
    if REQUIRED_DATASET_FILES_V4.issubset(file_set):
        return REQUIRED_DATASET_FILES_V4, "expert_v4.jsonl", "split_manifest_v4.json"
    if REQUIRED_DATASET_FILES_V3.issubset(file_set):
        return REQUIRED_DATASET_FILES_V3, "expert_v3.jsonl", "split_manifest_v3.json"
    if REQUIRED_DATASET_FILES_V2.issubset(file_set):
        return REQUIRED_DATASET_FILES_V2, "expert_v2.jsonl", "split_manifest_v2.json"
    return REQUIRED_DATASET_FILES, "expert_v1.jsonl", "split_manifest.json"


def _trajectory_filename_from_metrics(
    metrics: dict[str, Any], dataset_files: tuple[str, ...]
) -> str | None:
    """Return the trajectory artifact that belongs to the model metrics, when known."""
    filename = metrics.get("trajectory_filename")
    if isinstance(filename, str) and filename:
        return filename
    if "release_status" in metrics and REQUIRED_DATASET_FILES_V3.issubset(set(dataset_files)):
        return "expert_v3.jsonl"
    return None


def _download_text(
    repo_id: str,
    *,
    filename: str,
    repo_type: str,
    token: str | None,
    downloader: DownloadFile,
    revision: str | None = None,
) -> str:
    """Download and read one UTF-8 Hub file."""
    path = Path(
        downloader(
            repo_id,
            filename=filename,
            repo_type=repo_type,
            revision=revision,
            token=token,
        )
    )
    return path.read_text(encoding="utf-8")


def _load_json(
    repo_id: str,
    *,
    filename: str,
    repo_type: str,
    token: str | None,
    downloader: DownloadFile,
) -> dict[str, Any]:
    """Download and parse a JSON object."""
    payload = json.loads(
        _download_text(
            repo_id,
            filename=filename,
            repo_type=repo_type,
            token=token,
            downloader=downloader,
        )
    )
    if not isinstance(payload, dict):
        raise ReleaseVerificationError(f"{repo_id}/{filename} must contain a JSON object.")
    return payload


def _assert_no_placeholders(text: str, *, repo_id: str, filename: str) -> None:
    """Reject model or dataset cards with obvious unresolved placeholders."""
    match = PLACEHOLDER_RE.search(text)
    if match:
        raise ReleaseVerificationError(
            f"{repo_id}/{filename} contains unresolved placeholder text: {match.group(0)!r}"
        )


def _assert_split_manifest_contract(text: str, *, repo_id: str) -> None:
    """Reject split manifests that are missing or leak label-bearing fields."""
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ReleaseVerificationError(f"{repo_id}/split_manifest.json must be a JSON object.")
    if payload.get("schema_version") != "split_manifest_v1":
        raise ReleaseVerificationError(
            f"{repo_id}/split_manifest.json schema_version must be split_manifest_v1."
        )
    forbidden = {
        "clean",
        "clean_value",
        "ground_truth",
        "new_value",
        "normalization_candidates",
        "repairs",
        "suggested_value",
    }

    def walk(value: object, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                key_text = str(key)
                if key_text in forbidden:
                    raise ReleaseVerificationError(
                        f"{repo_id}/split_manifest.json leaks label-bearing field {path}.{key_text}."
                    )
                walk(child, f"{path}.{key_text}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")

    walk(payload, "manifest")


def _validate_metrics(
    metrics: dict[str, Any],
    *,
    model_repo: str,
    dataset_repo: str,
    expected_model_license: str,
) -> None:
    """Validate model-card metrics and repo linkage."""
    missing = sorted(REQUIRED_METRIC_FIELDS - set(metrics))
    if missing:
        raise ReleaseVerificationError(
            "training_metrics.json missing required fields: " + ", ".join(missing)
        )
    if metrics["repo_id"] != model_repo:
        raise ReleaseVerificationError(
            f"training_metrics repo_id={metrics['repo_id']!r} does not match {model_repo!r}."
        )
    metric_dataset_repo = str(metrics["dataset_repo"])
    if metric_dataset_repo != dataset_repo and not metric_dataset_repo.startswith("kaggle://"):
        raise ReleaseVerificationError(
            "training_metrics dataset_repo="
            f"{metrics['dataset_repo']!r} does not match {dataset_repo!r}."
        )
    if not license_matches(metrics["model_license"], expected_model_license):
        raise ReleaseVerificationError(
            f"model_license must be {expected_model_license} for {model_repo}."
        )
    for field in ("training_examples", "kaggle_hours", "base_f1", "sft_f1"):
        if not isinstance(metrics[field], int | float):
            raise ReleaseVerificationError(f"training_metrics field {field} must be numeric.")
    for field in ("base_f1", "sft_f1"):
        value = float(metrics[field])
        if value < 0.0 or value > 1.0:
            raise ReleaseVerificationError(f"{field} must be in [0, 1], got {value}.")


def _quality_gate_failures(metrics: dict[str, Any]) -> list[str]:
    """Return missing or failed quality gates for an F1-improved release."""
    failures: list[str] = []
    base_f1 = float(metrics["base_f1"])
    sft_f1 = float(metrics["sft_f1"])
    if sft_f1 <= base_f1:
        failures.append("sft_f1>base_f1")
    parse_success = metrics.get("parse_success_rate")
    if not isinstance(parse_success, int | float) or float(parse_success) < 0.99:
        failures.append("parse_success_rate>=0.99")
    schema_case_errors = metrics.get("schema_case_error_count")
    if not isinstance(schema_case_errors, int | float) or int(schema_case_errors) != 0:
        failures.append("schema_case_error_count==0")
    if metrics.get("prompt_contract_drift") not in (False, 0):
        failures.append("prompt_contract_drift==false")
    if metrics.get("heldout_leakage_detected") not in (False, 0):
        failures.append("heldout_leakage_detected==false")
    return failures


def _validate_dataset_sha_metric(
    metrics: dict[str, Any],
    *,
    api: HubApi,
    dataset_repo: str,
    dataset_sha: str,
    required_files: frozenset[str],
    token: str | None,
    require_sha_metrics: bool,
) -> bool:
    """Check optional training-time dataset SHA linkage.

    The model records the dataset revision used at training time. The dataset
    repository may later receive report-only commits, so the recorded SHA need
    not remain the dataset HEAD. Require that it is a real Hub revision with the
    expected training handoff files.
    """
    recorded_sha = metrics.get("dataset_sha")
    if recorded_sha is None:
        if require_sha_metrics:
            raise ReleaseVerificationError(
                "training_metrics.json must include dataset_sha for a contract-v2 release."
            )
        return False
    if str(metrics.get("dataset_repo", "")).startswith("kaggle://"):
        if require_sha_metrics:
            raise ReleaseVerificationError(
                "training_metrics dataset_sha cannot be checked as an HF revision "
                "because dataset_repo points to an archived Kaggle handoff."
            )
        return False
    recorded_text = str(recorded_sha)
    if recorded_text == dataset_sha:
        return True
    try:
        training_info = api.repo_info(
            dataset_repo,
            repo_type="dataset",
            revision=recorded_text,
            token=token,
        )
    except Exception as exc:
        raise ReleaseVerificationError(
            f"training_metrics dataset_sha={recorded_sha!r} is not a resolvable dataset revision."
        ) from exc
    missing = _missing(required_files, _repo_files(training_info))
    if missing:
        raise ReleaseVerificationError(
            f"training-time dataset revision {recorded_text!r} is missing required files: "
            + ", ".join(missing)
        )
    return True


def _release_status(metrics: dict[str, Any]) -> tuple[bool, str, tuple[str, ...]]:
    """Classify the release without overstating model quality."""
    failures = _quality_gate_failures(metrics)
    if not failures:
        return True, "quality_improved_verified", ()
    if "sft_f1>base_f1" in failures:
        return False, "diagnostic_complete_no_gain", tuple(failures)
    return False, "quality_gate_failed", tuple(failures)


def _validate_eval_diagnostics(
    model_repo: str,
    *,
    files: tuple[str, ...],
    token: str | None,
    downloader: DownloadFile,
) -> bool:
    """Validate per-task diagnostics for a contract-v2 release."""
    if "eval_diagnostics.json" not in files:
        raise ReleaseVerificationError(f"{model_repo} missing required file: eval_diagnostics.json")
    diagnostics = _load_json(
        model_repo,
        filename="eval_diagnostics.json",
        repo_type="model",
        token=token,
        downloader=downloader,
    )
    if diagnostics.get("schema_version") != "dataforge_eval_diagnostics_v1":
        raise ReleaseVerificationError("eval_diagnostics.json has an unknown schema_version.")
    for section in ("base", "sft"):
        payload = diagnostics.get(section)
        if not isinstance(payload, dict) or not isinstance(payload.get("task_scores"), list):
            raise ReleaseVerificationError(
                f"eval_diagnostics.json must include {section}.task_scores."
            )
    return True


def _count_jsonl_records(
    repo_id: str,
    *,
    filename: str,
    token: str | None,
    downloader: DownloadFile,
) -> int:
    """Count non-empty trajectory JSONL rows."""
    text = _download_text(
        repo_id,
        filename=filename,
        repo_type="dataset",
        token=token,
        downloader=downloader,
    )
    records = 0
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ReleaseVerificationError(
                f"{repo_id}/{filename}:{line_number} must contain a JSON object."
            )
        records += 1
    return records


def verify_sft_release(
    *,
    model_repo: str = DEFAULT_MODEL_REPO,
    dataset_repo: str = DEFAULT_DATASET_REPO,
    min_dataset_records: int = DEFAULT_MIN_DATASET_RECORDS,
    api: HubApi | None = None,
    downloader: DownloadFile | None = None,
    token: str | None = None,
    require_sha_metrics: bool = False,
    require_eval_diagnostics: bool = False,
    require_quality_improvement: bool = False,
    expected_model_license: str | None = None,
) -> ReleaseEvidence:
    """Verify model and dataset release artifacts on Hugging Face."""
    resolved_api: HubApi
    if api is None:
        from huggingface_hub import HfApi

        resolved_api = cast(HubApi, HfApi(token=token))
    else:
        resolved_api = api
    if downloader is None:
        from huggingface_hub import hf_hub_download

        downloader = hf_hub_download

    model_info = resolved_api.repo_info(model_repo, repo_type="model", token=token)
    dataset_info = resolved_api.repo_info(dataset_repo, repo_type="dataset", token=token)
    model_files = _repo_files(model_info)
    dataset_files = _repo_files(dataset_info)

    missing_model = _missing(REQUIRED_MODEL_FILES, model_files)
    if missing_model:
        raise ReleaseVerificationError(
            f"{model_repo} missing required files: {', '.join(missing_model)}"
        )
    metrics = _load_json(
        model_repo,
        filename="training_metrics.json",
        repo_type="model",
        token=token,
        downloader=downloader,
    )
    resolved_expected_license = expected_model_license or expected_license_for_repo(model_repo)
    _validate_metrics(
        metrics,
        model_repo=model_repo,
        dataset_repo=dataset_repo,
        expected_model_license=resolved_expected_license,
    )
    requested_trajectory = _trajectory_filename_from_metrics(metrics, dataset_files)
    required_dataset_files, trajectory_filename, split_manifest_filename = _dataset_contract_files(
        dataset_files,
        trajectory_filename=requested_trajectory,
    )
    missing_dataset = _missing(required_dataset_files, dataset_files)
    if missing_dataset:
        raise ReleaseVerificationError(
            f"{dataset_repo} missing required files: {', '.join(missing_dataset)}"
        )
    dataset_sha_metric_checked = _validate_dataset_sha_metric(
        metrics,
        api=resolved_api,
        dataset_repo=dataset_repo,
        dataset_sha=dataset_info.sha or "unknown",
        required_files=required_dataset_files,
        token=token,
        require_sha_metrics=require_sha_metrics,
    )
    quality_milestone, release_status, gate_failures = _release_status(metrics)
    if require_quality_improvement and not quality_milestone:
        raise ReleaseVerificationError(
            "quality improvement gate failed: " + ", ".join(gate_failures or ("unknown",))
        )
    eval_diagnostics_checked = False
    if require_eval_diagnostics:
        eval_diagnostics_checked = _validate_eval_diagnostics(
            model_repo,
            files=model_files,
            token=token,
            downloader=downloader,
        )

    model_readme = _download_text(
        model_repo,
        filename="README.md",
        repo_type="model",
        token=token,
        downloader=downloader,
    )
    dataset_readme = _download_text(
        dataset_repo,
        filename="README.md",
        repo_type="dataset",
        token=token,
        downloader=downloader,
    )
    split_manifest_text = _download_text(
        dataset_repo,
        filename=split_manifest_filename,
        repo_type="dataset",
        token=token,
        downloader=downloader,
    )
    _assert_no_placeholders(model_readme, repo_id=model_repo, filename="README.md")
    _assert_no_placeholders(dataset_readme, repo_id=dataset_repo, filename="README.md")
    _assert_split_manifest_contract(split_manifest_text, repo_id=dataset_repo)

    dataset_records = _count_jsonl_records(
        dataset_repo,
        filename=trajectory_filename,
        token=token,
        downloader=downloader,
    )
    if dataset_records < min_dataset_records:
        raise ReleaseVerificationError(
            f"{dataset_repo}/{trajectory_filename} has {dataset_records} records; "
            f"need at least {min_dataset_records}."
        )
    if int(metrics["training_examples"]) > dataset_records:
        raise ReleaseVerificationError(
            "training_examples cannot exceed available dataset records "
            f"({metrics['training_examples']}>{dataset_records})."
        )

    return ReleaseEvidence(
        model=RepoEvidence(
            repo_id=model_repo,
            repo_type="model",
            sha=model_info.sha or "unknown",
            files=model_files,
        ),
        dataset=RepoEvidence(
            repo_id=dataset_repo,
            repo_type="dataset",
            sha=dataset_info.sha or "unknown",
            files=dataset_files,
        ),
        metrics=metrics,
        dataset_records=dataset_records,
        quality_milestone=quality_milestone,
        release_status=release_status,
        quality_gate_failures=gate_failures,
        dataset_sha_metric_checked=dataset_sha_metric_checked,
        eval_diagnostics_checked=eval_diagnostics_checked,
        model_readme_checked=True,
        dataset_readme_checked=True,
        trajectory_filename=trajectory_filename,
        split_manifest_filename=split_manifest_filename,
    )


def write_report(evidence: ReleaseEvidence, output: Path) -> None:
    """Write release verification evidence as stable JSON."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(evidence), indent=2, sort_keys=True), encoding="utf-8")


def _print_summary(evidence: ReleaseEvidence) -> None:
    """Render a compact terminal summary."""
    table = Table(title="DataForge SFT Release Verification")
    table.add_column("Check")
    table.add_column("Value")
    table.add_row("Model repo", evidence.model.repo_id)
    table.add_row("Model SHA", evidence.model.sha)
    table.add_row("Dataset repo", evidence.dataset.repo_id)
    table.add_row("Dataset SHA", evidence.dataset.sha)
    table.add_row("Dataset records", str(evidence.dataset_records))
    table.add_row("Training examples", str(evidence.metrics["training_examples"]))
    table.add_row("Base F1", str(evidence.metrics["base_f1"]))
    table.add_row("SFT F1", str(evidence.metrics["sft_f1"]))
    table.add_row("Release status", evidence.release_status)
    table.add_row("Gate failures", ", ".join(evidence.quality_gate_failures) or "none")
    Console().print(table)


def _build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-repo", default=DEFAULT_MODEL_REPO)
    parser.add_argument("--dataset-repo", default=DEFAULT_DATASET_REPO)
    parser.add_argument("--min-dataset-records", type=int, default=DEFAULT_MIN_DATASET_RECORDS)
    parser.add_argument("--require-sha-metrics", action="store_true")
    parser.add_argument("--require-eval-diagnostics", action="store_true")
    parser.add_argument("--require-quality-improvement", action="store_true")
    parser.add_argument("--output", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the release verifier CLI."""
    load_dotenv()
    args = _build_parser().parse_args(argv)
    token = (os.environ.get("HF_TOKEN") or "").strip() or None
    if token is None:
        try:
            from huggingface_hub import get_token

            token = get_token()
        except ImportError:
            token = None
    try:
        evidence = verify_sft_release(
            model_repo=args.model_repo,
            dataset_repo=args.dataset_repo,
            min_dataset_records=args.min_dataset_records,
            token=token,
            require_sha_metrics=args.require_sha_metrics,
            require_eval_diagnostics=args.require_eval_diagnostics,
            require_quality_improvement=args.require_quality_improvement,
        )
    except ReleaseVerificationError as exc:
        print(f"SFT release verification failed: {exc}", file=sys.stderr)
        return 2
    _print_summary(evidence)
    if args.output is not None:
        write_report(evidence, args.output)
        Console().print(f"Wrote verification report to {args.output}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
