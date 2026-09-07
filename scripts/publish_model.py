"""Publish merged Week 9 SFT weights and a completed model card to Hugging Face."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from string import Formatter
from typing import Any, Protocol

from rich.console import Console

DEFAULT_MODEL_REPO_NAME = "DataForge-0.5B-SFT"
REQUIRED_METRIC_FIELDS = {
    "model_name",
    "model_license",
    "base_model",
    "teacher_model",
    "dataset_repo",
    "training_examples",
    "kaggle_hours",
    "base_f1",
    "sft_f1",
}


class HfModelApi(Protocol):
    """Protocol for the subset of HfApi used by the publisher."""

    def whoami(self, token: str | None = None) -> dict[str, Any]:
        """Return authentication metadata for the current HF token."""

    def create_repo(
        self,
        *,
        repo_id: str,
        repo_type: str,
        exist_ok: bool,
        token: str | None = None,
    ) -> object:
        """Create a model repository if needed."""

    def upload_folder(
        self,
        *,
        folder_path: str,
        repo_id: str,
        repo_type: str,
        token: str | None = None,
        commit_message: str,
    ) -> object:
        """Upload a local folder to a model repository."""


class ModelCardError(ValueError):
    """Raised when a model card cannot be rendered safely."""


def resolve_repo_id(repo_id: str, *, api: HfModelApi, token: str | None) -> str:
    """Resolve `auto` to `<hf_user>/DataForge-0.5B-SFT`."""
    if repo_id != "auto":
        return repo_id
    whoami = api.whoami(token=token)
    name = whoami.get("name")
    if not isinstance(name, str) or not name:
        raise ModelCardError("Could not resolve Hugging Face username from HF_TOKEN.")
    return f"{name}/{DEFAULT_MODEL_REPO_NAME}"


def _template_fields(template_text: str) -> set[str]:
    """Return named format fields used by a model-card template."""
    fields: set[str] = set()
    for _, field_name, _, _ in Formatter().parse(template_text):
        if field_name:
            fields.add(field_name)
    return fields


def render_model_card(template_text: str, metrics: dict[str, Any]) -> str:
    """Render a model-card template, refusing unfilled placeholders."""
    missing = sorted(_template_fields(template_text) - set(metrics))
    if missing:
        raise ModelCardError(f"Missing model-card fields: {', '.join(missing)}")
    return template_text.format(**metrics)


def _load_metrics(model_dir: Path) -> dict[str, Any]:
    """Load required training metrics from a merged model directory."""
    metrics_path = model_dir / "training_metrics.json"
    if not metrics_path.exists():
        raise ModelCardError(f"Missing required metrics file: {metrics_path}")
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ModelCardError("training_metrics.json must contain a JSON object.")
    return payload


def _validate_required_metrics(metrics: dict[str, Any]) -> None:
    """Ensure the model card has enough evidence to be publishable."""
    missing = sorted(REQUIRED_METRIC_FIELDS - set(metrics))
    if missing:
        raise ModelCardError(f"Missing required training metrics: {', '.join(missing)}")
    license_value = str(metrics["model_license"]).lower()
    if license_value != "apache-2.0":
        raise ModelCardError("model_license must be apache-2.0 after verifying base metadata.")


def _metrics_with_card_defaults(metrics: dict[str, Any], *, repo_id: str) -> dict[str, Any]:
    """Add model-card fields that older diagnostic metrics did not record."""
    enriched = dict(metrics)
    enriched.setdefault("repo_id", repo_id)
    enriched.setdefault("trajectory_filename", "expert_v3.jsonl")
    return enriched


def publish_model(
    *,
    model_dir: Path,
    card_template: Path,
    repo_id: str,
    api: HfModelApi | None = None,
    token: str | None = None,
) -> str:
    """Render README.md, create the HF model repo, and upload merged weights."""
    if api is None:
        from huggingface_hub import HfApi

        api = HfApi(token=token)

    if not model_dir.exists() or not model_dir.is_dir():
        raise FileNotFoundError(f"Merged model directory does not exist: {model_dir}")
    if not card_template.exists():
        raise FileNotFoundError(f"Model-card template does not exist: {card_template}")

    metrics = _load_metrics(model_dir)
    _validate_required_metrics(metrics)
    resolved_repo_id = resolve_repo_id(repo_id, api=api, token=token)
    metrics = _metrics_with_card_defaults(metrics, repo_id=resolved_repo_id)
    card_text = render_model_card(card_template.read_text(encoding="utf-8"), metrics)
    (model_dir / "README.md").write_text(card_text, encoding="utf-8")

    api.create_repo(
        repo_id=resolved_repo_id,
        repo_type="model",
        exist_ok=True,
        token=token,
    )
    api.upload_folder(
        folder_path=str(model_dir),
        repo_id=resolved_repo_id,
        repo_type="model",
        token=token,
        commit_message="Publish Week 9 DataForge 0.5B SFT checkpoint",
    )
    return resolved_repo_id


def _build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--card",
        dest="card_template",
        type=Path,
        default=Path("archive/training/MODEL_CARD_TEMPLATE.md"),
    )
    parser.add_argument("--repo-id", default="auto")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the model publisher CLI."""
    args = _build_parser().parse_args(argv)
    repo_id = publish_model(
        model_dir=args.model_dir,
        card_template=args.card_template,
        repo_id=args.repo_id,
        token=os.environ.get("HF_TOKEN"),
    )
    Console().print(f"Published merged model to {repo_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
