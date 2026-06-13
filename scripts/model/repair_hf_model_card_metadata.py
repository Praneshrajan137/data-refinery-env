"""Repair Hugging Face model-card metadata for a DataForge family repo."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.release.model_family import load_model_family_manifest  # noqa: E402


class UploadApi(Protocol):
    """Subset of HfApi used by the card repair tool."""

    def upload_file(
        self,
        *,
        path_or_fileobj: bytes,
        path_in_repo: str,
        repo_id: str,
        repo_type: str,
        token: str | None = None,
        commit_message: str,
    ) -> object:
        """Upload a single Hub file."""


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
        """Download a file and return a local path."""


@dataclass(frozen=True, slots=True)
class CardRepairResult:
    """Result of a model-card metadata repair operation."""

    repo_id: str
    changed: bool
    applied: bool
    changed_fields: tuple[str, ...]
    readme_bytes: int


def repair_model_card_metadata(
    *,
    repo_id: str,
    apply: bool = False,
    token: str | None = None,
    api: UploadApi | None = None,
    downloader: DownloadFile | None = None,
) -> CardRepairResult:
    """Repair README front matter for a manifest-backed DataForge repo."""
    manifest = load_model_family_manifest()
    entry = manifest.entry_for_repo(repo_id)
    if entry is None:
        raise RuntimeError(f"{repo_id} is not present in the model-family manifest.")
    if downloader is None:
        from huggingface_hub import hf_hub_download

        downloader = hf_hub_download
    readme_path = Path(downloader(repo_id, filename="README.md", repo_type="model", token=token))
    original = readme_path.read_text(encoding="utf-8")
    metadata, body = split_front_matter(original)
    required = entry.model_card_metadata(manifest.dataset_repo)
    if "tags" in metadata:
        required["tags"] = metadata["tags"]
    changed_fields = tuple(key for key, value in required.items() if metadata.get(key) != value)
    repaired = render_front_matter({**metadata, **required}, body)
    if apply and changed_fields:
        if api is None:
            from huggingface_hub import HfApi

            api = HfApi(token=token)
        api.upload_file(
            path_or_fileobj=repaired.encode("utf-8"),
            path_in_repo="README.md",
            repo_id=repo_id,
            repo_type="model",
            token=token,
            commit_message="Repair DataForge model-card metadata",
        )
    return CardRepairResult(
        repo_id=repo_id,
        changed=bool(changed_fields),
        applied=apply and bool(changed_fields),
        changed_fields=changed_fields,
        readme_bytes=len(repaired.encode("utf-8")),
    )


def split_front_matter(text: str) -> tuple[dict[str, Any], str]:
    """Split a Markdown document into YAML front matter and body."""
    if not text.startswith("---\n"):
        return {}, text
    marker = "\n---\n"
    end = text.find(marker, 4)
    if end == -1:
        return {}, text
    raw_metadata = text[4:end]
    payload = yaml.safe_load(raw_metadata) or {}
    if not isinstance(payload, dict):
        payload = {}
    return dict(payload), text[end + len(marker) :]


def render_front_matter(metadata: dict[str, Any], body: str) -> str:
    """Render Markdown with YAML front matter."""
    rendered = yaml.safe_dump(
        metadata,
        sort_keys=False,
        allow_unicode=False,
        default_flow_style=False,
    )
    return f"---\n{rendered}---\n\n{body.lstrip()}"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-id", default="Praneshrajan15/DataForge-0.5B-SFT")
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the model-card repair CLI."""
    load_dotenv()
    args = _build_parser().parse_args(argv)
    token = (os.environ.get("HF_TOKEN") or "").strip() or None
    try:
        result = repair_model_card_metadata(
            repo_id=args.repo_id,
            apply=args.apply,
            token=token,
        )
    except Exception as exc:
        print(f"model-card metadata repair failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
