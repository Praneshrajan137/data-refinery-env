"""Upload the DataForge SFT dataset README to Hugging Face."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Protocol

from dotenv import load_dotenv
from rich.console import Console

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.model.verify_sft_release import DEFAULT_DATASET_REPO  # noqa: E402

DEFAULT_README = Path("archive/training/DATASET_README.md")


class DatasetCardApi(Protocol):
    """Protocol for the subset of HfApi used to publish the dataset README."""

    def upload_file(
        self,
        *,
        path_or_fileobj: str,
        path_in_repo: str,
        repo_id: str,
        repo_type: str,
        token: str | None = None,
        commit_message: str,
    ) -> Any:
        """Upload one file to a dataset repository."""


def publish_dataset_readme(
    *,
    repo_id: str = DEFAULT_DATASET_REPO,
    readme: Path = DEFAULT_README,
    token: str | None = None,
    api: DatasetCardApi | None = None,
) -> str:
    """Upload the checked-in dataset README to the canonical dataset repo."""
    if not readme.exists():
        raise FileNotFoundError(f"Dataset README does not exist: {readme}")
    if api is None:
        from huggingface_hub import HfApi

        api = HfApi(token=token)
    api.upload_file(
        path_or_fileobj=str(readme),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
        token=token,
        commit_message="Add DataForge SFT dataset card",
    )
    return repo_id


def _resolve_hf_token() -> str | None:
    """Resolve an upload token from env, .env, or the local HF token store."""
    token = (os.environ.get("HF_TOKEN") or "").strip()
    if token:
        return token
    try:
        from huggingface_hub import get_token
    except ImportError:
        return None
    cached_token = get_token()
    return cached_token.strip() if cached_token else None


def _build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-id", default=DEFAULT_DATASET_REPO)
    parser.add_argument("--readme", type=Path, default=DEFAULT_README)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the dataset README publisher CLI."""
    load_dotenv()
    args = _build_parser().parse_args(argv)
    token = _resolve_hf_token()
    if not token:
        raise RuntimeError("HF_TOKEN is required to upload the dataset README.")
    repo_id = publish_dataset_readme(
        repo_id=args.repo_id,
        readme=args.readme,
        token=token,
    )
    Console().print(f"Uploaded dataset README to {repo_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
