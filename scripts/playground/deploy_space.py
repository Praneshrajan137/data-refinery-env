"""Deploy the playground API backend to a Hugging Face Docker Space."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

from huggingface_hub import HfApi, get_token

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from stage_space import stage_space  # noqa: E402

DEFAULT_REPO_ID = "Praneshrajan15/dataforge-playground"
DEFAULT_FRONTEND_ORIGIN = "https://dataforge.praneshrajan15.workers.dev"


def deploy_space(
    *,
    repo_id: str = DEFAULT_REPO_ID,
    origins: str,
    stage_dir: Path | None = None,
) -> str:
    """Stage and upload the API-only Space using the Python Hub API."""
    token = get_token()
    if not token:
        raise RuntimeError("Missing Hugging Face token; run `hf auth login` or set HF_TOKEN.")

    api = HfApi(token=token)
    api.create_repo(repo_id, repo_type="space", space_sdk="docker", exist_ok=True)
    api.add_space_variable(repo_id, "DATAFORGE_PLAYGROUND_ORIGINS", origins, token=token)
    for key in ("GROQ_API_KEY", "GEMINI_API_KEY"):
        value = os.environ.get(key)
        if value:
            api.add_space_secret(repo_id, key, value, token=token)

    if stage_dir is None:
        with tempfile.TemporaryDirectory() as tmpdir:
            staged = Path(tmpdir) / "space"
            stage_space(staged)
            commit = api.upload_folder(
                repo_id=repo_id,
                repo_type="space",
                folder_path=staged,
                delete_patterns="*",
                commit_message="Deploy DataForge playground API",
                token=token,
            )
            return str(commit.commit_url)

    stage_space(stage_dir)
    commit = api.upload_folder(
        repo_id=repo_id,
        repo_type="space",
        folder_path=stage_dir,
        delete_patterns="*",
        commit_message="Deploy DataForge playground API",
        token=token,
    )
    return str(commit.commit_url)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-id", default=DEFAULT_REPO_ID)
    parser.add_argument(
        "--origins",
        default=DEFAULT_FRONTEND_ORIGIN,
        help="Comma-separated CORS allowlist for the deployed frontend.",
    )
    parser.add_argument("--stage-dir", type=Path, default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    commit_url = deploy_space(
        repo_id=args.repo_id,
        origins=args.origins,
        stage_dir=args.stage_dir.resolve() if args.stage_dir else None,
    )
    print(f"Deployed playground Space: {commit_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
