"""Assemble the authoritative Hugging Face Space deploy tree.

This script stages the minimum repo snapshot needed to build the API-only
playground backend on Hugging Face Spaces while keeping the monorepo as the
single source of truth.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SPACE_README = PROJECT_ROOT / "playground" / "api" / "README.md"
SPACE_DOCKERFILE = PROJECT_ROOT / "playground" / "api" / "Dockerfile"


def _copy_tree(source: Path, destination: Path) -> None:
    """Copy a directory tree while dropping Python cache artifacts."""
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )


def stage_space(output_dir: Path) -> None:
    """Create a clean Hugging Face Space build tree at ``output_dir``."""
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    shutil.copy2(SPACE_README, output_dir / "README.md")
    shutil.copy2(SPACE_DOCKERFILE, output_dir / "Dockerfile")
    shutil.copy2(PROJECT_ROOT / "pyproject.toml", output_dir / "pyproject.toml")

    api_output_dir = output_dir / "playground" / "api"
    api_output_dir.mkdir(parents=True)
    shutil.copy2(PROJECT_ROOT / "playground" / "api" / "app.py", api_output_dir / "app.py")
    shutil.copy2(
        PROJECT_ROOT / "playground" / "api" / "requirements.txt",
        api_output_dir / "requirements.txt",
    )
    # Intentionally empty, and required by the Dockerfile: it stops SlowAPI reading a repository
    # .env at import time.
    shutil.copy2(
        PROJECT_ROOT / "playground" / "api" / "slowapi.env",
        api_output_dir / "slowapi.env",
    )
    _copy_tree(PROJECT_ROOT / "playground" / "api" / "samples", api_output_dir / "samples")

    # The frontend sources, because the Dockerfile now builds the SPA in a `web` stage and serves
    # it from the same origin as the API. Without these the Space build fails at `npm ci`.
    #
    # This makes the Space a COMPLETE playground rather than an API alone, which matters because
    # it is the fallback for the Azure deployment -- and the Azure subscription is a Free Trial
    # that will expire. A fallback that only serves JSON is not a fallback for a web app.
    shutil.copytree(
        PROJECT_ROOT / "playground" / "web",
        output_dir / "playground" / "web",
        ignore=shutil.ignore_patterns(
            "__pycache__",
            "*.pyc",
            "node_modules",
            "dist",
            "test-results",
            "playwright-report",
            ".vite",
        ),
    )

    _copy_tree(PROJECT_ROOT / "dataforge", output_dir / "dataforge")
    constitution_dir = PROJECT_ROOT / "dataforge" / "safety" / "constitutions"
    if constitution_dir.exists():
        _copy_tree(constitution_dir, output_dir / "constitutions")


def _build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser for the staging script."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / ".hf-space-stage",
        help="Directory to populate with the staged Space contents.",
    )
    return parser


def main() -> None:
    """Run the staging workflow from the command line."""
    args = _build_parser().parse_args()
    stage_space(args.output_dir.resolve())
    print(f"Staged Hugging Face Space files at {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
