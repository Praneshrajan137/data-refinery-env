"""Validate the Hugging Face Space YAML front-matter on the playground API README.

The Space will not boot with the wrong ``sdk`` or ``app_port``, and the failure surfaces
as a dead hosted surface rather than as a red build, so this is checked before push.

**Why this is a script and not an inline ``python -c`` heredoc.** Until 2026-09-01 this
logic lived only inside ``.github/workflows/ci.yml``, which made it the one gate in the
repo with no module, no import, and no test -- it could not be run locally, could not be
type-checked, and could not be exercised against a deliberately-broken input to show it
still refuses one. ``scripts/ci/test_map_coverage.py`` records that exact shape causing a
four-commit red ``main``: *"the schema lived in an inline heredoc here and nowhere else...
Two validators with different schemas over one file is drift by construction."* The
lesson was applied there and missed here.

The required-key set is deliberately a literal, and that is a considered exception to this
repo's derive-never-restate rule rather than an oversight: the authority for these keys is
Hugging Face's Spaces configuration reference, which is outside this repository and cannot
be imported. What can be derived from source is the *value* expectations -- ``app_port``
must match the port the Dockerfile exposes -- so those are read from the Dockerfile rather
than restated, and a mismatch between the two files is itself a failure.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml

_REPO = Path(__file__).resolve().parents[2]
_README = _REPO / "playground" / "api" / "README.md"
_DOCKERFILE = _REPO / "playground" / "api" / "Dockerfile"

# Hugging Face requires these for a Docker Space. Not derivable from this repo; see module
# docstring for why restating them here is the honest choice.
_REQUIRED_KEYS = frozenset({"title", "sdk", "app_port", "license"})
_REQUIRED_SDK = "docker"


def parse_front_matter(text: str) -> dict[str, Any]:
    """Return the YAML front-matter block from a Markdown document.

    Args:
        text: Full Markdown source.

    Returns:
        The parsed front-matter mapping.

    Raises:
        ValueError: If no front-matter block is present or it is not a mapping. A README
            with no front matter at all previously raised ``IndexError`` from a bare
            ``split('---')[1]``, which reads as a crash rather than as a verdict.
    """
    if not text.startswith("---"):
        raise ValueError("README does not open with a '---' front-matter fence.")
    parts = text.split("---")
    if len(parts) < 3:
        raise ValueError("README front-matter fence is not closed.")
    data = yaml.safe_load(parts[1])
    if not isinstance(data, dict):
        raise ValueError(f"README front-matter is not a mapping, got {type(data).__name__}.")
    return data


def dockerfile_exposed_port(text: str) -> int | None:
    """Return the port the Dockerfile EXPOSEs, or None if it declares none."""
    match = re.search(r"^\s*EXPOSE\s+(\d+)", text, re.MULTILINE)
    return int(match.group(1)) if match else None


def check(readme_text: str, dockerfile_text: str | None = None) -> list[str]:
    """Return every front-matter problem found, empty when the Space config is valid.

    Args:
        readme_text: Contents of the Space README.
        dockerfile_text: Contents of the Space Dockerfile, when available. Supplying it
            enables the cross-file port check.

    Returns:
        Human-readable error strings.
    """
    errors: list[str] = []
    try:
        data = parse_front_matter(readme_text)
    except ValueError as exc:
        return [str(exc)]

    missing = _REQUIRED_KEYS - set(data)
    if missing:
        errors.append(f"Missing HF metadata keys: {sorted(missing)}")

    sdk = data.get("sdk")
    if "sdk" in data and sdk != _REQUIRED_SDK:
        errors.append(f"sdk must be {_REQUIRED_SDK!r}, got {sdk!r}")

    port = data.get("app_port")
    if "app_port" in data and not isinstance(port, int):
        errors.append(f"app_port must be an integer, got {port!r}")
    elif dockerfile_text is not None and isinstance(port, int):
        exposed = dockerfile_exposed_port(dockerfile_text)
        if exposed is None:
            errors.append("Dockerfile declares no EXPOSE, so app_port cannot be corroborated.")
        elif exposed != port:
            errors.append(
                f"app_port {port} does not match the Dockerfile's EXPOSE {exposed}. "
                "The Space would boot and then be unreachable."
            )
    return errors


def main() -> int:
    """Validate the committed Space README and return a process exit code."""
    if not _README.exists():
        print(f"FAIL HF Space front-matter: {_README} not found.")
        return 1
    dockerfile_text = _DOCKERFILE.read_text(encoding="utf-8") if _DOCKERFILE.exists() else None
    errors = check(_README.read_text(encoding="utf-8"), dockerfile_text)
    if errors:
        print("HF Space YAML front-matter check FAILED:")
        for error in errors:
            print(f"  - {error}")
        return 1
    print("HF Space YAML front-matter valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
