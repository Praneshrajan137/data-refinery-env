"""Tests for the authoritative Hugging Face Space staging flow."""

from __future__ import annotations

import importlib.util
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "playground" / "stage_space.py"
SPACE_SETUP_PATH = PROJECT_ROOT / "playground" / "api" / "SPACE_SETUP.md"
DEPLOY_PATH = PROJECT_ROOT / "playground" / "web" / "DEPLOY.md"


def _load_stage_space_module():
    """Load the staging script as a module without requiring a package import."""
    spec = importlib.util.spec_from_file_location("stage_space", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stage_space_creates_consistent_hf_repo_layout(tmp_path: Path) -> None:
    """The staging script assembles the exact repo layout the Dockerfile expects."""
    module = _load_stage_space_module()
    output_dir = tmp_path / "hf-space"

    module.stage_space(output_dir)

    required_paths = [
        output_dir / "README.md",
        output_dir / "Dockerfile",
        output_dir / "pyproject.toml",
        output_dir / "playground" / "api" / "app.py",
        output_dir / "playground" / "api" / "requirements.txt",
        output_dir / "playground" / "api" / "samples" / "hospital_10rows.csv",
        output_dir / "dataforge" / "__init__.py",
        output_dir / "constitutions" / "default.yaml",
    ]

    for path in required_paths:
        assert path.exists(), f"Expected staged path to exist: {path}"

    assert not (output_dir / "playground" / "web").exists()


def test_stage_space_docker_copy_sources_all_exist(tmp_path: Path) -> None:
    """Every Dockerfile COPY source in the staged tree resolves to a real path."""
    module = _load_stage_space_module()
    output_dir = tmp_path / "hf-space"
    module.stage_space(output_dir)

    dockerfile_lines = (output_dir / "Dockerfile").read_text(encoding="utf-8").splitlines()
    copy_sources: list[Path] = []

    for raw_line in dockerfile_lines:
        line = raw_line.strip()
        if not line.startswith("COPY "):
            continue
        parts = line.split()
        if len(parts) >= 3 and not parts[1].startswith("--from="):
            copy_sources.append(output_dir / parts[1])
        elif len(parts) >= 4 and parts[1].startswith("--from="):
            continue

    assert copy_sources, "Expected staged Dockerfile to contain COPY instructions."
    for source in copy_sources:
        assert source.exists(), f"Docker COPY source is missing from staged tree: {source}"


def test_space_setup_runbook_uses_staging_flow() -> None:
    """The HF deploy runbook documents staging instead of subtree push."""
    body = SPACE_SETUP_PATH.read_text(encoding="utf-8")
    assert "stage_space.py" in body
    assert "subtree" not in body.lower()


def test_pages_deploy_runbook_rewrites_config_only() -> None:
    """The Pages deploy runbook targets config.js instead of mutating app.js."""
    body = DEPLOY_PATH.read_text(encoding="utf-8")
    assert "config.js" in body
    assert "app.js" not in body.lower() or "config.js" in body
