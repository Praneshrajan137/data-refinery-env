"""Fetch only JSON evidence reports from a Kaggle kernel output."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_KAGGLE_CREDENTIALS = Path.home() / ".kaggle" / "credentials.json"
DEFAULT_REPORT_PATTERN = (
    r"(sft_v[0-9]+_candidate_eval_report|eval_diagnostics|training_metrics|"
    r"kaggle_sft_v[0-9]+_candidate_report).*\.json$"
)
LEGACY_KAGGLE_ENV_VARS = (
    "KAGGLE_USERNAME",
    "KAGGLE_KEY",
    "KAGGLE_API_TOKEN",
    "KAGGLE_API_V1_TOKEN",
)


def _resolve_kaggle_cli(kaggle_cli: Path | None = None) -> Path:
    if kaggle_cli is not None:
        return kaggle_cli
    local_cli = ROOT / ".venv" / "Scripts" / "kaggle.exe"
    if local_cli.exists():
        return local_cli
    discovered = shutil.which("kaggle") or shutil.which("kaggle.exe")
    if discovered is None:
        raise RuntimeError("Kaggle CLI executable not found.")
    return Path(discovered)


def build_kaggle_output_command(
    *,
    kernel: str,
    output_dir: Path,
    kaggle_cli: Path | None = None,
    file_pattern: str = DEFAULT_REPORT_PATTERN,
) -> list[str]:
    """Return the targeted Kaggle output command for JSON reports."""
    return [
        str(_resolve_kaggle_cli(kaggle_cli)),
        "kernels",
        "output",
        kernel,
        "-p",
        str(output_dir),
        "--file-pattern",
        file_pattern,
        "-q",
    ]


def fetch_kaggle_json_reports(
    *,
    kernel: str,
    output_dir: Path,
    credentials_path: Path = DEFAULT_KAGGLE_CREDENTIALS,
    kaggle_cli: Path | None = None,
    file_pattern: str = DEFAULT_REPORT_PATTERN,
) -> int:
    """Fetch JSON reports while isolating stale legacy Kaggle credentials."""
    output_dir.mkdir(parents=True, exist_ok=True)
    command = build_kaggle_output_command(
        kernel=kernel,
        output_dir=output_dir,
        kaggle_cli=kaggle_cli,
        file_pattern=file_pattern,
    )
    with tempfile.TemporaryDirectory(prefix="dataforge-kaggle-config-") as clean_config:
        env = os.environ.copy()
        for key in LEGACY_KAGGLE_ENV_VARS:
            env.pop(key, None)
        env["KAGGLE_CONFIG_DIR"] = clean_config
        env["KAGGLE_CREDENTIALS_FILE"] = str(credentials_path)
        result = subprocess.run(command, env=env, check=False)
    return int(result.returncode)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kernel")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--credentials-path", type=Path, default=DEFAULT_KAGGLE_CREDENTIALS)
    parser.add_argument("--kaggle-cli", type=Path, default=None)
    parser.add_argument("--file-pattern", default=DEFAULT_REPORT_PATTERN)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return fetch_kaggle_json_reports(
        kernel=args.kernel,
        output_dir=args.output_dir,
        credentials_path=args.credentials_path,
        kaggle_cli=args.kaggle_cli,
        file_pattern=args.file_pattern,
    )


if __name__ == "__main__":
    raise SystemExit(main())
