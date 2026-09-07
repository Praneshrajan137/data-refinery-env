"""Write a non-claim GRPO readiness diagnostic report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, cast

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from archive.training.grpo_config import load_grpo_config  # noqa: E402
from archive.training.grpo_readiness import (  # noqa: E402
    GrpoReadinessSettings,
    analyze_grpo_readiness_paths,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--trajectory",
        type=Path,
        default=PROJECT_ROOT / "data" / "sft_traj" / "expert_v4.jsonl",
        help="Local expert_v4 JSONL trajectory path.",
    )
    parser.add_argument(
        "--split-manifest",
        type=Path,
        default=PROJECT_ROOT / "data" / "sft_traj" / "split_manifest_v4.json",
        help="Local split manifest used to reject held-out row leakage.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=PROJECT_ROOT / "training" / "configs" / "grpo_05b.yaml",
        help="GRPO YAML config whose readiness thresholds should be enforced.",
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--fail-on-block",
        action="store_true",
        help="Return exit code 2 when the diagnostic status is block.",
    )
    return parser


def _settings_from_config(path: Path) -> GrpoReadinessSettings:
    """Load config through the hard GRPO preflight and return readiness settings."""
    config = cast(dict[str, Any], load_grpo_config(path))
    return GrpoReadinessSettings.from_config(config)


def main(argv: list[str] | None = None) -> int:
    """Run the GRPO readiness report CLI."""
    args = _build_parser().parse_args(argv)
    try:
        settings = _settings_from_config(args.config)
        report = analyze_grpo_readiness_paths(
            trajectory_path=args.trajectory,
            split_manifest_path=args.split_manifest,
            settings=settings,
        )
    except (OSError, RuntimeError, ValueError, yaml.YAMLError) as exc:
        print(f"GRPO readiness report failed: {exc}", file=sys.stderr)
        return 2
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    if args.fail_on_block and report.get("status") == "block":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
