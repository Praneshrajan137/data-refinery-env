"""Validate a Kaggle GRPO smoke report before any longer candidate run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "dataforge_grpo_smoke_validation_v1"
EXPECTED_REPORT_SCHEMA = "dataforge_kaggle_grpo_smoke_report_v1"


def _as_bool(payload: dict[str, Any], key: str) -> bool:
    return bool(payload.get(key, True))


def validate_smoke_report(
    payload: dict[str, Any],
    *,
    min_plausible_prompt_tokens: int = 64,
    candidate_steps: int = 500,
    max_candidate_gpu_hours: float = 12.0,
) -> dict[str, Any]:
    """Return a JSON-ready validation report for no-upload smoke evidence."""
    blockers: list[str] = []
    attempted_steps = int(payload.get("attempted_steps", 0) or 0)
    gpu_hours = float(payload.get("gpu_hours", 0.0) or 0.0)
    max_prompt_tokens = int(payload.get("max_prompt_tokens", 0) or 0)
    prompt_token_budget = int(payload.get("prompt_token_budget", 0) or 0)
    projected_candidate_gpu_hours = (
        round(gpu_hours * candidate_steps / attempted_steps, 4) if attempted_steps else None
    )

    if payload.get("schema_version") != EXPECTED_REPORT_SCHEMA:
        blockers.append("wrong_schema_version")
    if payload.get("status") != "pass":
        blockers.append("smoke_status_not_pass")
    if payload.get("training_stage") != "smoke":
        blockers.append("not_smoke_stage")
    if attempted_steps <= 0 or attempted_steps != int(payload.get("configured_max_steps", 0) or 0):
        blockers.append("attempted_steps_mismatch")
    if payload.get("readiness_status") != "pass" or payload.get("readiness_blockers"):
        blockers.append("readiness_not_pass")
    if _as_bool(payload, "model_upload_attempted"):
        blockers.append("model_upload_attempted")
    if _as_bool(payload, "model_repo_created"):
        blockers.append("model_repo_created")
    if _as_bool(payload, "public_claim_updated"):
        blockers.append("public_claim_updated")
    if max_prompt_tokens < min_plausible_prompt_tokens:
        blockers.append("prompt_token_telemetry_implausible")
    if prompt_token_budget and max_prompt_tokens > prompt_token_budget:
        blockers.append("prompt_token_budget_exceeded")
    train_metrics = payload.get("train_metrics")
    if not isinstance(train_metrics, dict) or not {
        "train_runtime",
        "train_loss",
        "train_steps_per_second",
    } <= set(train_metrics):
        blockers.append("training_metrics_incomplete")
    if projected_candidate_gpu_hours is None:
        blockers.append("candidate_runtime_unprojectable")
    elif projected_candidate_gpu_hours > max_candidate_gpu_hours:
        blockers.append("candidate_runtime_over_budget")

    status = "pass" if not blockers else "block"
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "ok": status == "pass",
        "blockers": sorted(set(blockers)),
        "checks": {
            "attempted_steps": attempted_steps,
            "candidate_steps": candidate_steps,
            "gpu_hours": gpu_hours,
            "projected_candidate_gpu_hours": projected_candidate_gpu_hours,
            "max_candidate_gpu_hours": max_candidate_gpu_hours,
            "max_prompt_tokens": max_prompt_tokens,
            "min_plausible_prompt_tokens": min_plausible_prompt_tokens,
            "prompt_token_budget": prompt_token_budget,
            "model_upload_attempted": payload.get("model_upload_attempted"),
            "model_repo_created": payload.get("model_repo_created"),
            "public_claim_updated": payload.get("public_claim_updated"),
        },
        "limitations": [
            "This validates smoke-run evidence only; it does not verify GRPO model quality.",
            "Projected candidate runtime is a planning guard, not a performance guarantee.",
        ],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--candidate-steps", type=int, default=500)
    parser.add_argument("--max-candidate-gpu-hours", type=float, default=12.0)
    parser.add_argument("--min-plausible-prompt-tokens", type=int, default=64)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = json.loads(args.report.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("GRPO smoke report must contain a JSON object.")
    validation = validate_smoke_report(
        payload,
        min_plausible_prompt_tokens=args.min_plausible_prompt_tokens,
        candidate_steps=args.candidate_steps,
        max_candidate_gpu_hours=args.max_candidate_gpu_hours,
    )
    rendered = json.dumps(validation, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if validation["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
