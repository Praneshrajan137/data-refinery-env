"""Validate the canonical DataForge evidence ledger."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LEDGER = ROOT / "docs" / "evidence" / "ledger.json"
SCHEMA_VERSION = "dataforge_evidence_ledger_v1"
ALLOWED_STATUSES = frozenset(
    {
        "shipped",
        "beta",
        "verified_research",
        "failed_diagnostic",
        "smoke_submitted",
        "blocked",
        "roadmap",
    }
)
NON_PUBLIC_STATUSES = frozenset({"failed_diagnostic", "smoke_submitted", "blocked", "roadmap"})
EVIDENCE_REQUIRED_STATUSES = frozenset(
    {"shipped", "beta", "verified_research", "failed_diagnostic", "smoke_submitted"}
)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT)).replace("\\", "/")
    except ValueError:
        return str(path)


def _entry_path(entry_id: str, raw_path: object) -> tuple[Path | None, str | None]:
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None, f"{entry_id}: evidence_paths entries must be non-empty strings."
    path = ROOT.joinpath(*raw_path.replace("\\", "/").split("/"))
    return path, None


def validate_ledger(path: Path = DEFAULT_LEDGER) -> list[str]:
    """Return validation errors for the evidence ledger."""
    errors: list[str] = []
    if not path.exists():
        return [f"{_display(path)} is missing."]
    try:
        ledger = _load_json(path)
    except (json.JSONDecodeError, ValueError) as exc:
        return [f"{_display(path)} is not a valid evidence ledger: {exc}"]

    if ledger.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"{_display(path)} has unexpected schema_version.")
    if not isinstance(ledger.get("north_star"), str) or not ledger["north_star"].strip():
        errors.append(f"{_display(path)} is missing north_star.")
    entries = ledger.get("entries")
    if not isinstance(entries, list) or not entries:
        errors.append(f"{_display(path)} must contain a non-empty entries list.")
        return errors

    seen: set[str] = set()
    for index, raw_entry in enumerate(entries, start=1):
        if not isinstance(raw_entry, dict):
            errors.append(f"entry {index} must be a JSON object.")
            continue
        entry_id = str(raw_entry.get("id", "")).strip()
        status = str(raw_entry.get("status", "")).strip()
        surface = str(raw_entry.get("surface", "")).strip()
        claim = str(raw_entry.get("claim", "")).strip()
        if not entry_id:
            errors.append(f"entry {index} is missing id.")
            entry_id = f"entry {index}"
        elif entry_id in seen:
            errors.append(f"entry {index} duplicates id {entry_id!r}.")
        seen.add(entry_id)
        if status not in ALLOWED_STATUSES:
            errors.append(f"{entry_id}: status {status!r} is not allowed.")
        if not surface:
            errors.append(f"{entry_id}: surface is required.")
        if not claim:
            errors.append(f"{entry_id}: claim is required.")
        if raw_entry.get("public_claim_allowed") is True and status in NON_PUBLIC_STATUSES:
            errors.append(f"{entry_id}: {status} entries cannot allow public claims.")
        evidence_paths = raw_entry.get("evidence_paths", [])
        if not isinstance(evidence_paths, list):
            errors.append(f"{entry_id}: evidence_paths must be a list.")
            evidence_paths = []
        if status in EVIDENCE_REQUIRED_STATUSES and not evidence_paths:
            errors.append(f"{entry_id}: status {status} requires evidence_paths.")
        for raw_path in evidence_paths:
            evidence_path, error = _entry_path(entry_id, raw_path)
            if error is not None:
                errors.append(error)
            elif evidence_path is not None and not evidence_path.exists():
                errors.append(f"{entry_id}: evidence path {_display(evidence_path)} is missing.")
        blockers = raw_entry.get("blockers", [])
        if not isinstance(blockers, list):
            errors.append(f"{entry_id}: blockers must be a list.")
        if status == "blocked" and not blockers:
            errors.append(f"{entry_id}: blocked entries must name blockers.")
        claim_policy = str(raw_entry.get("claim_policy", "")).strip()
        if not claim_policy:
            errors.append(f"{entry_id}: claim_policy is required.")
    return errors


def summarize_ledger(path: Path = DEFAULT_LEDGER) -> dict[str, Any]:
    """Return a compact status summary for tooling and release notes."""
    ledger = _load_json(path)
    entries = ledger.get("entries", [])
    counts: dict[str, int] = {}
    blocked: list[str] = []
    if not isinstance(entries, list):
        return {"schema_version": ledger.get("schema_version"), "status_counts": counts}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        status = str(entry.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1
        if status in {"blocked", "failed_diagnostic", "smoke_submitted", "roadmap"}:
            blocked.append(str(entry.get("id", "")))
    return {
        "schema_version": ledger.get("schema_version"),
        "north_star": ledger.get("north_star"),
        "status_counts": dict(sorted(counts.items())),
        "non_release_entries": sorted(item for item in blocked if item),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--summary", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    errors = validate_ledger(args.ledger)
    if errors:
        for error in errors:
            print(error)
        return 1
    if args.summary:
        print(json.dumps(summarize_ledger(args.ledger), indent=2, sort_keys=True))
    else:
        print(f"Evidence ledger valid: {_display(args.ledger)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
