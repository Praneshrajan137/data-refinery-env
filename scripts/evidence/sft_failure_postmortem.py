"""Summarize bounded SFT failure samples into a contract postmortem."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

DEFAULT_DIAGNOSTICS = Path("eval/results/hf_current/DataForge-0.5B-SFT/eval_diagnostics.json")
DEFAULT_JSON_OUTPUT = Path("eval/results/hf_current/DataForge-0.5B-SFT/failure_postmortem_v3.json")
DEFAULT_MD_OUTPUT = Path("eval/results/hf_current/DataForge-0.5B-SFT/failure_postmortem_v3.md")


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def summarize_failures(diagnostics: dict[str, Any], *, section: str = "sft") -> dict[str, Any]:
    """Return aggregate evidence from the bounded failure samples."""
    model_payload = diagnostics.get(section)
    if not isinstance(model_payload, dict):
        raise ValueError(f"Diagnostics missing {section!r} section.")
    samples = model_payload.get("failure_samples")
    if not isinstance(samples, list):
        raise ValueError(f"Diagnostics {section!r} section missing failure_samples.")

    datasets: Counter[str] = Counter()
    taxonomy: Counter[str] = Counter()
    predicted_columns: Counter[str] = Counter()
    schema_case_columns: Counter[str] = Counter()
    briefs: list[dict[str, Any]] = []
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        dataset = str(sample.get("dataset", "unknown"))
        datasets[dataset] += 1
        failure_taxonomy = sample.get("failure_taxonomy", {})
        if isinstance(failure_taxonomy, dict):
            taxonomy.update({str(key): int(value) for key, value in failure_taxonomy.items()})
        for repair in sample.get("predicted_repairs", []):
            if not isinstance(repair, dict):
                continue
            column = str(repair.get("column", ""))
            predicted_columns[column] += 1
            target_rows = sample.get("target_rows", [])
            allowed = (
                set(target_rows[0]) - {"_row"}
                if target_rows and isinstance(target_rows[0], dict)
                else set()
            )
            if column not in allowed and column.lower() in {item.lower() for item in allowed}:
                schema_case_columns[column] += 1
        briefs.append(
            {
                "task_index": sample.get("task_index"),
                "dataset": dataset,
                "failure_taxonomy": failure_taxonomy,
                "predicted_repairs": sample.get("predicted_repairs", [])[:8],
                "ground_truth": sample.get("ground_truth", [])[:12],
            }
        )

    return {
        "schema_version": "dataforge_sft_failure_postmortem_v1",
        "source_section": section,
        "sample_count": len(samples),
        "dataset_counts": dict(sorted(datasets.items())),
        "failure_taxonomy_counts": dict(taxonomy.most_common()),
        "top_predicted_columns": dict(predicted_columns.most_common(12)),
        "schema_case_columns": dict(schema_case_columns.most_common()),
        "headline_findings": [
            "Schema/case mistakes such as Index, Id, and Abv remain frequent.",
            "Wrong-cell index/address/provider repairs show weak row-id discipline.",
            "Beer samples overrepair style or preserve percent/unit text instead of normalizing.",
            "Flights samples invent, copy, or date-prefix times instead of abstaining.",
        ],
        "briefs": briefs,
    }


def render_markdown(summary: dict[str, Any]) -> str:
    """Render a compact Markdown postmortem."""
    lines = [
        "# DataForge SFT v3 Failure Postmortem",
        "",
        f"- Failure samples analyzed: `{summary['sample_count']}`",
        "- Dataset counts: "
        + ", ".join(f"`{key}`={value}" for key, value in summary["dataset_counts"].items()),
        "- Failure taxonomy: "
        + ", ".join(
            f"`{key}`={value}" for key, value in summary["failure_taxonomy_counts"].items()
        ),
        "",
        "## Findings",
        "",
    ]
    lines.extend(f"- {finding}" for finding in summary["headline_findings"])
    lines.extend(["", "## Top Predicted Columns", ""])
    lines.extend(f"- `{key}`: {value}" for key, value in summary["top_predicted_columns"].items())
    return "\n".join(lines) + "\n"


def write_postmortem(
    *,
    diagnostics_path: Path = DEFAULT_DIAGNOSTICS,
    json_output: Path = DEFAULT_JSON_OUTPUT,
    md_output: Path = DEFAULT_MD_OUTPUT,
    section: str = "sft",
) -> dict[str, Any]:
    """Read diagnostics and write JSON/Markdown postmortem artifacts."""
    summary = summarize_failures(_load_json(diagnostics_path), section=section)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    md_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_output.write_text(render_markdown(summary), encoding="utf-8")
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--diagnostics", type=Path, default=DEFAULT_DIAGNOSTICS)
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON_OUTPUT)
    parser.add_argument("--md-output", type=Path, default=DEFAULT_MD_OUTPUT)
    parser.add_argument("--section", default="sft")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    summary = write_postmortem(
        diagnostics_path=args.diagnostics,
        json_output=args.json_output,
        md_output=args.md_output,
        section=args.section,
    )
    print(json.dumps({k: summary[k] for k in ("sample_count", "dataset_counts")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
