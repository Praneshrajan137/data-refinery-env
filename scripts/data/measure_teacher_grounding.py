"""Measure whether gpt-5.6-sol teacher repairs are ROBUSTLY grounded.

This is the reproducible evidence behind the decision to retract the
grounded-rationale SFT track (DECISIONS 2026-07-25). It contrasts two notions of
"functional-dependency grounded" for each verified teacher repair:

* naive window check (the spurious-prone baseline): any single column that is
  coincidentally unanimous across the prompt's own ~6-row, entity-clustered
  context window. This is what an earlier draft used as a training-data gate.
* guarded check (the project's single source of truth): the value agrees with a
  robust, full-table unanimous determinant group under
  ``dataforge.verifier.inferred.fd_consensus_violation`` over the support/
  near-key/confidence-guarded ``infer_verification_schema``.

The gap between them is the finding: the naive check over-counts grounding by
roughly 6-11x, because entity-clustered windows make many columns coincidentally
unanimous - exactly the in-table-indistinguishable spurious FDs the project
already refuses to mine. The naive logic lives here (a diagnostic), never in the
product contract, so it cannot be mistaken for a verification primitive.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.schema_inference import infer_verification_schema  # noqa: E402
from dataforge.verifier.inferred import fd_consensus_violation  # noqa: E402

_PROBE = "__PROBE_NOT_A_REAL_VALUE__"
_CONSENSUS_RE = re.compile(r"consistently show '(.*)'")


def _window_rows(state: Mapping[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for key in ("target_rows", "context_rows"):
        for row in state.get(key, []) or []:
            if isinstance(row, Mapping):
                rows.append({str(k): str(v) for k, v in row.items()})
    return rows


def naive_window_grounded(
    rows: Sequence[Mapping[str, str]], *, target_row: int, column: str, new_value: str
) -> bool:
    """Spurious-prone baseline: unanimous single-column determinant in the window."""
    target = next((r for r in rows if str(r.get("_row")) == str(target_row)), None)
    if target is None:
        return False
    wanted = str(new_value).strip()
    for determinant in target:
        if determinant in ("_row", column):
            continue
        det_value = str(target.get(determinant, "")).strip()
        if det_value == "":
            continue
        peers = [
            r
            for r in rows
            if str(r.get("_row")) != str(target_row)
            and str(r.get(determinant, "")).strip() == det_value
        ]
        if peers and {str(r.get(column, "")).strip() for r in peers} == {wanted}:
            return True
    return False


def main(argv: list[str] | None = None) -> int:
    """Report naive vs guarded grounding rates for teacher-repair JSONL files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="hospital")
    parser.add_argument(
        "--input",
        action="append",
        default=None,
        help="Teacher JSONL path (repeatable). Defaults to the gpt-5.6-sol files.",
    )
    args = parser.parse_args(argv)

    inputs = args.input or [
        "data/sft_traj/expert_v1_gpt56sol.jsonl",
        "data/sft_traj/expert_v1_gpt56sol_full.jsonl",
    ]

    dataset = load_real_world_dataset(args.dataset, verify_hashes=True)
    df = dataset.dirty_df
    schema = infer_verification_schema(df)
    fd_columns = {fd.dependent for fd in schema.functional_dependencies}
    print(f"dataset={args.dataset} inferred_fds={len(schema.functional_dependencies)}")

    for path_str in inputs:
        path = Path(path_str)
        if not path.exists():
            continue
        total = naive = has_fd = robust = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            fix = record.get("fix") or []
            if not fix:
                continue
            rows = _window_rows(record.get("state") or {})
            for item in fix:
                row = int(item["row"])
                column = str(item["column"])
                new_value = str(item["new_value"])
                total += 1
                if naive_window_grounded(rows, target_row=row, column=column, new_value=new_value):
                    naive += 1
                if column in fd_columns:
                    has_fd += 1
                message = fd_consensus_violation(df, row, column, _PROBE, schema)
                if message is not None:
                    match = _CONSENSUS_RE.search(message)
                    if match and match.group(1).strip() == new_value.strip():
                        robust += 1
        if total == 0:
            continue
        print(f"\n{path}")
        print(f"  total teacher repairs:            {total}")
        print(f"  naive window 'grounded':          {naive} ({naive / total:.0%})")
        print(f"  column has a guarded inferred FD: {has_fd} ({has_fd / total:.0%})")
        print(f"  ROBUSTLY grounded (guarded FD):   {robust} ({robust / total:.0%})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
