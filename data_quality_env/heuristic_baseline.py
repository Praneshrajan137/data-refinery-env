#!/usr/bin/env python3
# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Rule-based heuristic baseline agent for the Data Quality environment.

Uses deterministic pattern matching to detect and fix common data quality
issues without any learning or LLM access.  Expected scores:
    - Task 1 (format_fixer):        ~0.40-0.60
    - Task 2 (duplicate_detective): ~0.25-0.40
    - Task 3 (integrity_auditor):   ~0.10-0.25

Strategy:
    Phase 1 — Inspect all rows in batches of 10
    Phase 2 — Apply heuristic detectors (null, format, range, cross-field)
    Phase 3 — Diagnose detected issues
    Phase 4 — Fix issues with computed values where possible

Usage::

    python heuristic_baseline.py                   # Single run, all tasks
    python heuristic_baseline.py --seeds 10        # 10 seeds per task
    python heuristic_baseline.py --task task_1_format_fixer
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from typing import Any

from .models import DataQualityAction, IssueType
from .server.data_quality_environment import DataQualityEnvironment


TASKS = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]

EMAIL_RE = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PHONE_RE = re.compile(r"^\+1-\d{3}-\d{3}-\d{4}$")


# ── Heuristic Detectors ─────────────────────────────────────────────────


def detect_nulls(rows: list[dict], schema: dict) -> list[dict]:
    """Find null or empty-string values (high confidence only)."""
    issues = []
    # Only flag nulls in columns likely to be important
    important_cols = {
        "email",
        "name",
        "first_name",
        "last_name",
        "phone",
        "address",
        "order_total",
        "unit_price",
        "quantity",
    }
    for i, row in enumerate(rows):
        idx = row.get("_row_index", i)
        for col in schema:
            if col not in important_cols:
                continue
            val = row.get(col)
            if val is None or val == "":
                issues.append(
                    {
                        "row": idx,
                        "column": col,
                        "type": "missing_value",
                        "fix": None,
                        "confidence": 0.9,
                    }
                )
    return issues


def detect_format_errors(rows: list[dict], schema: dict) -> list[dict]:
    """Detect common format issues (email, phone, date)."""
    issues = []
    for i, row in enumerate(rows):
        idx = row.get("_row_index", i)
        for col, col_type in schema.items():
            val = row.get(col)
            if val is None:
                continue

            sval = str(val)

            if col_type == "string" and "email" in col.lower():
                if not EMAIL_RE.match(sval):
                    fixed = sval.strip().lower()
                    fixed = fixed.replace(" ", "").replace("..", ".")
                    fix_val = fixed if EMAIL_RE.match(fixed) else None
                    issues.append(
                        {
                            "row": idx,
                            "column": col,
                            "type": "format_error",
                            "fix": fix_val,
                            "confidence": 0.85,
                        }
                    )

            if col_type == "date":
                if not DATE_RE.match(sval):
                    issues.append(
                        {
                            "row": idx,
                            "column": col,
                            "type": "type_mismatch",
                            "fix": None,
                            "confidence": 0.9,
                        }
                    )

            if col_type == "string" and "phone" in col.lower():
                if isinstance(val, (int, float)):
                    issues.append(
                        {
                            "row": idx,
                            "column": col,
                            "type": "type_mismatch",
                            "fix": None,
                            "confidence": 0.9,
                        }
                    )

            if col_type == "integer" and not isinstance(val, int):
                issues.append(
                    {
                        "row": idx,
                        "column": col,
                        "type": "type_mismatch",
                        "fix": None,
                        "confidence": 0.9,
                    }
                )

    return issues


def detect_cross_field(
    rows: list[dict], schema: dict, business_rules: dict, secondary: list[dict]
) -> list[dict]:
    """Detect cross-field consistency issues (task 3 focused)."""
    issues = []

    product_by_id = {}
    for p in secondary:
        pid = p.get("product_id") or p.get("_row_index")
        if pid is not None:
            product_by_id[pid] = p

    for i, row in enumerate(rows):
        idx = row.get("_row_index", i)

        # order_total formula check
        qty = row.get("quantity")
        price = row.get("unit_price")
        discount = row.get("discount_pct")
        total = row.get("order_total")

        if all(v is not None for v in [qty, price, discount, total]):
            try:
                fqty, fprice, fdisc, ftotal = (
                    float(qty),
                    float(price),
                    float(discount),
                    float(total),
                )
                expected_total = round(fqty * fprice * (1 - fdisc / 100), 2)
                diff = abs(ftotal - expected_total)
                # Only flag large mismatches (>1.0) to avoid FPs from rounding
                if diff > 1.0:
                    issues.append(
                        {
                            "row": idx,
                            "column": "order_total",
                            "type": "cross_field",
                            "fix": f"{expected_total:.2f}",
                            "confidence": min(1.0, diff / 10.0),
                        }
                    )
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        # ship_date >= order_date
        odate = row.get("order_date")
        sdate = row.get("ship_date")
        if odate and sdate and isinstance(odate, str) and isinstance(sdate, str):
            if sdate < odate:
                issues.append(
                    {
                        "row": idx,
                        "column": "ship_date",
                        "type": "cross_field",
                        "fix": odate,
                        "confidence": 0.95,
                    }
                )

        # product_category cross-reference
        pid = row.get("product_id")
        pcat = row.get("product_category")
        if pid in product_by_id and pcat:
            real_cat = product_by_id[pid].get("category")
            if real_cat and real_cat != pcat:
                issues.append(
                    {
                        "row": idx,
                        "column": "product_category",
                        "type": "cross_field",
                        "fix": real_cat,
                        "confidence": 0.98,
                    }
                )

        # referential integrity — only flag clearly invalid IDs
        # (negative or unreasonably large) since we see limited products
        if pid is not None and (pid < 0 or pid > 9000):
            issues.append(
                {
                    "row": idx,
                    "column": "product_id",
                    "type": "referential_integrity",
                    "fix": None,
                    "confidence": 1.0,
                }
            )

    # Business rule checks
    max_discount = business_rules.get("max_discount_pct")
    min_qty = business_rules.get("min_quantity")
    max_qty = business_rules.get("max_quantity")
    min_price = business_rules.get("min_unit_price")
    year_range = business_rules.get("valid_order_year_range")

    for i, row in enumerate(rows):
        idx = row.get("_row_index", i)

        discount = row.get("discount_pct")
        if discount is not None and max_discount is not None:
            if float(discount) > max_discount:
                issues.append(
                    {
                        "row": idx,
                        "column": "discount_pct",
                        "type": "business_rule",
                        "fix": str(max_discount),
                        "confidence": 1.0,
                    }
                )
            if float(discount) < 0:
                issues.append(
                    {
                        "row": idx,
                        "column": "discount_pct",
                        "type": "business_rule",
                        "fix": "0",
                        "confidence": 1.0,
                    }
                )

        qty = row.get("quantity")
        if qty is not None:
            if max_qty and int(qty) > max_qty:
                issues.append(
                    {
                        "row": idx,
                        "column": "quantity",
                        "type": "business_rule",
                        "fix": str(max_qty),
                        "confidence": 1.0,
                    }
                )
            if min_qty and int(qty) < min_qty:
                issues.append(
                    {
                        "row": idx,
                        "column": "quantity",
                        "type": "business_rule",
                        "fix": str(min_qty),
                        "confidence": 1.0,
                    }
                )

        price = row.get("unit_price")
        if price is not None and min_price is not None:
            if float(price) < min_price:
                issues.append(
                    {
                        "row": idx,
                        "column": "unit_price",
                        "type": "business_rule",
                        "fix": f"{min_price:.2f}",
                        "confidence": 1.0,
                    }
                )

        odate = row.get("order_date")
        if odate and year_range and isinstance(odate, str):
            try:
                year = int(odate[:4])
                if year > year_range[1]:
                    issues.append(
                        {
                            "row": idx,
                            "column": "order_date",
                            "type": "business_rule",
                            "fix": f"{year_range[1]}{odate[4:]}",
                            "confidence": 1.0,
                        }
                    )
            except (ValueError, IndexError):
                pass

    return issues


def detect_duplicates(rows: list[dict], schema: dict) -> list[dict]:
    """Find exact or near-duplicate rows."""
    issues = []
    seen: dict[str, int] = {}

    key_cols = [
        c
        for c in schema
        if c in ("customer_id", "product_id", "order_date", "email", "name", "first_name")
    ]
    if not key_cols:
        key_cols = list(schema.keys())[:3]

    for i, row in enumerate(rows):
        idx = row.get("_row_index", i)
        key = tuple(str(row.get(c, "")) for c in key_cols)
        if key in seen:
            issues.append(
                {
                    "row": idx,
                    "column": "_row",
                    "type": "duplicate",
                    "fix": "DELETE_ROW",
                    "dup_of": seen[key],
                    "confidence": 0.95,
                }
            )
        else:
            seen[key] = idx

    return issues


# ── Episode Runner ───────────────────────────────────────────────────────


def run_episode(
    task_id: str,
    env_seed: int | None = None,
) -> dict[str, Any]:
    """Run one episode with heuristic strategy."""
    env = DataQualityEnvironment()
    obs = env.reset(task_id=task_id, seed=env_seed)

    total_rows = obs.total_rows or 50
    schema = dict(obs.schema_info) if obs.schema_info else {}
    all_rows: list[dict] = []
    secondary: list[dict] = []
    business_rules: dict = {}
    steps = 0

    # Phase 1: Inspect all rows
    for start in range(0, total_rows, 10):
        if obs.done:
            break
        indices = list(range(start, min(start + 10, total_rows)))
        obs = env.step(
            DataQualityAction(
                action_type="inspect",
                row_indices=indices,
            )
        )
        steps += 1
        if obs.visible_rows:
            all_rows.extend(obs.visible_rows)

    # Inspect secondary table and business rules (task 3)
    if not obs.done and task_id == "task_3_integrity_auditor":
        obs = env.step(
            DataQualityAction(
                action_type="inspect",
                row_indices=[0],
                related_table="products",
            )
        )
        steps += 1
        if obs.secondary_table_rows:
            secondary = list(obs.secondary_table_rows)

        obs = env.step(
            DataQualityAction(
                action_type="inspect",
                row_indices=[0],
                related_table="business_rules",
            )
        )
        steps += 1
        if obs.secondary_table_rows:
            business_rules = dict(obs.secondary_table_rows[0])

    # Phase 2: Detect issues heuristically
    detected: list[dict] = []
    detected.extend(detect_nulls(all_rows, schema))
    detected.extend(detect_format_errors(all_rows, schema))
    detected.extend(detect_duplicates(all_rows, schema))
    if task_id == "task_3_integrity_auditor":
        detected.extend(detect_cross_field(all_rows, schema, business_rules, secondary))

    # Deduplicate by (row, column), keep highest confidence
    seen_keys: dict[tuple[int, str], dict] = {}
    for d in detected:
        key = (d["row"], d["column"])
        if key not in seen_keys or d.get("confidence", 0) > seen_keys[key].get("confidence", 0):
            seen_keys[key] = d
    detected = sorted(seen_keys.values(), key=lambda x: -x.get("confidence", 0))

    # Phase 3-4: Diagnose and fix
    for issue in detected:
        if obs.done:
            break

        col = issue["column"] if issue["column"] != "_row" else list(schema.keys())[0]

        # Diagnose
        obs = env.step(
            DataQualityAction(
                action_type="diagnose",
                row_index=issue["row"],
                column_name=col,
                issue_type=issue["type"],
            )
        )
        steps += 1

        # Fix if we have a value
        if not obs.done and issue.get("fix"):
            if issue["fix"] == "DELETE_ROW":
                obs = env.step(
                    DataQualityAction(
                        action_type="fix",
                        row_index=issue["row"],
                        column_name=col,
                        fix_type="delete_row",
                        justification="Duplicate row detected",
                    )
                )
            else:
                obs = env.step(
                    DataQualityAction(
                        action_type="fix",
                        row_index=issue["row"],
                        column_name=col,
                        fix_type="correct_value",
                        new_value=str(issue["fix"]),
                        justification="Heuristic fix",
                    )
                )
            steps += 1

    # Finalize
    if not obs.done:
        obs = env.step(DataQualityAction(action_type="finalize"))
        steps += 1

    return {
        "task_id": task_id,
        "score": round(float(obs.cumulative_reward), 4),
        "steps": steps,
        "issues_detected": len(detected),
        "env_seed": env_seed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Heuristic baseline agent")
    parser.add_argument("--seeds", type=int, default=1, help="Number of seeds per task")
    parser.add_argument("--task", type=str, default=None, help="Run only this task")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    tasks = [args.task] if args.task else TASKS
    all_results: list[dict] = []
    start = time.time()

    for task_id in tasks:
        for s in range(args.seeds):
            env_seed = s if args.seeds > 1 else None
            result = run_episode(task_id, env_seed=env_seed)
            all_results.append(result)
            if not args.json:
                print(
                    f"  {task_id:35s} seed={s:3d}  "
                    f"score={result['score']:.4f}  "
                    f"steps={result['steps']}  "
                    f"detected={result['issues_detected']}"
                )

    elapsed = time.time() - start

    if args.json:
        print(json.dumps(all_results, indent=2))
    else:
        print(f"\n{'=' * 70}")
        for task_id in tasks:
            task_results = [r for r in all_results if r["task_id"] == task_id]
            scores = [r["score"] for r in task_results]
            mean = sum(scores) / len(scores) if scores else 0
            std = (
                (sum((s - mean) ** 2 for s in scores) / max(len(scores) - 1, 1)) ** 0.5
                if len(scores) > 1
                else 0
            )
            print(f"  {task_id:35s}  mean={mean:.4f} +/- {std:.4f}  n={len(scores)}")
        print(f"  Elapsed: {elapsed:.1f}s")
        print(f"{'=' * 70}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
