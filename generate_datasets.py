# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Generate deterministic static datasets for the Data Quality environment.

Run once::

    python generate_datasets.py

Outputs::

    datasets/task1_customers.json
    datasets/task1_ground_truth.json
    datasets/task2_contacts.json
    datasets/task2_ground_truth.json
    datasets/task3_orders.json
    datasets/task3_products.json
    datasets/task3_ground_truth.json

DESIGN PRINCIPLES:
    1. ``random.seed(42)`` for full reproducibility.
    2. Clean data generated first, issues planted at exact indices.
    3. Every ground truth entry has unambiguous detection criteria.
    4. ``expected`` key present ONLY when the correct fix is logically derivable
       from the dataset itself (or from explicit ``business_rules`` metadata).
    5. Issues without derivable fixes are detection-only (no ``expected`` key).
    6. All original values saved via ``corrupt()`` before mutation — provenance
       tracking eliminates an entire class of ground-truth bugs.
    7. All ``expected`` values are canonical strings via ``canonical_str()``.
    8. Post-generation ``validate_ground_truth()`` ensures structural integrity.

DESIGN CORRECTIONS:
    [FIX-02] Indefensible ground truth removed — detection-only where ambiguous.
    [FIX-03] Duplicate row diagnosis uses ``column="_row"`` sentinel.
    [FIX-04] All expected values saved before corruption, never from corrupted data.
    [FIX-05] Near-duplicate emails explicitly match planted names (v2 fix).
    [FIX-06] Consistent ``canonical_str()`` formatting for all expected values.
    [FIX-07] Self-validating ground truth with automated integrity checks.
"""

from __future__ import annotations

import json
import os
import random
import sys
from datetime import datetime, timezone
from typing import Any

# ═══════════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════════

SEED: int = 42
VERSION: str = "3.0"

DATASETS_DIR: str = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "datasets"
)

# Must match IssueType enum in models.py (Phase 2)
VALID_ISSUE_TYPES: frozenset[str] = frozenset({
    "format_error", "missing_value", "duplicate", "near_duplicate",
    "type_mismatch", "outlier", "referential_integrity", "cross_field",
    "business_rule",
})

# ═══════════════════════════════════════════════════════════════════════════════
# Helper Data Pools
# ═══════════════════════════════════════════════════════════════════════════════

FIRST_NAMES: list[str] = [
    "Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George",
    "Hannah", "Ivan", "Julia", "Kevin", "Laura", "Michael", "Nora",
    "Oscar", "Patricia", "Quincy", "Rachel", "Steven", "Tina",
    "Uma", "Victor", "Wendy", "Xavier", "Yolanda", "Zachary",
    "Amelia", "Brian", "Cynthia", "David", "Elena", "Frank",
    "Grace", "Henry", "Irene", "James", "Karen", "Leonard",
    "Maria", "Nathan", "Olivia", "Peter", "Quinn", "Robert",
    "Susan", "Thomas", "Ursula", "Vincent", "Wanda", "Xander",
]

LAST_NAMES: list[str] = [
    "Johnson", "Smith", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson",
    "Martin", "Lee", "Perez", "Thompson", "White", "Harris",
    "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres",
    "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson",
    "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter",
    "Roberts", "Phillips",
]

DOMAINS: list[str] = [
    "example.com", "gmail.com", "yahoo.com", "outlook.com",
    "company.org", "mail.net",
]
COUNTRIES: list[str] = [
    "US", "CA", "UK", "AU", "DE", "FR", "JP", "BR", "IN", "MX",
]
CITIES: list[str] = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "San Antonio", "Dallas", "San Jose", "Austin", "Jacksonville",
    "San Francisco", "Seattle", "Denver", "Boston", "Portland",
    "Atlanta", "Miami", "Detroit", "Minneapolis", "Nashville",
]
STREETS: list[str] = [
    "Main St", "Oak Ave", "Elm Dr", "Pine Rd", "Maple Ln",
    "Cedar Blvd", "Birch Way", "Walnut Ct", "Cherry Pl", "Spruce Ter",
]
CATEGORIES: list[str] = [
    "Electronics", "Furniture", "Clothing", "Books", "Sports", "Kitchen",
]
PRODUCT_NAMES: dict[str, list[str]] = {
    "Electronics": ["Laptop Pro", "Wireless Mouse", "USB Hub",
                    "Monitor 27in", "Keyboard"],
    "Furniture":   ["Office Chair", "Standing Desk", "Bookshelf",
                    "Filing Cabinet", "Desk Lamp"],
    "Clothing":    ["Cotton T-Shirt", "Denim Jeans", "Wool Sweater",
                    "Rain Jacket", "Sneakers"],
    "Books":       ["Python Guide", "Data Science Handbook", "AI Primer",
                    "ML Textbook", "Stats 101"],
    "Sports":      ["Yoga Mat", "Dumbbells 10lb", "Jump Rope",
                    "Resistance Bands", "Water Bottle"],
    "Kitchen":     ["Chef Knife", "Cutting Board", "Mixing Bowl Set",
                    "Blender", "Toaster"],
}


# ═══════════════════════════════════════════════════════════════════════════════
# Random Generators — clean-data primitives
# ═══════════════════════════════════════════════════════════════════════════════

def rand_email(first: str, last: str) -> str:
    """Generate a plausible email from a name.  Always contains exactly one @."""
    sep = random.choice([".", "_", ""])
    domain = random.choice(DOMAINS)
    return f"{first.lower()}{sep}{last.lower()}@{domain}"


def rand_phone() -> str:
    """Generate a US-format phone string: +1-555-XXX-XXXX."""
    return f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def rand_date(start_year: int = 1960, end_year: int = 2004) -> str:
    """Generate an ISO date (YYYY-MM-DD).  Day capped at 28 to avoid invalids."""
    y = random.randint(start_year, end_year)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y:04d}-{m:02d}-{d:02d}"


def rand_zip() -> str:
    """Generate a 5-digit US-style zip code string."""
    return f"{random.randint(10000, 99999)}"


def rand_address() -> str:
    """Generate a simple street address string."""
    return f"{random.randint(1, 9999)} {random.choice(STREETS)}"


# ═══════════════════════════════════════════════════════════════════════════════
# Provenance & Encoding Utilities
# ═══════════════════════════════════════════════════════════════════════════════

# Global provenance registry: task_name → {(row_idx, column) → original_value}
_originals: dict[str, dict[tuple[int, str], Any]] = {}


def corrupt(
    task: str,
    rows: list[dict[str, Any]],
    idx: int,
    column: str,
    new_value: Any,
) -> Any:
    """Corrupt a single cell, saving the original value for ground-truth derivation.

    Args:
        task: Task identifier (e.g. "task1") for provenance namespacing.
        rows: The mutable row list.
        idx: Row index to corrupt.
        column: Column name to corrupt.
        new_value: The corrupted value to plant.

    Returns:
        The original value before corruption.

    Raises:
        ValueError: If the same (task, idx, column) has already been corrupted
            (detects accidental double-corruption bugs).
    """
    _originals.setdefault(task, {})
    key = (idx, column)
    if key in _originals[task]:
        raise ValueError(
            f"Double corruption at {task}[{idx}][{column}] — "
            f"original was {_originals[task][key]!r}, "
            f"attempted new value {new_value!r}"
        )
    original = rows[idx][column]
    _originals[task][key] = original
    rows[idx][column] = new_value
    return original


def get_original(task: str, idx: int, column: str) -> Any:
    """Retrieve the original value before corruption.

    Raises:
        KeyError: If no corruption was recorded for this cell.
    """
    return _originals[task][(idx, column)]


def canonical_str(value: Any) -> str:
    """Convert any value to a canonical string for ground-truth comparison.

    Rules:
        - ``float`` → always 2 decimal places (e.g. ``"50.00"``)
        - everything else → ``str(value)``

    This ensures the environment's equality check is consistent regardless
    of whether the original was ``50.0``, ``50``, or ``50.00``.
    """
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


# ═══════════════════════════════════════════════════════════════════════════════
# Ground-Truth Validation
# ═══════════════════════════════════════════════════════════════════════════════

def validate_ground_truth(
    ground_truth: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    schema: dict[str, str],
    task_label: str,
) -> None:
    """Validate ground-truth integrity.  Raises ``AssertionError`` on failure.

    Checks:
        1. Required keys present (``row``, ``column``, ``type``, ``description``).
        2. Row index in range ``[0, len(rows))``.
        3. Column exists in schema (or is ``_row`` sentinel).
        4. ``type`` value matches ``IssueType`` enum vocabulary.
        5. ``expected``, when present, is a ``str``.
        6. No duplicate ``(row, column)`` entries.
    """
    seen: set[tuple[int, str]] = set()
    for i, entry in enumerate(ground_truth):
        prefix = f"{task_label}[{i}]"

        # Required keys
        for k in ("row", "column", "type", "description"):
            assert k in entry, f"{prefix}: missing required key '{k}'"

        # Row range
        assert 0 <= entry["row"] < len(rows), (
            f"{prefix}: row {entry['row']} out of range [0, {len(rows)})"
        )

        # Column in schema or _row sentinel
        if entry["column"] != "_row":
            assert entry["column"] in schema, (
                f"{prefix}: column '{entry['column']}' not in schema "
                f"{set(schema.keys())}"
            )

        # Issue type validity
        assert entry["type"] in VALID_ISSUE_TYPES, (
            f"{prefix}: invalid type '{entry['type']}'"
        )

        # Expected value must be a string
        if "expected" in entry:
            assert isinstance(entry["expected"], str), (
                f"{prefix}: expected must be str, "
                f"got {type(entry['expected']).__name__}: {entry['expected']!r}"
            )

        # No duplicate (row, column) entries
        key = (entry["row"], entry["column"])
        assert key not in seen, (
            f"{prefix}: duplicate entry for (row={entry['row']}, "
            f"column='{entry['column']}')"
        )
        seen.add(key)

    print(f"  ✓ {task_label}: {len(ground_truth)} entries validated")


# ═══════════════════════════════════════════════════════════════════════════════
# I/O Utilities
# ═══════════════════════════════════════════════════════════════════════════════

_GENERATION_TIMESTAMP: str = datetime.now(timezone.utc).isoformat()


def _make_meta(**extra: Any) -> dict[str, Any]:
    """Build a ``_meta`` dict with standard provenance fields."""
    meta = {
        "generator": "generate_datasets.py",
        "version": VERSION,
        "seed": SEED,
        "generated_at": _GENERATION_TIMESTAMP,
        "row_indexing": "0-based",
    }
    meta.update(extra)
    return meta


def _write_dataset(
    filename: str,
    schema: dict[str, str],
    rows: list[dict[str, Any]],
    **extra: Any,
) -> None:
    """Write a dataset file with metadata envelope."""
    payload: dict[str, Any] = {
        "_meta": _make_meta(row_count=len(rows)),
        "schema": schema,
    }
    payload.update(extra)  # e.g. business_rules
    payload["rows"] = rows
    path = os.path.join(DATASETS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _write_ground_truth(
    filename: str,
    issues: list[dict[str, Any]],
) -> None:
    """Write a ground-truth file with metadata envelope."""
    fixable = sum(1 for g in issues if "expected" in g)
    payload = {
        "_meta": _make_meta(
            total_issues=len(issues),
            fixable_issues=fixable,
            detection_only_issues=len(issues) - fixable,
        ),
        "issues": issues,
    }
    path = os.path.join(DATASETS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 1: FORMAT FIXER — 50 rows, 8 planted issues
#
# 5 fixable (expected derivable from data), 3 detection-only
# [FIX-02] phone / zip detection-only (correct value not derivable)
# [FIX-04] all expected values provenance-tracked
# ═══════════════════════════════════════════════════════════════════════════════

def generate_task1() -> None:
    """Generate the Format Fixer dataset (Task 1).

    Scenario: A customer database with format violations in emails, phones,
    dates, and zip codes.  The agent must detect and fix formatting issues.
    """
    schema = {
        "name": "string", "email": "string", "phone": "string",
        "date_of_birth": "date", "country": "string", "zip_code": "string",
    }

    rows: list[dict[str, Any]] = []
    for i in range(50):
        first = FIRST_NAMES[i % len(FIRST_NAMES)]
        last = LAST_NAMES[i % len(LAST_NAMES)]
        rows.append({
            "name": f"{first} {last}",
            "email": rand_email(first, last),
            "phone": rand_phone(),
            "date_of_birth": rand_date(),
            "country": random.choice(COUNTRIES),
            "zip_code": rand_zip(),
        })

    # ── Plant issues (provenance-tracked) ─────────────────────────────────

    # FIXABLE: expected value is logically derivable
    corrupt("task1", rows, 3,  "email",         "john.doeexample.com")
    corrupt("task1", rows, 11, "email",         "sarah@@gmail.com")
    corrupt("task1", rows, 15, "date_of_birth", "2000-02-30")
    corrupt("task1", rows, 31, "date_of_birth", "1990-04-31")    # [C-6 fix]
    corrupt("task1", rows, 40, "zip_code",      "1234")

    # DETECTION-ONLY: correct value not derivable [FIX-02]
    corrupt("task1", rows, 7,  "phone",    "+1-555-12345")
    corrupt("task1", rows, 22, "phone",    "555-ABC-1234")
    corrupt("task1", rows, 45, "zip_code", "ABCDE")

    # ── Ground truth ──────────────────────────────────────────────────────

    ground_truth: list[dict[str, Any]] = [
        # --- FIXABLE ---
        {"row": 3, "column": "email", "type": "format_error",
         "original": "john.doeexample.com",
         "expected": "john.doe@example.com",
         "description": (
             "Missing @ — 'doeexample' unambiguously splits at "
             "'doe' + 'example' (known domain)"
         )},
        {"row": 11, "column": "email", "type": "format_error",
         "original": "sarah@@gmail.com",
         "expected": "sarah@gmail.com",
         "description": "Double @@ — remove duplicate @ symbol"},
        {"row": 15, "column": "date_of_birth", "type": "format_error",
         "original": "2000-02-30",
         "expected": "2000-02-29",
         "description": (
             "Feb 30 invalid; 2000 is a leap year (divisible by 400) "
             "so maximum valid day is Feb 29"
         )},
        {"row": 31, "column": "date_of_birth", "type": "format_error",
         "original": "1990-04-31",
         "expected": "1990-04-30",
         "description": (
             "April 31 invalid — April has exactly 30 days; "
             "clamp to maximum valid day"
         )},
        {"row": 40, "column": "zip_code", "type": "format_error",
         "original": "1234",
         "expected": "01234",
         "description": (
             "Zip code has only 4 digits — expected 5-digit format "
             "per dataset schema; likely missing leading zero"
         )},

        # --- DETECTION-ONLY [FIX-02] ---
        {"row": 7, "column": "phone", "type": "format_error",
         "original": "+1-555-12345",
         "description": (
             "Phone number incomplete — missing digit group; "
             "correct number unknown without external data"
         )},
        {"row": 22, "column": "phone", "type": "format_error",
         "original": "555-ABC-1234",
         "description": (
             "Phone contains letters — correct digits unknown "
             "without external data"
         )},
        {"row": 45, "column": "zip_code", "type": "format_error",
         "original": "ABCDE",
         "description": (
             "Zip code is all letters — correct code unknown "
             "without external data"
         )},
    ]

    validate_ground_truth(ground_truth, rows, schema, "task1")
    _write_dataset("task1_customers.json", schema, rows)
    _write_ground_truth("task1_ground_truth.json", ground_truth)

    fixable = sum(1 for g in ground_truth if "expected" in g)
    print(
        f"Task 1: {len(rows)} rows, {len(ground_truth)} issues "
        f"({fixable} fixable, {len(ground_truth) - fixable} detection-only)"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 2: DUPLICATE DETECTIVE — 100 rows, 12 planted issues
#
# [FIX-03] Exact duplicates: column="_row" sentinel
# [FIX-05] Near-duplicate emails explicitly match planted names
# [FIX-02] Missing values / type mismatches: detection-only
# ═══════════════════════════════════════════════════════════════════════════════

def generate_task2() -> None:
    """Generate the Duplicate Detective dataset (Task 2).

    Scenario: A contacts database with exact duplicates, near-duplicate
    records (typos/reformatting), missing values, and type mismatches.
    """
    schema = {
        "id": "integer", "first_name": "string", "last_name": "string",
        "email": "string", "phone": "string", "address": "string",
        "city": "string", "registration_date": "date",
    }

    rows: list[dict[str, Any]] = []
    for i in range(100):
        # Batch-offset indexing: rows 50-99 shift last names by 1 to avoid
        # accidental name collisions with rows 0-49.
        batch = i // len(FIRST_NAMES)
        first = FIRST_NAMES[i % len(FIRST_NAMES)]
        last = LAST_NAMES[(i + batch) % len(LAST_NAMES)]
        rows.append({
            "id": i + 1,
            "first_name": first,
            "last_name": last,
            "email": rand_email(first, last),
            "phone": rand_phone(),
            "address": rand_address(),
            "city": random.choice(CITIES),
            "registration_date": rand_date(2020, 2025),
        })

    # ── Exact duplicates (DELETE_ROW) ─────────────────────────────────────
    # Copy content from source row, preserve unique ID.
    # These are NOT tracked via corrupt() since they replace entire rows.

    for dup_idx, src_idx in [(12, 5), (34, 20), (67, 50)]:
        rows[dup_idx] = dict(rows[src_idx])
        rows[dup_idx]["id"] = dup_idx + 1  # keep unique ID

    # ── Near-duplicates (fixable — derivable from matching row) ───────────
    # [FIX-05] All names and emails explicitly set for self-consistency.

    # Pair 1: "John Walker" (row 25) vs "Jon Walker" (row 8), same email
    corrupt("task2", rows, 25, "first_name", "John")
    corrupt("task2", rows, 25, "last_name",  "Walker")
    corrupt("task2", rows, 25, "email",      "john.walker@example.com")
    corrupt("task2", rows, 8,  "first_name", "Jon")       # typo
    corrupt("task2", rows, 8,  "last_name",  "Walker")
    corrupt("task2", rows, 8,  "email",      "john.walker@example.com")

    # Pair 2: "Sarah Miller" (row 30) vs same name, email domain typo (row 42)
    corrupt("task2", rows, 30, "first_name", "Sarah")
    corrupt("task2", rows, 30, "last_name",  "Miller")
    corrupt("task2", rows, 30, "email",      "sarah.miller@gmail.com")
    corrupt("task2", rows, 42, "first_name", "Sarah")
    corrupt("task2", rows, 42, "last_name",  "Miller")
    corrupt("task2", rows, 42, "email",      "sarah.miller@gmal.com")

    # Pair 3: "David Chen" (row 60) vs same name, phone reformatted (row 78)
    corrupt("task2", rows, 60, "first_name", "David")
    corrupt("task2", rows, 60, "last_name",  "Chen")
    corrupt("task2", rows, 60, "phone",      "+1-555-123-4567")
    corrupt("task2", rows, 78, "first_name", "David")
    corrupt("task2", rows, 78, "last_name",  "Chen")
    corrupt("task2", rows, 78, "phone",      "15551234567")

    # ── Missing values (detection-only) [FIX-02] ─────────────────────────
    corrupt("task2", rows, 19, "email",   None)
    corrupt("task2", rows, 55, "phone",   "")
    corrupt("task2", rows, 88, "address", None)

    # ── Type mismatches (detection-only) [FIX-02] ────────────────────────
    corrupt("task2", rows, 27, "registration_date", "not_a_date")
    corrupt("task2", rows, 63, "id",                "abc")
    corrupt("task2", rows, 91, "phone",             12345)

    # ── Ground truth ──────────────────────────────────────────────────────

    ground_truth: list[dict[str, Any]] = [
        # Exact duplicates: column="_row" sentinel [FIX-03]
        {"row": 12, "column": "_row", "type": "duplicate",
         "expected": "DELETE_ROW", "duplicate_of": 5,
         "description": "Exact content duplicate of row 5 (different ID only)"},
        {"row": 34, "column": "_row", "type": "duplicate",
         "expected": "DELETE_ROW", "duplicate_of": 20,
         "description": "Exact content duplicate of row 20 (different ID only)"},
        {"row": 67, "column": "_row", "type": "duplicate",
         "expected": "DELETE_ROW", "duplicate_of": 50,
         "description": "Exact content duplicate of row 50 (different ID only)"},

        # Near-duplicates: fixable (derivable from anchor row)
        {"row": 8, "column": "first_name", "type": "near_duplicate",
         "expected": "John",
         "description": (
             "Typo 'Jon' → 'John'; shares email john.walker@example.com "
             "and last name 'Walker' with row 25"
         )},
        {"row": 42, "column": "email", "type": "near_duplicate",
         "expected": "sarah.miller@gmail.com",
         "description": (
             "Domain typo 'gmal.com' → 'gmail.com'; same person as row 30 "
             "(identical name 'Sarah Miller')"
         )},
        {"row": 78, "column": "phone", "type": "near_duplicate",
         "expected": "+1-555-123-4567",
         "description": (
             "Unformatted phone '15551234567' → '+1-555-123-4567'; "
             "same person as row 60 (identical name 'David Chen')"
         )},

        # Missing values: detection-only [FIX-02]
        {"row": 19, "column": "email", "type": "missing_value",
         "description": "Email is null — critical field for contact record"},
        {"row": 55, "column": "phone", "type": "missing_value",
         "description": "Phone is empty string — functionally missing"},
        {"row": 88, "column": "address", "type": "missing_value",
         "description": "Address is null — required for mailing"},

        # Type mismatches: detection-only [FIX-02]
        {"row": 27, "column": "registration_date", "type": "type_mismatch",
         "description": "Value 'not_a_date' is not a valid ISO date"},
        {"row": 63, "column": "id", "type": "type_mismatch",
         "description": "ID 'abc' should be integer per schema"},
        {"row": 91, "column": "phone", "type": "type_mismatch",
         "description": (
             "Phone stored as integer 12345 instead of string — "
             "schema requires string type"
         )},
    ]

    validate_ground_truth(ground_truth, rows, schema, "task2")
    _write_dataset("task2_contacts.json", schema, rows)
    _write_ground_truth("task2_ground_truth.json", ground_truth)

    fixable = sum(1 for g in ground_truth if "expected" in g)
    print(
        f"Task 2: {len(rows)} rows, {len(ground_truth)} issues "
        f"({fixable} fixable, {len(ground_truth) - fixable} detection-only)"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 3: INTEGRITY AUDITOR — 150 orders + 30 products, 15 planted issues
#
# [FIX-04] All originals saved BEFORE corruption via corrupt()
# [FIX-06] All expected values via canonical_str()
# ═══════════════════════════════════════════════════════════════════════════════

def generate_task3() -> None:
    """Generate the Integrity Auditor dataset (Task 3).

    Scenario: An orders database with a linked products table.  Issues include
    referential integrity violations, cross-field inconsistencies, outliers,
    business rule violations, and category mismatches.

    Business rules are encoded in dataset metadata so the agent can derive fixes.
    """
    # ── Products table ────────────────────────────────────────────────────

    products: list[dict[str, Any]] = []
    pid = 1
    for cat in CATEGORIES:
        for name in PRODUCT_NAMES[cat]:
            products.append({
                "product_id": pid,
                "product_name": name,
                "category": cat,
                "base_price": round(random.uniform(10, 500), 2),
                "stock": random.randint(0, 1000),
            })
            pid += 1

    valid_pids = {p["product_id"] for p in products}

    # Product lookup for cross-field validation
    product_by_id: dict[int, dict[str, Any]] = {
        p["product_id"]: p for p in products
    }

    # ── Orders table (clean) ──────────────────────────────────────────────

    orders_schema = {
        "order_id": "integer", "product_id": "integer",
        "quantity": "integer", "unit_price": "float",
        "order_total": "float", "discount_pct": "float",
        "order_date": "date", "ship_date": "date",
        "product_category": "string",
    }

    business_rules = {
        "max_discount_pct": 50,
        "valid_order_year_range": [2024, 2025],
        "min_quantity": 1,
        "max_quantity": 100,
        "order_total_formula": "quantity * unit_price * (1 - discount_pct / 100)",
        "ship_date_constraint": "ship_date >= order_date",
    }

    orders: list[dict[str, Any]] = []
    for i in range(150):
        prod = random.choice(products)
        qty = random.randint(1, 10)
        price = prod["base_price"]
        discount = random.choice([0, 0, 0, 5, 10, 15, 20])
        total = round(qty * price * (1 - discount / 100), 2)
        odate = rand_date(2024, 2025)
        y, m, d = map(int, odate.split("-"))
        sd = min(d + random.randint(1, 14), 28)
        sdate = f"{y:04d}-{m:02d}-{sd:02d}"

        orders.append({
            "order_id": i + 1,
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": price,
            "order_total": total,
            "discount_pct": discount,
            "order_date": odate,
            "ship_date": sdate,
            "product_category": prod["category"],
        })

    # ── 1–3: Referential integrity (detection-only) ──────────────────────

    corrupt("task3", orders, 14,  "product_id", 999)
    corrupt("task3", orders, 56,  "product_id", -1)
    corrupt("task3", orders, 112, "product_id", 9999)

    # ── 4–6: Cross-field errors (fixable — derivable) ────────────────────

    # Row 23: corrupt total (save original BEFORE corruption) [FIX-04]
    orig_total_23 = corrupt(
        "task3", orders, 23, "order_total",
        round(orders[23]["order_total"] * 3.7, 2),
    )

    # Row 78: ship date before order date
    corrupt("task3", orders, 78, "ship_date", "2023-01-01")
    # Expected = order_date (minimum valid ship date)
    expected_ship_78 = orders[78]["order_date"]

    # Row 130: negative total (sign flip) [FIX-04]
    orig_total_130 = orders[130]["order_total"]
    corrupt("task3", orders, 130, "order_total", -orig_total_130)

    # ── 7–9: Outliers (fixable — derivable from cross-field formula) ─────
    # [FIX-04] Originals saved; expected = original value.
    # Agent derives via: qty = round(total / (price * (1 - discount/100)))

    orig_qty_8 = corrupt("task3", orders, 8, "quantity", 99999)

    # Row 45: negative price (sign flip of original base_price)
    orig_price_45 = orders[45]["unit_price"]
    corrupt("task3", orders, 45, "unit_price", -orig_price_45)

    orig_qty_99 = corrupt("task3", orders, 99, "quantity", 0)

    # ── 10–12: Business rule violations (fixable — rules in metadata) ────

    corrupt("task3", orders, 33,  "discount_pct", 150)
    corrupt("task3", orders, 71,  "order_date",   "2035-01-01")
    orig_qty_140 = orders[140]["quantity"]
    corrupt("task3", orders, 140, "quantity",      -orig_qty_140)

    # ── 13–15: Category mismatches (fixable — derivable from products) ───
    # Keep original product_id, only corrupt the category field.
    # Expected = actual category of the product referenced by product_id.

    # Find rows with specific categories for deterministic setup
    cat_18_actual = product_by_id[orders[18]["product_id"]]["category"]
    cat_18_wrong = next(c for c in CATEGORIES if c != cat_18_actual)
    corrupt("task3", orders, 18, "product_category", cat_18_wrong)

    cat_62_actual = product_by_id[orders[62]["product_id"]]["category"]
    cat_62_wrong = next(c for c in CATEGORIES if c != cat_62_actual)
    corrupt("task3", orders, 62, "product_category", cat_62_wrong)

    cat_105_actual = product_by_id[orders[105]["product_id"]]["category"]
    corrupt("task3", orders, 105, "product_category", "")  # empty = missing

    # ── Ground truth ──────────────────────────────────────────────────────

    ground_truth: list[dict[str, Any]] = [
        # Referential integrity: detection-only
        {"row": 14, "column": "product_id", "type": "referential_integrity",
         "description": "References non-existent product_id 999"},
        {"row": 56, "column": "product_id", "type": "referential_integrity",
         "description": "product_id is -1 — invalid reference"},
        {"row": 112, "column": "product_id", "type": "referential_integrity",
         "description": "References non-existent product_id 9999"},

        # Cross-field: fixable (derivable from other columns)
        {"row": 23, "column": "order_total", "type": "cross_field",
         "expected": canonical_str(orig_total_23),
         "description": (
             "Total doesn't match qty × unit_price × (1 − discount/100); "
             f"correct value {canonical_str(orig_total_23)} derivable from "
             "row's own fields"
         )},
        {"row": 78, "column": "ship_date", "type": "cross_field",
         "expected": expected_ship_78,
         "description": (
             f"Ship date 2023-01-01 precedes order date {expected_ship_78}; "
             "minimum valid ship date = order date"
         )},
        {"row": 130, "column": "order_total", "type": "cross_field",
         "expected": canonical_str(orig_total_130),
         "description": (
             "Negative total (sign flip); correct value derivable from "
             "qty × unit_price × (1 − discount/100)"
         )},

        # Outliers: fixable (derivable from cross-field formula) [FIX-04]
        {"row": 8, "column": "quantity", "type": "outlier",
         "expected": str(orig_qty_8),
         "description": (
             f"Quantity 99999 is extreme outlier; correct value {orig_qty_8} "
             "derivable from round(total / (unit_price × (1 − discount/100)))"
         )},
        {"row": 45, "column": "unit_price", "type": "outlier",
         "expected": canonical_str(orig_price_45),
         "description": (
             f"Negative unit price (sign flip); correct value "
             f"{canonical_str(orig_price_45)} derivable from products table "
             f"base_price or abs(unit_price)"
         )},
        {"row": 99, "column": "quantity", "type": "outlier",
         "expected": str(orig_qty_99),
         "description": (
             f"Quantity 0 with positive total; correct value {orig_qty_99} "
             "derivable from round(total / (unit_price × (1 − discount/100)))"
         )},

        # Business rules: fixable (rules documented in business_rules metadata)
        {"row": 33, "column": "discount_pct", "type": "business_rule",
         "expected": str(business_rules["max_discount_pct"]),
         "description": (
             f"Discount 150% exceeds max_discount_pct "
             f"({business_rules['max_discount_pct']}%); "
             "clamped to business rule maximum"
         )},
        {"row": 71, "column": "order_date", "type": "business_rule",
         "expected": f"{business_rules['valid_order_year_range'][1]}-01-01",
         "description": (
             "Order date 2035-01-01 exceeds valid_order_year_range "
             f"{business_rules['valid_order_year_range']}; "
             "clamped to range maximum"
         )},
        {"row": 140, "column": "quantity", "type": "business_rule",
         "expected": str(orig_qty_140),
         "description": (
             f"Negative quantity (-{orig_qty_140}); sign flip — "
             "absolute value restores original"
         )},

        # Category mismatches: fixable (derivable from products table)
        {"row": 18, "column": "product_category", "type": "cross_field",
         "expected": cat_18_actual,
         "description": (
             f"Category '{cat_18_wrong}' but product "
             f"{orders[18]['product_id']} is {cat_18_actual} "
             "per products table"
         )},
        {"row": 62, "column": "product_category", "type": "cross_field",
         "expected": cat_62_actual,
         "description": (
             f"Category '{cat_62_wrong}' but product "
             f"{orders[62]['product_id']} is {cat_62_actual} "
             "per products table"
         )},
        {"row": 105, "column": "product_category", "type": "cross_field",
         "expected": cat_105_actual,
         "description": (
             f"Category is empty string but product "
             f"{orders[105]['product_id']} is {cat_105_actual} "
             "per products table"
         )},
    ]

    products_schema = {
        "product_id": "integer", "product_name": "string",
        "category": "string", "base_price": "float", "stock": "integer",
    }

    validate_ground_truth(ground_truth, orders, orders_schema, "task3")
    _write_dataset(
        "task3_orders.json", orders_schema, orders,
        business_rules=business_rules,
    )
    _write_dataset("task3_products.json", products_schema, products)
    _write_ground_truth("task3_ground_truth.json", ground_truth)

    fixable = sum(1 for g in ground_truth if "expected" in g)
    print(
        f"Task 3: {len(orders)} orders, {len(products)} products, "
        f"{len(ground_truth)} issues "
        f"({fixable} fixable, {len(ground_truth) - fixable} detection-only)"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Post-Generation Verification Suite
# ═══════════════════════════════════════════════════════════════════════════════

def verify_all() -> bool:
    """Load all generated files and run structural + semantic integrity checks.

    Returns:
        True if all checks pass, False otherwise.
    """
    SEP = "─" * 64
    print(f"\n{SEP}")
    print("  Post-Generation Verification Suite")
    print(SEP)

    passed = 0
    failed = 0

    def check(label: str, fn: Any) -> None:
        nonlocal passed, failed
        try:
            fn()
            print(f"  [OK]   {label}")
            passed += 1
        except Exception as exc:
            print(f"  [FAIL] {label}: {exc}")
            failed += 1

    # Gate 1: All 7 files exist
    expected_files = [
        "task1_customers.json", "task1_ground_truth.json",
        "task2_contacts.json", "task2_ground_truth.json",
        "task3_orders.json", "task3_products.json",
        "task3_ground_truth.json",
    ]
    for fname in expected_files:
        path = os.path.join(DATASETS_DIR, fname)
        check(
            f"File exists: {fname}",
            lambda p=path: _assert(os.path.isfile(p), f"missing: {p}"),
        )

    # Gate 2: All files are valid JSON with _meta
    loaded: dict[str, Any] = {}
    for fname in expected_files:
        path = os.path.join(DATASETS_DIR, fname)

        def _load(p: str = path, f: str = fname) -> None:
            with open(p, encoding="utf-8") as fh:
                data = json.load(fh)
            _assert("_meta" in data, f"{f} missing '_meta' key")
            loaded[f] = data

        check(f"Valid JSON with _meta: {fname}", _load)

    # Gate 3: Ground truth issue counts
    gt_expectations = {
        "task1_ground_truth.json": (8, 5),   # total, fixable
        "task2_ground_truth.json": (12, 6),
        "task3_ground_truth.json": (15, 12),
    }
    for fname, (exp_total, exp_fixable) in gt_expectations.items():
        def _check_counts(
            f: str = fname, et: int = exp_total, ef: int = exp_fixable,
        ) -> None:
            data = loaded[f]
            issues = data["issues"]
            _assert(len(issues) == et, f"expected {et} issues, got {len(issues)}")
            fixable = sum(1 for g in issues if "expected" in g)
            _assert(fixable == ef, f"expected {ef} fixable, got {fixable}")

        check(f"Issue counts: {fname} ({exp_total} total, {exp_fixable} fixable)",
              _check_counts)

    # Gate 4: All expected values are strings
    for fname in gt_expectations:
        def _check_types(f: str = fname) -> None:
            for g in loaded[f]["issues"]:
                if "expected" in g:
                    _assert(
                        isinstance(g["expected"], str),
                        f"row {g['row']}: expected is {type(g['expected']).__name__}"
                    )

        check(f"Expected values are strings: {fname}", _check_types)

    # Gate 5: All issue types match IssueType enum
    for fname in gt_expectations:
        def _check_issue_types(f: str = fname) -> None:
            for g in loaded[f]["issues"]:
                _assert(g["type"] in VALID_ISSUE_TYPES,
                        f"invalid type '{g['type']}'")

        check(f"Issue types valid: {fname}", _check_issue_types)

    # Gate 6: No duplicate (row, column) entries
    for fname in gt_expectations:
        def _check_no_dups(f: str = fname) -> None:
            keys = [(g["row"], g["column"]) for g in loaded[f]["issues"]]
            _assert(len(keys) == len(set(keys)), "duplicate (row, column) entries")

        check(f"No duplicate entries: {fname}", _check_no_dups)

    # Gate 7: Task 3 business_rules present
    def _check_business_rules() -> None:
        data = loaded["task3_orders.json"]
        _assert("business_rules" in data, "missing business_rules key")
        br = data["business_rules"]
        _assert("max_discount_pct" in br, "missing max_discount_pct")
        _assert("valid_order_year_range" in br, "missing valid_order_year_range")

    check("Task 3 business_rules metadata present", _check_business_rules)

    # Gate 8: Task 3 cross-field totals are mathematically verifiable
    def _check_cross_field_math() -> None:
        data = loaded["task3_orders.json"]
        gt = loaded["task3_ground_truth.json"]
        for g in gt["issues"]:
            if g["type"] == "cross_field" and g["column"] == "order_total":
                row = data["rows"][g["row"]]
                # Verify expected matches formula from uncorrupted fields
                # (qty and price may ALSO be corrupted, so just verify format)
                _assert(
                    "." in g["expected"],
                    f"row {g['row']}: expected total '{g['expected']}' "
                    "should have decimal point"
                )

    check("Task 3 cross-field expected values well-formed", _check_cross_field_math)

    # Summary
    print(SEP)
    total = passed + failed
    if failed == 0:
        print(f"  Status: ALL {total} CHECKS PASSED [OK]")
    else:
        print(f"  Status: {failed}/{total} CHECKS FAILED")
    print(f"{SEP}\n")

    return failed == 0


def _assert(condition: bool, msg: str = "") -> None:
    """Lightweight assertion that raises AssertionError with message."""
    if not condition:
        raise AssertionError(msg or "assertion failed")


# ═══════════════════════════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    random.seed(SEED)

    print("=" * 64)
    print("  Data Quality Environment — Dataset Generator v" + VERSION)
    print("=" * 64)
    print()

    os.makedirs(DATASETS_DIR, exist_ok=True)

    generate_task1()
    generate_task2()
    generate_task3()

    all_ok = verify_all()

    print(f"All datasets generated in: {DATASETS_DIR}")
    print(f"Files: {len(os.listdir(DATASETS_DIR))}")
    for f in sorted(os.listdir(DATASETS_DIR)):
        size = os.path.getsize(os.path.join(DATASETS_DIR, f))
        print(f"  {f:40s} {size:>8,d} bytes")

    if not all_ok:
        print("\n⚠ VERIFICATION FAILED — review errors above")
        sys.exit(1)
    else:
        print("\n✓ All verification gates passed")
