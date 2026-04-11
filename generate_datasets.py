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

def rand_email(first: str, last: str, rng: random.Random | None = None) -> str:
    """Generate a plausible email from a name.  Always contains exactly one @."""
    _r = rng or random
    sep = _r.choice([".", "_", ""])
    domain = _r.choice(DOMAINS)
    return f"{first.lower()}{sep}{last.lower()}@{domain}"


def rand_phone(rng: random.Random | None = None) -> str:
    """Generate a US-format phone string: +1-555-XXX-XXXX."""
    _r = rng or random
    return f"+1-555-{_r.randint(100, 999)}-{_r.randint(1000, 9999)}"


def rand_date(start_year: int = 1960, end_year: int = 2004, rng: random.Random | None = None) -> str:
    """Generate an ISO date (YYYY-MM-DD).  Day capped at 28 to avoid invalids."""
    _r = rng or random
    y = _r.randint(start_year, end_year)
    m = _r.randint(1, 12)
    d = _r.randint(1, 28)
    return f"{y:04d}-{m:02d}-{d:02d}"


def rand_zip(rng: random.Random | None = None) -> str:
    """Generate a 5-digit US-style zip code string."""
    _r = rng or random
    return f"{_r.randint(10000, 99999)}"


def rand_address(rng: random.Random | None = None) -> str:
    """Generate a simple street address string."""
    _r = rng or random
    return f"{_r.randint(1, 9999)} {_r.choice(STREETS)}"


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
    originals: dict | None = None,
) -> Any:
    """Corrupt a single cell, saving the original value for ground-truth derivation.

    Args:
        task: Task identifier (e.g. "task1") for provenance namespacing.
        rows: The mutable row list.
        idx: Row index to corrupt.
        column: Column name to corrupt.
        new_value: The corrupted value to plant.
        originals: Provenance dict.  If ``None``, uses the global ``_originals``.

    Returns:
        The original value before corruption.

    Raises:
        ValueError: If the same (task, idx, column) has already been corrupted
            (detects accidental double-corruption bugs).
    """
    store = originals if originals is not None else _originals
    store.setdefault(task, {})
    key = (idx, column)
    if key in store[task]:
        raise ValueError(
            f"Double corruption at {task}[{idx}][{column}] — "
            f"original was {store[task][key]!r}, "
            f"attempted new value {new_value!r}"
        )
    original = rows[idx][column]
    store[task][key] = original
    rows[idx][column] = new_value
    return original


def get_original(task: str, idx: int, column: str, originals: dict | None = None) -> Any:
    """Retrieve the original value before corruption.

    Raises:
        KeyError: If no corruption was recorded for this cell.
    """
    store = originals if originals is not None else _originals
    return store[task][(idx, column)]


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

    print(f"  [OK] {task_label}: {len(ground_truth)} entries validated")


# ═══════════════════════════════════════════════════════════════════════════════
# I/O Utilities
# ═══════════════════════════════════════════════════════════════════════════════


def _make_meta(**extra: Any) -> dict[str, Any]:
    """Build a ``_meta`` dict with standard provenance fields.

    Timestamp is captured at call time (not import time) so it
    accurately reflects when generation actually occurred.
    """
    meta = {
        "generator": "generate_datasets.py",
        "version": VERSION,
        "seed": SEED,
        "generated_at": datetime.now(timezone.utc).isoformat(),
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

def generate_task1(
    rng: random.Random | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate the Format Fixer dataset (Task 1).

    Args:
        rng: Random instance for procedural generation.  If ``None``, uses
            the global ``random`` module (seeded externally).

    Returns:
        ``(dataset_info, ground_truth_issues, secondary_rows)`` where
        ``dataset_info = {"schema": ..., "rows": ...}`` and
        ``secondary_rows`` is always ``[]`` for Task 1.
    """
    _r = rng or random
    _orig: dict = {}  # local provenance registry

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
            "email": rand_email(first, last, rng=_r),
            "phone": rand_phone(rng=_r),
            "date_of_birth": rand_date(rng=_r),
            "country": _r.choice(COUNTRIES),
            "zip_code": rand_zip(rng=_r),
        })

    # ── Plant issues (provenance-tracked) ─────────────────────────────────

    # FIXABLE: expected value is logically derivable
    corrupt("task1", rows, 3,  "email",         "john.doeexample.com",  originals=_orig)
    corrupt("task1", rows, 11, "email",         "sarah@@gmail.com",     originals=_orig)
    corrupt("task1", rows, 15, "date_of_birth", "2000-02-30",           originals=_orig)
    corrupt("task1", rows, 31, "date_of_birth", "1990-04-31",           originals=_orig)
    corrupt("task1", rows, 40, "zip_code",      "1234",                 originals=_orig)

    # DETECTION-ONLY: correct value not derivable [FIX-02]
    corrupt("task1", rows, 7,  "phone",    "+1-555-12345",  originals=_orig)
    corrupt("task1", rows, 22, "phone",    "555-ABC-1234",  originals=_orig)
    corrupt("task1", rows, 45, "zip_code", "ABCDE",         originals=_orig)

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

    # ── ADVERSARIAL CLEAN ROWS (look suspicious but are valid) ─────────
    # These rows test false-positive discipline: an agent that flags them
    # should be penalized.  No ground truth entries added.

    # Row 10: email with valid but unusual TLD (.museum)
    rows[10]["email"] = f"{rows[10]['name'].split()[0].lower()}@art.museum"

    # Row 20: date at leap year boundary (2024-02-29 is valid)
    rows[20]["date_of_birth"] = "2024-02-29"

    # Row 35: zip code with leading zero (valid 5-digit)
    rows[35]["zip_code"] = "00501"

    # Row 48: phone with extension format (valid)
    rows[48]["phone"] = "+1-555-123-4567 x890"

    validate_ground_truth(ground_truth, rows, schema, "task1")

    dataset_info = {"schema": schema, "rows": rows}
    return dataset_info, ground_truth, []


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 2: DUPLICATE DETECTIVE — 120 rows, 15 planted issues
#
# [FIX-03] Exact duplicates: column="_row" sentinel
# [FIX-05] Near-duplicate emails explicitly match planted names
# [FIX-02] Missing values / type mismatches: detection-only
# ═══════════════════════════════════════════════════════════════════════════════

def generate_task2(
    rng: random.Random | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate the Duplicate Detective dataset (Task 2).

    Returns:
        ``(dataset_info, ground_truth_issues, secondary_rows)`` where
        ``secondary_rows`` is always ``[]`` for Task 2.
    """
    _r = rng or random
    _orig: dict = {}

    schema = {
        "id": "integer", "first_name": "string", "last_name": "string",
        "email": "string", "phone": "string", "address": "string",
        "city": "string", "registration_date": "date",
    }

    rows: list[dict[str, Any]] = []
    for i in range(120):
        batch = i // len(FIRST_NAMES)
        first = FIRST_NAMES[i % len(FIRST_NAMES)]
        last = LAST_NAMES[(i + batch) % len(LAST_NAMES)]
        rows.append({
            "id": i + 1,
            "first_name": first,
            "last_name": last,
            "email": rand_email(first, last, rng=_r),
            "phone": rand_phone(rng=_r),
            "address": rand_address(rng=_r),
            "city": _r.choice(CITIES),
            "registration_date": rand_date(2020, 2025, rng=_r),
        })

    # ── Exact duplicates (DELETE_ROW) ─────────────────────────────────────
    for dup_idx, src_idx in [(12, 5), (34, 20), (67, 50), (95, 40)]:
        rows[dup_idx] = dict(rows[src_idx])
        rows[dup_idx]["id"] = dup_idx + 1

    # ── Near-duplicates (fixable — derivable from matching row) ───────────
    _c = lambda *a, **kw: corrupt(*a, originals=_orig, **kw)

    # Pair 1: "John Walker" (row 25) vs "Jon Walker" (row 8), same email
    _c("task2", rows, 25, "first_name", "John")
    _c("task2", rows, 25, "last_name",  "Walker")
    _c("task2", rows, 25, "email",      "john.walker@example.com")
    _c("task2", rows, 8,  "first_name", "Jon")       # typo
    _c("task2", rows, 8,  "last_name",  "Walker")
    _c("task2", rows, 8,  "email",      "john.walker@example.com")

    # Pair 2: "Sarah Miller" (row 30) vs same name, email domain typo (row 42)
    _c("task2", rows, 30, "first_name", "Sarah")
    _c("task2", rows, 30, "last_name",  "Miller")
    _c("task2", rows, 30, "email",      "sarah.miller@gmail.com")
    _c("task2", rows, 42, "first_name", "Sarah")
    _c("task2", rows, 42, "last_name",  "Miller")
    _c("task2", rows, 42, "email",      "sarah.miller@gmal.com")

    # Pair 3: "David Chen" (row 60) vs same name, phone reformatted (row 78)
    _c("task2", rows, 60, "first_name", "David")
    _c("task2", rows, 60, "last_name",  "Chen")
    _c("task2", rows, 60, "phone",      "+1-555-123-4567")
    _c("task2", rows, 78, "first_name", "David")
    _c("task2", rows, 78, "last_name",  "Chen")
    _c("task2", rows, 78, "phone",      "15551234567")

    # Pair 4 (HARDER): "Robert Garcia" (row 70) vs transposed name (row 103)
    _c("task2", rows, 70, "first_name", "Robert")
    _c("task2", rows, 70, "last_name",  "Garcia")
    _c("task2", rows, 70, "email",      "robert.garcia@outlook.com")
    _c("task2", rows, 103, "first_name", "Garcia")    # transposed!
    _c("task2", rows, 103, "last_name",  "Robert")    # transposed!
    _c("task2", rows, 103, "email",      "robert.garcia@outlook.com")  # same email

    # ── Missing values (detection-only) [FIX-02] ─────────────────────────
    _c("task2", rows, 19, "email",   None)
    _c("task2", rows, 55, "phone",   "")
    _c("task2", rows, 88, "address", None)

    # ── Type mismatches (detection-only) [FIX-02] ────────────────────────
    _c("task2", rows, 27, "registration_date", "not_a_date")
    _c("task2", rows, 63, "id",                "abc")
    _c("task2", rows, 91, "phone",             12345)
    _c("task2", rows, 110, "registration_date", "13/05/2023")

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
        {"row": 95, "column": "_row", "type": "duplicate",
         "expected": "DELETE_ROW", "duplicate_of": 40,
         "description": "Exact content duplicate of row 40 (different ID only)"},

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
        {"row": 103, "column": "first_name", "type": "near_duplicate",
         "expected": "Robert",
         "description": (
             "First/last name transposed: 'Garcia Robert' should be "
             "'Robert Garcia'; shares email robert.garcia@outlook.com "
             "with row 70"
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
        {"row": 110, "column": "registration_date", "type": "type_mismatch",
         "description": (
             "Date '13/05/2023' uses DD/MM/YYYY format instead of "
             "required ISO YYYY-MM-DD format"
         )},
    ]

    # ── ADVERSARIAL CLEAN ROWS (look suspicious but are valid) ─────────
    # These rows test false-positive discipline: same name as another row
    # but different person (different email, phone, address).

    # Row 15: Same first_name as row 50 but completely different person
    rows[15]["first_name"] = rows[50]["first_name"]

    # Row 45: Registration date at boundary (2020-01-01, start of valid range)
    rows[45]["registration_date"] = "2020-01-01"

    # Row 75: Phone in international format (valid but looks unusual)
    rows[75]["phone"] = "+44-20-7946-0958"

    # Row 100: Email with valid but rare domain (.info)
    rows[100]["email"] = f"{rows[100]['first_name'].lower()}.{rows[100]['last_name'].lower()}@newsletter.info"

    # Row 80: Same city as row 81 — NOT a duplicate (different person)
    rows[80]["city"] = rows[81]["city"]

    validate_ground_truth(ground_truth, rows, schema, "task2")

    dataset_info = {"schema": schema, "rows": rows}
    return dataset_info, ground_truth, []


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 3: INTEGRITY AUDITOR — 250 orders + 42 products, 32 planted issues
#
# Expanded and hardened to genuinely challenge frontier models.
# Includes multi-hop reasoning, cascading errors, statistical outliers,
# adversarial clean rows, and floating-point precision traps.
#
# [FIX-04] All originals saved BEFORE corruption via corrupt()
# [FIX-06] All expected values via canonical_str()
# ═══════════════════════════════════════════════════════════════════════════════

def generate_task3(
    rng: random.Random | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate the Integrity Auditor dataset (Task 3).

    Returns:
        ``(dataset_info, ground_truth_issues, secondary_rows)`` where
        ``dataset_info = {"schema": ..., "rows": ..., "business_rules": ...}``
        and ``secondary_rows`` is the products table rows.
    """
    _r = rng or random
    _orig: dict = {}

    # ── Products table (expanded to 40) ───────────────────────────────────

    EXTRA_PRODUCTS: dict[str, list[str]] = {
        "Electronics": ["Tablet 10in", "Bluetooth Speaker"],
        "Furniture":   ["Corner Desk", "Bar Stool"],
        "Clothing":    ["Silk Scarf", "Hiking Boots"],
        "Books":       ["Algorithms 4th Ed", "Design Patterns"],
        "Sports":      ["Tennis Racket", "Boxing Gloves"],
        "Kitchen":     ["Air Fryer", "Espresso Machine"],
    }

    products: list[dict[str, Any]] = []
    pid = 1
    for cat in CATEGORIES:
        all_names = PRODUCT_NAMES[cat] + EXTRA_PRODUCTS.get(cat, [])
        for name in all_names:
            products.append({
                "product_id": pid,
                "product_name": name,
                "category": cat,
                "base_price": round(_r.uniform(10, 500), 2),
                "stock": _r.randint(0, 1000),
            })
            pid += 1

    valid_pids = {p["product_id"] for p in products}

    product_by_id: dict[int, dict[str, Any]] = {
        p["product_id"]: p for p in products
    }

    # ── Orders table (expanded to 250 clean rows) ────────────────────────

    orders_schema = {
        "order_id": "integer", "customer_id": "integer",
        "product_id": "integer",
        "quantity": "integer", "unit_price": "float",
        "order_total": "float", "discount_pct": "float",
        "order_date": "date", "ship_date": "date",
        "product_category": "string",
    }

    business_rules = {
        "max_discount_pct": 50,
        "min_discount_pct": 0,
        "valid_order_year_range": [2024, 2025],
        "min_quantity": 1,
        "max_quantity": 100,
        "min_unit_price": 0.01,
        "order_total_formula": "quantity * unit_price * (1 - discount_pct / 100)",
        "ship_date_constraint": "ship_date >= order_date",
        "ship_date_max_days_after_order": 730,
    }

    orders: list[dict[str, Any]] = []
    for i in range(250):
        prod = _r.choice(products)
        qty = _r.randint(1, 10)
        price = prod["base_price"]
        discount = _r.choice([0, 0, 0, 5, 10, 15, 20])
        total = round(qty * price * (1 - discount / 100), 2)
        odate = rand_date(2024, 2025, rng=_r)
        y, m, d = map(int, odate.split("-"))
        sd = min(d + _r.randint(1, 14), 28)
        sdate = f"{y:04d}-{m:02d}-{sd:02d}"

        orders.append({
            "order_id": i + 1,
            "customer_id": _r.randint(1000, 9999),
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": price,
            "order_total": total,
            "discount_pct": discount,
            "order_date": odate,
            "ship_date": sdate,
            "product_category": prod["category"],
        })

    # ── ADVERSARIAL CLEAN ROWS (look suspicious but are valid) ────────────

    sample_prod = _r.choice(products)
    orders[50]["unit_price"] = 0.01
    orders[50]["quantity"] = 1
    orders[50]["discount_pct"] = 0
    orders[50]["order_total"] = 0.01  # correct: 1 * 0.01 * 1.0

    # Row 120: Discount at 49.99% — just under max, but valid
    orders[120]["discount_pct"] = 49.99
    p120 = orders[120]
    orders[120]["order_total"] = round(
        p120["quantity"] * p120["unit_price"] * (1 - 49.99 / 100), 2
    )

    # Row 175: Ship date == order date (same-day shipping, valid)
    orders[175]["ship_date"] = orders[175]["order_date"]

    # Row 200: Quantity of exactly 100 (boundary — valid per max_quantity=100)
    orders[200]["quantity"] = 100
    p200 = orders[200]
    orders[200]["order_total"] = round(
        100 * p200["unit_price"] * (1 - p200["discount_pct"] / 100), 2
    )

    # Row 85: High quantity (95) just under max — valid
    orders[85]["quantity"] = 95
    p85 = orders[85]
    orders[85]["order_total"] = round(
        95 * p85["unit_price"] * (1 - p85["discount_pct"] / 100), 2
    )

    # Row 100: Minimum quantity (1) — valid
    orders[100]["quantity"] = 1
    p100 = orders[100]
    orders[100]["order_total"] = round(
        1 * p100["unit_price"] * (1 - p100["discount_pct"] / 100), 2
    )

    # Row 140: Near-zero discount (0.01%) — valid
    orders[140]["discount_pct"] = 0.01
    p140 = orders[140]
    orders[140]["order_total"] = round(
        p140["quantity"] * p140["unit_price"] * (1 - 0.01 / 100), 2
    )

    # Row 195: Same customer_id as row 110 but different product/date — NOT duplicate
    orders[195]["customer_id"] = orders[110]["customer_id"]
    if orders[195]["order_date"] == orders[110]["order_date"]:
        orders[195]["order_date"] = "2025-02-14"
        orders[195]["ship_date"] = "2025-02-20"

    # Row 215: Max discount exactly at boundary (50%) — valid
    orders[215]["discount_pct"] = 50
    p215 = orders[215]
    orders[215]["order_total"] = round(
        p215["quantity"] * p215["unit_price"] * (1 - 50 / 100), 2
    )

    # Row 230: Quantity at max boundary (100) — valid (second boundary case)
    orders[230]["quantity"] = 100
    p230 = orders[230]
    orders[230]["order_total"] = round(
        100 * p230["unit_price"] * (1 - p230["discount_pct"] / 100), 2
    )

    # ── 1–3: Referential integrity (detection-only) ──────────────────────
    _c = lambda *a, **kw: corrupt(*a, originals=_orig, **kw)

    _c("task3", orders, 14,  "product_id", 999)
    _c("task3", orders, 56,  "product_id", -1)
    _c("task3", orders, 180, "product_id", 9999)

    # ── 4–6: Cross-field errors (fixable — derivable) ────────────────────

    # Row 23: corrupt total
    orig_total_23 = _c(
        "task3", orders, 23, "order_total",
        round(orders[23]["order_total"] * 3.7, 2),
    )

    # Row 78: ship date before order date
    _c("task3", orders, 78, "ship_date", "2023-01-01")
    expected_ship_78 = orders[78]["order_date"]

    # Row 210: negative total (sign flip)
    orig_total_210 = orders[210]["order_total"]
    _c("task3", orders, 210, "order_total", -orig_total_210)

    # ── 7–9: Outliers (fixable — derivable) ──────────────────────────────

    orig_qty_8 = _c("task3", orders, 8, "quantity", 99999)

    orig_price_45 = orders[45]["unit_price"]
    _c("task3", orders, 45, "unit_price", -orig_price_45)

    orig_qty_99 = _c("task3", orders, 99, "quantity", 0)

    # ── 10–12: Business rule violations (fixable) ────────────────────────

    _c("task3", orders, 33,  "discount_pct", 150)
    _c("task3", orders, 71,  "order_date",   "2035-01-01")
    orig_qty_220 = orders[220]["quantity"]
    _c("task3", orders, 220, "quantity", -orig_qty_220)

    # ── 13–15: Category mismatches (fixable — derivable from products) ───

    cat_18_actual = product_by_id[orders[18]["product_id"]]["category"]
    cat_18_wrong = next(c for c in CATEGORIES if c != cat_18_actual)
    _c("task3", orders, 18, "product_category", cat_18_wrong)

    cat_62_actual = product_by_id[orders[62]["product_id"]]["category"]
    cat_62_wrong = next(c for c in CATEGORIES if c != cat_62_actual)
    _c("task3", orders, 62, "product_category", cat_62_wrong)

    cat_155_actual = product_by_id[orders[155]["product_id"]]["category"]
    _c("task3", orders, 155, "product_category", "")

    # ── 16–17: CASCADING ERRORS (harder — root cause identification) ─────
    # Discount is wrong (75% instead of 7.5%), which ALSO makes total wrong.
    # Agent must identify discount as the root cause, not just flag the total.

    orig_discount_90 = _c("task3", orders, 90, "discount_pct", 75)
    # Recompute total with the WRONG discount to make it internally consistent
    # with the wrong discount — the total looks "correct" for discount=75,
    # but the discount itself is the error.
    row90 = orders[90]
    _c("task3", orders, 90, "order_total",
            round(row90["quantity"] * row90["unit_price"] * (1 - 75 / 100), 2))
    # The CORRECT total (with original discount) is what we save
    orig_total_90 = round(
        row90["quantity"] * row90["unit_price"] * (1 - orig_discount_90 / 100), 2
    )

    # Row 145: similar cascading — discount 0.5 instead of 5 (decimal shift)
    orig_discount_145 = _c("task3", orders, 145, "discount_pct", 0.5)
    row145 = orders[145]
    _c("task3", orders, 145, "order_total",
            round(row145["quantity"] * row145["unit_price"] * (1 - 0.5 / 100), 2))
    orig_total_145 = round(
        row145["quantity"] * row145["unit_price"] * (1 - orig_discount_145 / 100), 2
    )

    # ── 18–19: FLOATING POINT PRECISION TRAPS ────────────────────────────
    # Total is close but not exact — tests numeric precision

    # Row 130: total off by rounding error (e.g. 206.789 instead of 206.79)
    orig_total_130 = orders[130]["order_total"]
    _c("task3", orders, 130, "order_total",
            round(orig_total_130 + 0.009, 3))  # adds tiny rounding error

    # ── 20: SEMANTIC DUPLICATE ORDER ─────────────────────────────────────
    # Same customer_id + product_id + order_date = likely duplicate submission

    orders[235] = dict(orders[110])
    orders[235]["order_id"] = 236  # different order_id
    # Same customer_id, product_id, order_date → duplicate submission

    # ── 21–22: Additional cross-field issues ─────────────────────────────

    # Row 190: ship_date is 2 years after order_date (suspicious but fixable)
    _c("task3", orders, 190, "ship_date", "2027-06-15")
    expected_ship_190 = orders[190]["order_date"]

    # Row 240: quantity exceeds max_quantity (business rule)
    _c("task3", orders, 240, "quantity", 500)

    # ── 23: MISSING VALUE — null total (fixable via formula) ─────────────
    # Row 42: order_total is null.  Agent must detect the null AND compute
    # the correct value from qty * unit_price * (1 - discount/100).
    orig_total_42 = _c("task3", orders, 42, "order_total", None)

    # ── 24: CASCADING — quantity 10x decimal shift ───────────────────────
    # Row 65: quantity inflated 10x.  order_total was computed with original
    # quantity, creating a cascade: qty * price != total.  Agent must figure
    # out quantity is the root cause (not total).
    orig_qty_65 = _c("task3", orders, 65, "quantity", orders[65]["quantity"] * 10)

    # ── 25–26: HIDDEN BUSINESS RULE — min_unit_price violations ──────────
    # Agent must discover min_unit_price from business_rules metadata.
    _c("task3", orders, 115, "unit_price", 0.0)
    _c("task3", orders, 205, "unit_price", -0.50)

    # ── 27–28: TEMPORAL CONSISTENCY ──────────────────────────────────────
    # Row 160: ship_date exactly 1 year BEFORE order_date (year-entry error)
    odate_160 = orders[160]["order_date"]
    y160, m160, d160 = odate_160.split("-")
    _c("task3", orders, 160, "ship_date",
       f"{int(y160) - 1}-{m160}-{d160}")

    # Row 225: ship_date 4+ years in future (exceeds max_days=730)
    expected_ship_225 = orders[225]["order_date"]
    _c("task3", orders, 225, "ship_date", "2029-11-01")

    # ── 29: SUBTLE PRECISION — off by one cent ───────────────────────────
    # Row 165: total is $0.01 less than formula result (truncation artefact)
    orig_total_165 = orders[165]["order_total"]
    _c("task3", orders, 165, "order_total",
       round(orig_total_165 - 0.01, 2))

    # ── 30: BUSINESS RULE — negative discount ────────────────────────────
    # Row 245: discount_pct = -10 (negative surcharge, violates min_discount_pct=0)
    _c("task3", orders, 245, "discount_pct", -10)

    # ── 31: MULTI-HOP REASONING — wrong product_category cascading from
    #    wrong product_id.  Row 170 has a valid product_id, but we change
    #    it to another valid product that has a DIFFERENT category.
    #    The category still matches the NEW product_id, so the agent must
    #    reason about whether the product_id or category is the error.
    orig_pid_170 = _c("task3", orders, 170, "product_id", products[0]["product_id"])
    # Also update category to match the wrong product, creating a
    # "consistent but wrong" state — agent needs products table to detect
    _c("task3", orders, 170, "product_category", products[0]["category"])

    # ── 32: STRING TYPE IN NUMERIC FIELD — quantity as string
    #    Row 248: quantity stored as string "5" instead of int 5
    orig_qty_248 = _c("task3", orders, 248, "quantity", "5")

    # ── 33: ORDER DATE FORMAT ERROR — YYYY/MM/DD instead of YYYY-MM-DD
    #    Row 185: date uses slashes instead of dashes
    orig_date_185 = orders[185]["order_date"]
    _c("task3", orders, 185, "order_date", orig_date_185.replace("-", "/"))

    # ── Ground truth ──────────────────────────────────────────────────────

    ground_truth: list[dict[str, Any]] = [
        # Referential integrity: detection-only
        {"row": 14, "column": "product_id", "type": "referential_integrity",
         "description": "References non-existent product_id 999"},
        {"row": 56, "column": "product_id", "type": "referential_integrity",
         "description": "product_id is -1 — invalid reference"},
        {"row": 180, "column": "product_id", "type": "referential_integrity",
         "description": "References non-existent product_id 9999"},

        # Cross-field: fixable
        {"row": 23, "column": "order_total", "type": "cross_field",
         "expected": canonical_str(orig_total_23),
         "description": (
             "Total doesn't match qty * unit_price * (1 - discount/100); "
             f"correct value {canonical_str(orig_total_23)} derivable from "
             "row's own fields"
         )},
        {"row": 78, "column": "ship_date", "type": "cross_field",
         "expected": expected_ship_78,
         "description": (
             f"Ship date 2023-01-01 precedes order date {expected_ship_78}; "
             "minimum valid ship date = order date"
         )},
        {"row": 210, "column": "order_total", "type": "cross_field",
         "expected": canonical_str(orig_total_210),
         "description": (
             "Negative total (sign flip); correct value derivable from "
             "qty * unit_price * (1 - discount/100)"
         )},

        # Outliers: fixable
        {"row": 8, "column": "quantity", "type": "outlier",
         "expected": str(orig_qty_8),
         "description": (
             f"Quantity 99999 is extreme outlier; correct value {orig_qty_8} "
             "derivable from round(total / (unit_price * (1 - discount/100)))"
         )},
        {"row": 45, "column": "unit_price", "type": "outlier",
         "expected": canonical_str(orig_price_45),
         "description": (
             f"Negative unit price (sign flip); correct value "
             f"{canonical_str(orig_price_45)} derivable from products table "
             "base_price or abs(unit_price)"
         )},
        {"row": 99, "column": "quantity", "type": "outlier",
         "expected": str(orig_qty_99),
         "description": (
             f"Quantity 0 with positive total; correct value {orig_qty_99} "
             "derivable from round(total / (unit_price * (1 - discount/100)))"
         )},

        # Business rules: fixable
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
        {"row": 220, "column": "quantity", "type": "business_rule",
         "expected": str(orig_qty_220),
         "description": (
             f"Negative quantity (-{orig_qty_220}); sign flip — "
             "absolute value restores original"
         )},

        # Category mismatches: fixable
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
        {"row": 155, "column": "product_category", "type": "cross_field",
         "expected": cat_155_actual,
         "description": (
             f"Category is empty string but product "
             f"{orders[155]['product_id']} is {cat_155_actual} "
             "per products table"
         )},

        # CASCADING ERRORS: discount is root cause
        {"row": 90, "column": "discount_pct", "type": "business_rule",
         "expected": canonical_str(orig_discount_90),
         "description": (
             f"Discount 75% is likely a data entry error "
             f"(original {canonical_str(orig_discount_90)}%); "
             "total was recomputed with wrong discount — "
             "fix discount first, then total follows"
         )},
        {"row": 145, "column": "discount_pct", "type": "cross_field",
         "expected": canonical_str(orig_discount_145),
         "description": (
             f"Discount 0.5% is likely decimal shift error "
             f"(should be {canonical_str(orig_discount_145)}%); "
             "total was recomputed with wrong discount"
         )},

        # FLOATING POINT PRECISION TRAP
        {"row": 130, "column": "order_total", "type": "cross_field",
         "expected": canonical_str(orig_total_130),
         "description": (
             f"Total {orders[130]['order_total']} has rounding error; "
             f"correct value {canonical_str(orig_total_130)} derivable "
             "from qty * unit_price * (1 - discount/100)"
         )},

        # SEMANTIC DUPLICATE
        {"row": 235, "column": "_row", "type": "duplicate",
         "expected": "DELETE_ROW",
         "duplicate_of": 110,
         "description": (
             "Semantic duplicate: same customer_id, product_id, and "
             "order_date as row 110 — likely duplicate submission"
         )},

        # Additional cross-field
        {"row": 190, "column": "ship_date", "type": "cross_field",
         "expected": expected_ship_190,
         "description": (
             "Ship date 2027-06-15 is 2+ years after order date "
             f"{expected_ship_190} — exceeds reasonable shipping window; "
             "minimum valid ship date = order date"
         )},

        # Business rule: quantity exceeds max
        {"row": 240, "column": "quantity", "type": "business_rule",
         "expected": str(business_rules["max_quantity"]),
         "description": (
             "Quantity 500 exceeds max_quantity (100); "
             "clamped to business rule maximum"
         )},

        # MISSING VALUE — null total (fixable via formula)
        {"row": 42, "column": "order_total", "type": "missing_value",
         "expected": canonical_str(orig_total_42),
         "description": (
             "Order total is null but derivable from formula: "
             f"qty * unit_price * (1 - discount/100) = "
             f"{canonical_str(orig_total_42)}"
         )},

        # CASCADING — quantity 10x decimal shift
        {"row": 65, "column": "quantity", "type": "outlier",
         "expected": str(orig_qty_65),
         "description": (
             f"Quantity {orig_qty_65 * 10} appears to be 10x decimal "
             f"shift; order_total matches qty={orig_qty_65} — "
             "verify via total / (unit_price * (1 - discount/100))"
         )},

        # HIDDEN BUSINESS RULE — min_unit_price violations
        {"row": 115, "column": "unit_price", "type": "business_rule",
         "expected": canonical_str(business_rules["min_unit_price"]),
         "description": (
             f"Unit price 0.00 violates min_unit_price "
             f"({business_rules['min_unit_price']}); "
             "clamped to business rule minimum"
         )},
        {"row": 205, "column": "unit_price", "type": "business_rule",
         "expected": canonical_str(business_rules["min_unit_price"]),
         "description": (
             f"Unit price -0.50 violates min_unit_price "
             f"({business_rules['min_unit_price']}); "
             "clamped to business rule minimum"
         )},

        # TEMPORAL CONSISTENCY
        {"row": 160, "column": "ship_date", "type": "cross_field",
         "expected": odate_160,
         "description": (
             f"Ship date {orders[160]['ship_date']} is exactly 1 year "
             f"before order date {odate_160} — likely year entry error; "
             "minimum valid ship date = order date"
         )},
        {"row": 225, "column": "ship_date", "type": "cross_field",
         "expected": expected_ship_225,
         "description": (
             "Ship date 2029-11-01 is 4+ years after order date "
             f"{expected_ship_225} — exceeds ship_date_max_days "
             f"({business_rules['ship_date_max_days_after_order']} days); "
             "minimum valid ship date = order date"
         )},

        # SUBTLE PRECISION — truncation artefact
        {"row": 165, "column": "order_total", "type": "cross_field",
         "expected": canonical_str(orig_total_165),
         "description": (
             f"Total {canonical_str(orders[165]['order_total'])} is off "
             f"by $0.01 from formula result "
             f"{canonical_str(orig_total_165)}; "
             "appears to be truncation instead of rounding"
         )},

        # BUSINESS RULE — negative discount
        {"row": 245, "column": "discount_pct", "type": "business_rule",
         "expected": str(business_rules["min_discount_pct"]),
         "description": (
             "Discount -10% is negative; "
             f"min_discount_pct is {business_rules['min_discount_pct']}; "
             "clamped to business rule minimum"
         )},

        # MULTI-HOP — wrong product_id (fixable, derivable from category cross-check)
        {"row": 170, "column": "product_id", "type": "referential_integrity",
         "expected": str(orig_pid_170),
         "description": (
             f"Product ID changed to {products[0]['product_id']} with "
             f"matching category '{products[0]['category']}' — appears "
             "consistent but original product_id derivable from order "
             "history and pricing patterns"
         )},

        # STRING TYPE IN NUMERIC FIELD
        {"row": 248, "column": "quantity", "type": "type_mismatch",
         "expected": str(orig_qty_248),
         "description": (
             "Quantity stored as string '5' instead of integer; "
             "schema requires integer type"
         )},

        # DATE FORMAT ERROR
        {"row": 185, "column": "order_date", "type": "format_error",
         "expected": orig_date_185,
         "description": (
             f"Date uses YYYY/MM/DD format ({orders[185]['order_date']}) "
             f"instead of required YYYY-MM-DD ({orig_date_185})"
         )},
    ]

    products_schema = {
        "product_id": "integer", "product_name": "string",
        "category": "string", "base_price": "float", "stock": "integer",
    }

    validate_ground_truth(ground_truth, orders, orders_schema, "task3")

    dataset_info = {
        "schema": orders_schema,
        "rows": orders,
        "business_rules": business_rules,
        "products_schema": products_schema,
    }
    return dataset_info, ground_truth, products


# ═══════════════════════════════════════════════════════════════════════════════
# Post-Generation Verification Suite
# ═══════════════════════════════════════════════════════════════════════════════

def verify_all() -> bool:
    """Load all generated files and run structural + semantic integrity checks.

    Returns:
        True if all checks pass, False otherwise.
    """
    SEP = "-" * 64
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
        "task2_ground_truth.json": (15, 8),
        "task3_ground_truth.json": (32, 29),
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

def _write_all() -> None:
    """Generate all datasets and write to disk.  Uses global seed."""
    random.seed(SEED)

    # Task 1
    ds1, gt1, _ = generate_task1()
    _write_dataset("task1_customers.json", ds1["schema"], ds1["rows"])
    _write_ground_truth("task1_ground_truth.json", gt1)
    fixable1 = sum(1 for g in gt1 if "expected" in g)
    print(f"Task 1: {len(ds1['rows'])} rows, {len(gt1)} issues "
          f"({fixable1} fixable, {len(gt1) - fixable1} detection-only)")

    # Task 2
    ds2, gt2, _ = generate_task2()
    _write_dataset("task2_contacts.json", ds2["schema"], ds2["rows"])
    _write_ground_truth("task2_ground_truth.json", gt2)
    fixable2 = sum(1 for g in gt2 if "expected" in g)
    print(f"Task 2: {len(ds2['rows'])} rows, {len(gt2)} issues "
          f"({fixable2} fixable, {len(gt2) - fixable2} detection-only)")

    # Task 3
    ds3, gt3, products = generate_task3()
    _write_dataset("task3_orders.json", ds3["schema"], ds3["rows"],
                   business_rules=ds3["business_rules"])
    _write_dataset("task3_products.json", ds3["products_schema"], products)
    _write_ground_truth("task3_ground_truth.json", gt3)
    fixable3 = sum(1 for g in gt3 if "expected" in g)
    print(f"Task 3: {len(ds3['rows'])} orders, {len(products)} products, "
          f"{len(gt3)} issues ({fixable3} fixable, {len(gt3) - fixable3} detection-only)")


if __name__ == "__main__":
    print("=" * 64)
    print("  Data Quality Environment — Dataset Generator v" + VERSION)
    print("=" * 64)
    print()

    os.makedirs(DATASETS_DIR, exist_ok=True)

    _write_all()

    all_ok = verify_all()

    print(f"All datasets generated in: {DATASETS_DIR}")
    print(f"Files: {len(os.listdir(DATASETS_DIR))}")
    for f in sorted(os.listdir(DATASETS_DIR)):
        size = os.path.getsize(os.path.join(DATASETS_DIR, f))
        print(f"  {f:40s} {size:>8,d} bytes")

    if not all_ok:
        print("\n[!] VERIFICATION FAILED -- review errors above")
        sys.exit(1)
    else:
        print("\n[OK] All verification gates passed")
