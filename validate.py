# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Cross-platform pre-submission validation for the Data Quality environment.

Verifies file existence, Python imports, test suite, dataset integrity,
optional tooling (openenv, Docker), and secret scanning — all without
requiring Docker or API keys.

Usage::

    python validate.py              # Run all checks
    python validate.py --skip-docker  # Skip Docker checks

Exit code: 0 = all critical checks pass, 1 = failures found.

Replaces validate.sh for Windows/cross-platform use; validate.sh is
retained for CI/Linux environments.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════════
# §1  Configuration
# ═══════════════════════════════════════════════════════════════════════════

PROJECT_ROOT = Path(__file__).resolve().parent

# Required files (relative to project root)
REQUIRED_FILES: List[str] = [
    "openenv.yaml",
    "models.py",
    "compat.py",
    "client.py",
    "inference.py",
    "test_env.py",
    "__init__.py",
    "README.md",
    "LICENSE",
    "pyproject.toml",
    str(Path("server") / "app.py"),
    str(Path("server") / "data_quality_environment.py"),
    str(Path("server") / "Dockerfile"),
    str(Path("server") / "requirements.txt"),
    str(Path("datasets") / "task1_customers.json"),
    str(Path("datasets") / "task1_ground_truth.json"),
    str(Path("datasets") / "task2_contacts.json"),
    str(Path("datasets") / "task2_ground_truth.json"),
    str(Path("datasets") / "task3_orders.json"),
    str(Path("datasets") / "task3_products.json"),
    str(Path("datasets") / "task3_ground_truth.json"),
]

# Secret patterns to scan for (compiled regex patterns)
SECRET_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("OpenAI API key", re.compile(r"sk-[a-zA-Z0-9]{20,}")),
    ("HuggingFace token", re.compile(r"hf_[a-zA-Z0-9]{20,}")),
    ("Generic Bearer token", re.compile(r'["\']Bearer\s+[a-zA-Z0-9._\-]{20,}["\']')),
    ("Hardcoded password", re.compile(r'(?i)password\s*=\s*["\'][^"\']{8,}["\']')),
]


# ═══════════════════════════════════════════════════════════════════════════
# §2  Output Helpers — colored terminal output
# ═══════════════════════════════════════════════════════════════════════════

# ANSI color codes (safe on most terminals including Windows 10+)
_BOLD = "\033[1m"
_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_CYAN = "\033[36m"
_NC = "\033[0m"

# Enable ANSI on Windows
if sys.platform == "win32":
    try:
        os.system("")  # Enable ANSI escape codes on Windows 10+
    except Exception:
        pass

_pass_count = 0
_fail_count = 0
_warn_count = 0
_errors: List[str] = []


def _pass(msg: str) -> None:
    global _pass_count
    _pass_count += 1
    print(f"  {_GREEN}[PASS]{_NC} {msg}")


def _fail(msg: str) -> None:
    global _fail_count
    _fail_count += 1
    print(f"  {_RED}[FAIL]{_NC} {msg}")
    _errors.append(msg)


def _warn(msg: str) -> None:
    global _warn_count
    _warn_count += 1
    print(f"  {_YELLOW}[WARN]{_NC} {msg}")


def _section(title: str) -> None:
    print(f"\n{_BOLD}{_CYAN}=== {title} ==={_NC}")


# ═══════════════════════════════════════════════════════════════════════════
# §3  Validation Steps
# ═══════════════════════════════════════════════════════════════════════════

def check_required_files() -> None:
    """Step 1: Verify all required files exist."""
    _section("Step 1: Required Files")

    for rel_path in REQUIRED_FILES:
        full_path = PROJECT_ROOT / rel_path
        if full_path.exists():
            _pass(f"{rel_path}")
        else:
            _fail(f"{rel_path} MISSING")


def check_python_imports() -> None:
    """Step 2: Verify Python modules import cleanly."""
    _section("Step 2: Python Imports")

    import_tests = [
        (
            "models.py core imports",
            "from models import DataQualityAction, DataQualityObservation, DataQualityState",
        ),
        (
            "models.py enum imports",
            "from models import IssueType, FixType, ActionResult, RemainingHint",
        ),
        (
            "compat.py imports",
            "from compat import Action, Observation, State",
        ),
        (
            "server environment import",
            "from server.data_quality_environment import DataQualityEnvironment",
        ),
    ]

    python_cmd = sys.executable  # Use same Python as current process

    for label, import_stmt in import_tests:
        try:
            result = subprocess.run(
                [python_cmd, "-c", import_stmt],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(PROJECT_ROOT),
            )
            if result.returncode == 0:
                _pass(label)
            else:
                _fail(f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr else 'unknown error'}")
        except subprocess.TimeoutExpired:
            _fail(f"{label}: TIMEOUT")
        except Exception as exc:
            _fail(f"{label}: {exc}")


def check_test_suite() -> None:
    """Step 3: Run the automated test suite."""
    _section("Step 3: Test Suite")

    test_file = PROJECT_ROOT / "test_env.py"
    if not test_file.exists():
        _fail("test_env.py not found")
        return

    python_cmd = sys.executable

    try:
        result = subprocess.run(
            [python_cmd, str(test_file)],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(PROJECT_ROOT),
        )

        if result.returncode == 0:
            # Extract pass count from output
            for line in result.stdout.splitlines():
                if "passed" in line.lower() or "RESULTS" in line:
                    print(f"    {line.strip()}")
            _pass("All tests pass")
        else:
            # Show failures
            for line in result.stdout.splitlines():
                if "[FAIL]" in line or "CRASH" in line or "RESULTS" in line:
                    print(f"    {line.strip()}")
            if result.stderr:
                for line in result.stderr.strip().splitlines()[-5:]:
                    print(f"    {line}")
            _fail("Test suite has failures")
    except subprocess.TimeoutExpired:
        _fail("Test suite TIMEOUT (>120s)")
    except Exception as exc:
        _fail(f"Test suite error: {exc}")


def check_dataset_integrity() -> None:
    """Step 4: Validate dataset files have correct structure and counts."""
    _section("Step 4: Dataset Integrity")

    datasets_dir = PROJECT_ROOT / "datasets"

    gt_files = {
        "task1_ground_truth.json": {"min_issues": 8, "label": "task1"},
        "task2_ground_truth.json": {"min_issues": 15, "label": "task2"},
        "task3_ground_truth.json": {"min_issues": 21, "label": "task3"},
    }

    for filename, config in gt_files.items():
        filepath = datasets_dir / filename
        if not filepath.exists():
            _fail(f"{filename} not found")
            continue

        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)

            # Check envelope structure
            if not isinstance(data, dict) or "issues" not in data:
                _fail(f"{filename}: missing 'issues' key (envelope format)")
                continue

            issues = data["issues"]
            label = config["label"]

            # Check _meta consistency
            meta = data.get("_meta", {})
            if meta:
                meta_total = meta.get("total_issues", -1)
                if meta_total != len(issues):
                    _fail(f"{label}: _meta.total_issues ({meta_total}) != actual ({len(issues)})")
                else:
                    _pass(f"{label}: _meta.total_issues consistent ({meta_total})")

                fixable_meta = meta.get("fixable_issues", 0)
                actual_fixable = sum(1 for i in issues if i.get("expected") is not None)
                if fixable_meta != actual_fixable:
                    _fail(f"{label}: _meta.fixable_issues ({fixable_meta}) != actual ({actual_fixable})")
                else:
                    _pass(f"{label}: _meta.fixable_issues consistent ({fixable_meta})")

            # Check minimum count
            min_expected = config["min_issues"]
            if len(issues) >= min_expected:
                _pass(f"{label}: {len(issues)} issues (>= {min_expected})")
            else:
                _fail(f"{label}: only {len(issues)} issues (expected >= {min_expected})")

            # Check required keys on all issues
            all_have_keys = True
            for i, issue in enumerate(issues):
                for key in ("row", "column", "type"):
                    if key not in issue:
                        _fail(f"{label}: issue {i} missing '{key}'")
                        all_have_keys = False
            if all_have_keys:
                _pass(f"{label}: all issues have required keys (row, column, type)")

        except json.JSONDecodeError as exc:
            _fail(f"{filename}: invalid JSON — {exc}")
        except Exception as exc:
            _fail(f"{filename}: {exc}")

    # Check dataset files exist and are valid JSON with 'rows' and 'schema'
    data_files = [
        ("task1_customers.json", "task1"),
        ("task2_contacts.json", "task2"),
        ("task3_orders.json", "task3"),
        ("task3_products.json", "task3 products"),
    ]

    for filename, label in data_files:
        filepath = datasets_dir / filename
        if not filepath.exists():
            _fail(f"{filename} not found")
            continue

        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)

            if "rows" in data:
                _pass(f"{label}: {filename} has {len(data['rows'])} rows")
            elif isinstance(data, list):
                _pass(f"{label}: {filename} has {len(data)} entries (list format)")
            else:
                _fail(f"{label}: {filename} missing 'rows' key")
        except Exception as exc:
            _fail(f"{label}: {filename} — {exc}")


def check_openenv_validate() -> None:
    """Step 5: Run openenv validate if available."""
    _section("Step 5: openenv validate")

    openenv_cmd = shutil.which("openenv")
    if openenv_cmd is None:
        _warn("openenv CLI not found — skipping (install: pip install openenv-core)")
        return

    try:
        result = subprocess.run(
            [openenv_cmd, "validate"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(PROJECT_ROOT),
        )
        if result.returncode == 0:
            _pass("openenv validate passed")
        else:
            output = (result.stdout + result.stderr).strip()
            _fail(f"openenv validate FAILED: {output[:200]}")
    except subprocess.TimeoutExpired:
        _fail("openenv validate TIMEOUT")
    except Exception as exc:
        _fail(f"openenv validate error: {exc}")


def check_docker_build(skip: bool = False) -> None:
    """Step 6: Verify Docker image builds successfully."""
    _section("Step 6: Docker Build")

    if skip:
        _warn("Skipped (--skip-docker)")
        return

    docker_cmd = shutil.which("docker")
    if docker_cmd is None:
        _warn("Docker not found — skipping")
        return

    try:
        result = subprocess.run(
            [
                docker_cmd, "build",
                "-t", "data_quality_env:validate",
                "-f", str(PROJECT_ROOT / "server" / "Dockerfile"),
                str(PROJECT_ROOT),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode == 0:
            _pass("Docker build succeeded")
        else:
            # Show last few lines of error
            stderr_lines = result.stderr.strip().splitlines()
            for line in stderr_lines[-5:]:
                print(f"    {line}")
            _fail("Docker build FAILED")
    except subprocess.TimeoutExpired:
        _fail("Docker build TIMEOUT (>300s)")
    except Exception as exc:
        _fail(f"Docker build error: {exc}")


def check_secret_scan() -> None:
    """Step 7: Scan for hardcoded secrets in Python files."""
    _section("Step 7: Secret Scan")

    found_secrets = False

    for py_file in PROJECT_ROOT.rglob("*.py"):
        # Skip __pycache__, .git, etc.
        if "__pycache__" in str(py_file) or ".git" in str(py_file):
            continue

        try:
            content = py_file.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        for secret_label, pattern in SECRET_PATTERNS:
            matches = pattern.findall(content)
            if matches:
                rel = py_file.relative_to(PROJECT_ROOT)
                _fail(f"{secret_label} found in {rel}: {matches[0][:30]}...")
                found_secrets = True

    if not found_secrets:
        _pass("No hardcoded secrets found")


def check_openenv_yaml() -> None:
    """Step 8: Validate openenv.yaml configuration."""
    _section("Step 8: openenv.yaml Validation")

    yaml_path = PROJECT_ROOT / "openenv.yaml"
    if not yaml_path.exists():
        _fail("openenv.yaml not found")
        return

    try:
        content = yaml_path.read_text(encoding="utf-8")

        # Basic checks without requiring PyYAML
        required_fields = ["spec_version", "name", "type", "app", "port"]
        for field in required_fields:
            if f"{field}:" in content:
                _pass(f"openenv.yaml has '{field}'")
            else:
                _fail(f"openenv.yaml missing '{field}'")

        # Check name matches
        if "data_quality_env" in content:
            _pass("openenv.yaml name matches project")
        else:
            _warn("openenv.yaml name may not match 'data_quality_env'")

    except Exception as exc:
        _fail(f"openenv.yaml read error: {exc}")


def check_readme() -> None:
    """Step 9: Validate README.md is not boilerplate."""
    _section("Step 9: README Validation")

    readme_path = PROJECT_ROOT / "README.md"
    if not readme_path.exists():
        _fail("README.md not found")
        return

    try:
        content = readme_path.read_text(encoding="utf-8")

        # Check it's not the echo-env boilerplate
        if "echoed_message" in content or "message_length" in content:
            _fail("README.md still contains echo-env boilerplate!")
        else:
            _pass("README.md is not boilerplate")

        # Check for key sections
        key_phrases = [
            ("Action Space", "action space documentation"),
            ("Observation Space", "observation space documentation"),
            ("Reward", "reward design documentation"),
            ("Task", "task documentation"),
        ]
        for phrase, desc in key_phrases:
            if phrase.lower() in content.lower():
                _pass(f"README has {desc}")
            else:
                _warn(f"README may be missing {desc}")

        # Check frontmatter
        if content.startswith("---"):
            _pass("README has YAML frontmatter")
            if "app_port:" in content:
                _pass("README specifies app_port")
            else:
                _warn("README missing app_port in frontmatter")
        else:
            _warn("README missing YAML frontmatter")

    except Exception as exc:
        _fail(f"README.md read error: {exc}")


def check_port_consistency() -> None:
    """Step 10: Verify port number is consistent across all configuration files."""
    _section("Step 10: Port Consistency")

    port_sources: Dict[str, Optional[int]] = {}

    # 1. openenv.yaml
    yaml_path = PROJECT_ROOT / "openenv.yaml"
    if yaml_path.exists():
        content = yaml_path.read_text(encoding="utf-8")
        import re as _re
        match = _re.search(r"^port:\s*(\d+)", content, _re.MULTILINE)
        if match:
            port_sources["openenv.yaml"] = int(match.group(1))

    # 2. Dockerfile HEALTHCHECK
    dockerfile = PROJECT_ROOT / "server" / "Dockerfile"
    if dockerfile.exists():
        content = dockerfile.read_text(encoding="utf-8")
        match = re.search(r"localhost:(\d+)/health", content)
        if match:
            port_sources["Dockerfile HEALTHCHECK"] = int(match.group(1))
        match = re.search(r"--port\s+(\d+)", content)
        if match:
            port_sources["Dockerfile CMD"] = int(match.group(1))
        match = re.search(r"EXPOSE\s+(\d+)", content)
        if match:
            port_sources["Dockerfile EXPOSE"] = int(match.group(1))

    # 3. README frontmatter
    readme_path = PROJECT_ROOT / "README.md"
    if readme_path.exists():
        content = readme_path.read_text(encoding="utf-8")
        match = re.search(r"app_port:\s*(\d+)", content)
        if match:
            port_sources["README app_port"] = int(match.group(1))

    if not port_sources:
        _warn("Could not extract port from any configuration file")
        return

    unique_ports = set(port_sources.values())
    if len(unique_ports) == 1:
        port = unique_ports.pop()
        _pass(f"All {len(port_sources)} port references are consistent ({port})")
    else:
        for source, port in port_sources.items():
            _fail(f"Port mismatch: {source} = {port}")


def check_pyproject_toml() -> None:
    """Step 11: Validate pyproject.toml structure."""
    _section("Step 11: pyproject.toml Validation")

    toml_path = PROJECT_ROOT / "pyproject.toml"
    if not toml_path.exists():
        _fail("pyproject.toml not found")
        return

    try:
        content = toml_path.read_text(encoding="utf-8")

        # Build system check
        if "setuptools.build_meta" in content:
            _pass("Uses setuptools.build_meta (not deprecated _legacy)")
        elif "build-backend" in content:
            _warn("Build backend present but may not be setuptools.build_meta")
        else:
            _fail("Missing [build-system] build-backend")

        # Required dependencies
        if "openenv-core" in content:
            _pass("Depends on openenv-core")
        else:
            _warn("openenv-core not in dependencies")

        if "pydantic" in content:
            _pass("Depends on pydantic")
        else:
            _warn("pydantic not in dependencies")

        # Python version
        if "requires-python" in content:
            _pass("Specifies requires-python")
        else:
            _warn("Missing requires-python")

        # Package configuration
        if "packages" in content and "data_quality_env" in content:
            _pass("Package configuration present")
        else:
            _warn("Package configuration may be incomplete")

    except Exception as exc:
        _fail(f"pyproject.toml read error: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
# §4  Main
# ═══════════════════════════════════════════════════════════════════════════

def main() -> int:
    """Run all validation checks and print summary."""
    skip_docker = "--skip-docker" in sys.argv

    print(f"\n{_BOLD}{'=' * 56}")
    print(f"  PRE-SUBMISSION VALIDATION")
    print(f"{'=' * 56}{_NC}")
    print(f"  Project: {PROJECT_ROOT}")
    print(f"  Python:  {sys.executable}")

    start = time.time()

    check_required_files()
    check_python_imports()
    check_test_suite()
    check_dataset_integrity()
    check_openenv_validate()
    check_docker_build(skip=skip_docker)
    check_secret_scan()
    check_openenv_yaml()
    check_readme()
    check_port_consistency()
    check_pyproject_toml()

    elapsed = time.time() - start

    # Summary
    print(f"\n{_BOLD}{'=' * 56}")
    print(f"  PASSED: {_pass_count}   FAILED: {_fail_count}   WARNINGS: {_warn_count}")
    print(f"  TIME:   {elapsed:.1f}s")

    if _fail_count == 0:
        print(f"  {_GREEN}{_BOLD}READY TO SUBMIT{_NC}")
    else:
        print(f"  {_RED}{_BOLD}FIX {_fail_count} FAILURE(S) BEFORE SUBMITTING{_NC}")
        print(f"\n  Failures:")
        for err in _errors:
            print(f"    - {err}")

    print(f"{'=' * 56}{_NC}\n")

    return 1 if _fail_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
