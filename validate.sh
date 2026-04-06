#!/usr/bin/env bash
# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT
#
# Pre-submission validation script for CI/Linux environments.
# Cross-platform equivalent: validate.py (use on Windows).
#
# Usage:
#     chmod +x validate.sh
#     ./validate.sh
#     ./validate.sh --skip-docker

set -euo pipefail

BOLD='\033[1m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0
ERRORS=()

pass_() { ((PASS++)); echo -e "  ${GREEN}[PASS]${NC} $1"; }
fail_() { ((FAIL++)); echo -e "  ${RED}[FAIL]${NC} $1"; ERRORS+=("$1"); }
warn_() { ((WARN++)); echo -e "  ${YELLOW}[WARN]${NC} $1"; }
section() { echo -e "\n${BOLD}${CYAN}=== $1 ===${NC}"; }

SKIP_DOCKER=false
[[ "${1:-}" == "--skip-docker" ]] && SKIP_DOCKER=true

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

echo -e "\n${BOLD}========================================================${NC}"
echo -e "${BOLD}  PRE-SUBMISSION VALIDATION${NC}"
echo -e "${BOLD}========================================================${NC}"
echo -e "  Project: ${PROJECT_ROOT}"

START_TIME=$(date +%s)

# ── Step 1: Required files ────────────────────────────────────────────────
section "Step 1: Required Files"
REQUIRED_FILES=(
    openenv.yaml models.py compat.py client.py inference.py test_env.py
    __init__.py README.md
    server/app.py server/data_quality_environment.py server/Dockerfile server/requirements.txt
    datasets/task1_customers.json datasets/task1_ground_truth.json
    datasets/task2_contacts.json datasets/task2_ground_truth.json
    datasets/task3_orders.json datasets/task3_products.json datasets/task3_ground_truth.json
)
for f in "${REQUIRED_FILES[@]}"; do
    [[ -f "$f" ]] && pass_ "$f" || fail_ "$f MISSING"
done

# ── Step 2: Python imports ────────────────────────────────────────────────
section "Step 2: Python Imports"
PYTHON=${PYTHON:-python3}
command -v "$PYTHON" &>/dev/null || PYTHON=python

$PYTHON -c "from models import DataQualityAction, DataQualityObservation, DataQualityState" 2>/dev/null \
    && pass_ "models.py imports" || fail_ "models.py import error"
$PYTHON -c "from models import IssueType, FixType, ActionResult, RemainingHint" 2>/dev/null \
    && pass_ "models.py enum imports" || fail_ "models.py enum import error"
$PYTHON -c "from compat import Action, Observation, State" 2>/dev/null \
    && pass_ "compat.py imports" || fail_ "compat.py import error"
$PYTHON -c "from server.data_quality_environment import DataQualityEnvironment" 2>/dev/null \
    && pass_ "server environment import" || fail_ "server environment import error"

# ── Step 3: Test suite ────────────────────────────────────────────────────
section "Step 3: Test Suite"
if [[ -f "test_env.py" ]]; then
    if $PYTHON test_env.py 2>&1 | tail -5; then
        [[ ${PIPESTATUS[0]} -eq 0 ]] && pass_ "All tests pass" || fail_ "Test failures"
    fi
else
    fail_ "test_env.py not found"
fi

# ── Step 4: Dataset integrity ─────────────────────────────────────────────
section "Step 4: Dataset Integrity"
$PYTHON -c "
import json, sys

errors = []
for task, filename in [
    ('task1', 'datasets/task1_ground_truth.json'),
    ('task2', 'datasets/task2_ground_truth.json'),
    ('task3', 'datasets/task3_ground_truth.json'),
]:
    with open(filename) as f:
        data = json.load(f)

    if not isinstance(data, dict) or 'issues' not in data:
        errors.append(f'{task}: missing issues key')
        continue

    issues = data['issues']
    meta = data.get('_meta', {})

    # Verify _meta consistency
    if meta.get('total_issues', -1) != len(issues):
        errors.append(f'{task}: _meta.total_issues mismatch')

    fixable_actual = sum(1 for i in issues if i.get('expected') is not None)
    if meta.get('fixable_issues', -1) != fixable_actual:
        errors.append(f'{task}: _meta.fixable_issues mismatch')

    # Required keys
    for idx, issue in enumerate(issues):
        for key in ('row', 'column', 'type'):
            if key not in issue:
                errors.append(f'{task} issue {idx}: missing {key}')

if errors:
    for e in errors:
        print(f'ERROR: {e}', file=sys.stderr)
    sys.exit(1)
else:
    print('Dataset integrity verified')
" && pass_ "Dataset integrity" || fail_ "Dataset integrity error"

# ── Step 5: openenv validate ──────────────────────────────────────────────
section "Step 5: openenv validate"
if command -v openenv &>/dev/null; then
    openenv validate 2>&1 && pass_ "openenv validate passed" || fail_ "openenv validate FAILED"
else
    warn_ "openenv CLI not found — skipping (install: pip install openenv-core)"
fi

# ── Step 6: Docker build ─────────────────────────────────────────────────
section "Step 6: Docker Build"
if $SKIP_DOCKER; then
    warn_ "Skipped (--skip-docker)"
elif command -v docker &>/dev/null; then
    # Capture Docker exit code properly (not via pipe)
    BUILD_OUTPUT=$(docker build -t data_quality_env:validate -f server/Dockerfile . 2>&1) || true
    BUILD_EXIT=$?
    if [[ $BUILD_EXIT -eq 0 ]]; then
        pass_ "Docker build succeeded"
    else
        echo "$BUILD_OUTPUT" | tail -5
        fail_ "Docker build FAILED"
    fi
else
    warn_ "Docker not found — skipping"
fi

# ── Step 7: Secret scan ──────────────────────────────────────────────────
section "Step 7: Secret Scan"
SECRETS_FOUND=false

# OpenAI key
if grep -rn 'sk-[a-zA-Z0-9]\{20,\}' --include="*.py" . 2>/dev/null | grep -v '__pycache__'; then
    fail_ "Hardcoded OpenAI API key found"
    SECRETS_FOUND=true
fi

# HuggingFace token
if grep -rn 'hf_[a-zA-Z0-9]\{20,\}' --include="*.py" . 2>/dev/null | grep -v '__pycache__'; then
    fail_ "Hardcoded HuggingFace token found"
    SECRETS_FOUND=true
fi

if ! $SECRETS_FOUND; then
    pass_ "No hardcoded secrets found"
fi

# ── Step 8: openenv.yaml validation ───────────────────────────────────────
section "Step 8: openenv.yaml Validation"
for field in spec_version name type app port; do
    grep -q "${field}:" openenv.yaml 2>/dev/null \
        && pass_ "openenv.yaml has '${field}'" \
        || fail_ "openenv.yaml missing '${field}'"
done

# ── Step 9: README validation ─────────────────────────────────────────────
section "Step 9: README Validation"
if grep -qi "echoed_message\|message_length" README.md 2>/dev/null; then
    fail_ "README.md still contains echo-env boilerplate!"
else
    pass_ "README.md is not boilerplate"
fi

for phrase in "Action Space" "Observation Space" "Reward" "Task"; do
    if grep -qi "$phrase" README.md 2>/dev/null; then
        pass_ "README has $(echo "$phrase" | tr '[:upper:]' '[:lower:]') documentation"
    else
        warn_ "README may be missing $(echo "$phrase" | tr '[:upper:]' '[:lower:]') documentation"
    fi
done

# ── Summary ───────────────────────────────────────────────────────────────
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo -e "\n${BOLD}========================================================${NC}"
echo -e "${BOLD}  PASSED: ${PASS}   FAILED: ${FAIL}   WARNINGS: ${WARN}${NC}"
echo -e "${BOLD}  TIME:   ${ELAPSED}s${NC}"

if [[ $FAIL -eq 0 ]]; then
    echo -e "  ${GREEN}${BOLD}READY TO SUBMIT${NC}"
else
    echo -e "  ${RED}${BOLD}FIX ${FAIL} FAILURE(S) BEFORE SUBMITTING${NC}"
    echo -e "\n  Failures:"
    for err in "${ERRORS[@]}"; do
        echo -e "    - $err"
    done
fi

echo -e "${BOLD}========================================================${NC}"
exit $FAIL
