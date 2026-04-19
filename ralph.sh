#!/usr/bin/env bash
# ralph.sh - autonomous implementation loop for a DataForge spec.
# Usage: ./ralph.sh [max_iterations] [spec_file]
# Example: ./ralph.sh 10 specs/SPEC_detectors.md
#
# Requirements: bash 4+, GNU coreutils (date -u), envsubst (gettext-base),
#               claude CLI (or cursor-agent), pytest.
# On Windows: run under WSL or Git Bash.

set -euo pipefail

for cmd in envsubst pytest; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: required command '$cmd' not found. Install it first." >&2
        echo "  envsubst -> apt install gettext-base (Linux) or brew install gettext (macOS)" >&2
        echo "  pytest   -> pip install -e '.[dev]'" >&2
        exit 2
    fi
done

if ! command -v claude &>/dev/null && ! command -v cursor-agent &>/dev/null; then
    echo "ERROR: neither 'claude' nor 'cursor-agent' CLI found." >&2
    echo "  Install one: https://docs.anthropic.com/claude-cli or cursor-agent." >&2
    exit 2
fi

MAX_ITERATIONS=${1:-10}
SPEC_FILE=${2:-"specs/SPEC_current.md"}
PROMPT_FILE="./prompt.md"
PROGRESS_FILE="./progress.md"

if [[ ! -f "$SPEC_FILE" ]]; then
    echo "Spec file not found: $SPEC_FILE" >&2
    exit 2
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "ERROR: worktree is dirty. Commit or stash changes before running Ralph." >&2
    exit 2
fi

echo "=== Ralph starting. Spec: $SPEC_FILE. Max iterations: $MAX_ITERATIONS ==="
echo "$(date -u +%Y-%m-%dT%H:%M:%S%z) | ralph started on $SPEC_FILE" >> "$PROGRESS_FILE"

SPEC_PATH="$SPEC_FILE" envsubst < prompt.template.md > "$PROMPT_FILE"

for ((i=1; i<=MAX_ITERATIONS; i++)); do
    echo ""
    echo "=== Ralph iteration $i of $MAX_ITERATIONS ==="
    echo "$(date -u +%Y-%m-%dT%H:%M:%S%z) | iteration $i" >> "$PROGRESS_FILE"

    if command -v claude &>/dev/null; then
        OUTPUT=$(claude -p --dangerously-skip-permissions < "$PROMPT_FILE" 2>&1 || true)
    else
        OUTPUT=$(cursor-agent --prompt-file "$PROMPT_FILE" 2>&1 || true)
    fi

    echo "$OUTPUT" >> "$PROGRESS_FILE"

    if echo "$OUTPUT" | grep -q "<complete/>"; then
        echo "=== All spec tasks complete ==="
        echo "$(date -u +%Y-%m-%dT%H:%M:%S%z) | COMPLETE" >> "$PROGRESS_FILE"
        exit 0
    fi

    if ! pytest tests/regression/ -x > /dev/null 2>&1; then
        echo "=== Regression broken; stopping for manual review ==="
        echo "$(date -u +%Y-%m-%dT%H:%M:%S%z) | STOP - regression tests failed" >> "$PROGRESS_FILE"
        exit 1
    fi
done

echo "=== Reached max iterations without <complete/>. Review progress.md ==="
exit 1
