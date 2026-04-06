# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Baseline inference script for the Data Quality RL environment.

Drives an LLM agent through all three data-quality tasks, streaming
structured log events that can be consumed by CI, dashboards, or
downstream evaluation pipelines.

Environment variables (all optional):

    API_BASE_URL      LLM provider base URL          (default: OpenAI)
    MODEL_NAME        Chat-completion model           (default: gpt-3.5-turbo)
    OPENAI_API_KEY    API key (also reads HF_TOKEN)   (required for real runs)
    ENV_URL           Environment server URL          (default: http://localhost:7860)
    TEMPERATURE       Sampling temperature            (default: 0.1)
    MAX_TOKENS        Max tokens per completion       (default: 512)
    INFERENCE_RETRIES Max retry attempts on API error (default: 3)

Usage::

    # Against a running server
    export OPENAI_API_KEY="sk-..."
    python inference.py

    # Against a local dev server on a custom port
    ENV_URL=http://localhost:7860 python inference.py

    # Using a HuggingFace-hosted model
    API_BASE_URL=https://api-inference.huggingface.co/v1 \\
    MODEL_NAME=meta-llama/Llama-3-70b-instruct \\
    HF_TOKEN=hf_... \\
    python inference.py

Bug fixes from review:
    [I-01]  Proper import resolution — no sys.path hacking.
    [I-06]  Action sanitization — strips unknown keys, coerces enums.
    [I-07]  Retry with exponential backoff on API errors.
    [I-09]  Robust JSON parsing with nested brace/bracket support.
    [I-13]  Fallback action is inspect (not premature finalize).
    [I-16]  Default port 7860 matches HuggingFace Spaces convention.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

# ── Logging ───────────────────────────────────────────────────────────────
# Structured log format: every line is machine-parseable.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("inference")


# ── Configuration from environment ────────────────────────────────────────

API_BASE_URL: str = os.environ.get("API_BASE_URL", "https://api.openai.com/v1")
MODEL_NAME: str = os.environ.get("MODEL_NAME", "gpt-3.5-turbo")
API_KEY: str = os.environ.get(
    "OPENAI_API_KEY", os.environ.get("HF_TOKEN", "")
)
ENV_URL: str = os.environ.get("ENV_URL", "http://localhost:7860")

TEMPERATURE: float = float(os.environ.get("TEMPERATURE", "0.1"))
MAX_TOKENS: int = int(os.environ.get("MAX_TOKENS", "512"))
INFERENCE_RETRIES: int = int(os.environ.get("INFERENCE_RETRIES", "3"))

TASKS: List[str] = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]

MAX_STEPS: Dict[str, int] = {
    "task_1_format_fixer": 30,
    "task_2_duplicate_detective": 50,
    "task_3_integrity_auditor": 80,
}


# ── Import models using project convention ────────────────────────────────
# [I-01] No sys.path hacking.  Follows the same try-relative / except-absolute
# pattern used by client.py, server/data_quality_environment.py, etc.

try:
    from .models import DataQualityAction, IssueType, FixType
    from .client import DataQualityEnv
except ImportError:
    try:
        from models import DataQualityAction, IssueType, FixType  # type: ignore[no-redef]
        from client import DataQualityEnv  # type: ignore[no-redef]
    except ImportError:
        logger.error(
            "Cannot import models/client.  Run from the project root:\n"
            "    cd data_quality_env && python inference.py"
        )
        sys.exit(2)


# ═══════════════════════════════════════════════════════════════════════════
# §1  LLM Client Setup
# ═══════════════════════════════════════════════════════════════════════════

try:
    from openai import OpenAI
except ImportError:
    logger.error(
        "openai package required for inference.  Install with:\n"
        "    pip install 'openai>=1.0'"
    )
    sys.exit(2)

_llm_client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)


# ═══════════════════════════════════════════════════════════════════════════
# §2  System Prompts — task-adaptive
# ═══════════════════════════════════════════════════════════════════════════

_BASE_SYSTEM_PROMPT = """\
You are a data quality analyst agent. Examine datasets, find issues, fix them.

## Actions (respond with ONE JSON object, no other text)

1. INSPECT rows:
   {"action_type": "inspect", "row_indices": [0, 1, 2]}

2. INSPECT column statistics:
   {"action_type": "inspect", "column_names": ["email", "phone"]}

3. INSPECT secondary/related table:
   {"action_type": "inspect", "row_indices": [0, 1], "related_table": "products"}

4. DIAGNOSE an issue:
   {"action_type": "diagnose", "row_index": 5, "column_name": "email", "issue_type": "<TYPE>"}

5. FIX an issue:
   {"action_type": "fix", "row_index": 5, "column_name": "email", "new_value": "correct@email.com", "fix_type": "correct_value", "justification": "Reason"}

6. FIX by deleting a duplicate row:
   {"action_type": "fix", "row_index": 12, "column_name": "email", "fix_type": "delete_row", "justification": "Duplicate of row 5"}

7. FINALIZE (end episode):
   {"action_type": "finalize"}

## Valid issue_type values
format_error, missing_value, duplicate, near_duplicate, type_mismatch, outlier, referential_integrity, cross_field, business_rule

## Valid fix_type values
correct_value (requires new_value), delete_row (no new_value), impute, standardize

## Strategy
- Inspect rows systematically (e.g., 10 at a time).
- When you find an anomaly, diagnose it with the correct issue_type.
- If you can determine the correct value, fix it with justification.
- If you cannot determine the fix, just diagnose — partial credit is given.
- Finalize when you believe all issues are found.

RESPOND WITH ONLY A SINGLE JSON OBJECT. No explanation, no markdown."""

_TASK_HINTS: Dict[str, str] = {
    "task_1_format_fixer": """
## Task-Specific Guidance: Format Fixer
Focus on: malformed emails (missing @, double @@), invalid dates (Feb 30, Apr 31),
phone number irregularities, and zip code formatting (missing leading zeros, letters).
Check every column value against its expected format per the schema.""",

    "task_2_duplicate_detective": """
## Task-Specific Guidance: Duplicate Detective
Focus on: exact duplicate rows (fix with delete_row), near-duplicates (typos in names,
domain typos in emails, unformatted phone numbers), missing values (null/empty fields),
and type mismatches (wrong data types per schema).
Compare rows with similar names/emails to find near-duplicates.""",

    "task_3_integrity_auditor": """
## Task-Specific Guidance: Integrity Auditor
Focus on: referential integrity (product_id references to products table — inspect it!),
cross-field consistency (order_total = qty × unit_price × (1 − discount/100), ship_date ≥ order_date),
outliers (extreme quantities, negative prices), business rule violations (discount > max_discount,
future order dates, negative quantities).
IMPORTANT: Inspect the products table early using {"action_type": "inspect", "row_indices": [0,1,2,3,4], "related_table": "products"}""",
}


def _system_prompt(task_id: str) -> str:
    """Build a task-adaptive system prompt."""
    hint = _TASK_HINTS.get(task_id, "")
    return _BASE_SYSTEM_PROMPT + hint


# ═══════════════════════════════════════════════════════════════════════════
# §3  JSON Parsing — robust handling of LLM output
# ═══════════════════════════════════════════════════════════════════════════

# [I-13] Fallback is inspect (gather information) not finalize (abort episode).
_FALLBACK_ACTION = '{"action_type": "inspect", "row_indices": [0, 1, 2, 3, 4]}'

# Valid keys per action type, for sanitization
_VALID_KEYS: Dict[str, set] = {
    "inspect": {"action_type", "row_indices", "column_names", "related_table", "metadata"},
    "diagnose": {"action_type", "row_index", "column_name", "issue_type", "related_table", "metadata"},
    "fix": {"action_type", "row_index", "column_name", "new_value", "fix_type", "justification", "related_table", "metadata"},
    "finalize": {"action_type", "metadata"},
}

# Enum value sets for coercion validation
_ISSUE_TYPES: set = {e.value for e in IssueType}
_FIX_TYPES: set = {e.value for e in FixType}


def _extract_json(text: str) -> Optional[str]:
    """Extract the first balanced JSON object from text.

    [I-09] Handles nested braces/brackets correctly, unlike a naive
    `[^{}]+` regex which fails on arrays inside objects.
    """
    # Strategy: find first '{', then track brace depth to find matching '}'
    start = text.find("{")
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape_next = False

    for i in range(start, len(text)):
        ch = text[i]
        if escape_next:
            escape_next = False
            continue
        if ch == "\\":
            escape_next = True
            continue
        if ch == '"' and not escape_next:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]

    return None


def _sanitize_action(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize an LLM-produced action dict for Pydantic construction.

    [I-06] Strips unknown keys (preventing ``extra="forbid"`` rejection)
    and coerces string enum values to their canonical forms.
    """
    action_type = str(raw.get("action_type", "inspect")).lower().strip()

    # Normalize common LLM hallucinations
    action_type_map = {
        "investigate": "inspect",
        "examine": "inspect",
        "check": "inspect",
        "detect": "diagnose",
        "report": "diagnose",
        "repair": "fix",
        "correct": "fix",
        "complete": "finalize",
        "done": "finalize",
        "finish": "finalize",
        "end": "finalize",
    }
    action_type = action_type_map.get(action_type, action_type)

    if action_type not in _VALID_KEYS:
        action_type = "inspect"

    # Filter to valid keys only
    allowed = _VALID_KEYS[action_type]
    sanitized: Dict[str, Any] = {"action_type": action_type}

    for key, value in raw.items():
        if key in allowed and key != "action_type":
            sanitized[key] = value

    # Coerce issue_type to valid enum value
    if "issue_type" in sanitized:
        it = str(sanitized["issue_type"]).lower().strip().replace(" ", "_").replace("-", "_")
        if it not in _ISSUE_TYPES:
            # Best-effort fuzzy match
            for valid in _ISSUE_TYPES:
                if it in valid or valid in it:
                    it = valid
                    break
            else:
                it = "format_error"  # Safe default
        sanitized["issue_type"] = it

    # Coerce fix_type to valid enum value
    if "fix_type" in sanitized:
        ft = str(sanitized["fix_type"]).lower().strip().replace(" ", "_").replace("-", "_")
        if ft not in _FIX_TYPES:
            ft = "correct_value"  # Safe default
        sanitized["fix_type"] = ft

    # Ensure row_indices are integers
    if "row_indices" in sanitized:
        try:
            sanitized["row_indices"] = [int(i) for i in sanitized["row_indices"]]
        except (TypeError, ValueError):
            sanitized["row_indices"] = [0, 1, 2, 3, 4]

    # Ensure row_index is an integer
    if "row_index" in sanitized:
        try:
            sanitized["row_index"] = int(sanitized["row_index"])
        except (TypeError, ValueError):
            del sanitized["row_index"]

    # Ensure new_value is a string if present
    if "new_value" in sanitized and sanitized["new_value"] is not None:
        sanitized["new_value"] = str(sanitized["new_value"])

    return sanitized


def parse_action(text: str) -> Dict[str, Any]:
    """Parse an LLM response into a sanitized action dict.

    Attempts direct JSON parse first, then balanced-brace extraction,
    then falls back to a safe inspect action.
    """
    text = text.strip()

    # Attempt 1: direct parse
    try:
        raw = json.loads(text)
        if isinstance(raw, dict):
            return _sanitize_action(raw)
    except (json.JSONDecodeError, TypeError):
        pass

    # Attempt 2: extract balanced JSON from surrounding text
    extracted = _extract_json(text)
    if extracted:
        try:
            raw = json.loads(extracted)
            if isinstance(raw, dict):
                return _sanitize_action(raw)
        except (json.JSONDecodeError, TypeError):
            pass

    # Attempt 3: regex for simple cases (no nesting)
    match = re.search(r"\{[^{}]+\}", text, re.DOTALL)
    if match:
        try:
            raw = json.loads(match.group())
            if isinstance(raw, dict):
                return _sanitize_action(raw)
        except (json.JSONDecodeError, TypeError):
            pass

    logger.warning("Failed to parse action from LLM response, using fallback")
    return json.loads(_FALLBACK_ACTION)


# ═══════════════════════════════════════════════════════════════════════════
# §4  LLM Interaction — with retry and backoff
# ═══════════════════════════════════════════════════════════════════════════

def _call_llm(
    system_prompt: str,
    user_content: str,
    retries: int = INFERENCE_RETRIES,
) -> str:
    """Call the LLM with exponential backoff retry.

    [I-07] Up to ``retries`` attempts with 1s / 2s / 4s delays.
    Returns the raw response text, or an empty string on total failure.
    """
    for attempt in range(retries):
        try:
            completion = _llm_client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                temperature=TEMPERATURE,
                max_tokens=MAX_TOKENS,
                stream=False,
                timeout=30,  # Prevent indefinite hangs on slow providers
            )
            content = completion.choices[0].message.content or ""
            return content

        except Exception as exc:
            wait = 2**attempt  # 1s, 2s, 4s
            logger.warning(
                "LLM API error (attempt %d/%d): %s — retrying in %ds",
                attempt + 1,
                retries,
                exc,
                wait,
            )
            if attempt < retries - 1:
                time.sleep(wait)

    logger.error("All %d LLM API attempts failed", retries)
    return ""


# ═══════════════════════════════════════════════════════════════════════════
# §5  Observation → Context String
# ═══════════════════════════════════════════════════════════════════════════

def _obs_to_context(obs: Any, task_id: str, step_num: int, max_steps: int) -> str:
    """Convert an observation object to a context string for the LLM.

    Handles both DataQualityObservation objects (attribute access) and
    raw dicts (key access) for compatibility across client types.
    """
    def _get(obj: Any, attr: str, default: Any = None) -> Any:
        if hasattr(obj, attr):
            return getattr(obj, attr, default)
        if isinstance(obj, dict):
            return obj.get(attr, default)
        return default

    parts: List[str] = [
        f"Task: {_get(obs, 'task_id', task_id)}",
        f"Schema: {json.dumps(_get(obs, 'schema_info', {}), separators=(',', ':'))}",
        f"Total rows: {_get(obs, 'total_rows', 0)}",
        f"Step {_get(obs, 'steps_taken', step_num)} / {_get(obs, 'max_steps', max_steps)}",
        f"Issues found so far: {_get(obs, 'issues_found', 0)}",
        f"Issues remaining (hint): {_get(obs, 'issues_remaining_hint', 'unknown')}",
        f"Last action result: {_get(obs, 'action_result', 'initial')}",
        f"Cumulative reward: {float(_get(obs, 'cumulative_reward', 0.0)):.4f}",
    ]

    message = _get(obs, "message", "")
    if message:
        parts.append(f"Message: {message}")

    visible = _get(obs, "visible_rows", None)
    if visible:
        # Show up to 10 rows to give the agent more data
        display = visible[:10] if isinstance(visible, list) else visible
        parts.append(f"Visible rows:\n{json.dumps(display, indent=2)}")

    stats = _get(obs, "column_statistics", None)
    if stats:
        parts.append(f"Column statistics:\n{json.dumps(stats, indent=2)}")

    secondary = _get(obs, "secondary_table_rows", None)
    if secondary:
        display = secondary[:10] if isinstance(secondary, list) else secondary
        parts.append(f"Secondary table rows:\n{json.dumps(display, indent=2)}")

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════
# §6  Task Runner
# ═══════════════════════════════════════════════════════════════════════════

def run_task(task_id: str) -> float:
    """Run a single task episode against the environment server.

    Returns:
        The final cumulative reward for the episode (float in [0, 1]).
    """
    max_steps = MAX_STEPS.get(task_id, 30)
    total_reward = 0.0
    system_prompt = _system_prompt(task_id)

    logger.info("[START] task_id=%s max_steps=%d", task_id, max_steps)

    try:
        env = DataQualityEnv(base_url=ENV_URL)

        # Handle both openenv EnvClient (async, needs sync() + context manager)
        # and our fallback client (sync, is its own context manager)
        ctx = env.sync() if hasattr(env, "sync") else env

        with ctx as e:
            result = e.reset(task_id=task_id)

            # Handle both StepResult wrapper and direct Observation
            obs = getattr(result, "observation", result)

            for step_num in range(max_steps):
                # Build context for the LLM
                user_content = _obs_to_context(obs, task_id, step_num, max_steps)

                # Call LLM with retry
                response_text = _call_llm(system_prompt, user_content)

                if not response_text:
                    # Total API failure — use fallback action
                    action_dict = json.loads(_FALLBACK_ACTION)
                else:
                    action_dict = parse_action(response_text)

                # Construct validated action
                try:
                    action = DataQualityAction(**action_dict)
                except Exception as exc:
                    logger.warning(
                        "Action construction failed: %s — using fallback", exc
                    )
                    action = DataQualityAction(**json.loads(_FALLBACK_ACTION))

                # Execute step
                result = e.step(action)
                obs = getattr(result, "observation", result)

                # Extract step metrics
                done = getattr(obs, "done", False)
                if isinstance(done, property):
                    done = False
                reward_delta = float(getattr(obs, "reward_delta", 0.0))
                total_reward = float(getattr(obs, "cumulative_reward", 0.0))

                logger.info(
                    "[STEP] task=%s step=%d action=%s delta=%+.4f cum=%.4f done=%s",
                    task_id,
                    step_num,
                    action_dict.get("action_type"),
                    reward_delta,
                    total_reward,
                    done,
                )

                if done:
                    break

    except Exception as exc:
        logger.error("[ERROR] task=%s error=%s", task_id, exc)
        import traceback

        traceback.print_exc(file=sys.stderr)

    logger.info("[END] task_id=%s final_reward=%.4f", task_id, total_reward)
    return total_reward


# ═══════════════════════════════════════════════════════════════════════════
# §7  Main — orchestrate all tasks and emit summary
# ═══════════════════════════════════════════════════════════════════════════

def main() -> int:
    """Run all tasks sequentially and print summary.

    Returns:
        Exit code: 0 if average score > 0, 1 otherwise.
    """
    logger.info(
        "Inference starting: model=%s base_url=%s env_url=%s",
        MODEL_NAME,
        API_BASE_URL,
        ENV_URL,
    )

    scores: Dict[str, float] = {}
    start_time = time.time()

    for task_id in TASKS:
        task_start = time.time()
        try:
            scores[task_id] = run_task(task_id)
        except Exception as exc:
            logger.error(
                "[END] task_id=%s final_reward=0.0 error=%s", task_id, exc
            )
            scores[task_id] = 0.0
        task_elapsed = time.time() - task_start
        logger.info(
            "Task %s completed in %.1fs — score=%.4f",
            task_id, task_elapsed, scores[task_id],
        )

    total_elapsed = time.time() - start_time

    # ── Human-readable summary ────────────────────────────────────────────
    print("\n" + "=" * 56)
    print("  BASELINE INFERENCE SCORES")
    print("=" * 56)
    for tid, score in scores.items():
        bar = "█" * int(score * 40) + "░" * (40 - int(score * 40))
        print(f"  {tid:30s}  {score:.4f}  {bar}")

    avg = sum(scores.values()) / len(scores) if scores else 0.0
    bar = "█" * int(avg * 40) + "░" * (40 - int(avg * 40))
    print("-" * 56)
    print(f"  {'AVERAGE':30s}  {avg:.4f}  {bar}")
    print(f"  Elapsed: {total_elapsed:.1f}s")
    print("=" * 56)

    # ── Machine-readable JSON summary ─────────────────────────────────────
    summary = {
        "model": MODEL_NAME,
        "env_url": ENV_URL,
        "scores": scores,
        "average": round(avg, 4),
        "elapsed_seconds": round(total_elapsed, 1),
    }
    print(f"\n[SUMMARY] {json.dumps(summary)}")

    return 0 if avg > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
