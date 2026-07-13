"""Policy abstraction for the DataForge verified autonomous agent.

A :class:`Policy` turns an :class:`AgentObservation` into the next typed
:class:`~dataforge.agent.tool_actions.Action` (or ``None`` to stop). The
controller owns the loop, the verified write gate, and the transaction commit;
the policy owns only *reasoning* — which tool to use next.

Selectable backends (via :func:`make_policy`):

``hosted`` (default)
    An :class:`LLMPolicy` over the hosted provider client (groq/gemini). Best
    accuracy now; needs an API key. Fails fast with an actionable message when
    no key is configured.

``local``
    An :class:`LLMPolicy` over the fine-tuned local model (free, private,
    offline). Fails fast if transformers/the model are unavailable.

``remote``
    An :class:`LLMPolicy` over a hosted model Space, driven over HTTP with no
    ``torch``/``transformers`` install (see
    :mod:`dataforge.agent.backends.remote`). Lets a CPU-only deployment run the
    real agent loop against the trained checkpoint. Fails fast if
    ``DATAFORGE_REMOTE_MODEL_URL`` is unset.

``deterministic``
    A no-op :class:`DeterministicPolicy`: the controller's deterministic floor
    already did everything provable. Used for the parity mode.

``custom:<name>``
    A user-registered policy (see :func:`register_policy`). Custom policies are
    still wrapped by the controller's executor, so they cannot bypass the gate.

The policy never writes data. Every ``FIX`` it proposes is gated by the
controller's executor (safety constitution + SMT verifier) before it can be
staged, so a weak policy can only *fail to help* — it can never corrupt data
or ship a repair below the deterministic baseline.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from pydantic import ValidationError

from dataforge.agent.providers import Message
from dataforge.agent.tool_actions import Action, parse_action

__all__ = [
    "AgentObservation",
    "CompletionFn",
    "DeterministicPolicy",
    "LLMPolicy",
    "Policy",
    "PolicyUnavailableError",
    "ResidualIssue",
    "available_policies",
    "build_system_prompt",
    "make_policy",
    "register_policy",
]

logger = logging.getLogger("dataforge.agent.policy")

# Built-in selectable policy kinds (custom kinds use the ``custom:<name>`` form).
_BUILTIN_KINDS = ("hosted", "local", "remote", "deterministic")

# Hosted provider -> required API key environment variable.
_PROVIDER_KEY_ENV = {
    "groq": "GROQ_API_KEY",
    "gemini": "GEMINI_API_KEY",
    "bedrock": "AWS_BEARER_TOKEN_BEDROCK",
    "azure": "AZURE_API_KEY",
    "grok": "XAI_API_KEY",
    "cerebras": "CEREBRAS_API_KEY",
}


class PolicyUnavailableError(RuntimeError):
    """Raised when a requested policy backend cannot be constructed.

    Carries an actionable message (e.g. a missing API key or an unavailable
    local model) so the CLI/MCP can tell the user exactly how to proceed.
    """


# A synchronous chat-completion callable: (messages, model, temperature) -> text.
CompletionFn = Callable[[list[Message], str | None, float], str]

# Sentinel action_type values the model may emit to declare it is finished.
_FINALIZE_TOKENS = frozenset({"FINALIZE", "DONE", "STOP", "FINISH", "COMPLETE"})


@dataclass(frozen=True)
class ResidualIssue:
    """A detected issue the deterministic floor did not repair.

    Args:
        row: Zero-indexed row number.
        column: Column name.
        issue_type: Detector issue category.
        severity: Severity label (``safe`` / ``review`` / ``unsafe``).
        expected: Expected value if the detector knows it, else ``None``.
        actual: The actual cell value.
        reason: Human-readable explanation of why the cell was flagged.
    """

    row: int
    column: str
    issue_type: str
    severity: str
    expected: str | None
    actual: str
    reason: str


@dataclass(frozen=True)
class AgentObservation:
    """Everything the policy sees on a single turn.

    Args:
        columns: Column names in CSV order.
        row_count: Number of rows in the dataset.
        residual_issues: Issues still unresolved after the deterministic floor.
        sample_rows: A slice of dataset rows (each a column->value mapping).
        scratchpad_summary: Compact summary of recorded hypotheses/dead-ends.
        last_result: Outcome text of the previous action. For a rejected FIX
            this carries the safety reason and SMT unsat-core so the policy can
            self-correct.
        steps_taken: Number of actions taken so far this episode.
        max_steps: Total step budget for the episode.
        staged_fix_count: Verified fixes staged for commit so far.
    """

    columns: tuple[str, ...]
    row_count: int
    residual_issues: tuple[ResidualIssue, ...]
    sample_rows: tuple[dict[str, str], ...]
    scratchpad_summary: str
    last_result: str
    steps_taken: int
    max_steps: int
    staged_fix_count: int


class Policy(Protocol):
    """Structural protocol implemented by every agent policy."""

    @property
    def name(self) -> str:
        """Stable policy identifier, surfaced in receipts and traces."""
        ...

    @property
    def provenance(self) -> str:
        """Provenance label applied to fixes this policy proposes.

        One of the :data:`dataforge.repairers.base.ProvenanceLiteral` values.
        """
        ...

    def reset(self, observation: AgentObservation) -> None:
        """Prepare for a new episode given the initial observation."""
        ...

    def propose_action(self, observation: AgentObservation) -> Action | None:
        """Return the next action, or ``None`` to finish the episode."""
        ...


class DeterministicPolicy:
    """A no-op policy: the deterministic floor has already done everything.

    Returns ``None`` immediately so the controller commits exactly the
    deterministic floor fixes. This guarantees byte-for-byte parity with the
    legacy ``run_repair_pipeline`` path and is the safe fallback when no model
    backend is available.
    """

    name = "deterministic"
    provenance = "deterministic"

    def reset(self, observation: AgentObservation) -> None:
        """No state to reset."""

    def propose_action(self, observation: AgentObservation) -> Action | None:
        """Always finish immediately; the floor covers everything we can do."""
        return None


def build_system_prompt() -> str:
    """Build the system prompt for the verified-agent LLM policy."""
    return (
        "You are DataForge's data-repair agent. A deterministic engine has already "
        "fixed every issue it could prove safe. Your job is to resolve the REMAINING "
        "issues that the rules could not, using exact, correct replacement values.\n\n"
        "## Hard guarantees you operate under\n"
        "- You never write data directly. Every FIX you propose is independently "
        "verified by an SMT solver and a safety constitution before it is accepted.\n"
        "- Row deletion and edits to PII or primary keys are DENIED by the constitution. "
        "Do not attempt them.\n"
        "- If a FIX is rejected you will be told why (safety reason and/or SMT unsat-core). "
        "Use that feedback to propose a corrected value. Do not repeat a rejected value.\n\n"
        "## Respond with EXACTLY ONE JSON object per turn. No prose, no markdown.\n"
        "Available actions:\n"
        '  {"action_type":"INSPECT_ROWS","row_indices":[0,1,2],"column_names":["a"]}\n'
        '  {"action_type":"PATTERN_MATCH","pattern":"^\\\\d+$","column":"a","expect_match":false}\n'
        '  {"action_type":"STAT_TEST","test_type":"zscore","column":"a"}\n'
        '  {"action_type":"HYPOTHESIS","claim":"...","affected_rows":[5],'
        '"affected_columns":["a"],"root_cause_type":"decimal_shift"}\n'
        '  {"action_type":"FIX","row":5,"column":"a","new_value":"4.5",'
        '"justification":"why this value is correct"}\n'
        '  {"action_type":"FINALIZE"}  // emit when no residual issue can be fixed\n\n'
        "## Strategy\n"
        "- Inspect the rows around a residual issue before fixing it.\n"
        "- Prefer the smallest edit that makes the value correct.\n"
        "- Only FIX when you can justify the exact correct value; otherwise FINALIZE.\n"
        "- When the residual list is empty, FINALIZE immediately."
    )


def _format_observation(observation: AgentObservation) -> str:
    """Render an observation as a compact, signal-dense user message."""
    lines: list[str] = [
        f"Step {observation.steps_taken}/{observation.max_steps}. "
        f"Staged verified fixes: {observation.staged_fix_count}.",
        f"Columns: {list(observation.columns)} ({observation.row_count} rows).",
        f"Scratchpad: {observation.scratchpad_summary}",
    ]
    if observation.last_result:
        lines.append(f"Last action result: {observation.last_result}")
    if observation.residual_issues:
        lines.append(f"Residual issues ({len(observation.residual_issues)}):")
        for issue in observation.residual_issues[:20]:
            expected = "" if issue.expected is None else f" expected={issue.expected!r}"
            lines.append(
                f"  - row {issue.row} col {issue.column!r} type={issue.issue_type} "
                f"actual={issue.actual!r}{expected} :: {issue.reason}"
            )
    else:
        lines.append("Residual issues: none. FINALIZE now.")
    if observation.sample_rows:
        lines.append("Sample rows:")
        for offset, row in enumerate(observation.sample_rows[:10]):
            lines.append(f"  [{offset}] {row}")
    return "\n".join(lines)


def _extract_action_type(text: str) -> str | None:
    """Best-effort read of the ``action_type`` field from raw model text."""
    import json
    import re

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        payload = json.loads(match.group())
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(payload, dict):
        value = payload.get("action_type")
        if isinstance(value, str):
            return value.strip().upper()
    return None


def _extract_json_object(text: str) -> dict[str, object] | None:
    """Extract the first balanced JSON object from text, if any."""
    import json

    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = in_string
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(text[start : i + 1])
                except (json.JSONDecodeError, TypeError):
                    return None
                return parsed if isinstance(parsed, dict) else None
    return None


class LLMPolicy:
    """A stateful, multi-turn LLM policy validated by the typed parser.

    Args:
        complete_fn: Synchronous chat-completion callable.
        model: Optional model name passed to ``complete_fn``.
        temperature: Sampling temperature.
        provenance: Provenance label for proposed fixes (``llm_live`` by default).
        name: Policy identifier for receipts and traces.
        max_history_messages: Hard cap on retained transcript messages (the
            system prompt is always kept) to bound context growth.
    """

    def __init__(
        self,
        complete_fn: CompletionFn,
        *,
        model: str | None = None,
        temperature: float = 0.1,
        provenance: str = "llm_live",
        name: str = "llm",
        max_history_messages: int = 24,
    ) -> None:
        self._complete = complete_fn
        self._model = model
        self._temperature = temperature
        self._provenance = provenance
        self._name = name
        self._max_history = max(4, max_history_messages)
        self._messages: list[Message] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def provenance(self) -> str:
        return self._provenance

    def reset(self, observation: AgentObservation) -> None:
        """Seed the transcript with the system prompt for a fresh episode."""
        self._messages = [{"role": "system", "content": build_system_prompt()}]

    def _truncate(self) -> None:
        """Keep the system prompt plus the most recent exchanges."""
        if len(self._messages) <= self._max_history:
            return
        system = self._messages[0]
        tail = self._messages[-(self._max_history - 1) :]
        self._messages = [system, *tail]

    def propose_action(self, observation: AgentObservation) -> Action | None:
        """Format the observation, call the model, and parse a typed action."""
        self._messages.append({"role": "user", "content": _format_observation(observation)})
        self._truncate()

        try:
            raw = self._complete(self._messages, self._model, self._temperature)
        except Exception as exc:  # provider/runtime failure -> safe fallback
            logger.warning("LLM completion failed (%s); falling back to read-only action", exc)
            raw = ""

        self._messages.append({"role": "assistant", "content": raw or ""})

        action_type = _extract_action_type(raw)
        if action_type in _FINALIZE_TOKENS:
            return None
        if not observation.residual_issues:
            # Nothing left to fix; finish even if the model rambled.
            return None

        payload = _extract_json_object(raw)
        if payload is None:
            return self._fallback(observation)
        try:
            return parse_action(payload)
        except (ValidationError, KeyError, ValueError) as exc:
            logger.debug("Unparseable action %r (%s); using read-only fallback", payload, exc)
            return self._fallback(observation)

    def _fallback(self, observation: AgentObservation) -> Action | None:
        """Inspect the rows around the first residual issue when output is bad."""
        if not observation.residual_issues:
            return None
        target = observation.residual_issues[0].row
        window = sorted({max(0, target - 1), target, min(observation.row_count - 1, target + 1)})
        try:
            return parse_action({"action_type": "INSPECT_ROWS", "row_indices": window})
        except (ValidationError, KeyError, ValueError):
            return None


# ── Custom policy registry ────────────────────────────────────────────────

# Factory signature: accepts model/temperature keyword args, returns a Policy.
PolicyFactory = Callable[..., "Policy"]
_POLICY_REGISTRY: dict[str, PolicyFactory] = {}


def register_policy(name: str, factory: PolicyFactory) -> None:
    """Register a custom policy factory selectable as ``custom:<name>``.

    Args:
        name: The custom policy name (used as ``custom:<name>``). Case-insensitive.
        factory: A callable returning a :class:`Policy`. It is invoked as
            ``factory(model=..., temperature=...)``; accept ``**kwargs`` to stay
            forward-compatible.

    Note:
        Custom policies are still wrapped by the controller's verified executor,
        so they cannot bypass the safety constitution or SMT verifier.
    """
    key = name.strip().lower()
    if not key:
        raise ValueError("Custom policy name must be non-empty.")
    _POLICY_REGISTRY[key] = factory


def available_policies() -> list[str]:
    """Return all selectable policy kinds (built-ins plus registered custom)."""
    return [*_BUILTIN_KINDS, *(f"custom:{name}" for name in sorted(_POLICY_REGISTRY))]


def make_policy(
    kind: str,
    *,
    model: str | None = None,
    temperature: float = 0.1,
    provider: str | None = None,
    completion_override: CompletionFn | None = None,
) -> Policy:
    """Build a policy by kind.

    Args:
        kind: ``"hosted"`` (default surface), ``"local"``, ``"remote"``,
            ``"deterministic"``, or ``"custom:<name>"`` for a registered custom
            policy.
        model: Optional model name for LLM policies.
        temperature: Sampling temperature for LLM policies.
        provider: Hosted provider override (``"groq"`` or ``"gemini"``). When
            omitted, the provider is autodetected from the environment.
        completion_override: Inject a completion callable (used by tests and by
            callers that already hold a backend); bypasses backend construction
            and credential checks.

    Returns:
        A ready policy.

    Raises:
        PolicyUnavailableError: If a hosted/local backend cannot be constructed
            (e.g. missing API key or unavailable local model).
        ValueError: If the kind is unknown or a custom policy is not registered.
    """
    normalized = kind.strip().lower()

    if normalized == "deterministic":
        return DeterministicPolicy()

    if completion_override is not None:
        return LLMPolicy(
            completion_override,
            model=model,
            temperature=temperature,
            provenance="llm_live",
            name=normalized,
        )

    if normalized == "hosted":
        return _build_hosted_policy(model=model, temperature=temperature, provider=provider)
    if normalized == "local":
        return _build_local_policy(model=model, temperature=temperature)
    if normalized == "remote":
        return _build_remote_policy(model=model, temperature=temperature)

    if normalized.startswith("custom:") or normalized in _POLICY_REGISTRY:
        registry_name = normalized.split(":", 1)[1] if ":" in normalized else normalized
        factory = _POLICY_REGISTRY.get(registry_name)
        if factory is None:
            raise ValueError(
                f"Custom policy {registry_name!r} is not registered. "
                f"Available: {available_policies()}"
            )
        return factory(model=model, temperature=temperature)

    raise ValueError(f"Unknown policy kind: {kind!r}. Available: {available_policies()}")


def _build_hosted_policy(*, model: str | None, temperature: float, provider: str | None) -> Policy:
    """Construct an LLM policy over the hosted provider client, failing fast.

    Args:
        model: Optional model name.
        temperature: Sampling temperature.
        provider: ``"groq"`` / ``"gemini"`` override, or ``None`` to autodetect.

    Raises:
        PolicyUnavailableError: If no usable provider credential is configured.
    """
    import asyncio
    import os

    from dataforge.agent.providers import complete as async_complete
    from dataforge.agent.providers import get_provider_name

    effective = (provider or get_provider_name()).strip().lower()
    if effective not in _PROVIDER_KEY_ENV:
        raise PolicyUnavailableError(
            f"Unsupported hosted provider {effective!r}. "
            f"Choose one of: {sorted(_PROVIDER_KEY_ENV)}."
        )
    key_env = _PROVIDER_KEY_ENV[effective]
    if not os.environ.get(key_env):
        raise PolicyUnavailableError(
            f"No API key found for hosted provider {effective!r}. "
            f"Set {key_env}, or use --policy local (offline) or --policy deterministic."
        )

    def _complete(messages: Sequence[Message], model_name: str | None, temp: float) -> str:
        # Pin the provider for this call so dispatch is explicit even when
        # DATAFORGE_LLM_PROVIDER is unset; restore the prior value afterward.
        previous = os.environ.get("DATAFORGE_LLM_PROVIDER")
        os.environ["DATAFORGE_LLM_PROVIDER"] = effective
        try:
            return asyncio.run(async_complete(list(messages), model=model_name, temperature=temp))
        finally:
            if previous is None:
                os.environ.pop("DATAFORGE_LLM_PROVIDER", None)
            else:
                os.environ["DATAFORGE_LLM_PROVIDER"] = previous

    return LLMPolicy(
        _complete,
        model=model,
        temperature=temperature,
        provenance="llm_live",
        name=f"hosted:{effective}",
    )


def _build_local_policy(*, model: str | None, temperature: float) -> Policy:
    """Construct an LLM policy over the local model backend, failing fast.

    Raises:
        PolicyUnavailableError: If transformers/torch or the model are missing.
    """
    try:
        from dataforge.agent.backends.local import build_local_completion

        complete_fn = build_local_completion(model)
    except Exception as exc:  # torch/transformers/model unavailable
        raise PolicyUnavailableError(
            "Local model backend is unavailable "
            f"({exc}). Install the agent extras (transformers, torch) and ensure the "
            "model is downloadable, or use --policy hosted or --policy deterministic."
        ) from exc
    return LLMPolicy(
        complete_fn, model=model, temperature=temperature, provenance="llm_live", name="local"
    )


def _build_remote_policy(*, model: str | None, temperature: float) -> Policy:
    """Construct an LLM policy over a hosted model Space, failing fast.

    Raises:
        PolicyUnavailableError: If the remote backend is not configured
            (``DATAFORGE_REMOTE_MODEL_URL`` unset) or cannot be built.
    """
    try:
        from dataforge.agent.backends.remote import build_remote_completion

        complete_fn = build_remote_completion(model)
    except Exception as exc:  # backend unavailable / not configured
        raise PolicyUnavailableError(
            "Remote model backend is unavailable "
            f"({exc}). Set DATAFORGE_REMOTE_MODEL_URL to your hosted model Space, "
            "or use --policy hosted, --policy local, or --policy deterministic."
        ) from exc
    return LLMPolicy(
        complete_fn, model=model, temperature=temperature, provenance="llm_live", name="remote"
    )
