"""DataForge agent package — verified autonomous repair.

The agent makes DataForge truly agentic without weakening its moat: an
autonomous policy proposes actions, but every write is gated by the safety
constitution and SMT verifier and committed through the reversible transaction
journal.

Public API:
    run_agent_repair     — Run the verified agent repair pipeline.
    AgentRepairRequest   — Input contract for the controller.
    AgentRepairResult    — Output contract for the controller.
    make_policy          — Build a policy (hosted / local / deterministic / custom).
    register_policy      — Register a custom policy selectable as custom:<name>.
    available_policies   — List selectable policy kinds.
    PolicyUnavailableError — Raised when a backend cannot be constructed.
    Policy               — Policy protocol.
    parse_action         — Parse raw dict into a typed Action model.
    Action               — Discriminated union of all action types.
    Scratchpad           — In-episode hypothesis tracker.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataforge.agent.scratchpad import Scratchpad
from dataforge.agent.tool_actions import Action, parse_action

if TYPE_CHECKING:
    from dataforge.agent.controller import (
        AgentRepairRequest,
        AgentRepairResult,
        run_agent_repair,
    )
    from dataforge.agent.policy import (
        Policy,
        PolicyUnavailableError,
        available_policies,
        make_policy,
        register_policy,
    )

__all__ = [
    "Action",
    "AgentRepairRequest",
    "AgentRepairResult",
    "Policy",
    "PolicyUnavailableError",
    "Scratchpad",
    "available_policies",
    "make_policy",
    "parse_action",
    "register_policy",
    "run_agent_repair",
]

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AgentRepairRequest": ("dataforge.agent.controller", "AgentRepairRequest"),
    "AgentRepairResult": ("dataforge.agent.controller", "AgentRepairResult"),
    "run_agent_repair": ("dataforge.agent.controller", "run_agent_repair"),
    "Policy": ("dataforge.agent.policy", "Policy"),
    "PolicyUnavailableError": ("dataforge.agent.policy", "PolicyUnavailableError"),
    "available_policies": ("dataforge.agent.policy", "available_policies"),
    "make_policy": ("dataforge.agent.policy", "make_policy"),
    "register_policy": ("dataforge.agent.policy", "register_policy"),
}


def __getattr__(name: str) -> Any:
    """Lazily resolve controller/policy exports to avoid import-time cost."""
    try:
        module_name, attribute = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    from importlib import import_module

    return getattr(import_module(module_name), attribute)
