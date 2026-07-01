"""Model backends for the DataForge verified agent.

Backends expose a synchronous completion callable
``(messages, model, temperature) -> str`` so one agent loop can drive local or
hosted policies interchangeably.
"""

from __future__ import annotations

__all__ = ["DEFAULT_LOCAL_MODEL", "build_local_completion"]


def __getattr__(name: str) -> object:
    """Lazily expose the local backend without importing torch at package load."""
    if name in {"DEFAULT_LOCAL_MODEL", "build_local_completion"}:
        from dataforge.agent.backends import local

        return getattr(local, name)
    raise AttributeError(name)
