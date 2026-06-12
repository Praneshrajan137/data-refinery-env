"""dataforge-evals: agent-agnostic evaluation harness for data-quality repair agents."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from dataforge_evals.agents.base import (
    Agent,
    AgentRunResult,
    AgentTask,
    Fix,
    GroundTruthCell,
    Task,
    Usage,
)
from dataforge_evals.grader import Grade

for _distribution_name in ("dataforge_07_evals", "dataforge-evals", "dataforge15-evals"):
    try:
        __version__: str = version(_distribution_name)
        break
    except PackageNotFoundError:
        continue
else:  # pragma: no cover - editable install normally has metadata
    __version__ = "0.0.0-dev"

__all__ = [
    "Agent",
    "AgentRunResult",
    "AgentTask",
    "Fix",
    "Grade",
    "GroundTruthCell",
    "Task",
    "Usage",
    "__version__",
]
