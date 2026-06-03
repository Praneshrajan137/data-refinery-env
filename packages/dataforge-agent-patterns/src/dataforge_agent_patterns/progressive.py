"""Progressive disclosure for agent tools."""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import BaseModel, Field

__all__ = ["ProgressiveToolDisclosure", "ToolDisclosure"]


class ToolDisclosure(BaseModel):
    """A tool and the minimum task difficulty required to reveal it.

    Args:
        name: Tool name.
        min_difficulty: Minimum task difficulty required for disclosure.
    """

    name: str = Field(min_length=1)
    min_difficulty: int = Field(ge=0)

    model_config = {"frozen": True}


class ProgressiveToolDisclosure:
    """Reveal tools only when task difficulty warrants them.

    Args:
        tools: Mapping from tool name to minimum difficulty.

    Example:
        >>> disclosure = ProgressiveToolDisclosure({"search": 1, "shell": 3})
        >>> disclosure.visible_tools(task_difficulty=2)
        ('search',)
    """

    def __init__(self, tools: Mapping[str, int]) -> None:
        self._tools = tuple(
            sorted(
                (
                    ToolDisclosure(name=name, min_difficulty=difficulty)
                    for name, difficulty in tools.items()
                ),
                key=lambda item: (item.min_difficulty, item.name),
            )
        )

    def visible_tools(self, *, task_difficulty: int) -> tuple[str, ...]:
        """Return tools visible at the requested task difficulty.

        Args:
            task_difficulty: Non-negative task difficulty level.

        Returns:
            Tool names whose minimum difficulty is met.
        """
        if task_difficulty < 0:
            raise ValueError("task_difficulty must be >= 0")
        return tuple(tool.name for tool in self._tools if tool.min_difficulty <= task_difficulty)

    def should_disclose(self, tool_name: str, *, task_difficulty: int) -> bool:
        """Return whether a single tool should be visible.

        Args:
            tool_name: Tool name to check.
            task_difficulty: Non-negative task difficulty level.

        Returns:
            True if the tool exists and should be disclosed.
        """
        return tool_name in self.visible_tools(task_difficulty=task_difficulty)
