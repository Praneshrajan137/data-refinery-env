"""Causal cascade detection for action-effect graphs."""

from __future__ import annotations

from pydantic import BaseModel, Field

__all__ = ["CausalCascadeDetector", "CascadeVerdict", "DirectedEffect"]


class DirectedEffect(BaseModel):
    """A directed effect between two action or state labels.

    Args:
        source: Upstream action or state label.
        target: Downstream action or state label.
        confidence: Confidence in the directed effect.
    """

    source: str = Field(min_length=1)
    target: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)

    model_config = {"frozen": True}


class CascadeVerdict(BaseModel):
    """Cascading-effect decision.

    Args:
        cascading: Whether any supplied action reaches another action.
        roots: Supplied actions that are not downstream of another supplied action.
        downstream: Supplied actions that are reachable from roots.
    """

    cascading: bool
    roots: tuple[str, ...]
    downstream: tuple[str, ...]

    model_config = {"frozen": True}


class CausalCascadeDetector:
    """Detect whether selected actions imply cascading downstream effects.

    Args:
        effects: Directed effect edges.

    Example:
        >>> effect = DirectedEffect(source="discount", target="total", confidence=0.9)
        >>> CausalCascadeDetector([effect]).detect(["discount", "total"]).cascading
        True
    """

    def __init__(self, effects: list[DirectedEffect] | tuple[DirectedEffect, ...]) -> None:
        self._adjacency: dict[str, set[str]] = {}
        for effect in effects:
            if effect.confidence > 0.0:
                self._adjacency.setdefault(effect.source, set()).add(effect.target)
                self._adjacency.setdefault(effect.target, set())

    def detect(self, actions: list[str] | tuple[str, ...]) -> CascadeVerdict:
        """Return whether the selected actions contain a cascade.

        Args:
            actions: Selected action or state labels.

        Returns:
            CascadeVerdict.
        """
        selected = tuple(dict.fromkeys(actions))
        roots: list[str] = []
        downstream: set[str] = set()
        for action in selected:
            has_upstream = any(
                other != action and self._reachable(other, action) for other in selected
            )
            if not has_upstream:
                roots.append(action)
            if any(other != action and self._reachable(action, other) for other in selected):
                downstream.add(action)
        for root in roots:
            downstream.update(
                action for action in selected if action != root and self._reachable(root, action)
            )
        return CascadeVerdict(
            cascading=bool(downstream),
            roots=tuple(roots),
            downstream=tuple(sorted(downstream)),
        )

    def _reachable(self, source: str, target: str) -> bool:
        """Return whether target is reachable from source."""
        frontier = list(self._adjacency.get(source, set()))
        seen: set[str] = set()
        while frontier:
            node = frontier.pop()
            if node == target:
                return True
            if node in seen:
                continue
            seen.add(node)
            frontier.extend(self._adjacency.get(node, set()))
        return False
