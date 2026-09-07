"""GiGPO-style advantage helpers for DataForge OpenEnv trajectories.

The production trainer remains gated behind external compute, but these local
helpers pin the DataForge-specific semantics: group rollouts by canonical
anchor observation, then compute macro episode and micro step advantages inside
each anchor group.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class EpisodeRollout:
    """One DataForge OpenEnv rollout attached to an anchor observation."""

    rollout_id: str
    anchor_observation: dict[str, Any]
    episode_reward: float
    step_rewards: tuple[float, ...]

    @property
    def anchor_hash(self) -> str:
        """Return the stable canonical hash for this rollout's anchor state."""
        return canonical_observation_hash(self.anchor_observation)


@dataclass(frozen=True, slots=True)
class EpisodeAdvantage:
    """Macro and micro advantages for one rollout."""

    rollout_id: str
    anchor_hash: str
    macro_episode_advantage: float
    micro_step_advantages: tuple[float, ...]


def canonical_observation_hash(observation: dict[str, Any]) -> str:
    """Return a stable SHA-256 hash for a DataForge OpenEnv observation."""
    encoded = json.dumps(
        _canonicalize(observation),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def group_rollouts_by_anchor(
    rollouts: Iterable[EpisodeRollout],
) -> dict[str, tuple[EpisodeRollout, ...]]:
    """Group rollouts by canonical anchor observation hash."""
    groups: dict[str, list[EpisodeRollout]] = {}
    for rollout in rollouts:
        groups.setdefault(rollout.anchor_hash, []).append(rollout)
    return {anchor_hash: tuple(items) for anchor_hash, items in groups.items()}


def compute_anchor_group_advantages(
    rollouts: Sequence[EpisodeRollout],
) -> tuple[EpisodeAdvantage, ...]:
    """Compute GiGPO macro and micro advantages within one anchor group."""
    if not rollouts:
        return ()
    anchor_hashes = {rollout.anchor_hash for rollout in rollouts}
    if len(anchor_hashes) != 1:
        raise ValueError("rollouts must share one canonical anchor hash")
    macro_baseline = sum(rollout.episode_reward for rollout in rollouts) / len(rollouts)
    max_steps = max((len(rollout.step_rewards) for rollout in rollouts), default=0)
    step_baselines = tuple(_step_baseline(rollouts, step_index) for step_index in range(max_steps))
    anchor_hash = next(iter(anchor_hashes))
    return tuple(
        EpisodeAdvantage(
            rollout_id=rollout.rollout_id,
            anchor_hash=anchor_hash,
            macro_episode_advantage=rollout.episode_reward - macro_baseline,
            micro_step_advantages=tuple(
                reward - step_baselines[index] for index, reward in enumerate(rollout.step_rewards)
            ),
        )
        for rollout in rollouts
    )


def compute_gigpo_advantages(
    rollouts: Iterable[EpisodeRollout],
) -> tuple[EpisodeAdvantage, ...]:
    """Compute advantages for all anchor groups."""
    advantages: list[EpisodeAdvantage] = []
    for group in group_rollouts_by_anchor(rollouts).values():
        advantages.extend(compute_anchor_group_advantages(group))
    return tuple(advantages)


def _step_baseline(rollouts: Sequence[EpisodeRollout], step_index: int) -> float:
    values = [
        rollout.step_rewards[step_index]
        for rollout in rollouts
        if step_index < len(rollout.step_rewards)
    ]
    if not values:
        return 0.0
    return sum(values) / len(values)


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonicalize(child) for key, child in sorted(value.items())}
    if isinstance(value, list | tuple):
        return [_canonicalize(item) for item in value]
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return round(value, 12)
    return value
