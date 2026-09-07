"""Stateless GRPO reward function for DataForge repair completions."""

from __future__ import annotations

from typing import Any

from archive.training.grpo_contract import batch_item, score_grpo_completion


def dataforge_reward(completions: list[Any], **kwargs: Any) -> list[float]:
    """Return one exact-repair reward per GRPO completion.

    Expected optional batch kwargs are ``ground_truth``, ``allowed_columns``,
    ``valid_rows``, and ``inferability``. The function is intentionally local
    and stateless: it parses JSON completions and scores exact cell repairs
    without calling an OpenEnv HTTP endpoint or any external provider.
    """
    raw_truth_batch = kwargs.get("ground_truth", kwargs.get("ground_truth_cells"))
    raw_columns_batch = kwargs.get("allowed_columns")
    raw_rows_batch = kwargs.get("valid_rows")
    raw_inferability_batch = kwargs.get("inferability")
    rewards: list[float] = []
    diagnostics: list[dict[str, Any]] = []
    for index, completion in enumerate(completions):
        reward, diagnostic = score_grpo_completion(
            completion,
            raw_truth=batch_item(raw_truth_batch, index, []),
            raw_allowed_columns=batch_item(raw_columns_batch, index, []),
            raw_valid_rows=batch_item(raw_rows_batch, index, []),
            raw_inferability=batch_item(raw_inferability_batch, index, None),
        )
        rewards.append(reward)
        diagnostics.append(diagnostic)
    dataforge_reward.last_diagnostics = diagnostics
    return rewards


dataforge_reward.last_diagnostics = []
