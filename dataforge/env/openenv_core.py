"""OpenEnv-core adapter for the DataForge RL environment."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from dataforge.env.environment import DataForgeEnv

if TYPE_CHECKING:

    class OpenEnvAction(BaseModel):
        """Typed stand-in for openenv-core's action model."""

        metadata: dict[str, Any] = Field(default_factory=dict)

    class OpenEnvObservation(BaseModel):
        """Typed stand-in for openenv-core's observation model."""

        done: bool = False
        reward: float | None = None
        metadata: dict[str, Any] = Field(default_factory=dict)

    class OpenEnvEnvironment:
        """Typed stand-in for openenv-core's environment base."""

        def __init__(self) -> None: ...

    def create_app(*args: Any, **kwargs: Any) -> Any:
        """Typed stand-in for openenv-core's FastAPI app factory."""
        ...

else:
    try:
        from openenv.core.env_server import Action as OpenEnvAction
        from openenv.core.env_server import Environment as OpenEnvEnvironment
        from openenv.core.env_server import Observation as OpenEnvObservation
        from openenv.core.env_server import create_app
    except ImportError as exc:  # pragma: no cover - exercised only without openenv extra
        raise RuntimeError(
            "The OpenEnv adapter requires the openenv extra: pip install 'dataforge[openenv]'."
        ) from exc


class DataForgeOpenEnvAction(OpenEnvAction):
    """OpenEnv action wrapper for DataForge's typed action payloads."""

    action_type: str = Field(min_length=1)
    row_indices: list[int] | None = None
    column_names: list[str] | None = None
    query: str | None = None
    sql: str | None = None
    test_type: str | None = None
    test: str | None = None
    column: str | None = None
    threshold: float | None = None
    pattern: str | None = None
    regex: str | None = None
    expect_match: bool | None = None
    claim: str | None = None
    affected_rows: list[int] | None = None
    affected_columns: list[str] | None = None
    root_cause_type: str | None = None
    error_indices: list[int] | None = None
    row: int | None = None
    issue_type: str | None = None
    new_value: str | None = None
    proposed_value: str | None = None
    justification: str | None = None
    fix_type: str | None = None

    def as_dataforge_payload(self) -> dict[str, Any]:
        """Return the action payload expected by ``DataForgeEnv.step``."""
        payload: dict[str, Any] = self.model_dump(exclude_none=True)
        payload.pop("metadata", None)
        return payload


class DataForgeOpenEnvObservation(OpenEnvObservation):
    """OpenEnv observation model mirroring DataForge's native observation."""

    visible_rows: list[dict[str, Any]] | None = None
    detector_hints: list[str] | None = None
    scratchpad_summary: str = ""
    step_budget_remaining: int = 0
    tool_usage_history: list[dict[str, Any]] = Field(default_factory=list)
    latest_result: dict[str, Any] | None = None
    cumulative_reward: float = 0.0


def _to_openenv_observation(payload: dict[str, Any]) -> DataForgeOpenEnvObservation:
    """Convert a native DataForge observation dictionary into OpenEnv shape."""
    return DataForgeOpenEnvObservation(
        visible_rows=payload.get("visible_rows"),
        detector_hints=payload.get("detector_hints"),
        scratchpad_summary=str(payload.get("scratchpad_summary", "")),
        step_budget_remaining=int(payload.get("step_budget_remaining", 0)),
        tool_usage_history=list(payload.get("tool_usage_history") or []),
        latest_result=payload.get("latest_result"),
        done=bool(payload.get("done", False)),
        reward=payload.get("reward"),
        cumulative_reward=float(payload.get("cumulative_reward", 0.0)),
        metadata=dict(payload.get("metadata") or {}),
    )


class DataForgeOpenEnv(OpenEnvEnvironment):
    """OpenEnv-native environment wrapper."""

    SUPPORTS_CONCURRENT_SESSIONS = True

    def __init__(self) -> None:
        super().__init__()
        self._env = DataForgeEnv()
        self._last_observation: DataForgeOpenEnvObservation | None = None

    def reset(
        self,
        seed: int | None = None,
        episode_id: str | None = None,
        **kwargs: Any,
    ) -> DataForgeOpenEnvObservation:
        """Reset the wrapped DataForge environment."""
        del episode_id, kwargs
        result = self._env.reset(seed=seed)
        observation = _to_openenv_observation(result.observation.model_dump(mode="json"))
        self._last_observation = observation
        return observation

    def step(
        self,
        action: DataForgeOpenEnvAction,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> DataForgeOpenEnvObservation:
        """Step the wrapped DataForge environment."""
        del timeout_s, kwargs
        result = self._env.step(action.as_dataforge_payload())
        observation = _to_openenv_observation(result.observation.model_dump(mode="json"))
        self._last_observation = observation
        return observation

    def state(self) -> DataForgeOpenEnvObservation:
        """Return the latest observation or reset lazily."""
        if self._last_observation is None:
            return self.reset()
        return self._last_observation

    def close(self) -> None:
        """Close the wrapped environment."""
        self._env.close()


app = create_app(
    DataForgeOpenEnv,
    DataForgeOpenEnvAction,
    DataForgeOpenEnvObservation,
    env_name="dataforge-env",
    max_concurrent_envs=64,
)
