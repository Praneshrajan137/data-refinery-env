"""Compatibility shim for the legacy top-level random baseline script."""

from __future__ import annotations

from data_quality_env import random_baseline as _impl
from data_quality_env.random_baseline import *  # noqa: F401,F403

main = _impl.main
run_episode = _impl.run_episode


if __name__ == "__main__":
    raise SystemExit(main())
