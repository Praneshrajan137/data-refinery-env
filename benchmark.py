"""Compatibility shim for the legacy top-level benchmark script."""

from __future__ import annotations

from data_quality_env import benchmark as _impl
from data_quality_env.benchmark import *  # noqa: F401,F403

main = _impl.main


if __name__ == "__main__":
    raise SystemExit(main())
