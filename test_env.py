"""Compatibility shim for the legacy top-level environment test harness."""

from __future__ import annotations

from data_quality_env import test_env as _impl
from data_quality_env.test_env import *  # noqa: F401,F403

main = _impl.main


if __name__ == "__main__":
    raise SystemExit(main())
