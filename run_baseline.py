"""Compatibility shim for the legacy top-level deterministic baseline script."""

from __future__ import annotations

from data_quality_env import run_baseline as _impl
from data_quality_env.run_baseline import *  # noqa: F401,F403

main = _impl.main


if __name__ == "__main__":
    raise SystemExit(main())
