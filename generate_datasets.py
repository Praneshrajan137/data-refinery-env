"""Compatibility shim for the legacy top-level dataset generator script."""

from __future__ import annotations

import runpy

from data_quality_env.generate_datasets import *  # noqa: F401,F403


if __name__ == "__main__":
    runpy.run_module("data_quality_env.generate_datasets", run_name="__main__")
