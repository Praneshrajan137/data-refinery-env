"""Run Groq Llama 70B on the canonical Hospital benchmark task.

Requires:
- ``GROQ_API_KEY`` environment variable or ``.env`` file
- ``dataforge`` package installed (for canonical Hospital dataset)

Usage:
    python examples/run_groq_on_hospital.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from dataforge_evals.agents.groq_llama import GroqLlamaAgent
from dataforge_evals.harness import HarnessConfig, run_harness
from dataforge_evals.report import write_report


def main() -> None:
    """Run Groq Llama on the canonical Hospital task."""
    load_dotenv()
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        print(
            "Error: Set GROQ_API_KEY in your environment or .env file.\n"
            "  export GROQ_API_KEY=gsk_...\n"
            "  # or on Windows:\n"
            "  set GROQ_API_KEY=gsk_...",
            file=sys.stderr,
        )
        sys.exit(1)
    run = run_harness(
        HarnessConfig(
            agents=(GroqLlamaAgent(api_key=api_key),),
            datasets=("hospital",),
            trials=3,
            seeds=(0, 1, 2),
            output=Path("reports/groq-hospital.md"),
        )
    )
    write_report(
        run, Path("reports/groq-hospital.md"), json_path=Path("reports/groq-hospital.json")
    )
    print("Report written to reports/groq-hospital.md")


if __name__ == "__main__":
    main()
