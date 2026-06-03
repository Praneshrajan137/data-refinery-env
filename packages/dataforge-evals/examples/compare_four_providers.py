"""Compare four hosted providers on the synthetic smoke-test task.

Requires all four provider API keys set in environment or .env file:
- ``GROQ_API_KEY``
- ``GEMINI_API_KEY``
- ``CEREBRAS_API_KEY``
- ``OPENROUTER_API_KEY``

Usage:
    python examples/compare_four_providers.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from dataforge_evals.agents.cerebras_llama import CerebrasLlamaAgent
from dataforge_evals.agents.gemini_flash import GeminiFlashAgent
from dataforge_evals.agents.groq_llama import GroqLlamaAgent
from dataforge_evals.agents.openrouter import OpenRouterAgent
from dataforge_evals.harness import HarnessConfig, run_harness
from dataforge_evals.report import write_report


def main() -> None:
    """Compare four hosted providers on the synthetic smoke-test task."""
    load_dotenv()
    required = {
        "GROQ_API_KEY": os.environ.get("GROQ_API_KEY", "").strip(),
        "GEMINI_API_KEY": os.environ.get("GEMINI_API_KEY", "").strip(),
        "CEREBRAS_API_KEY": os.environ.get("CEREBRAS_API_KEY", "").strip(),
        "OPENROUTER_API_KEY": os.environ.get("OPENROUTER_API_KEY", "").strip(),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        print(
            f"Error: Missing provider keys: {missing}\n"
            "Set them in your environment or .env file before running this example.",
            file=sys.stderr,
        )
        sys.exit(1)
    run = run_harness(
        HarnessConfig(
            agents=(
                GroqLlamaAgent(api_key=required["GROQ_API_KEY"]),
                GeminiFlashAgent(api_key=required["GEMINI_API_KEY"]),
                CerebrasLlamaAgent(api_key=required["CEREBRAS_API_KEY"]),
                OpenRouterAgent(api_key=required["OPENROUTER_API_KEY"]),
            ),
            datasets=("synthetic",),
            trials=3,
            seeds=(0, 1, 2),
            output=Path("reports/four-provider-comparison.md"),
        )
    )
    write_report(
        run,
        Path("reports/four-provider-comparison.md"),
        json_path=Path("reports/four-provider-comparison.json"),
    )
    print("Report written to reports/four-provider-comparison.md")


if __name__ == "__main__":
    main()
