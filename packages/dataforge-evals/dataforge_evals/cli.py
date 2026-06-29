"""Command-line interface for the dataforge-evals evaluation harness.

Provides subcommands for running evaluations, listing available agents
and datasets, and inspecting version information.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dataforge_evals.agents.base import Agent
from dataforge_evals.agents.cerebras_llama import CerebrasLlamaAgent
from dataforge_evals.agents.gemini_flash import GeminiFlashAgent
from dataforge_evals.agents.groq_llama import GroqLlamaAgent
from dataforge_evals.agents.hf_local import HfLocalAgent
from dataforge_evals.agents.local_ollama import LocalOllamaAgent
from dataforge_evals.agents.mock import MockAgent
from dataforge_evals.agents.openrouter import OpenRouterAgent
from dataforge_evals.harness import HarnessConfig, run_harness
from dataforge_evals.report import write_report
from dataforge_evals.tasks import available_datasets

app = typer.Typer(
    help="Agent-agnostic evaluation harness for data-quality repair agents.",
    no_args_is_help=True,
)
_console = Console()
_error_console = Console(stderr=True)


# Agent registry

_AGENT_REGISTRY: dict[str, dict[str, str]] = {
    "mock": {
        "provider": "local deterministic oracle",
        "env_var": "none",
        "description": "Returns perfect ground-truth fixes for harness validation.",
    },
    "groq-llama-70b": {
        "provider": "Groq",
        "env_var": "GROQ_API_KEY",
        "description": "Groq-hosted Llama 3.3 70B Versatile.",
    },
    "gemini-flash": {
        "provider": "Gemini",
        "env_var": "GEMINI_API_KEY",
        "description": "Google Gemini 2.0 Flash.",
    },
    "cerebras-llama": {
        "provider": "Cerebras",
        "env_var": "CEREBRAS_API_KEY",
        "description": "Cerebras-hosted Llama 3.1 70B.",
    },
    "openrouter": {
        "provider": "OpenRouter",
        "env_var": "OPENROUTER_API_KEY",
        "description": "OpenRouter with configurable model routing.",
    },
    "local-ollama": {
        "provider": "Ollama (local)",
        "env_var": "none",
        "description": "Local Ollama OpenAI-compatible endpoint.",
    },
    "hf-local": {
        "provider": "Hugging Face Transformers",
        "env_var": "HF_TOKEN optional",
        "description": "Loads a local or Hub model and evaluates generated fixes.",
    },
}


def _require_env(name: str, *, agent_id: str) -> str:
    """Return a required environment variable or raise a user-facing error.

    Args:
        name: Environment variable name.
        agent_id: Agent identifier for error context.

    Returns:
        Non-empty environment variable value.

    Raises:
        ValueError: If the variable is missing or empty.
    """
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(
            f"Agent '{agent_id}' requires environment variable {name}. "
            f"Set it in your shell or in a .env file."
        )
    return value


def _build_agent(
    agent_id: str,
    *,
    model_id: str | None = None,
    max_new_tokens: int = 384,
    device: str = "auto",
) -> Agent:
    """Instantiate a built-in agent adapter by CLI identifier.

    Args:
        agent_id: One of the registered agent identifiers.

    Returns:
        Initialized agent adapter.

    Raises:
        ValueError: If the agent identifier is unknown.
    """
    normalized = agent_id.strip().lower()
    if model_id is not None and normalized != "hf-local":
        raise ValueError("--model-id is currently supported only with --agent hf-local.")
    if normalized == "mock":
        return MockAgent()
    if normalized == "groq-llama-70b":
        return GroqLlamaAgent(api_key=_require_env("GROQ_API_KEY", agent_id=agent_id))
    if normalized == "gemini-flash":
        return GeminiFlashAgent(api_key=_require_env("GEMINI_API_KEY", agent_id=agent_id))
    if normalized == "cerebras-llama":
        return CerebrasLlamaAgent(api_key=_require_env("CEREBRAS_API_KEY", agent_id=agent_id))
    if normalized == "openrouter":
        return OpenRouterAgent(api_key=_require_env("OPENROUTER_API_KEY", agent_id=agent_id))
    if normalized == "local-ollama":
        return LocalOllamaAgent()
    if normalized == "hf-local":
        return HfLocalAgent(model_id=model_id, max_new_tokens=max_new_tokens, device=device)
    known = ", ".join(sorted(_AGENT_REGISTRY))
    raise ValueError(f"Unknown agent '{agent_id}'. Expected one of: {known}.")


def _seeds(trials: int, seed: int) -> tuple[int, ...]:
    """Build deterministic per-trial seeds.

    Args:
        trials: Number of trials.
        seed: Base seed value.

    Returns:
        Tuple of sequential seeds.
    """
    return tuple(seed + index for index in range(trials))


def _version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        from dataforge_evals import __version__

        _console.print(f"dataforge-evals {__version__}")
        raise typer.Exit()


# Subcommands


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version", help="Show version and exit.", callback=_version_callback, is_eager=True
        ),
    ] = False,
) -> None:
    """Agent-agnostic evaluation harness for data-quality repair agents."""


@app.command()
def run(
    agent: Annotated[str, typer.Option("--agent", help="Built-in agent identifier.")],
    dataset: Annotated[
        str, typer.Option("--dataset", help="Dataset name: synthetic, hospital, flights, or beers.")
    ] = "synthetic",
    trials: Annotated[int, typer.Option("--trials", help="Number of trials to run.")] = 3,
    output: Annotated[Path, typer.Option("--output", help="Markdown report path.")] = Path(
        "dataforge-evals-report.md"
    ),
    output_json: Annotated[
        Path | None, typer.Option("--output-json", help="Optional JSON report path.")
    ] = None,
    seed: Annotated[int, typer.Option("--seed", help="First deterministic seed.")] = 0,
    timeout_s: Annotated[
        float, typer.Option("--timeout-s", help="Per-trial timeout in seconds.")
    ] = 120.0,
    dirty_csv: Annotated[
        Path | None,
        typer.Option("--dirty-csv", help="Custom dirty CSV file for CSV-pair evaluation."),
    ] = None,
    clean_csv: Annotated[
        Path | None,
        typer.Option("--clean-csv", help="Custom clean CSV file for CSV-pair evaluation."),
    ] = None,
    cache_root: Annotated[
        Path | None, typer.Option("--cache-root", help="Optional DataForge cache root.")
    ] = None,
    model_id: Annotated[
        str | None,
        typer.Option("--model-id", help="HF model id override for --agent hf-local."),
    ] = None,
    max_new_tokens: Annotated[
        int,
        typer.Option("--max-new-tokens", help="Generation limit for --agent hf-local."),
    ] = 384,
    device: Annotated[
        str,
        typer.Option("--device", help="Device for --agent hf-local: auto, cpu, cuda, etc."),
    ] = "auto",
) -> None:
    """Run an evaluation and write a markdown report."""
    load_dotenv()
    try:
        adapter = _build_agent(
            agent,
            model_id=model_id,
            max_new_tokens=max_new_tokens,
            device=device,
        )

        # CSV-pair validation
        if (dirty_csv is not None) != (clean_csv is not None):
            raise ValueError(
                "Both --dirty-csv and --clean-csv are required for CSV-pair evaluation."
            )

        harness_run = run_harness(
            HarnessConfig(
                agents=(adapter,),
                datasets=(dataset,),
                trials=trials,
                seeds=_seeds(trials, seed),
                timeout_s=timeout_s,
                output=output,
                cache_root=cache_root,
                dirty_csv=dirty_csv,
                clean_csv=clean_csv,
            )
        )

        write_report(harness_run, output, json_path=output_json)
    except Exception as exc:
        _error_console.print(
            Panel(
                f"{exc}\n\n[dim]Hint: run 'dataforge-evals list-agents' to see available agents "
                f"and required environment variables.[/dim]",
                title="dataforge-evals error",
                style="red",
            )
        )
        raise typer.Exit(code=2) from exc

    # Print Rich summary table to stdout
    table = Table(title="dataforge-evals Summary")
    table.add_column("Agent")
    table.add_column("Dataset")
    table.add_column("Trials")
    table.add_column("F1")
    table.add_column("Quota Units")
    table.add_column("Runtime (s)")
    table.add_column("Failures")
    for aggregate in harness_run.aggregates:
        failures = (
            ", ".join(f"{kind}={count}" for kind, count in aggregate.failure_taxonomy.items())
            or "none"
        )
        table.add_row(
            aggregate.agent,
            aggregate.dataset,
            f"{aggregate.trials_completed}/{aggregate.trials_requested}",
            "Failed"
            if aggregate.f1_mean is None
            else f"{aggregate.f1_mean:.4f} \u00b1 {(aggregate.f1_std or 0.0):.4f}",
            f"{aggregate.quota_units_total:.4f}",
            "N/A" if aggregate.runtime_s_mean is None else f"{aggregate.runtime_s_mean:.2f}",
            failures,
        )
    _console.print(table)
    _console.print(f"Report written to [bold]{output}[/bold]")


@app.command(name="list-agents")
def list_agents() -> None:
    """List all built-in agent adapters with their required configuration."""
    table = Table(title="Built-in Agent Adapters")
    table.add_column("Agent ID", style="bold")
    table.add_column("Provider")
    table.add_column("Required Setup")
    table.add_column("Description")
    for agent_id, info in _AGENT_REGISTRY.items():
        table.add_row(agent_id, info["provider"], info["env_var"], info["description"])
    _console.print(table)


@app.command(name="list-datasets")
def list_datasets_cmd() -> None:
    """List all available built-in dataset identifiers."""
    table = Table(title="Available Datasets")
    table.add_column("Dataset ID", style="bold")
    table.add_column("Source")
    table.add_column("Notes")
    sources = {
        "synthetic": ("built-in", "Small deterministic task, no network required"),
        "hospital": ("DataForge (optional)", "Raha Hospital benchmark, requires dataforge"),
        "flights": ("DataForge (optional)", "Raha Flights benchmark, requires dataforge"),
        "beers": ("DataForge (optional)", "Raha Beers benchmark, requires dataforge"),
    }
    for ds_id in available_datasets():
        source, notes = sources.get(ds_id, ("unknown", ""))
        table.add_row(ds_id, source, notes)
    _console.print(table)
    _console.print(
        "\n[dim]Custom datasets: use --dirty-csv and --clean-csv with any --dataset name.[/dim]"
    )


if __name__ == "__main__":
    app()
