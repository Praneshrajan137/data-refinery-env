"""Smoke-test installed DataForge side packages and emit JSON evidence."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SmokeStep:
    """One installed-package smoke step."""

    name: str
    command: list[str]
    returncode: int
    stdout_tail: str
    stderr_tail: str

    @property
    def ok(self) -> bool:
        """Return whether the step passed."""
        return self.returncode == 0


@dataclass(frozen=True, slots=True)
class PackageSmokeReport:
    """Serializable installed-package smoke report."""

    schema_version: str
    package: str
    ok: bool
    workdir: str
    steps: list[SmokeStep]


def _tail(text: str, limit: int = 4000) -> str:
    """Return a bounded output tail."""
    return text[-limit:]


def _run(name: str, command: list[str], *, cwd: Path, steps: list[SmokeStep]) -> str:
    """Run a command and record its result."""
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    step = SmokeStep(
        name=name,
        command=command,
        returncode=result.returncode,
        stdout_tail=_tail(result.stdout),
        stderr_tail=_tail(result.stderr),
    )
    steps.append(step)
    if not step.ok:
        raise RuntimeError(f"{name} failed with exit code {result.returncode}: {result.stderr}")
    return result.stdout


def _command(name: str) -> str:
    """Resolve an installed console command."""
    resolved = shutil.which(name)
    if resolved is None:
        raise RuntimeError(f"Could not find installed command {name!r} on PATH.")
    return resolved


def _write_dirty_csv(workdir: Path) -> Path:
    """Write a small dirty CSV fixture."""
    path = workdir / "amounts.csv"
    path.write_text(
        "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n",
        encoding="utf-8",
    )
    return path


def _smoke_evals(workdir: Path, steps: list[SmokeStep]) -> None:
    """Smoke an installed dataforge-evals package."""
    command = _command("dataforge-evals")
    report_md = workdir / "evals-smoke.md"
    report_json = workdir / "evals-smoke.json"
    _run("evals_version", [command, "--version"], cwd=workdir, steps=steps)
    _run("evals_list_datasets", [command, "list-datasets"], cwd=workdir, steps=steps)
    _run(
        "evals_mock_run",
        [
            command,
            "run",
            "--agent",
            "mock",
            "--dataset",
            "synthetic",
            "--trials",
            "1",
            "--output",
            str(report_md),
            "--output-json",
            str(report_json),
        ],
        cwd=workdir,
        steps=steps,
    )
    if not report_json.is_file():
        raise RuntimeError("dataforge-evals did not write the JSON smoke report.")


def _smoke_dbt(workdir: Path, steps: list[SmokeStep]) -> None:
    """Smoke an installed dataforge-dbt package without running full dbt proof."""
    command = _command("dataforge-dbt")
    csv_path = _write_dirty_csv(workdir)
    target_path = workdir / "target"
    target_path.mkdir()
    _run("dbt_help", [command, "--help"], cwd=workdir, steps=steps)
    _run(
        "dbt_dry_run",
        [
            command,
            "--relation",
            "main.amounts",
            "--column",
            "amount",
            "--mode",
            "dry_run",
            "--input-csv",
            str(csv_path),
            "--target-path",
            str(target_path),
        ],
        cwd=workdir,
        steps=steps,
    )


def _smoke_agent_patterns(workdir: Path, steps: list[SmokeStep]) -> None:
    """Smoke an installed dataforge-agent-patterns package."""
    code = textwrap.dedent(
        """
        from dataforge_agent_patterns import (
            ConstitutionalFilter,
            ConstitutionalRule,
            ReversibleTransaction,
        )

        rule = ConstitutionalRule(
            rule_id="no-delete",
            description="Reject delete actions.",
            predicate=lambda action: action != "delete",
        )
        verdict = ConstitutionalFilter([rule]).evaluate("delete")
        assert verdict.allowed is False
        state = []
        tx = ReversibleTransaction()

        @tx.wrap("append")
        def append_item(value):
            state.append(value)
            return value, lambda: state.pop()

        assert append_item("x") == "x"
        tx.rollback_last()
        assert state == []
        """
    )
    _run("agent_patterns_import_contract", [sys.executable, "-c", code], cwd=workdir, steps=steps)


async def _mcp_profile_smoke(workdir: Path) -> None:
    """Run a real stdio MCP client smoke against the installed server."""
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    csv_path = _write_dirty_csv(workdir)
    env = os.environ.copy()
    env["DATAFORGE_MCP_ALLOWED_ROOTS"] = str(workdir)
    params = StdioServerParameters(command=_command("dataforge-mcp"), args=["serve"], env=env)
    async with (
        stdio_client(params) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        tools = await session.list_tools()
        names = {tool.name for tool in tools.tools}
        expected = {
            "dataforge_profile",
            "dataforge_detect_errors",
            "dataforge_verify_fix",
            "dataforge_apply_repairs",
            "dataforge_revert",
        }
        assert expected <= names, names
        profile = await session.call_tool("dataforge_profile", {"path": str(csv_path)})
        assert profile.isError is False
        detect = await session.call_tool("dataforge_detect_errors", {"path": str(csv_path)})
        assert detect.isError is False
        dry_run = await session.call_tool(
            "dataforge_apply_repairs",
            {"path": str(csv_path), "mode": "dry_run"},
        )
        assert dry_run.isError is False
        blocked_apply = await session.call_tool(
            "dataforge_apply_repairs",
            {"path": str(csv_path), "mode": "apply"},
        )
        assert blocked_apply.isError is True


def _smoke_mcp(workdir: Path, steps: list[SmokeStep]) -> None:
    """Smoke an installed dataforge-mcp package."""
    command = _command("dataforge-mcp")
    _run("mcp_help", [command, "--help"], cwd=workdir, steps=steps)
    asyncio.run(_mcp_profile_smoke(workdir))
    steps.append(
        SmokeStep(
            name="mcp_stdio_tools",
            command=[command, "serve"],
            returncode=0,
            stdout_tail="stdio MCP tool smoke passed",
            stderr_tail="",
        )
    )


def run_smoke(package: str) -> PackageSmokeReport:
    """Run the requested installed-package smoke."""
    steps: list[SmokeStep] = []
    with tempfile.TemporaryDirectory(prefix=f"{package}-installed-smoke-") as tmp:
        workdir = Path(tmp)
        if package == "dataforge_07_mcp":
            _smoke_mcp(workdir, steps)
        elif package == "dataforge_07_evals":
            _smoke_evals(workdir, steps)
        elif package == "dataforge_07_dbt":
            _smoke_dbt(workdir, steps)
        elif package == "dataforge_07_agent_patterns":
            _smoke_agent_patterns(workdir, steps)
        else:
            raise ValueError(f"Unsupported package smoke: {package}")
        return PackageSmokeReport(
            schema_version="dataforge_installed_package_smoke_v1",
            package=package,
            ok=all(step.ok for step in steps),
            workdir=str(workdir),
            steps=steps,
        )


def main(argv: list[str] | None = None) -> int:
    """Run a package smoke from CI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--package",
        required=True,
        choices=[
            "dataforge_07_mcp",
            "dataforge_07_evals",
            "dataforge_07_dbt",
            "dataforge_07_agent_patterns",
        ],
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = run_smoke(args.package)
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    print(payload)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
