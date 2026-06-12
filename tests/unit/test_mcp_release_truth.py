"""Release-truth checks for the DataForge MCP package."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_mcp_readme_requires_inspector_smoke_before_agent_ready_claims() -> None:
    """Agent-ready MCP claims need a repeatable Inspector smoke command."""
    readme = (PROJECT_ROOT / "dataforge-mcp" / "README.md").read_text(encoding="utf-8")

    assert "MCP Inspector smoke check" in readme
    assert "npx @modelcontextprotocol/inspector" in readme
    assert "--allowed-root" in readme
    assert "--enable-apply" in readme
