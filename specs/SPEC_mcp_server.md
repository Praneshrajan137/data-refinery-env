# SPEC: dataforge-mcp

> Status: Reviewed
> Owner: Praneshrajan15
> Last updated: 2026-06-01

## 1. Purpose

Expose DataForge's shipped CSV profiling, detection, repair, verification, and
revert behavior through a local Model Context Protocol server. The server is a
standalone package so Claude Desktop, Cursor, Windsurf, and other MCP clients
can call DataForge without parsing terminal UI.

## 2. Outcomes

- [x] `pip install dataforge_07_mcp` installs a `dataforge-mcp` console command.
- [x] `dataforge-mcp serve` starts a stdio MCP server with five DataForge tools.
- [x] MCP tool calls return structured JSON-compatible results backed by the
      real detector, safety, verifier, and transaction paths.
- [x] Dry-run is the default safe mode; apply mode requires explicit
      `--enable-apply` or `DATAFORGE_MCP_ENABLE_APPLY=1`.
- [x] File paths are accepted only under configured allowed roots.
- [x] A trusted-publishing GitHub Actions workflow can publish the package to
      PyPI from `dataforge-mcp-v*` tags.

## 3. Scope

**IN**:
- Local stdio MCP server using the official Python `mcp` SDK.
- Optional Streamable HTTP transport for local development.
- Tools: profile, detect errors, verify one fix, apply repairs, revert.
- README snippets for Claude Desktop, Cursor, and Windsurf.

**OUT**:
- Remote hosted MCP service.
- Authentication, multi-user state, browser-based MCP clients, or cloud storage.
- LLM-backed repairs unless DataForge explicitly enables them in a later spec.

## 4. Constraints

- Compatibility: Python `>=3.11,<3.13`.
- Safety: apply mode must keep DataForge's SafetyFilter -> SMTVerifier ->
  transaction-log invariant.
- Path sandbox: every CSV and optional schema path must resolve under
  `--allowed-root` / `DATAFORGE_MCP_ALLOWED_ROOTS`; default root is the server
  working directory.
- Mutation safety: MCP apply mode is disabled until explicitly enabled.
- Transport default: stdio.
- Documentation must not claim unsupported MCP client configuration features.

## 5. Prior decisions

- `dataforge-mcp/` is nested in this repository but builds the standalone
  `dataforge_07_mcp` distribution.
- The package depends on `dataforge` and `mcp`; it does not vendor
  DataForge.
- Tool outputs are typed Pydantic models rather than Rich CLI text.

## 6. Task breakdown

### 6.1 Package scaffold
- Acceptance: package metadata, console script, README, and importable module exist.
- Depends on: none.
- Estimated complexity: S.

### 6.2 Tool implementation
- Acceptance: each requested tool calls DataForge internals and returns stable
  structured output.
- Depends on: 6.1.
- Estimated complexity: M.

### 6.3 Server registration
- Acceptance: `create_server()` registers all five tools and `main()` serves
  stdio by default.
- Depends on: 6.2.
- Estimated complexity: S.

### 6.3.1 MCP safety configuration
- Acceptance: `dataforge-mcp serve --allowed-root <path> --enable-apply`
  configures path sandboxing and mutation capability; direct tool functions
  expose the same settings for tests and embedded hosts.
- Depends on: 6.2.
- Estimated complexity: S.

### 6.4 Release workflow
- Acceptance: GitHub Actions workflow builds from `dataforge-mcp/` and uses
  PyPI trusted publisher permissions.
- Depends on: 6.1.
- Estimated complexity: S.

## 7. Verification

- Unit tests: `dataforge-mcp/tests/test_tools.py`.
- Integration tests: `dataforge-mcp/tests/test_server_integration.py`.
- Root regression: existing `make lint && make type && make test`.
- Package gate: `python -m pytest dataforge-mcp/tests -v`.

## 8. Acceptance gate

- [x] Section 2 outcomes are met.
- [x] No existing regression test fails.
- [x] Dry-run MCP repair does not mutate source bytes.
- [x] Apply followed by revert restores source bytes.
- [x] README snippets use `dataforge-mcp serve`.
- [x] Apply without explicit enablement is rejected.
- [x] Stdio integration tests configure an allowed root before reading files.

## Appendix A - Toy cases

### Case A.1: Decimal-shift dry run
Input: `id,amount` CSV with one `1020` outlier among `100..105`.
Expected output: dry-run receipt with one accepted decimal-shift fix and no file
mutation.
Reasoning: proves MCP wraps the real detector and repair path.

### Case A.2: Apply and revert
Input: same CSV, called with mode `apply`, then `dataforge_revert(txn_id)`.
Expected output: transaction id is returned and source bytes match the original
after revert.
Reasoning: protects the transaction safety invariant.

### Case A.3: Apply disabled by default
Input: `dataforge_apply_repairs(path, "apply")` without apply enablement.
Expected output: structured tool error explaining that apply must be explicitly enabled.
Reasoning: prevents an MCP host from accidentally granting mutation capability.

### Case A.4: Path outside allowed roots
Input: a CSV path outside the configured root set.
Expected output: tool rejects the path before reading or writing.
Reasoning: keeps local MCP access scoped to the user's chosen workspace.
