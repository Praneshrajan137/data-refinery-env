# RFC-003: API Versioning

## Problem

DataForge now exposes a CLI, Python modules, an OpenEnv environment, benchmark
JSON, MCP tools, and playground APIs. Versioning must make compatibility
expectations clear before downstream integrations rely on unstable shapes.

## Alternatives

- Treat all interfaces as unstable until 1.0.
- Version only the Python package and CLI.
- Add explicit schema versions to machine-readable contracts while keeping
  alpha Python APIs documented as provisional.

## Decision

Use explicit schema versions for machine-readable artifacts and document the CLI
as the primary public interface for 0.x. Python modules remain importable but
may change until a stable API policy is accepted.

## Rollout Plan

1. Keep `schema_version` fields on release evidence and evaluation tasks.
   Repair receipts also expose `receipt_version` so CLI, playground, and MCP
   clients can identify that public shape directly.
2. Add compatibility tests for public CLI behavior and committed JSON schemas.
3. Document breaking changes in the changelog for every 0.x release.
4. Draft a stable Python API list before 0.2.

## Open Questions

- Which Python modules should become stable first?
- Should MCP tool schemas use independent semver?
- How long should old JSON contract readers be supported?
