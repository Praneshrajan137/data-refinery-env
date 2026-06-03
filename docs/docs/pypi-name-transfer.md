# PyPI Distribution Name

DataForge keeps the product name, import namespace, and CLI command as
`dataforge`, but publishes to PyPI/TestPyPI under the `dataforge_07*`
distribution family. This avoids depending on transfer of the occupied
unqualified `dataforge` project name.

## Decision

- Core distribution: `dataforge_07`
- MCP distribution: `dataforge_07_mcp`
- Eval harness distribution: `dataforge_07_evals`
- dbt integration distribution: `dataforge_07_dbt`
- Agent-patterns distribution: `dataforge_07_agent_patterns`

The core distribution must still install:

- Python import namespace: `dataforge`
- CLI command: `dataforge`

## Required Evidence

- `python -m pip install dataforge_07` works in a clean Python 3.12 environment.
- `dataforge --version`, `profile`, `repair --dry-run`, `repair --apply`,
  `audit`, `revert`, `watch`, and `bench` pass from the installed artifact.
- The side packages publish from this monorepo under `packages/` and install as
  `dataforge_07_mcp`, `dataforge_07_evals`, `dataforge_07_dbt`, and
  `dataforge_07_agent_patterns`.
- `docs/evidence/pypi/publish_report.json` records Trusted Publishing,
  attestations, fresh-install proof, package URLs, distribution hashes, and
  smoke logs for every `dataforge_07*` package.

## Rejected Path

Waiting for `dataforge` name transfer is no longer the release path. The
unqualified name remains occupied by unrelated projects, and the project accepts
the small install-command tradeoff for a publishable, auditable release.
