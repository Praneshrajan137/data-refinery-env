# Release

## Release Standard

DataForge is release-ready only when the local source tree, public contracts,
and external evidence tell the same story: CLI-first CSV/DuckDB repair, only
when `pip install dataforge_07` works from PyPI and installs the `dataforge`
CLI/import namespace,
`https://dataforge.praneshrajan15.workers.dev/playground` serves the production
playground, MCP tools keep apply disabled by default, OpenEnv/eval support is
usable, dbt-duckdb proof exists, design-partner evidence exists, and every
published model-quality claim has verifier evidence.

Required local gate:

```bash
python scripts/ci/backend_gate.py --require-optional
python -m dataforge.release.gate --json
```

The optional backend-gate checks become mandatory before release: dependency
audit, SBOM generation, and package builds must succeed. The repository threat
model must be reviewed before tagging.

## Public Contracts

The following machine-readable shapes are treated as public 0.1 contracts:

- CLI `--json` outputs for `profile`, `constraints review`, `repair`, `audit`,
  `revert`, `watch`, `bench`, and `release`.
- Repair receipts with `schema_version`, `receipt_version`,
  `contract_version`, source hashes, accepted constraint IDs, candidate
  repairs, proof obligations, root causes, limitations, and revert command.
- Playground OpenAPI snapshots under `specs/openapi/`.
- MCP tool schemas and Pydantic result models.
- Evaluation and benchmark JSON schemas.

Any intentional public-contract change must update tests, docs, OpenAPI
snapshots, and the changelog in the same change.

## TestPyPI Rehearsal

Status: pending maintainer configuration and TestPyPI run.

Required trusted-publisher setup before tagging:

- TestPyPI pending publishers for `dataforge_07`, `dataforge_07_mcp`,
  `dataforge_07_evals`, `dataforge_07_agent_patterns`, and `dataforge_07_dbt`.
- PyPI pending publishers for the same five projects.
- Each publisher uses owner `Aegis15`, repository `dataforge`, the matching
  workflow filename, and environment `testpypi` or `pypi`.
- GitHub environment approval rules for `pypi`; `testpypi` remains
  environment-scoped for OIDC but can stay lower-friction.

Evidence to record after the workflow passes:

- Git SHA and tag.
- Wheel and sdist filenames plus SHA-256 hashes.
- TestPyPI project URL.
- PyPI project URL after the real release.
- Trusted Publishing workflow URL and PyPI attestation URL.
- Stored TestPyPI and PyPI fresh-install smoke logs under `docs/evidence/pypi/`.
- Generated `docs/evidence/pypi/publish_report.json` from
  `scripts/ci/pypi_publish_report.py` after every package is public.
- Installed-package smoke output for `dataforge --version`, `profile`,
  `profile --constraints-out`, `constraints review --accept ... --no-tui`,
  `repair --constraints --dry-run --json`, and
  `release doctor --core --json`.

Real PyPI remains blocked until the TestPyPI evidence above is complete and
the `dataforge_07` trusted publisher is configured. The real PyPI workflow
refuses pre-release package metadata.

## Package Matrix

- `dataforge_07`: core CLI/library distribution. It installs the `dataforge`
  import namespace and CLI. Publish to TestPyPI first, then real PyPI only
  after fresh-install smoke evidence is recorded.
- `dataforge_07_mcp`: nested MCP distribution. Publish only after MCP Inspector
  smoke covers list tools, profile, detect, verify, dry-run apply, blocked
  apply by default, enabled apply inside an allowed root, and revert.
- `dataforge_07_evals`, `dataforge_07_agent_patterns`, and
  `dataforge_07_dbt`:
  monorepo packages under `packages/` that must pass their own clean virtualenv
  tests and TestPyPI fresh-install smoke before any real PyPI publication.

## Evidence Manifest Discipline

The full-vision manifests are release records, not status pages. Do not write
`docs/evidence/pypi/publish_report.json`,
`docs/evidence/dbt_duckdb/fresh_env_report.json`,
`docs/evidence/design_partners/manifest.json`, or
`docs/evidence/models/model_family_report.json` until the referenced public
state exists and every manifest path points at a stored artifact.

Before the model-family manifest may mark an HF artifact as passed, each
training stage must have a verifier report, eval report, complete Hub model
card, dataset reference, and training run URL.

## External Gates

These gates cannot be completed by local source edits alone:

- PyPI/TestPyPI trusted-publisher configuration and publication evidence for
  every `dataforge_07*` package.
- Cloudflare and Hugging Face deployed playground verification.
- Cloudflare Workers playground monitoring and live repair-flow proof.
- Design-partner evidence from real users.
- Model-card and verifier evidence for any trained-model quality claim.

The command `dataforge release full-vision --json` is the mandatory external
completion gate. It is expected to fail until every item above is backed by
public state or committed evidence.
