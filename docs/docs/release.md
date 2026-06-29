# Release

## Release Standard

DataForge is release-ready only when the local source tree, public contracts,
and external evidence tell the same story: CLI-first CSV/DuckDB repair,
`pip install dataforge_07` works from PyPI and installs the `dataforge`
CLI/import namespace,
`https://dataforge.praneshrajan15.workers.dev/playground` serves the production
playground, MCP tools keep apply disabled by default, OpenEnv/eval support is
usable, dbt-duckdb proof exists, and every published model-quality claim has
verifier evidence. Design-partner evidence and full model-family evidence are
separate full-vision gates until their manifests exist.

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

## PyPI And TestPyPI Evidence

Status: complete for `0.1.0`.

`docs/evidence/pypi/publish_report.json` records the generated
`dataforge_pypi_publish_report_v2` for `dataforge_07`,
`dataforge_07_mcp`, `dataforge_07_evals`, `dataforge_07_agent_patterns`, and
`dataforge_07_dbt`.

Required evidence for each package:

- Wheel and sdist filenames plus SHA-256 hashes for both TestPyPI and PyPI.
- TestPyPI and PyPI project URLs.
- Trusted Publishing workflow URL, PyPI Simple API provenance URL, Integrity
  API publish predicate, and GitHub Trusted Publisher identity for every wheel
  and sdist.
- Stored TestPyPI and PyPI fresh-install smoke logs under `docs/evidence/pypi/`.

DataForge targets SLSA v1.2-style provenance verification for released
artifacts. The project does not claim a SLSA level unless a separate verifier
proves that level for the exact artifacts being released.

The real PyPI workflow refuses pre-release package metadata. Future releases
must regenerate the publish report from `scripts/ci/pypi_publish_report.py`
instead of editing it by hand.

## Package Matrix

- `dataforge_07`: core CLI/library distribution. It installs the `dataforge`
  import namespace and CLI.
- `dataforge_07_mcp`: nested MCP distribution. MCP Inspector smoke must cover
  list tools, profile, detect, verify, dry-run apply, blocked apply by default,
  enabled apply inside an allowed root, and revert before future releases.
- `dataforge_07_evals`, `dataforge_07_agent_patterns`, and
  `dataforge_07_dbt`:
  monorepo packages under `packages/` that must pass their own clean virtualenv
  tests and fresh-install smoke before future PyPI publication.

## Evidence Manifest Discipline

The full-vision manifests are release records, not status pages.

The current generated release manifests are:

- `docs/evidence/ledger.json`
- `docs/evidence/pypi/publish_report.json`
- `docs/evidence/dbt_duckdb/fresh_env_report.json`

Do not write
`docs/evidence/design_partners/manifest.json` or
`docs/evidence/models/model_family_report.json` until the referenced
design-partner or model-family evidence exists and every manifest path points
at a stored artifact.

The PyPI report must be generated, not hand-filled. It verifies public registry
metadata, downloaded artifact hashes, PyPI Integrity API provenance for wheel
and sdist files, and the expected `Aegis15/dataforge` GitHub Trusted Publisher
workflow identity.

Design-partner manifests must point at sanitized evidence notes and separate
consent JSON records. The evidence note records task outcome, timing, trust
signal, and blocking findings; the consent record is the only place
permission-to-list is captured.

Before the full-vision model-family evidence manifest may mark an HF artifact
as passed, each training stage must have a verifier report, eval report,
complete Hub model card, dataset reference, and training run URL. The current
public leaf-verifier evidence covers `DataForge-0.5B-SFT` and
`DataForge-0.5B-GRPO`; the GRPO row is verified research evidence with strict
macro F1 `0.1393`, not production-grade quality and not a completed family
claim.
The later 0.5B-GRPO v2 Kaggle run completed training and strict eval but is
diagnostic-only evidence: it stopped at `quality_gate_failed_no_upload` with
strict macro F1 `0.1212` (not release evidence), parse success `0.99`, schema-case errors `0`, and
missed the `grpo_f1>=0.25` and `not_inferable_from_prompt_f1>=0.95` gates.
SFT-v5/v6/v7/v8 are private failed or blocked diagnostic paths until
verifier-passed evidence exists. SFT-v8 completed only the 40-step smoke rung:
label-mask audit passed, but strict macro F1 was `0.0`, parse success was
`0.03`, schema-case errors were `26`, and `promote_to_grpo` was `false`.
SFT-v9 is staged only as a private action-envelope curriculum/preflight:
completion parse success is `1.0`, held-out leakage is `0`, and negative
contrast target leakage is `0`, but no SFT-v9 checkpoint has passed strict
held-out eval or uploaded. GRPO-v4 is therefore blocked until an SFT-v9-or-later
private predecessor passes its promotion gates and uploads.
The model-family evidence schema is
`dataforge_model_family_report_v2`: each row must include artifact status,
quality status, upstream/base license metadata, training backend, training run
URL, source Git commit, dataset/model Hub SHAs, eval metrics, verifier/eval
paths, and limitations. GRPO rows depend on verified SFT predecessors, and
GiGPO rows depend on verified GRPO predecessors. Missing or blocked rows are
valid roadmap state, but they cannot satisfy the full-vision gate.

## External Gates

These gates cannot be completed by local source edits alone:

- Design-partner evidence from real users.
- Model-card and verifier evidence for any trained-model quality claim.

The command `dataforge release full-vision --json` is the mandatory external
completion gate. It is expected to fail while the design-partner manifest or
full model-family report is absent.
