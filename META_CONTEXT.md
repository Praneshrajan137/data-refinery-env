# DataForge Meta-Context

Last updated: 2026-05-15.

This document is the pre-work context for any agent or maintainer editing
DataForge. Read it before making product, architecture, or documentation
changes.

## 0.1 What DataForge Is

DataForge is an open-source data-quality repair project for tabular data. The
current shipped core is a local CLI/library that profiles CSVs, proposes repairs
for three detector families, verifies proposed changes, and records reversible
transactions.

Current surfaces:

- CLI: `dataforge profile`, `dataforge repair`, `dataforge revert`,
  `dataforge bench`
- OpenEnv-compatible RL environment with eight typed actions
- Real-world benchmark harness for Hospital, Flights, and Beers
- Causal root-cause analyzer for cascading tabular errors
- Local MCP server package under `dataforge-mcp/`, published as `dataforge-mcp`
- Hugging Face Docker Space backend and Cloudflare static playground frontend
- Week 9 SFT warmup workflow, dataset/model cards, Kaggle notebook, and release
  verifier
- Separate Gradio model demo for the published 0.5B SFT smoke checkpoint

Mandatory full-vision gates must stay labeled as incomplete until external
proof exists: PyPI/TestPyPI ownership for `dataforge`, production verification
for `https://dataforge.praneshrajan15.workers.dev/playground`, published
sibling packages, dbt-duckdb fresh-env proof, not yet met design-partner
evidence, and production model-family evidence.

## 0.2 Who It Is For

- Data engineers who need a local, auditable CSV profiling and repair workflow.
- Applied AI engineers testing data-repair agents and OpenEnv training loops.
- Maintainers integrating DataForge into local agent tools through MCP.
- Reviewers evaluating whether the project is honest about what is shipped.

## 0.3 Quality Bar

The project succeeds when a senior engineer can inspect the repo quickly and see
three things:

- The README describes only behavior that exists or is clearly labeled future
  work.
- Safety and reversibility are enforced in code, not only promised in prose.
- Benchmarks, model cards, and release claims point to scripts or verifier
  artifacts.

Anti-signals:

- Claims for unshipped integrations or hosted domains.
- Replacing the working Workers playground with speculative domain work.
- Benchmark or model-quality numbers without a committed reproduction path.
- Generated staging directories edited by hand as if they were source docs.
- LLM-styled filler in committed docs.

## 0.4 Non-Negotiables

1. Public behavior changes require a spec update.
2. Existing tests are not weakened to make an implementation pass.
3. Applied repairs pass through SafetyFilter -> SMTVerifier -> transaction log.
4. `dataforge revert <txn_id>` must preserve byte-for-byte restore semantics.
5. No browser storage APIs in the playground unless a future spec explicitly
   changes the privacy model.
6. No browser-visible API keys and no browser-run LLM calls.
7. No commercial API training data is committed to the repo.
8. Specs and tests lead implementation work.
9. Commit messages use Conventional Commits.
10. Documentation must be as honest as the code.

## 0.5 Current Source-Of-Truth Docs

- `README.md`: public truth source for shipped capabilities.
- `ARCHITECTURE.md`: current architecture and dependency justification.
- `DECISIONS.md`: decision log and reversal criteria.
- `.cursor/rules/dataforge.md`: always-applied contribution rules.
- `CLAUDE.md`: living gotcha log.
- `specs/`: module-level contracts.
- `training/`: SFT dataset/model documentation.
- `dataforge-mcp/README.md`, `playground/api/SPACE_SETUP.md`,
  `playground/web/DEPLOY.md`, `playground-model/README.md`: surface-specific
  runbooks.

Generated or staged mirrors are not source docs:

- `.hf-space-repo/`
- `.hf-space-stage/`
- `.hf-space-stage-plan/`
- cache directories
- local logs

## 0.6 Pre-Mortem

Likely ways DataForge fails:

- Scope fragmentation: many half-built surfaces instead of one credible core.
- README drift: public docs claim features the code does not ship.
- Weak safety evidence: repair safety exists only in prose, not tests.
- Synthetic-only benchmarks: numbers do not reflect real-world data.
- Design Partner Gate is not met: no attributed design-partner evidence is
  claimed until permissioned feedback artifacts exist.
- Model overclaiming: a smoke checkpoint is presented as a quality milestone.
- Key/data leakage: playground or MCP paths silently send data to providers.

Mitigations:

- Keep the core CLI, safety, transactions, and benchmark harness as P1.
- Label future surfaces explicitly.
- Require verifier output for model and dataset claims.
- Keep provider calls opt-in and backend-only.
- Preserve the README truth checker and documentation integrity scans.

## 0.7 Competitive Positioning

DataForge is not a data catalog, observability platform, warehouse, lineage
system, or replacement for Great Expectations/dbt tests. Its differentiator is
the conjunction of executable repair proposals, safety gating, SMT verification,
and reversible transactions in a local open-source workflow.

Do not use DataForge today for streaming data, very large warehouse-scale jobs,
strict regulated workflows where every fix must be human-authored, or production
autonomous repair. Use it for local CSV repair experiments, auditable demos,
benchmarking, environment research, and agent integration prototypes.
