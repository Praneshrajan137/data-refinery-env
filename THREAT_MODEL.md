# DataForge Threat Model

Status: release-blocking for 0.1.0
Last updated: 2026-06-01

## Scope

This threat model covers the 0.1 DataForge release surfaces:

- CLI commands for CSV and DuckDB profiling, repair, audit, revert, watch, and
  benchmark workflows.
- Public repair receipts emitted by the CLI, playground API, and MCP server.
- Hosted playground API and static frontend, which are stateless and dry-run
  only.
- Local MCP server, including disabled-by-default apply mode.
- Optional LLM fallback, SFT/GRPO training scripts, and model-demo artifacts.
- Release and supply-chain workflows for `dataforge_07` and `dataforge_07_mcp`.

Out of scope for 0.1: credentialed Snowflake, BigQuery, or Databricks mutation;
Airbyte; autonomous hosted repair; production model-quality claims; multi-user
accounts; and persistent playground storage.

## Assets

- User data in CSVs, DuckDB tables, warehouse extracts, dbt artifacts, uploaded
  playground samples, and MCP-accessible files.
- Transaction journals, source snapshots, patch plans, OpenAPI snapshots, and
  repair receipts.
- API keys and credentials for optional model providers, Hugging Face,
  Cloudflare, PyPI/TestPyPI, Kaggle, and GitHub Actions.
- Model training datasets, evaluation reports, benchmark evidence, and model
  cards.
- The PyPI `dataforge_07*` distribution namespace and GitHub release tags.

## Trust Boundaries

- CLI process to local filesystem.
- Repair proposal to `SafetyFilter` to `SMTVerifier` to transaction journal.
- Playground browser to hosted FastAPI backend.
- Hosted API temp directory to discarded request-local state.
- MCP client to local `dataforge-mcp` process and configured allowed roots.
- Optional LLM provider boundary when `--allow-llm` or advanced playground mode
  is explicitly enabled.
- GitHub Actions to PyPI/TestPyPI trusted publishing.

## Threats And Controls

### Unsafe or incorrect data mutation

Threats: wrong-cell edits, row deletion, stale writes, concurrent edits,
unsupported warehouse rollback, and unverifiable repairs.

Controls:

- Applied CSV and DuckDB repairs must pass detector proposal, `SafetyFilter`,
  `SMTVerifier`, patch-plan hashing, transaction snapshot creation, and
  post-state hash checks.
- Revert refuses when the current file does not match the recorded post-state
  hash.
- Cloud warehouse mutation remains dry-run-only until credentialed apply,
  audit, and rollback conformance suites pass.
- The hosted playground never applies repairs.

Release gates:

- Byte-identical apply/revert tests.
- DuckDB patch-plan and rollback tests.
- CLI JSON receipt snapshots for dry-run and apply.
- OpenAPI snapshot checks for playground receipts.

### Prompt injection and tool abuse

Threats: malicious CSV text instructing an LLM or MCP host to exfiltrate data,
ignore safety policy, mutate outside the workspace, or escalate privileges.

Controls:

- The default repair path is deterministic and does not call an LLM.
- Optional LLM fallback is explicit and is not part of hosted mutation.
- MCP paths must resolve under allowed roots.
- MCP apply mode is disabled unless `--enable-apply` or
  `DATAFORGE_MCP_ENABLE_APPLY=1` is set.
- Tool outputs are structured Pydantic models, not shell commands.

Release gates:

- MCP path-allowlist tests.
- Disabled-apply tests.
- Prompt-injection text fixtures in safety tests.
- Manual MCP Inspector smoke before publishing `dataforge_07_mcp`.

### Sensitive data exposure

Threats: uploaded data persistence, browser storage leakage, over-broad CORS,
provider-key leakage, receipt over-disclosure, and logs containing private data.

Controls:

- Playground uploads stay in request-local temporary directories.
- The frontend uses no browser storage and contains no API keys.
- Production CORS is exact-origin only.
- Playground errors use RFC 9457 problem details without dumping source rows.
- PII-like overwrites require explicit local confirmation or are denied.

Release gates:

- Browser-storage and frontend-key scans.
- Playground CORS tests.
- Problem-detail tests.
- Secret scan in backend gate.

### Supply-chain compromise

Threats: long-lived PyPI tokens, compromised dependencies, malicious release
tags, missing provenance, and accidental publication of prerelease artifacts.

Controls:

- Use PyPI/TestPyPI Trusted Publishing through GitHub OIDC instead of stored
  tokens.
- Real PyPI workflow refuses prerelease versions.
- TestPyPI fresh-install smoke must pass before real PyPI.
- Dependency audit and SBOM generation are part of the backend gate when tools
  are available, and can be made required for release.
- Release artifacts should include hashes and provenance evidence.

Release gates:

- `python -m dataforge.release.gate --json`.
- `scripts/ci/backend_gate.py --require-optional` for release candidates.
- Wheel/sdist SHA-256 hashes recorded in release evidence.
- Maintainer verifies PyPI trusted-publisher ownership before tagging.

### Training, eval, and model claims

Threats: data leakage into training examples, unverified benchmark claims,
overstated model quality, poisoned trajectories, and model cards that omit
limitations.

Controls:

- Generated trajectory files remain untracked unless intentionally published.
- SFT and GRPO release verifiers must pass before model claims appear in docs.
- The 0.5B SFT artifact remains a smoke/demo checkpoint unless fresh evals
  prove model quality.
- Eval harness claims must include dataset, metric, seed, and artifact hashes.

Release gates:

- SFT/GRPO verifier output committed before any quality claim.
- Model card includes intended use, limitations, training data, and evals.
- Benchmark truth script prevents hand-edited public benchmark tables.

## Open Risks

- No design-partner validation has been claimed yet.
- The hosted playground depends on free-tier Cloudflare and Hugging Face
  availability; production SLOs are not claimed.
- Warehouse apply/revert outside DuckDB is intentionally blocked.
- Broader PII detection is incomplete and tracked as future work.
- MCP security still depends on the local host configuring allowed roots
  correctly.

## References

- OWASP Top 10 for LLM Applications:
  https://owasp.org/www-project-top-10-for-large-language-model-applications/
- NIST Secure Software Development Framework SP 800-218:
  https://csrc.nist.gov/pubs/sp/800/218/final
- SLSA specification:
  https://slsa.dev/spec/v1.0/
- PyPI Trusted Publishing:
  https://docs.pypi.org/trusted-publishers/
- RFC 9457 problem details:
  https://www.rfc-editor.org/rfc/rfc9457
