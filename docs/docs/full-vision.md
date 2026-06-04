# Full Vision Gate

The full original DataForge vision is complete only when external state proves
it. Local code, tests, and docs are necessary but not enough.

Run:

```bash
dataforge release full-vision --json
```

The gate checks:

- PyPI and TestPyPI publish the final distributions: `dataforge_07`,
  `dataforge_07_mcp`, `dataforge_07_evals`, `dataforge_07_dbt`, and
  `dataforge_07_agent_patterns`.
- `dataforge_07` installs the `dataforge` import namespace and `dataforge` CLI.
- Trusted Publishing, Integrity API publish attestations, and fresh-install
  smoke evidence exist for every package, with workflow URLs, wheel/sdist
  provenance URLs, downloaded artifact SHA-256 verification, expected GitHub
  publisher identity, and stored smoke logs.
- `https://dataforge.praneshrajan15.workers.dev/playground` serves the
  Cloudflare Workers frontend and points at the expected Hugging Face backend.
- The HF Space backend is production, CORS-compatible with the Workers origin,
  and can be tied to the release SHA.
- `dataforge_07_dbt` has a fresh Python 3.12 `dbt-duckdb` proof with no skipped
  end-to-end test, dry-run/refuse/apply/revert evidence, command logs, and an
  audit artifact.
- Marcus, Priya, Shreya, and agent-user design-partner paths have sanitized
  evidence notes, separate consent records, timings, trust signals, and closed
  blocking findings.
- The full Hugging Face model family has public model cards and verifier-passed
  quality evidence for SFT, GRPO, and GiGPO stages.

The evidence manifests are not placeholders. Every referenced log, consent
record, dbt artifact, eval report, and verifier report must exist under
`docs/evidence/` or as an absolute file path on the verifier machine. Missing
evidence is the correct state until the external event actually happened.

Until this command passes, the project must not claim the full original
DataForge vision is achieved.
