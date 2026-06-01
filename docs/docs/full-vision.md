# Full Vision Gate

The full original DataForge vision is complete only when external state proves
it. Local code, tests, and docs are necessary but not enough.

Run:

```bash
dataforge release full-vision --json
```

The gate checks:

- PyPI and TestPyPI publish the final packages: `dataforge`,
  `dataforge-mcp`, `dataforge-evals`, `dataforge-dbt`, and
  `dataforge-agent-patterns`.
- Trusted Publishing, attestations, and fresh-install smoke evidence exist for
  every package.
- `https://dataforge.dev/playground` resolves, serves the Cloudflare frontend,
  and points at the expected Hugging Face backend.
- The HF Space backend is production, CORS-compatible with `dataforge.dev`, and
  can be tied to the release SHA.
- `dataforge-dbt` has a fresh Python 3.12 `dbt-duckdb` proof with no skipped
  end-to-end test and an audit artifact.
- Marcus, Priya, Shreya, and agent-user design-partner paths have consented
  evidence.
- The full Hugging Face model family has public model cards and verifier-passed
  quality evidence.

Until this command passes, the project must not claim the full original
DataForge vision is achieved.
