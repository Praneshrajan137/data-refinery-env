# Release Evidence

This directory is reserved for proof that cannot be manufactured by local code
changes alone.

`dataforge release full-vision --json` reads these manifests only after the
external events have happened:

- `pypi/publish_report.json`
- `dbt_duckdb/fresh_env_report.json`
- `design_partners/manifest.json`
- `models/model_family_report.json`

Do not create positive evidence files before the public state, user validation,
or model verification exists. Use `docs/evidence/templates/` when preparing a
runbook.

Every positive manifest entry must point at inspectable logs, artifacts,
consent records, eval reports, or verifier reports. Booleans alone are not
release evidence.

Generate `pypi/publish_report.json` only after public PyPI and TestPyPI
publication is complete:

```bash
python scripts/ci/pypi_publish_report.py \
  --workflow-run-url dataforge_07=https://github.com/Aegis15/dataforge/actions/runs/<run-id> \
  --workflow-run-url dataforge_07_mcp=https://github.com/Aegis15/dataforge/actions/runs/<run-id> \
  --workflow-run-url dataforge_07_evals=https://github.com/Aegis15/dataforge/actions/runs/<run-id> \
  --workflow-run-url dataforge_07_dbt=https://github.com/Aegis15/dataforge/actions/runs/<run-id> \
  --workflow-run-url dataforge_07_agent_patterns=https://github.com/Aegis15/dataforge/actions/runs/<run-id>
```

The script writes `dataforge_pypi_publish_report_v2` and refuses optimistic
evidence when package metadata, downloaded artifact hashes, wheel/sdist
provenance URLs, PyPI Integrity API publish attestations, expected GitHub
Trusted Publisher identity, or referenced smoke logs are missing.

Design-partner evidence must use sanitized notes plus separate consent records.
Start from `templates/design_partner_evidence_note.template.md` and
`templates/design_partner_consent.template.json`; do not commit private data or
permission-to-list claims before consent exists.
