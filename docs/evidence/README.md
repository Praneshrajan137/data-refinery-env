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
