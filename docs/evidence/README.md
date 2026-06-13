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

Per-model verifier reports may be committed once the relevant public Hub model
passes its leaf verifier. The current `0.5B` public evidence is
`models/DataForge-0.5B-SFT.verification.json` and
`models/DataForge-0.5B-GRPO.verification.json`; `DataForge-0.5B-GRPO` is
verified research evidence, not a production-quality repair model and not proof
that the full model family is complete. The public-row family snapshot in
`eval/results/model_family_public_verified_20260609.json` is diagnostic output,
not `models/model_family_report.json`.

Failed training candidates are retained as diagnostic evidence when training and
strict eval completed. The 0.5B-GRPO v2 run is recorded under
`eval/results/kaggle_grpo_v2_failed_20260611/` with status
`quality_gate_failed_no_upload`; it must not be cited as a release-quality model
because it missed strict macro F1 `>=0.25` and not-inferable slice F1 `>=0.95`.
SFT-v5 and GRPO-v3 remain private candidate work until their own verifier-passed
public evidence exists.

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

Generate `models/model_family_report.json` through
`scripts/model/verify_model_family.py` only after the relevant Hugging Face
model repos, model-card metadata, eval reports, verifier reports, training run
URLs, dataset SHAs, and model SHAs exist. The schema is
`dataforge_model_family_report_v2`; blocked rows may be recorded in planning
outputs, but the full-vision gate passes only when every row is public and
quality-verified.

Design-partner evidence must use sanitized notes plus separate consent records.
Start from `templates/design_partner_evidence_note.template.md` and
`templates/design_partner_consent.template.json`; do not commit private data or
permission-to-list claims before consent exists.
