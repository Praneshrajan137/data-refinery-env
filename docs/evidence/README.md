# Release Evidence

This directory is reserved for proof that cannot be manufactured by local code
changes alone.

`ledger.json` is the canonical machine-readable claim index. It records each
release, product, model, diagnostic, blocked, and roadmap entry with evidence
paths, blockers, and claim policy. Run `python scripts/evidence/evidence_ledger.py`
before upgrading any public claim.

`dataforge release full-vision --json` reads these manifests only after the
external events have happened:

- `pypi/publish_report.json`
- `dbt_duckdb/fresh_env_report.json`
- `design_partners/manifest.json`
- `models/model_family_report.json`

Do not create positive evidence files before the public state, user validation,
or model verification exists. Use `docs/evidence/templates/` when preparing a
runbook.

Current completed external evidence:

- `pypi/publish_report.json` for the `dataforge_07*` PyPI/TestPyPI package
  family.
- `dbt_duckdb/fresh_env_report.json` for the dbt DuckDB fresh-install proof.

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
SFT-v5/v6/v7/v8 remain failed or blocked private diagnostic evidence. The SFT-v8
smoke completed 40 steps, passed the label-mask audit, then correctly stopped as
`quality_gate_failed_no_upload` with strict macro F1 `0.0`, parse success
`0.03`, schema-case errors `26`, and `promote_to_grpo: false`. SFT-v9 is now
only a private action-envelope curriculum/preflight candidate:
`eval/results/sft_v9_action_envelope_curriculum_report.json` records completion
parse success `1.0`, held-out leakage `0`, `finish_with_repairs` `0`, and zero
negative-contrast target leakage. The private Kaggle smoke was submitted as
kernel version `1` and is recorded at
`eval/results/kaggle_sft_v9_smoke_launch_v1/launch_report.json`; this is only a
launch receipt. Smoke v1 failed before training on an over-strict P100
capability guard and is preserved at
`eval/results/kaggle_sft_v9_smoke_v1_failure/failure_report.json`. Smoke v2
with the fixed P100-compatible runner is running and recorded at
`eval/results/kaggle_sft_v9_smoke_v2_relaunch/launch_report.json`. GRPO-v4
remains blocked until a future SFT-v9-or-later private checkpoint has
`promote_to_grpo: true`.

Regenerate `pypi/publish_report.json` only after public PyPI and TestPyPI
publication is complete for a new release:

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

PyPI provenance evidence is used to verify how artifacts were published. It is
not, by itself, a SLSA level claim; DataForge should only claim a SLSA v1.2
level after a dedicated verifier proves that level for the exact release
artifacts.

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
