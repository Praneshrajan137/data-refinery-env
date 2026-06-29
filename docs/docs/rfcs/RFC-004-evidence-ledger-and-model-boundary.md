# RFC-004: Evidence Ledger And Model Boundary

Status: accepted
Date: 2026-06-14

## Decision

DataForge will optimize for a verified, reversible data-repair pipeline. Model
training is valuable only when it improves repair proposals without weakening
the deterministic safety and verification path.

Every material product or model claim must appear in `docs/evidence/ledger.json`
with a closed-vocabulary status, evidence paths, blockers, and claim policy.
Failed training candidates are preserved as diagnostic evidence and cannot
unlock public claims or downstream GRPO stages.

Raw research evaluation and product constrained-decoding evaluation must remain
separate. Constrained decoding can guarantee action shape, but it does not prove
repair correctness; repair F1, no-op preservation, and inferability slices are
still scored independently.

## Consequences

- SFT-v8 smoke evidence blocks SFT-v8 continuation. SFT-v9 action-envelope
  curriculum preflight may seed only an SFT-v9 smoke/diagnostic; GRPO-v4 remains
  blocked until SFT-v9-or-later has a promoted private checkpoint.
- Product work should strengthen CLI/playground/MCP/dbt surfaces over the same
  repair contract instead of creating parallel mutation semantics.
- Future docs should cite the ledger or generated verifier reports, not
  hand-written optimism.
