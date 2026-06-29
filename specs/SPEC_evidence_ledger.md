# SPEC: Evidence Ledger And Model Boundary

> Status: Draft
> Owner: DataForge maintainers
> Last updated: 2026-06-26

## Purpose

Make DataForge claims evidence-led instead of prose-led. The ledger is the
canonical machine-readable index of shipped, beta, verified-research,
failed-diagnostic, blocked, and roadmap claims.

## Outcomes

- `docs/evidence/ledger.json` records every externally meaningful product,
  release, and model claim with evidence paths, blockers, and claim policy.
- `scripts/evidence/evidence_ledger.py` rejects missing paths, duplicate ids,
  unknown statuses, and public claims for failed/blocked/roadmap entries.
- Model candidates are split into raw research metrics and product constrained
  decoding metadata so constrained parse reliability cannot hide weak repair
  quality.
- GRPO launch tooling treats missing or failed SFT predecessor evidence as a
  clean blocked state before GPU work starts.

## Prior Decisions

- Product north star: verified repair pipeline, not an LLM data-cleaning demo.
- Models are repair proposers; the product write path remains detector,
  SafetyFilter, SMTVerifier, patch plan, receipt, journal, and audit/revert.
- Public model updates require verifier evidence. Failed candidates are useful
  evidence but never model wins.
- Raw model evaluation and constrained product evaluation are reported
  separately.

## Acceptance Gate

- `python scripts/evidence/evidence_ledger.py` passes.
- Focused tests prove SFT-v8 is recorded as failed diagnostic evidence, SFT-v9
  is only private curriculum/preflight evidence, and GRPO-v4 remains blocked.
- README and claim docs cite the ledger without upgrading model-family claims.
