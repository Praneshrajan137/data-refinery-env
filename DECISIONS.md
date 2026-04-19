# DataForge - Decisions Log

Format for every entry:

## YYYY-MM-DD - <decision title>
**Context**: what triggered the decision; what problem it solves.
**Alternatives**: 2-4 options considered with honest pros/cons.
**Decision**: the pick.
**Reasoning**: why this over the others.
**Reviewed with**: who (if anyone) sanity-checked it.
**Reversal criteria**: what evidence would make us switch.

---

## 2026-04-19 - Ship an honest scaffold before feature code
**Context**: the repository needed a clean DataForge monorepo foundation
without pretending the future implementation already exists.
**Alternatives**:
- Port the older hackathon environment directly. Pros: faster apparent progress.
  Cons: mixes product lines and muddies the DataForge architecture.
- Ship a scaffold first. Pros: clean package boundaries, honest README, and
  reproducible Week 0 setup. Cons: little immediate end-user functionality.
- Wait to create the repo until feature code is ready. Pros: fewer visible
  placeholders. Cons: delays CI, packaging, and spec-first workflow discipline.
**Decision**: ship the scaffold first.
**Reasoning**: the scaffold creates a clean baseline for future PRs, keeps the
repository honest about current capabilities, and preserves the spec-first
workflow required by the project rules.
**Reviewed with**: Codex implementation pass.
**Reversal criteria**: if the scaffold blocks incremental delivery or creates
avoidable churn for early feature PRs, collapse unused structure in a follow-up.
