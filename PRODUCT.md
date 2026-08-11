# DataForge — Product Constitution

This is the canonical statement of what DataForge is, why it exists, and the
principles it must never violate. When any other document (README, ARCHITECTURE,
DECISIONS, docs/) conflicts with this file on purpose, philosophy, or principle,
**this file wins** and the other should be corrected to defer to it. Keep other
docs short and pointed here rather than restating the thesis.

This file states purpose and principle. It does not restate measured numbers,
release status, or command surfaces — those live in `README.md`,
`BENCHMARK_REPORT.md`, `docs/evidence/`, and the generated benchmark blocks,
which are the authoritative sources for anything with a number in it.

---

## 1. The thesis (one sentence)

**DataForge is the data-repair system that fixes only what it can prove correct,
honestly flags everything it cannot, never corrupts data, and proves all three
with reproducible numbers and a reversible, self-verifying certificate.**

Every subsystem — detectors, repairers, the optional LLM corrector, the verified
agent loop, calibration, the playground, the model family — exists only in
service of that sentence.

---

## 2. Purpose

Teams and agents that repair tabular data face an asymmetric risk: a
confidently-wrong fix is worse than no fix, because it silently corrupts the
source of truth and is discovered late, if ever. The tooling landscape mostly
optimizes apparent accuracy (aggregate F1) and leaves the operator to trust that
nothing incorrect slipped through.

DataForge exists to remove that leap of faith. Its purpose is to make **trust
mechanical**: an auto-applied change is formally verified, policy-checked,
byte-for-byte reversible, and reported with detection and correction measured
separately, so the real limits are visible instead of hidden behind a single
score.

---

## 3. Philosophy

- **Trust is the product, not accuracy.** Accuracy is necessary but not
  sufficient. A repair you cannot prove and cannot reverse is a liability, not a
  feature.
- **Abstention is a first-class, high-integrity output.** When the correct value
  is not derivable from the data and not provable, refusing to guess — and saying
  why — is the correct behavior, not a failure.
- **Detection and correction are different problems.** Flagging that a value is
  wrong is often easy; producing the exact right value is frequently impossible
  without external knowledge. DataForge measures and reports them separately and
  never lets a high detection rate imply a correction it cannot make.
- **The floor is deterministic; everything else is additive and gated.** The
  proven deterministic path is the guaranteed baseline. New detectors, LLM
  corrections, and the agent loop may only add on top of it and may never regress
  it.

---

## 4. First principles

1. A repair you cannot reverse is not a repair; it is a risk.
2. A confidence you cannot certify distribution-free is marketing, not evidence.
3. A benchmark you cannot reproduce byte-for-byte is an anecdote, not a result.
4. The correct value is often not inferable in-table; when it is not, abstention
   is the correct output and must be reported honestly.
5. A bug in a verifier must only ever *withhold* a fix, never wave a corrupting
   one through. Verification therefore fails closed and is cross-checked by an
   independently-written implementation.

---

## 5. The honesty doctrine (non-negotiable)

These rules bind all public artifacts — code comments, docs, benchmark reports,
model cards, and release evidence:

- **Report detection and correction separately, per error class.** Never publish
  only an aggregate that hides where the tool is weak.
- **Qualify every claim precisely.** State exactly what a number measures and
  against what baseline, **and whether two numbers are protocol-comparable.**
  DataForge's hospital correction result (0.7926) is measured by its own harness;
  the Raha+Baran baseline (0.73, transcribed from BClean Table 4) is measured under
  BClean's protocol. They are **not a protocol-controlled head-to-head**, so the
  honest phrasing is "competitive with / in the range of the Raha+Baran baseline
  under our scoring," never an unqualified "beats," and never "absolute
  state-of-the-art" (other systems report higher elsewhere). Precision in claims is
  itself part of the product.
- **When you cannot certify, say so with a reason, not a magic number.** A
  disabled auto-apply threshold is recorded with its cause (e.g.
  `uncertified_classes`), never presented as a model judgment.
- **Never pre-claim an external event.** Publication, live surfaces,
  design-partner validation, and model-quality milestones ship their evidence
  *after* the event, never before.
- **Never rewrite frozen historical evidence.** Past run snapshots, training
  curricula, and released tokenizer vocab are factual records of what happened and
  must not be edited to look better.

---

## 6. Vision

The default, auditable trust layer for tabular data repair: the component a
regulated team, an autonomous agent, or a warehouse pipeline can call and *know*
that nothing incorrect was silently written — because every applied change
arrives with a certificate it can independently re-verify and reverse.

---

## 7. Mission

Ship a local-first, provider-agnostic, formally-verified repair engine — usable
identically from the CLI, an MCP server, a verified agent loop, and a hosted
playground — where every auto-applied fix carries a self-verifying trust
certificate, and every published claim is backed by reproducible evidence.

---

## 8. The safety invariant (must hold on every surface)

Every applied repair, regardless of which surface requested it, must pass this
order before touching disk:

```
detect -> propose -> SafetyFilter (constitution)
       -> differential verify (SMT + Direct, fail-closed)
       -> auto-apply gate (provable-only; conformal + drift for LLM values)
       -> hash-chained journal + immutable source snapshot
       -> atomic apply -> byte-for-byte reversible
```

- **Provable-only auto-apply.** A fix auto-applies only if it is `proven`
  (deterministic, or verified against an authoritative schema). A
  `plausibility_only` fix (an LLM value with no authoritative schema) is held for
  review unless an explicit opt-in is set, and even then is recorded truthfully as
  plausibility-only. This is enforced *inside* the mutation primitives, not at each
  calling surface, so a surface cannot opt out of it by forgetting to ask.
- **One write path.** The CLI, MCP server, verified agent, playground, and OpenEnv
  environment must route writes through the single core engine. No surface may
  create parallel write semantics.

  Stated exactly, because an earlier version of this bullet claimed "there are two
  mutation primitives, not one" and that was **false**. There are four leaf write
  primitives. Two are gated by the provable-only invariant
  (`engine.repair.apply_transaction` for files; `DuckDBStore.apply_patch_plan` for
  warehouse SQL). Two are not, for stated reasons: `revert_transaction` restores bytes
  the tool itself recorded, so there is no new value to prove; and
  `write_constraint_review_artifact_atomic` rewrites the user's *constraints* artifact,
  which changes the premise of provenness rather than a value
  (`docs/trust/authority-is-mutable.md`). A static scan for callers of one primitive
  cannot see the others, which is why the registry is keyed by **primitive** and the
  runtime invariant is parametrized over **surfaces** —
  `docs/trust/write-surface-uniformity.md`.
- **The certificate travels.** The receipt is self-contained (source/post hashes,
  applied fixes with verification strength, proof obligations, revert command) and
  can be independently re-verified and reversed away from the machine that
  produced it.

---

## 9. Where DataForge fits (honest positioning)

- **Assertion frameworks (Great Expectations, dbt tests, Soda)** detect and
  assert but do not repair. DataForge repairs *and* proves the repair safe and
  reversible.
- **Statistical/Bayesian correctors (HoloClean, Raha+Baran, BClean)** achieve
  strong correction F1 but offer no per-fix formal proof, no reversibility
  guarantee, and no distribution-free auto-apply certificate.
- **LLM "clean my data" tools** are fluent but miscalibrated and unverifiable.
  DataForge does not assert this — it *measured* it, and its LLM corrector
  therefore stays propose-not-apply until measurement earns otherwise.

DataForge's differentiator is not a higher score; it is that its applied changes
are provable, reversible, and honestly bounded.

---

## 10. When DataForge is the wrong tool

Do not use DataForge for streaming data, very large warehouse tables under strict
low-latency SLAs, regulated workflows that require every fix to be human-authored,
or teams already well served by maintained assertion suites. It is currently best
suited to local tabular profiling, provable repair, benchmark research, and
training/evaluation work. Choosing the honest scope is part of the doctrine.

---

## 11. Authority and pointers

- Measured numbers, commands, and release status: [README.md](README.md),
  [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md), `docs/evidence/`.
- System design and the safety invariant in depth:
  [ARCHITECTURE.md](ARCHITECTURE.md).
- Trust properties: the accuracy frontier ([docs/trust/accuracy-frontier.md](docs/trust/accuracy-frontier.md)),
  the inferred-guard gap registry ([docs/trust/inferred-guard-gaps.md](docs/trust/inferred-guard-gaps.md)),
  and constraint circularity ([docs/trust/constraint-circularity.md](docs/trust/constraint-circularity.md)).
- Decision history and rationale: [DECISIONS.md](DECISIONS.md).
- Product center of gravity (verification-layer-first, staged): [docs/STRATEGY.md](docs/STRATEGY.md).
- Agent-session gotchas and conventions: [CLAUDE.md](CLAUDE.md).
- The external "full original vision" gate: `dataforge release full-vision`
  and [docs/docs/full-vision.md](docs/docs/full-vision.md); the honest,
  dependency-ordered plan to reach it is [docs/ROADMAP_FULL_VISION.md](docs/ROADMAP_FULL_VISION.md).

License: Apache-2.0.
