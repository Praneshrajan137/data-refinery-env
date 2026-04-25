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

---

## 2026-04-20 - Issue severity tiers — 3 levels (SAFE / REVIEW / UNSAFE)
**Context**: the detector subsystem needs a severity classification for
data-quality issues. The choice of how many tiers affects the entire
downstream pipeline: auto-apply logic, UI filtering, and safety gates.
**Alternatives**:
- 2 tiers (safe/unsafe). Pros: simplest possible model. Cons: loses the
  critical "human should look at this" signal — most real issues are ambiguous.
- 3 tiers (SAFE/REVIEW/UNSAFE). Pros: maps to actionable workflows (auto-apply,
  show in table, block). Cons: boundary between REVIEW and UNSAFE requires
  calibration per detector.
- 5 tiers (fine-grained confidence bands). Pros: maximum granularity. Cons:
  creates decision paralysis — where does "probably wrong" end and "suspicious"
  begin? Forces users to configure thresholds.
**Decision**: 3 tiers — SAFE, REVIEW, UNSAFE.
**Reasoning**: 3 tiers match the three fundamental actions a pipeline can take
(auto-apply, present for review, block). The REVIEW tier captures the vast
majority of real-world ambiguous cases without forcing premature classification.
**Reviewed with**: SPEC_detectors.md Section 5.
**Reversal criteria**: if user feedback shows >30% of REVIEW items are
consistently auto-approved or auto-rejected, collapse to 2 tiers.

---

## 2026-04-20 - Transaction-first repair with immutable source snapshots
**Context**: Week 2 adds `dataforge repair --apply` and `dataforge revert`.
The core risk is losing the original file state or claiming byte-identical
revert while relying on a pandas read/write cycle that normalizes formatting.
**Alternatives**:
- Apply file edits first, then write a transaction record. Pros: simplest code path.
  Cons: violates the safety invariant; a crash between write and log loses auditability.
- Write a mutable JSON transaction record and update it in place. Pros: simple to inspect.
  Cons: not append-only; weak audit semantics; higher corruption risk on partial writes.
- Journal first and rely on inverse cell writes for revert. Pros: compact storage.
  Cons: cannot honestly guarantee byte-identical restore for arbitrary CSV formatting.
- Journal first and persist an immutable source snapshot. Pros: true byte-identical
  restore, append-only audit trail, and safe recovery from apply-time failures.
  Cons: more disk usage per transaction.
**Decision**: write the transaction journal and source snapshot before apply,
then use the snapshot as the source of truth for revert.
**Reasoning**: transaction-first ordering preserves the audit trail even when
apply fails, and immutable snapshots are the only honest way to guarantee
byte-for-byte restore after a lossy DataFrame rewrite.
**Reviewed with**: SPEC_transactions.md and the Week 2 implementation pass.
**Reversal criteria**: if snapshot storage cost becomes a real operational
problem and we have a proven patch-based writer that preserves exact bytes on
apply, revisit snapshot-backed revert.

---

## 2026-04-20 - Select Z3 over cvc5 for the Week 3 verifier
**Context**: Week 3 needs a local SMT solver for domain-bound and
functional-dependency verification in the repair path. The solver choice affects
Python integration quality, unsat-core ergonomics, packaging friction, and the
ability to ship a credible local verifier on Windows, macOS, and Linux.
**Alternatives**:
- Use Z3. Pros: mature Python bindings, broad community familiarity, reliable
  support for tracked assertions and unsat cores, already present in the project
  dependency set. Cons: large binary wheel, string-theory ergonomics are not
  always intuitive, quantifiers still require careful handling for performance.
- Use cvc5. Pros: strong SMT support, modern solver implementation, good theory
  coverage. Cons: weaker Python ergonomics for the current repo, higher
  packaging / contributor-friction risk, and less existing team familiarity.
- Avoid an SMT solver and use imperative checks only. Pros: simplest code path,
  easiest to debug. Cons: breaks the architectural promise of SMT-verified
  repairs and weakens explainability / extensibility for future constraints.
**Decision**: use Z3 for Week 3.
**Reasoning**: Z3 is the fastest route to a production-quality local verifier in
this repository because it combines proven Python support, tracked-assertion
APIs, and low adoption friction for contributors. cvc5 remains technically
credible, but the integration overhead is not justified for the Week 3 ship
goal.
**Reviewed with**: SPEC_smt_verifier.md and the Week 3 implementation pass.
**Reversal criteria**: if Z3 fails the benchmark target (`p95 < 200 ms` on the
1,000-row / 2-FD benchmark), proves materially unstable on Windows wheels, or
blocks a needed future theory that cvc5 handles cleanly, revisit the solver
choice.

---

## 2026-04-21 - Separate reproduced local benchmark rows from citation-only SOTA rows
**Context**: Week 4 adds benchmark reporting on the Raha Hospital, Flights, and
Beers datasets. The upstream files reveal dirty/clean header mismatches for
Hospital and Beers, and the external literature rows are not reproduced under
the exact same protocol as the shipped local DataForge runs.
**Alternatives**:
- Force a single mixed table. Pros: compact. Cons: blends reproduced local
  numbers with citation-only literature rows and hides protocol differences.
- Publish only local DataForge rows. Pros: maximal purity. Cons: loses the
  external calibration reviewers expect from benchmark sections.
- Use positional dirty/clean alignment plus dual tables. Pros: preserves honest
  local reproducibility while keeping literature references clearly labeled.
  Cons: slightly more reporting complexity.
**Decision**: align dirty/clean files by column position and report dual tables:
reproduced local rows plus citation-only SOTA rows.
**Reasoning**: positional alignment matches the actual upstream dataset shape,
and dual-table reporting keeps the benchmark section methodologically honest.
**Reviewed with**: SPEC_benchmarks.md and the Week 4 implementation pass.
**Reversal criteria**: if later work reproduces comparable external methods
under the same protocol, collapse the two tables into one fully reproduced
comparison.

---

## 2026-04-21 - Design-partner gate as a Week-4-to-5 go/no-go
**Context**: META_CONTEXT.md §F3 identifies "no design partner" as a
top-three kill risk. The project needs an explicit checkpoint that forces
user-validation work before feature work proceeds. Without a gate, the
playground ships into a vacuum.
**Alternatives**:
- No gate. Pros: maximum velocity on feature code. Cons: ignores the
  highest-probability failure mode; ships a playground nobody asked for;
  the reviewer sees zero external users and reaches judgment 2 or 3.
- Informal gate ("try to find someone"). Pros: low ceremony. Cons: no
  artifact trail; easy to rationalize "I'll do it next week" forever;
  indistinguishable from no gate in retrospect.
- Artifacted gate with bookkeeping (this choice). Pros: committed
  template, issue form, outreach log, and tally table create accountability
  and a visible trail; the gate is pass/fail on concrete criteria (>= 1
  named partner, >= 1 filed issue or verbatim quote). Cons: overhead of
  maintaining the tally table; risk of cargo-culting the form without
  genuine outreach.
**Decision**: artifacted gate with bookkeeping.
**Reasoning**: the overhead is minimal (a template, an issue form, a
progress appendix), and the alternative is pretending user-validation
happened. The artifacts also serve a second purpose: they are themselves
a product-thinking signal for reviewers evaluating the repo.
**Reviewed with**: META_CONTEXT.md §F3, SPEC_playground.md.
**Reversal criteria**: if recruit rate exceeds 1 partner per week sustained,
the gate becomes unnecessary overhead and can be dropped. If recruit rate is
less than 1 per month after 4 weeks of active outreach, pause feature work
further and make outreach the sole Week-6+ activity.

---

## 2026-04-21 - Cloudflare Pages + HF Docker Spaces for the hosted playground
**Context**: the playground needs a free-tier host for both a static frontend
and a Python backend (FastAPI + pandas + dataforge). The choice must survive
indefinitely on zero-cost infrastructure without maintenance burden.
**Alternatives**:
- Vercel + serverless function. Pros: mature DX, fast deploys. Cons: Python
  serverless functions on Vercel have cold-start latency and dependency size
  limits that make pandas + z3-solver impractical; free tier has invocation
  limits that could throttle a public playground.
- Railway. Pros: great Docker support, generous free tier. Cons: free tier
  has a monthly credit cap ($5/month) that can be exhausted by sustained
  traffic; the project would need to monitor credits or risk downtime.
- Render. Pros: Docker support, free tier. Cons: free-tier containers spin
  down after 15 minutes and cold-start takes ~30 s; the free plan has limited
  RAM (512 MB) which is tight for pandas + z3.
- Cloudflare Pages (frontend) + HF Docker Space (backend). Pros: Pages is
  truly free with global CDN and no invocation limits; HF Spaces support
  Docker SDK with auto-sleep and no monthly credit cap; the combination
  survives indefinitely at zero cost. Cons: HF free-tier Spaces have ~15 min
  sleep timeout and ~30 s cold-start; the frontend must handle this gracefully.
**Decision**: Cloudflare Pages (frontend) + HF Docker Space (backend).
**Reasoning**: this is the only combination that (a) has no monthly credit cap,
(b) supports a full Python + pandas + z3 stack, (c) survives indefinitely
without human intervention, and (d) provides a global CDN for the static
frontend. The cold-start tradeoff is acceptable for a demo playground.
**Additional design decisions**:
- Stateless by design: no persistence, no sessions, no browser storage. This
  eliminates entire classes of security and privacy concerns and makes the
  playground safe to leave running unattended.
- Heuristic-only default: no LLM call unless the user explicitly opts in AND
  a provider key is configured in Space Secrets. This ensures the playground
  works without any external API dependencies.
**Reviewed with**: SPEC_playground.md, META_CONTEXT.md §0.4 rules 4 and 6.
**Reversal criteria**: if free-tier limits are hit (HF downgrades free Spaces
or Cloudflare changes Pages pricing), or a sponsor donates compute, revisit
the hosting choice. If cold-start UX proves unacceptable in design-partner
feedback, consider a paid tier or a keep-alive cron.
