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

**DataForge is the reference implementation of a verifiable data-mutation protocol: it
changes only what it can prove correct, honestly flags everything it cannot, never
silently corrupts data, and emits a portable attestation that a third party can verify
without trusting DataForge.**

Every subsystem — detectors, repairers, the optional LLM corrector, the verified
agent loop, calibration, the playground, the model family — exists only in
service of that sentence.

### 1.1 Why the sentence changed (2026-08-13)

It previously read "the data-repair system that fixes only what it can prove correct
... and proves all three with reproducible numbers and a reversible, self-verifying
certificate." Two things were wrong with that, and both were wrong in the same
direction: they claimed more than the artifact delivered.

**"Self-verifying" was not true of anything a third party could hold.** The certificate
was printed to stdout, reprojected lossily over HTTP, wrapped a third way by the
browser, carried no tool version or timestamp, embedded none of the constraints it was
verified against, referred to the transaction journal by a bare string, was unsigned,
and had no CLI command that could check it. `docs/STRATEGY.md` names the consequence:
*"A certificate with a named consumer is a product; one without is a log line."*

**Leading with "repair" put the weakest axis first.** Measured deterministic correction
is one dataset at F1 0.7926, one at 0.0000, one sampled at 0.0000 with 696 false
positives, and no LLM error class has earned auto-apply at any tested error budget. The
verification machinery is what holds: on an adversarial corpus, 0 of 14
constraint-violating proposals were written under a properly constrained schema. Leading
with repair invited a comparison the product does not win and does not need to.

The new sentence is narrower and checkable. "Verifiable" now means something specific:
a published format, two independent implementations, and committed conformance vectors
covering every rejection case.

### 1.2 What the protocol does NOT claim

Stated here because a protocol that implies guarantees it lacks is worse than no
protocol.

- **Not that a written value is true.** It says a change was proven against *stated
  constraints* and is reversible. A value that satisfies every declared constraint and
  is nonetheless false will be written. Measured: 3 of 3 such proposals in the
  adversarial corpus. The guarantee covering them is reversibility, not correctness.
- **Not that a schema makes a proof strong.** The strength of a proof is the strength of its
  premise. A schema declaring every column `str` covers every column and, until 2026-08-25, was
  therefore granted authority over all of them: it admitted 10 of 14 constraint-violating
  proposals that a typed, bounded, patterned schema refused, and labelled every one `proven`. A
  declared type must now be able to reject something before it confers proof, so that premise
  admits 0 of 14 and can write nothing at all, while the typed premise keeps writing its
  legitimate repairs. Measured both before and after in
  `eval/results/trust_ledger_adversarial.json`; predicted in advance in
  `eval/preregistration/entailment_strength.md`. What has **not** changed is the limit itself: a
  weak premise still yields a weak proof, and the gate cannot judge whether the constraints you
  declared are the right ones.
- **Not authenticity without a key.** Signing is optional and proves a keyholder
  produced the payload. Key distribution and trust roots are deployment policy and out
  of scope; an unsigned attestation is reported `unsigned`, never `verified`.

### 1.3 The same lesson, learned twice (2026-08-25)

Section 1.1 quotes `docs/STRATEGY.md`: *"A certificate with a named consumer is a product; one
without is a log line."* That principle was then violated by a second certificate.

`SessionCertification` -- the artifact carrying per-table calibrated auto-apply thresholds -- is
printed to stdout and discarded. No serializer, no loader, and `dataforge repair` reads a different
artifact whose schema it cannot satisfy. **No certified threshold has ever influenced a byte.** It
was hardened over several commits with a stratified label-noise bound, self-checking per-class
tallies and five validators, while the code path that actually writes was labelling proposals
`proven` because a column had been declared `str`. That premise was measured admitting 10 of 14
constraint-violating writes.

Two durable rules follow, and they belong here rather than in a decision log because they are about
where effort goes, not about a single fix:

- **Before hardening a component, name its consumer.** If nothing reads it, rigour there buys
  correctness of a report, not of the product. The check is one grep for the type name.
- **Derive the population a gate polices; never restate it.** Added 2026-08-26, and it is the rule
  above applied one level up — to the machinery that checks our claims rather than to the product.
  `scripts/ci/readme_truth.py` imported the write-authority allowlist from source of truth and then
  subtracted it from a hardcoded eight-name set literal, while the closed `IssueTypeLiteral`
  vocabulary had grown to eleven. Three issue types were invisible to that gate in both directions: a
  document could assert any of them auto-applies and CI would pass. Four public documents said "Eight
  detector families" for the same reason, so **the prose and the gate agreed with each other and both
  disagreed with the code** — two mutually-consistent wrong artifacts reading as verification.

  A gate that hardcodes any part of the universe it polices can only detect changes to the part it
  derives, and freezing the population is invisible precisely because the frozen literal was correct
  on the day it was written. The same defect existed a second time, in the generator that projects the
  trust vocabulary into TypeScript: it derived every projected *value* and hand-enumerated *which*
  vocabularies to project, so adding a constant failed CI loudly and then resolved by omission —
  regenerate, hash updates, CI green, constant never projected.

  The corollary is about what a green gate means. Widening the population found no stale claim, which
  is not evidence the old gate worked: it could not have found one. **A gate nobody has seen fail on
  a case it newly covers has not been shown to cover it**, so a fix of this kind must ship with a
  planted claim that fails against the frozen version and passes against the derived one.
- **Measure the path that writes, unconditionally.** The label-free repairer reported precision
  1.0000 on hospital when scored only on cells that were already errors, and corrupted 86
  previously-correct cells when scored over everything it touched. Conditional precision cannot show
  a write path to be safe, because the failure that costs a user data is not in its denominator.

What the label-free path is actually worth is now measured rather than asserted, in
`docs/trust/deductive-coverage-result.md`: write precision between 0.5618 and 1.0000 and coverage
between 0.0 and 0.8861, jointly determined by corpus, premise source and decision rule. There is no
single number, and quoting one would be quoting the corpus it came from.

### 1.4 No premise, no unsupervised write (2026-08-25)

This is now literally true, and it was not before.

The three detectors whose `deterministic` fixes skip the calibration threshold were audited against
the rule the allowlist sets for itself -- that a member is calibration-bound "until it earns an entry
here with a committed measurement". Two of the three had never earned one.
`docs/trust/bypass-allowlist-evidence.md` supplies all three:

- `missing_value` -- **427 writes, 427 repaired, 0 harmful, precision 1.0000.** The strongest measured
  result of any repairer here, bought with unanimity rather than majority and therefore with low
  coverage.
- `fd_violation` -- write precision 0.6602 to 1.0000; 2037 repaired against 700 harmful.
- `type_mismatch` -- **156 flags, zero proposals** across 4,376 rows and 6,377 real errors. Removed
  from the bypass.

Removing it emptied the last unpremised write path in the product. That was the only
zero-configuration write, and losing it is the right trade: it had no measurement, its trigger was a
hardcoded threshold on the column's own distribution, it discarded the schema so no premise rule could
reach it, and it erased values rather than copying them.

**Zero writes is not a safety result**, and the reason belongs in this file because it generalises.
`decimal_shift` was benchmark-quiet too -- 39, 92 and 112 flags at precision 0.0000 -- and what removed
it was a *fourth* dataset where it would have rewritten 263,428 values. A detector whose failure
population is absent from your corpora has not been shown to be safe; it has been shown to be
unreachable by your evidence. Treat the two differently.

### 1.4 The premise is the product, and I found a signal I chose not to ship

The write gate turned out not to be what determines corruption. Under a premise whose dependencies
all hold, `fd_violation` overwrote **0** already-correct hospital cells. Under the product's own
mined premise it overwrote **86**, and all 25 sampled corruptions traced to dependencies that are
false on ground truth -- 23 of them to `ZipCode -> HospitalName`. A zip code does not determine a
hospital name.

**Corrected 2026-08-26, and the correction is worse than the original.** That 86 was measured against
a premise at confidence >= 0.95, and no user is given one. `ConstraintReviewArtifact.to_schema()`
applies **no floor at all**, so the premise a zero-config user actually accepts is the miner's full
output at >= 0.90 -- 85 dependencies rather than 81. Measured through the real artifact and merge in
`docs/trust/shipped-premise-result.md`: **116** clean cells corrupted, not 86.

The decisive number is the one that did not move. `repaired_a_real_error` is **451 in both arms**: the
four additional dependencies are all false on ground truth, repaired **nothing**, and corrupted thirty
more clean cells. There was no trade to weigh.

Two rules follow, and they belong here rather than in a decision log because they are about how
evidence is built:

- **An arm that models a user journey must be built from the code that journey runs.** The proxy here
  erred conservative, which is the safe direction, and it still meant every published figure described
  a product that does not exist. A measurement of something adjacent to the product is not a
  measurement of the product.
- **Premise precision does not predict corruption.** Two of the four added dependencies are equally
  false and corrupted **nothing**, because a false dependency is inert where its determinant group
  holds no visible disagreement. So FD-set precision is the wrong single quantity to optimise, and a
  confidence floor tuned on it is tuned on the wrong axis. What determines harm is whether a false
  premise meets a group that disagrees.

So the miner's precision was measured for the first time: **0.8655** on hospital, **1.0000** on tax,
and **no candidates at all** on flights or rayyan. Every false dependency this project has ever
measured comes from one corpus.

Then the uncomfortable part. Confidence measured only on the rows that can actually falsify a
dependency -- excluding singleton determinant groups, which are consistent with any value and
therefore inflate the shipped score -- separates true from false dependencies **perfectly** on
hospital: false at most 0.9554, true at least 0.9599, with every true dependency retained. The
shipped confidence overlaps and cannot do this at any threshold.

**I did not ship it as a gate.** The separating constant is fitted to 85 candidates from a single
corpus, and there is nothing to validate it against, because the other three corpora produce
either no candidates or no false ones. The pre-registered kill criterion forbade introducing the
parameter and I abided by it. The number is instead reported to the human who accepts the
constraint.

This is the discipline the rest of this file argues for, applied at the one moment it was expensive:
the change would have looked like a clean win in any table I published, and the reason to refuse it
is only visible if you insist on asking what would have validated it.

It also **vindicates** an earlier decision made on reasoning alone. `docs/trust/constraint-circularity.md`
recorded 696-708 false-positive corrections with zero correct ones on tax, and argued the defense had
to be architectural. That was measured before the miner's determinant guards existed. On the same
200,000-row corpus the miner now emits four candidates and all four are true.

Two smaller things followed. The miner stopped emitting dependencies whose dependent is a **constant
column** -- a single-valued column is determined by everything, so the dependency is vacuous, and 34
of hospital's 119 candidates were of that kind. It is justified on reviewer burden, not precision:
it *lowers* the measured figure, because the candidates it removes were true-but-vacuous and the
truth label had been rewarding them. And `missing_value`'s mined-premise arm, which had no test at
all, turns out to be **unreachable on every corpus** -- hospital mines 85 dependencies but has zero
missing values, flights and rayyan mine none, and tax's four do not overlap its missing columns. All
427 writes behind that repairer's precision of 1.0000 came from the declared arm. The path is live
for users and unmeasured by evidence, which is the worst combination and why it is now unit-tested.

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
