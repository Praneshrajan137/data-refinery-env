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
is one dataset at F1 0.8352, one at 0.0000, one sampled at 0.0000 with 696 false
positives, and no LLM error class has earned auto-apply at any tested error budget. The
verification machinery is what holds: on an adversarial corpus, 0 of 14
constraint-violating proposals were written under a properly constrained schema. Leading
with repair invited a comparison the product does not win and does not need to.

**And that 0.8352 is measured at proposal stage, before this product's own gates.** It counts
what the detector and repairer propose; it does not survive the verifier and the auto-apply gate.
Through `dataforge repair` on the same table with the same premise the figure is **0.0039** — one
cell, written correctly — because 570 of 571 candidates are discarded. The gap is 214.2x, it is a
stage difference rather than a scoring one, and it is measured in
[docs/trust/capability-measurement-stage.md](docs/trust/capability-measurement-stage.md). The
cited BClean and Cocoon figures are end-to-end results, so this is a comparability defect on an
axis entirely within this project's control — deeper than the dataset and premise differences
recorded in
[docs/trust/baseline-protocol-comparability.md](docs/trust/baseline-protocol-comparability.md).

**Settled 2026-09-08: none of these numbers anchors a capability claim.** The question left open
above — which figure should — was decided by measuring the arm it needed, under a rule fixed
before the result was known
([docs/trust/declared-premise-capability.md](docs/trust/declared-premise-capability.md)). A
premise authored from the corpus's public data dictionary, entering through
`dataforge repair --schema`, writes **zero** cells. It is not vacuous: it raises 8,223 dependency
violations and the repairer proposes 399 repairs, more than the ground-truth-admitted premise's
397. That ceiling — a premise **no user can author** — writes 54 cells for **F1 0.1918**.

So **there is no demonstrated end-to-end correction capability on hospital**, and 0.8352 stands
only as a proposal-stage measurement of the detector and repairer. The honest claim is the one
section 1.3 already reaches for: detection, advisory triage and reversibility.

**Retracted the same day, by measuring one flag.** The paragraph above is wrong, and it is left
standing because section 5 forbids rewriting evidence to look better. The declared premise's zero was
not a refusal on evidence — it was a refusal on **volume**.
`dataforge/engine/repair.py` discards an entire batch that would rewrite more than 100 cells, and
discards it **silently**, into neither the applied set nor the review queue. With
`--confirm-escalations`, the same premise through the same write path writes **152 of 509 real errors
correctly at precision 1.0000 with zero corruptions — F1 0.4599**
([fd-repair-yield-mechanism.md](docs/trust/fd-repair-yield-mechanism.md)). That is the first
end-to-end correction result this project has measured, and the stage gap narrows from 214.2x to
about 1.8x. It must always be quoted with its flag, because the same premise scores 0.0000 without
it.

Two consequences worth stating plainly. **The oracle premise never looked better than the declared
premise — it looked smaller**, because 54 cells is under the cap and 152 is not; a premise that finds
more correct repairs wrote fewer cells. And the durable defect is in the **instrument**: the receipt
carried `safety_verdict = escalate` and the exact `NO_HIGH_VOLUME_AUTO_APPLY` reason the whole time,
while five separate harnesses reported a bare zero and none reported why, because they recorded
writes and true positives and never the batch verdict.

Two things follow that were previously assumed the other way. The gap is **not** a
premise-quality problem — a premise of thirteen dependencies every one of which ground truth
admits also writes zero — so no amount of schema authoring closes it. And the advice
*"Prefer --schema"*, which shipped in `dataforge repair --help` on the strength of a figure
belonging to the ground-truth-derived ceiling, had no support at any premise quality. It is
corrected.

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
- **Not that a MINED constraint can authorise a write.** Since 2026-09-07 write authority
  follows a constraint's **provenance**, not the miner's confidence in it. A dependency
  discovered in your table and accepted in `dataforge constraints review` drives detection and
  verification but confers **no right to write**; that needs a declared schema, or the explicit
  `--trust-mined-constraints` opt-in. The reason is measured, not assumed: across ten
  externally annotated tables, the best of four in-table quality measures **discards 16 of 143
  hand-annotated true dependencies** when its threshold is carried to a table it was not fitted
  on, so no confidence floor can rescue a mined premise
  ([docs/trust/premise-acquisition-result.md](docs/trust/premise-acquisition-result.md)).
  **This makes the zero-config path write nothing by default. That is a trade, not a safety
  result** -- see section 1.5.
- **Not authenticity without a key.** Signing is optional and proves a keyholder
  produced the payload. Key distribution and trust roots are deployment policy and out
  of scope; an unsigned attestation is reported `unsigned`, never `verified`.

### 1.5 What C4 costs, stated where a reader cannot miss it

`PRODUCT.md` says elsewhere, and means: **zero writes is not a safety result.** Withholding
write authority from mined constraints removes the product's only path from a table with no
declared schema to an unsupervised repair.

**Measured through the shipped pipeline, that cost is one cell.** On hospital, with all 85 mined
dependencies accepted, `dataforge repair` writes **1** cell before the change and **0** after;
flights and rayyan write 0 in both arms because their miner finds no dependency
(`eval/results/premise_acquisition_write_exposure.json`).

**A correction, because an earlier draft of this section made exactly the error this document
polices.** The figures *451 repairs / 116 corruptions / write precision 0.7954* are widely quoted
in this repository, including by an earlier version of this section, as what a mined premise
does. They are **repairer-proposal figures from `measure_deductive_coverage.py`, which runs no
verifier and no auto-apply gate** -- not pipeline writes. Presenting them as the cost of C4
attributed one instrument's numbers to another, which is the protocol-comparability defect
[docs/trust/baseline-protocol-comparability.md](docs/trust/baseline-protocol-comparability.md)
was written about. The three instruments do not agree, and only the pipeline is what a user runs;
the discrepancy is recorded and left open in
[docs/trust/premise-acquisition-result.md](docs/trust/premise-acquisition-result.md).

So the honest case for C4 is **architectural, not empirical**. It is not that the change prevents
116 corruptions -- measured through the pipeline it prevents none, because the shipped repairer
was already abstaining. It is that authority to write rested on a premise nobody can validate:
across ten externally annotated tables no in-table quality measure transfers, and the best of
four discards 16 of 143 hand-annotated true dependencies when carried to an unseen table. C4
makes a refusal that was previously **incidental** -- a by-product of the repairer's choice rule
-- into one that is **principled and named**. A guarantee that holds by accident is not a
guarantee.

What a user gets by default is detection, a named refusal reason, and a one-line way to
authorise the writes deliberately. What a user loses is one repair on the reference corpus, and a
repair that would have happened without them saying what was true about their data.

Two consequences worth naming rather than discovering:

- The hosted playground's guardrail demo now shows **uniform refusal** rather than a
  proven-vs-blocked split, because its premise is mined. Restoring the split needs a declared
  premise in the scenario, not a flag; the playground deliberately does not pass the opt-in,
  because a demo that writes where the CLI refuses would misrepresent the product.
- `independent_verification` is reported `not_run` where no value is a write candidate. That is
  weaker than the previous `agreed`, and it is correct: claiming agreement at a gate nothing
  reached would be an unearned certificate claim.

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
  coverage. **Read the arm before reading the number:** this is the `oracle` premise, which mines
  its dependencies from the clean frame and is therefore *not available to any user*
  (`docs/trust/bypass-allowlist-evidence.md:146`). Under the mined premise a user actually
  gets, this repairer writes **0 on every corpus tested** -- so the mined-premise exposure
  that cost `fd_violation` 86 cells is **untested for this repairer, not absent**. Quoting
  1.0000 without the arm was the same error as quoting a conditional precision: the
  configuration that produces the number is not the configuration that ships.
- `fd_violation` -- write precision 0.6602 to 1.0000. **The aggregate this line used to
  quote -- "2037 repaired against 700 harmful" -- is not a capability figure and is no
  longer stated as one.** It sums `oracle` and `mined`, which
  `docs/trust/bypass-allowlist-evidence.md:22` records as *alternative premise
  configurations, not additive*: hospital appears in it twice under two mutually
  exclusive premises. It was a criteria-evaluation total that read as a capability total.
  The user-reachable figure is the shipped mined premise on hospital -- **567 writes, 451
  real errors repaired, 116 already-correct cells corrupted, write precision 0.7954** --
  and **0 writes on flights and rayyan**, because the miner finds no dependency there.
  Everything above 0.7954 in that precision range requires the clean frame, which no user
  has.
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
- **A reimplementation of a measurement reproduces the defect the vetted path exists to avoid.** This
  is the generalisation of the rule above, and it was violated within the hour by whoever wrote it. The
  first attempt at `docs/trust/constraint-additivity.md` reimplemented the write loop inline instead of
  importing `_write_exposure`. It omitted the no-change filter, because `_rule_choice`'s docstring says
  it returns values *"before the no-change check"* -- a caveat that is correct, easy to read, and easy
  to not act on. Write counts came out **959 where the truth is 74**, and it nearly published a finding
  that writes are 95% no-ops, which was entirely the bug. It was caught because the number looked
  implausible, not by any gate. The measurement scripts therefore import the shipped path even when a
  local loop would be shorter, and a shorter local loop is now evidence against a measurement rather
  than for it.
- **Premise precision does not predict corruption.** Two of the four added dependencies are equally
  false and corrupted **nothing**, because a false dependency is inert where its determinant group
  holds no visible disagreement. So FD-set precision is the wrong single quantity to optimise, and a
  confidence floor tuned on it is tuned on the wrong axis. What determines harm is whether a false
  premise meets a group that disagrees.

A third rule arrived from the same corner of the codebase, and it is about where a claim lives rather
than how it is built. **A number a user reads is a published claim regardless of the file extension it
lives in.** `readme_truth.py` policed documents and `docs_truth.py` bound document prose, so the
sentence printed at the moment a human authorises unsupervised writes -- the least-guarded claim in the
product, with the most consequence attached -- was bound by nothing, and it went stale in the
reassuring direction within hours of the correct number being published.
It now states **116** and **0.2046**, both artifact-bound;
`tests/unit/test_user_facing_numbers.py` gates the class, not
the instance. No new gate was needed for the binding itself: `docs_truth.py` never cared about the file
extension. The gap was not in the instrument but in the belief about which claims were claims.

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
  DataForge's hospital correction result (0.8352) is measured by its own harness;
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
       -> verify (fail-closed; see below for which verifier runs)
       -> auto-apply gate (provable-only; conformal + drift for LLM values)
       -> hash-chained journal + immutable source snapshot
       -> atomic apply -> byte-for-byte reversible
```

**Which verifier runs is conditional, and this line previously claimed otherwise.**
Until 2026-09-01 the step above read "differential verify (SMT + Direct, fail-closed)"
without qualification, which described only one of the two live paths:

- **Schema present** (and `require_independent_agreement`, the default): `SMTVerifier`
  and `DirectVerifier` both run and are combined fail-closed, so only a fix both accept
  passes. This is the path the sentence described.
- **Schema absent**: `DirectVerifier` is never consulted, and for a `deterministic` fix
  the advisory inferred guard is deliberately not engaged either
  (`dataforge/engine/repair.py:704-714`), so `SMTVerifier.verify` reaches a
  structural-only ACCEPT that checks the row is in bounds and the column exists and
  **examines no value** (`dataforge/verifier/smt.py:670-687`).

The second path is defensible -- a deterministic fix is correct by construction, and
what actually stands behind it is `enforce_constraint_checkable_only`'s detector
allowlist, not value verification -- but it is a *different* guarantee, and the diagram
asserted the stronger one for both. `docs/trust/write-surface-uniformity.md` describes
this same structural-only ACCEPT as a past defect on the agent surface; here it is
reached by design, which is precisely why it has to be stated rather than drawn over.

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
  guarantee, and no distribution-free auto-apply certificate. **Stronger than this file
  used to imply:** the very table this project transcribes its Raha+Baran baseline from
  reports its own system, and one other, *above* DataForge on hospital. So the correct
  summary is that these systems win on accuracy and DataForge does not compete there.
  Figures and provenance: the citation-only table in `BENCHMARK_REPORT.md`, which this file
  deliberately does not restate (see the note at the top on where numbers live).
  **Why none of it is head-to-head, and the one axis that actually differs:**
  [docs/trust/baseline-protocol-comparability.md](docs/trust/baseline-protocol-comparability.md).
  Their premises were supplied by a user or an oracle; ours was mined from the dirty table.
  That is a harder problem and it is where our measured failures live -- so the honest
  reading is not "we are behind", it is that the numbers answer different questions.
- **LLM-based cleaners are now ahead of us too, and one of them corroborates abstention.**
  Cocoon (arXiv:2410.15547) outscores DataForge on hospital. On flights it abstains, arguing
  the benchmark's arrival-time dependency is ambiguous and that preserving the uncertainty is
  preferable -- the same conclusion this project reached about the same dependency, reached
  independently by a system that beats it on accuracy. That convergence is better evidence for
  the abstention thesis than any score of ours, and it is not protocol-comparable either.
- **Conformal / guaranteed automatic repair is prior art, and must be cited as such.**
  Jäger & Biessmann, *From Data Imputation to Data Cleaning* (AISTATS 2024, PMLR v238),
  proposes Conformal Data Cleaning: cell-level automatic identification *and fixing* of
  tabular errors with distribution-free guarantees from conformal prediction. "Automatic
  repair with a statistical guarantee" is therefore occupied ground, and any novelty claim
  here has to name the distinction rather than assume it. The distinction is real: theirs
  is a coverage guarantee over a predictive interval, ours is an SMT-discharged proof that
  a written value satisfies stated constraints, plus reversibility. Those are different
  claims with different failure modes -- a conformal guarantee degrades silently under
  distribution shift, a constraint proof is only as strong as the constraints -- but they
  are not unrelated, and this file implied a gap in the literature that does not exist.
- **LLM "clean my data" tools** are fluent but miscalibrated and unverifiable.
  DataForge does not assert this — it *measured* it, and its LLM corrector
  therefore stays propose-not-apply until measurement earns otherwise.
- **Observability platforms now do gated remediation.** Monte Carlo shipped a Remediation
  Agent (2026-08) that emits a root cause, a confidence level, explicit abstention when
  evidence is thin, and one action from a closed set -- then delegates execution rather
  than performing it. So "nobody attempts automated repair, everyone stops at detection" is
  no longer true and must not be used as positioning. What still does not exist anywhere is
  repair gated by a *proof* rather than by a human confirmation dialog. That is the honest
  wedge, and it is narrower than the one this section previously implied.

DataForge's differentiator is not a higher score; it is that its applied changes
are provable, reversible, and honestly bounded.

**One qualification on "reversible", because the storage layer already provides it.**
Apache Iceberg and Delta Lake give snapshot-based rollback and time travel as a table
primitive, across Spark, Trino, Snowflake, BigQuery, DuckDB and more. Byte-level undo is a
commodity on any table in a lakehouse. What is *not* a commodity is a receipt stating what
was changed, on what premise, verified how, and reversible to which recorded hash -- so the
differentiator is **justified** reversibility, not reversibility. Leading with the latter
invites the correct answer "Iceberg does that."

---

## 10. When DataForge is the wrong tool

Do not use DataForge for streaming data, very large warehouse tables under strict
low-latency SLAs, regulated workflows that require every fix to be human-authored,
or teams already well served by maintained assertion suites. It is currently best
suited to local tabular profiling, provable repair, benchmark research, and
training/evaluation work. Choosing the honest scope is part of the doctrine.

**And one thing it is no longer for at all.** The supervised/RL training subsystem was
excised on 2026-09-07 and now lives, frozen, in [archive/training/](archive/README.md). It
never passed its own gate and never contributed a write: best recorded `sft_f1` 0.0202, with
the v7 candidate proposing nothing on 576 opportunities. This is section 1.3's rule applied to
this repository rather than restated by it -- an audit found that "name the consumer before
hardening" had been satisfied by *disclosure* rather than *reallocation*, with the subsystem
documented as unconsumed and then maintained anyway. The excision deleted **no tests and no
coverage**: every test still runs against the archived package, because three of them are
parity tests over `dataforge/repair_contract.py` and `dataforge/release/model_family.py`, and
removing them would have destroyed product coverage under cover of a cleanup.
`tests/unit/test_archive_excision.py` fails if any product module imports the archive.

---

## 11. Authority and pointers

- Measured numbers, commands, and release status: [README.md](README.md),
  [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md), `docs/evidence/`.
- System design and the safety invariant in depth:
  [ARCHITECTURE.md](ARCHITECTURE.md).
- Trust properties: the accuracy frontier ([docs/trust/accuracy-frontier.md](docs/trust/accuracy-frontier.md)),
  the inferred-guard gap registry ([docs/trust/inferred-guard-gaps.md](docs/trust/inferred-guard-gaps.md)),
  and constraint circularity ([docs/trust/constraint-circularity.md](docs/trust/constraint-circularity.md)).
- External positioning and why no cited number is head-to-head:
  [docs/trust/baseline-protocol-comparability.md](docs/trust/baseline-protocol-comparability.md).
- Decision history and rationale: [DECISIONS.md](DECISIONS.md).
- Product center of gravity (verification-layer-first, staged): [docs/STRATEGY.md](docs/STRATEGY.md).
- Agent-session gotchas and conventions: [CLAUDE.md](CLAUDE.md).
- The external "full original vision" gate: `dataforge release full-vision`
  and [docs/docs/full-vision.md](docs/docs/full-vision.md); the honest,
  dependency-ordered plan to reach it is [docs/ROADMAP_FULL_VISION.md](docs/ROADMAP_FULL_VISION.md).

License: Apache-2.0.
