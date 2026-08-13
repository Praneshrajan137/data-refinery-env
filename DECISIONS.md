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

## 2026-08-11 - The perceptual language had no grammar for quantity, so the product drew nothing

**Context**: asked for 3D visualization, I found the more basic defect first. The frontend renders
**no quantity graphically at all**: no chart library, no canvas, no SVG data graphics, no `style={{}}`
anywhere in the 3,687-line `App.tsx`, and not one CSS custom property ever set from JS. Every number
is text. The three graphics that exist are a CSS `scaleX` rail sweep, a fixed-42%-width loading
sweep, and categorical border styles. The `--df-{data,action,proof}-glow` tokens have zero consumers.

That is not missing craft. `docs/design/perceptual-language.md` is a complete grammar for the *rung*
of a claim and has **no vocabulary for magnitude** — no scale, axis, comparison, distribution, or
aggregation — and its one magnitude token, `confidence`, is deliberately neutralised. A designer
working honestly under it has no sanctioned way to draw a quantity, so the safe move is to draw
nothing. The absence of a law produced the absence of the feature.

Worse, the law it does have is **incomplete under superposition**. Drawing the measured
hospital-with-inferred-FD queue (10,373 flagged cells, 52% of 20,000, per
`eval/results/detector_queue_composition.json`) into an 800x400 region means more marks than pixels.
Under additive blending forty stacked plausibility cells out-glow one proven cell: overplotting
converts density into intensity, and intensity is legally bound to epistemic strength. Nobody writes
a lie; the renderer manufactures one out of arithmetic. A constitution that governs single signals
but not their superposition is enforceable only on an empty screen.

**Alternatives**:
1. *Add a quantitative grammar (L1-L5) as a companion constitution, with a build gate.* Chosen.
2. *Add charts under the existing language, case by case.* Rejected: it is what produced zero charts.
   Each component would re-derive the magnitude question privately and the overplot hole would stay
   open, because it is invisible until someone draws 10,000 marks.
3. *Extend `perceptual-language.md` in place.* Rejected: that document's authority comes from being
   about one thing — is this signal's claim true. Quantity, aggregation, absence and attention cost
   are a second subject with its own falsifiers, and mixing them would dilute both.
4. *Decorative 3D as requested.* Rejected on measurement, not taste. Cleveland & McGill (1984, JASA
   79(387):531-544) rank volume 6th of 7 magnitude channels and position 1st; a 3D chart recruits
   the worst channel and adds foreshortening. Orbit and parallax are independently forbidden three
   times over: WCAG 2.3.3 (AAA) names parallax a vestibular trigger, `audit_motion.mjs` permits
   `infinite` only on `hover`/`resolve`, and camera state cannot be persisted because
   `localStorage` is grep-banned across the playground.

**Decision**: (1). New `docs/design/quantitative-grammar.md` under the perceptual language's
authority, with `src/design/quantitative-tokens.json` as single source and
`scripts/audit_quantitative.mjs` as a build gate. Five laws: **L1** magnitude is position or length
only; **L2** collided marks aggregate by **minimum** rung and may never overstate; **L3** zero,
not-measured and truncated are three different claims and must render differently; **L4** attention
is budgeted (at most one looping progress indicator per view, no physics layouts); **L5** depth
encodes ordinal epistemic strength only — ground contact is the proven cue, orthographic, <=6px,
no camera.

**Reasoning**: the grammar makes drawing quantity legal, bounded and falsifiable, and it makes the
overplot lie *unrepresentable* rather than discouraged: aggregation happens in a pure CPU function
that emits non-overlapping marks, so the GPU never receives two rung-bearing marks in one place.
Earned depth is admitted because it is position (channel 1) rather than volume (channel 6), because
the ladder it encodes is already ordinal, and because it is the most detachable law here — if it
fails validation it is removed and nothing else changes.

**CORRECTION (same day, before implementation)**: the first formulation of L2 took the **maximum**
rung, on the reasoning that a bin should show the strongest claim it contains. That is backwards.
Max-rung promotes a whole bin to its strongest member, so a region of forty unproven cells containing
one proven fix renders with proven form, fill and glow — on the hospital queue a max-rung map would
look dramatically more proven than the run was. That is exactly the overtrust bias L2 exists to
prevent, so the rule contradicted its own motivation. Minimum-rung cannot overstate by construction.
The accepted cost is that min-rung *understates* good news, which is why the `mixed` marker and the
neutral count channel are mandatory rather than optional. Recorded rather than edited away, because
choosing the intuitive aggregate is the failure mode, not the wording.

**CORRECTION 2 (during implementation)**: the plan for this work said to wire a `review_ranker` into
`_analyze_upload` so `review_ranking` would be populated, citing its measured ROC-AUC 0.9796. Three
things were wrong with that. `ReviewRanker` is an **LLM scorer** with no free variant, so firing it
would put a paid call on every playground analysis. Auto-firing it contradicts the 2026-08-04 entry
below, which shipped the triager as an explicit opt-in and recorded the auto-fire gate as a measured
NO-GO. And 0.9796 is an **in-sample, single-dataset** figure that the 2026-08-05 entry records as not
generalising — presenting it as "what to look at first" for an arbitrary uploaded table is exactly
the claim-scope error this project has already made and corrected three times.
What shipped instead: `review_ranking` is surfaced on the receipt view so a CLI or library caller
that DID opt in can render it, the playground supplies no ranker, and the evidence surface orders
cells by severity then detector confidence — free, already computed, and carrying no accuracy claim.

**CORRECTION 3 (during implementation)**: the plan included a calibration/reliability plot. It is not
built, because no calibration, conformal, reliability or ECE data appears anywhere in the playground
API response: those artifacts come from a calibration session over labelled samples, not a stateless
analyse call. Plotting `eval/results/selective_repair_calibration.json` instead would present one
dataset's measurement as if it described the user's run. Its registry entry was **removed** rather
than left in place, because a registry entry for a component that does not exist is the vacuity the
new gate exists to catch. Recorded as an open unknown in the grammar's §9.

**Also fixed**: `/evidence` was never axe-scanned by any test, and had a live `landmark-unique`
violation — two `RiskSummaryPanel` landmarks sharing the label "Risk reasons", indistinguishable to a
screen reader. Adding the scan is what found it.

**Also found and fixed while reading the rung source** (two drifts in the function every trust
surface routes through): `strengthOf` in `observatory.ts` tested membership in
`LLM_PROVENANCE = {llm_live, llm_cache, external}`, but the engine's `_UNTRUSTED_PROVENANCE`
(`repair.py:848`) also contains `entity_consensus`. With `verification_strength` absent, an
entity-consensus value was therefore labelled **proven** in the browser. And `REVIEW_REASON_COPY`
carried 12 of the 13 `ReviewReason` values, missing `unverified_entity_consensus`, while
`dataforge/ui/trust_vocab.py` carries all 13 — so the visual and text twins did not deliver identical
claims, contrary to perceptual-language §9.

**Reviewed with**: nobody yet; the grammar's five falsifiers and the two P1 comprehension tests in
its §9 are the review that matters and are unrun.

**Verification**: `audit_quantitative.mjs` fails closed on a vacuous registry (verified by running it
against an empty `src/viz`), on a non-minimum collision aggregate, on a glow-eligible unproven rung,
on out-of-bound depth, on an undeclared or unreachable absence state, on perspective/orbit/parallax
references, and on colour literals. Encoder tests cover Appendix A cases A.1-A.4 of
`specs/SPEC_quantitative_visualization.md`. hospital F1 unchanged at 0.7926 (this component is
read-only over engine output and introduces no write path).

**Reversal criteria**: L5 is removed if users cannot name a rung from ground contact alone with hue
removed (grammar §9 item 1) — it is deliberately separable. L2 gains a higher-resolution composition
channel, rather than a relaxation, if min-rung aggregation causes users to discount genuine proven
fixes (§9 item 2). The whole grammar is reconsidered if the Evidence Surface measurably fails to
help a user locate proven work in a flooded queue, which is the only reason it exists.

**Residual gap, stated**: a per-step agent replay over the table is not buildable.
`ActionOutcome.resolved_cell` and `.unsat_core` are discarded in `controller.py` before any view
layer sees them. `tax` (200,000 x 15) exceeds every playground cap and has no measured issue count
in any artifact, so its scale is unvisualisable and unmeasured rather than merely unrendered.

**CORRECTION 4 (2026-08-12, after adversarial re-review of the shipped work)**: five of the six
claims above failed verification against the code. L2 and L5 are replaced, `ogl` is removed, and the
visualisation is split into two scales. Recorded in full because each failure is a distinct lesson.

1. **L5 shipped dead.** Depth was unrenderable on every real dataset:
   `heightPx = clamp(rows,180,420)` = 420, `binCount = min(rows, floor(420/3))` = 140, so marks are
   **3.0px** against a `depthLegibleMinPx` of 8. Depth rendered only at **rows <= 22** — the 10-row
   fixture the tests used. The tests passed on a configuration that never occurs in production. This
   is the §4.1 legend-only defect one level up: a law whose channel nobody can see. **A perceptual
   law must state the conditions under which its channel is perceivable, or it is not a law.**
2. **L2 was still solving the wrong problem.** Min-rung cannot overstate, but it was measured to
   *suppress* proof in the dense FD regime (~7 cells/bin, proven co-binned with held and erased),
   while being near-identity when sparse — collisions are per-(column, band). Both min-rung and
   max-rung tried to pick a *representative* of a set, and every representative of a set misreports
   it. Replaced by the **addressability law**: a mark that is not individually addressable may not
   carry a rung. Aggregated marks carry only a neutral count. This deletes the aggregation rule
   rather than choosing a third one, and makes overtrust structurally impossible instead of
   carefully avoided.
3. **Root cause: the three stages were collapsed into one artifact.** Shneiderman, *"The eyes have
   it: A task by data type taxonomy for information visualizations"* (IEEE Visual Languages, 1996)
   prescribes overview first, zoom and filter, then details-on-demand. Forcing one mark to carry
   density *and* rung *and* identity is what made a lie unavoidable.
4. **`ogl` was unjustified by the design that followed it.** Marks are bounded at
   `binCount x columns <= 140 x 128 = 17,920`, typically 2,800, one-shot and non-animated. The
   binning that produced that bound was added *after* the dependency was justified by "instanced
   quads at scale", and the requirement was never re-derived. Worse: WebGL cannot be read back
   without `preserveDrawingBuffer`, so **the renderer chosen was the one that could not be
   pixel-verified**. Removed; both triage records revert to being true.
5. **No test verified a pixel.** 117 tests passed while jsdom yielded the `dom` tier, where `draw()`
   returns null, and ~130 lines of GLSL were never executed by anything. A blank canvas satisfied
   every assertion. Pixel readback via `getImageData` is now a gate — and is only possible because
   the GPU path is gone.
6. **The payload was ~20x oversized.** `FlaggedCellView` at ~350 B/cell x 10,373 cells is ~3.6 MB
   from a 1 MiB upload. Split into a columnar index for the map, bounded detail records, and a
   per-class confidence histogram.
7. **Near-miss, recorded because it is the same error class as CORRECTION 2.** A "confidence vs
   auto-apply threshold" plot was one step from being proposed. `partition_auto_apply`
   (`repair.py:955`) states *"deterministic ones always auto-apply"*: `_DEFAULT_THRESHOLDS` gates
   only `_LLM_PROVENANCE` fixes, and the playground's corrector policy is all-1.01. That plot would
   have visualised a gate that never fires. The confidence panel that ships instead draws **no
   threshold line** and states that confidence is near-degenerate (10,261 of 10,373 cells at exactly
   0.95) and therefore not a ranking signal.

Also corrected: the gate written for the new L2 initially globbed for filenames matching `density`
and caught only the *painter*, which is rung-free because it consumes rung-derived fields — so it
passed while the encoder held 23 rung references. Components now declare their encoder explicitly.
A gate that inspects the wrong file is not a gate. And L4 is now labelled **guidance, not a gate**:
the declared looping count is checked, but nothing measures runtime motion, so calling it a law
overstated the implementation.

**CORRECTION 5 (2026-08-12, from executing CORRECTION 4 rather than describing it)**: six further
defects, every one found by measurement or mutation, none visible by reading the code.

1. **`ogl` deleted on evidence, and the evidence disagreed with the earlier estimate.** Isolated
   measurement of the true worst case (17,920 marks = 140 bands x 128 columns, the real bounds) is
   **best 4.10 ms, median 5.30 ms, worst 9.70 ms** — a whole redraw inside one 16 ms frame, drawn once
   and never animated. The GPU path bought nothing. Frontend runtime dependencies return to five and
   the lockfile is byte-identical to the committed one (226 packages, 226 integrity, 226 resolved).
   Bundle: 131.93 KiB baseline -> 142.59 KiB, so the entire quantitative layer costs **+10.66 KiB**
   against +23.57 KiB with the GPU path.
2. **The perf assertion was measuring the test runner.** The same draw reports 5.30 ms alone and
   **22.20 ms** with six Playwright workers running. The original median-based threshold therefore
   passed at `--workers=2` and failed at the default — a false signal in both directions. Switching to
   best-of-N was not enough, because contention inflates every sample in the window. The measurement
   now runs under its own `playwright.perf.config.ts` with one worker and **zero retries**: a
   measurement that needs a retry to pass is not a measurement. It is excluded from the default suite.
3. **The payload split helps only above its own limit, and slightly hurts below it.** Measured, not
   asserted: at 100 flagged cells it is **0.9x** (every cell appears in both the index and the detail
   set, plus histogram overhead); at 800 cells **1.5x**; at the 10,000-row API cap with 3,000 flagged
   cells **764.8 KiB -> 151.9 KiB = 5.0x**, index at 6.6 B/cell. The earlier "~20x" was an
   extrapolation from a cell count this API cannot reach on a 10-column table. Scale-dependence is now
   stated rather than hidden behind a single flattering ratio.
4. **The confidence histogram is a correctness fix, not an optimisation.** `flagged_cells.cells` is a
   prefix ordered by severity then descending confidence, so computing a distribution from it is
   biased towards the high-severity, high-confidence tail by construction. The population statistics
   must be computed where the population exists. A unit test now pins `totalCells` to the histogram
   population and asserts it is *not* the detail length.
5. **The overtrust guard was tautological and could never fire.** `claimSetViolations` read only
   `rungSpecs[claim.rung]`, so it re-derived the rung from the rung. It now carries `provenance` on
   every claim and re-derives expected strength from `UNTRUSTED_PROVENANCE` independently — the
   mutation that flips an `llm_live` claim to `proven` is killed only because of that independence.
6. **Mutation testing found a hole in the new gate itself, again.** The L2 check looked for the word
   `rung` and for rung CSS token families, so an encoder could assign a rung *name* as a bare string
   (`"proven"`) and pass. Now every rung id is checked by name. The harness
   (`scripts/mutate_quantitative.mjs`, wired into `npm test`) runs 13 mutants against the verifier that
   should catch each one, and routes runtime invariants to the unit suite rather than pretending a
   static audit can see them: **13/13 killed**.

Also fixed, and pre-existing rather than new: a full-width `<canvas>` contributes its **intrinsic
pixel width** as max-content, so an auto-sized grid track grew past the viewport and put 9 px of
horizontal overflow on the evidence page. `min-width: 0` on the canvas cannot fix this, because
max-content contribution is what sizes the track; `grid-template-columns: minmax(0, 1fr)` on the lens
containers can. The latent bug existed before this work and surfaced only once the page grew tall
enough to be measured with the claim panel open.

---

## 2026-08-09 - Scope authority per column: one accepted constraint was granting blanket `proven` status

**Context**: found while documenting the authority-mutation gap, and worse than the defect that
started that work. `authoritative_schema_present` was a table-level boolean summarising per-column
evidence. Verified end to end with a real write to disk: with no declared schema, accepting exactly
ONE inferred `column_type` candidate on column `id` produced an effective schema of `{"id": "int"}`,
which flipped the boolean to `True`, which caused an `external` fix setting the UNRELATED column
`city` to `ZZZ_GARBAGE` to be **applied and stamped `proven`** in the certificate.

| Constraints accepted | Fix on `city` |
| --- | --- |
| none | held (correct) |
| one `column_type` on `id` | applied, labelled `proven` |

Severity, stated carefully this time. It needed no LLM, no agent and no unusual flags -- just
`verify_and_apply`, which ships as a CLI command and an MCP tool, plus one accepted constraint. It had
no calibration backstop, because `external` is not in `_LLM_PROVENANCE` and so auto-applies without
clearing a per-class threshold. And the certificate said `proven`, which makes it a **truthfulness**
violation rather than merely an unproven write -- the failure mode this product exists to prevent.

**Alternatives**:
1. *Scope authority to the columns the schema constrains.* Chosen.
2. *Declared-only authority: a schema assembled purely from accepted inferred constraints grants
   nothing.* Has precedent in `require_declared_fds_for_autoapply`. Rejected as both too strong and
   too weak -- it would discard the legitimate purpose of constraint review, while still granting
   blanket authority to any hand-written schema that happens to omit a column.
3. *Refuse any fix on a column the schema does not mention.* Rejected: strictly larger behavioural
   change than the defect requires, and it converts a labelling bug into a capability loss.

**Decision**: (1). `authoritative_columns(schema)` returns the columns a schema actually constrains --
declared types plus every column named in any constraint, with a functional dependency covering both
its determinant and its dependent. `strength_for_fix` decides per fix, for that fix's own column.
`partition_auto_apply`, `enforce_proven_only`, `_verified_fixes` and `apply_transaction` now take
`covered_columns: frozenset[str]` in place of the boolean, so the type no longer *permits* the table-level
mistake. `PatchPlan` carries `authoritative_columns` so the warehouse primitive makes the same
per-column decision from the plan alone.

**Reasoning**: the boolean was not a bad value, it was the wrong *type* -- a summary that discarded
exactly the distinction the decision needed. Replacing it with the set makes the correct decision the
only expressible one, which is the same move as putting the gate inside the primitives rather than at
each caller.

**Verification**: `tests/unit/test_column_scoped_authority.py` pins both directions -- a fix on an
uncovered column is held, and a fix on the covered column still applies, so this is not a blanket
refusal. Mutation-verified: restoring table-level authority makes the test fail by writing garbage to
`city`. Full suite 1621 passed; hospital F1 unchanged.

**Reversal criteria**: if per-column scoping proves too strict in practice -- users with legitimate
declared schemas that intentionally omit columns -- the fix is NOT to restore the boolean but to let
those users opt in explicitly per column, or to use `allow_unproven_autoapply` which already records
the choice truthfully.

**Residual gap, stated**: covering a column is not the same as constraining its value. `city: str`
covers `city`, so a string there is "proven" against a constraint almost nothing can violate. Column
scoping removes blanket authority; it does not make a weak constraint strong. See
`docs/trust/authority-is-mutable.md`.

---

## 2026-08-09 - Enforce proven-only inside the mutation primitives: the 2026-07-11 invariant held on 2 of 4 write surfaces

**Context**: `DECISIONS.md` 2026-07-11 declared proven-only auto-apply an enforced invariant and
justified it as making "the gaps stay latent" true **under any policy**. Enforcement was implemented
as a call to `_partition_auto_apply` inside `engine/repair.py`. That function had exactly two callers
-- `run_repair_pipeline` and `verify_and_apply`. Two other surfaces reached a mutation primitive
directly and were never gated:

* `agent/controller.py:380` called `apply_transaction(source_path, all_fixes, source_bytes)` with no
  partition. Compounding it, `agent/executor.py:212` called `SMTVerifier.verify(df, [fix], schema)`
  with three positional arguments, omitting `verification_schema`, so with `schema=None` the verifier
  short-circuits to a vacuous ACCEPT (row in bounds, column exists). A schema-less `llm_live` value
  was therefore written to a user's CSV having had *nothing* examine the value.
* `stores/repair.py` fed `propose_repairs` output straight into `store.apply_patch_plan`, which for
  DuckDB is a raw SQL `UPDATE` -- a second mutation primitive that the static `apply_transaction`
  caller allowlist in `test_surface_uniformity.py` structurally cannot see.

Three shipped claims were false as a consequence: `agent/controller.py` ("nothing unverified ever
reaches disk -- regardless of how weak or adversarial the policy is"), `docs/concepts/agent-loop.md`
(the same sentence, plus "cannot bypass the gate"), and the MCP `dataforge_agent_repair` docstring
("the agent can only add proven-safe fixes"). On MCP the *flag defaults* were the unsafe ones:
`confirm_escalations` defaulted to `True` there while every other surface defaulted `False`, and
`schema=None` was hardcoded, so a proven agent fix was not even reachable.

**CORRECTION (2026-08-09, same day).** The paragraph above originally read "On MCP the unsafe path
was the *default*". That overstated reachability and is corrected in place. All three MCP apply tools
are behind `_apply_is_enabled()` (`dataforge-mcp/dataforge_mcp/tools.py:394, 492, 640`), which
requires `--enable-apply` or `DATAFORGE_MCP_ENABLE_APPLY=1`. The *capability* was therefore off by
default; only the flag defaults within it were unsafe. The distinction matters because it is the
difference between "any MCP client could trigger this" and "an operator who had already opted into
apply could trigger this", and I asserted the stronger version without checking. Recorded rather
than edited away, because asserting severity without measuring it is the failure mode, not the
wording.

Round 3 had stamped `verification_strength` truthfully onto the agent receipt, so the certificate was
honest while the write had already landed -- an honest label on an unsafe action.

**Why the suite was green**: `test_no_corruption_invariant.py` claimed to prove the guarantee "for ANY
configuration" but varied only the *corrector policy* on one surface. `test_surface_uniformity.py`
delegated "nothing unverified is auto-applied" to that file. Each guard assumed the other covered the
agent and table-store surfaces; neither did.

**Alternatives**:
1. *Add the partition call to the two missing surfaces.* Smallest diff. Rejected: it re-creates the
   exact failure mode -- safety by convention, where a fifth surface is one forgotten call from the
   same bug.
2. *Document the agent surface as exempt.* Cheapest. Rejected: it contradicts the 2026-07-11 decision
   and would require writing down that the default MCP path may write unproven LLM values, which is a
   product regression in claim strength, not a clarification.
3. *Enforce inside the mutation primitives.* Chosen.
4. *Remove `apply_transaction`/`apply_fixes_to_csv` from the public `engine.__all__`.* Rejected as
   unnecessary once (3) landed, on the grounds that "the raw primitives are now safe by default".

   **CORRECTION (2026-08-09, same day): that rejection was wrong on the facts, and it was the most
   consequential error in this entry.** (3) made `apply_transaction` safe. It did NOT make
   `apply_fixes_to_csv` safe. That function takes `list[CellFix]`, which carries no `provenance`, so
   strength is not merely unchecked there but *undecidable* -- and it writes a user's CSV with no
   journal, no snapshot and no source lock, so a write through it is **irreversible**. Reversibility
   is a stronger invariant in `PRODUCT.md` than the proven-only one this entry closed, and I rejected
   the alternative that would have protected it using a claim I had not verified. `apply_fixes_to_csv`
   is privatized to `_apply_fixes_to_csv` in the follow-up work; see the 2026-08-09 entry on the
   write-primitive registry.

**Decision**: (3). `enforce_proven_only` is called at the top of `apply_transaction`, and
`enforce_plan_proven_only` at the top of `DuckDBStore.apply_patch_plan`. Both default to the safe
behaviour, so a caller that passes neither keyword gets proven-only rather than a silent unproven
write. `PatchPlan` gained `authoritative_schema_present` so the SQL primitive can make the decision
from the plan alone instead of trusting its caller; plans persisted before the field existed
deserialize to the safe `False`. Strength is **computed** from `provenance` in both, never read from
`ProposedFix.verification_strength`, which is stamped late and would make the gate spoofable.
`_verification_strength` and `_partition_auto_apply` were promoted to public names, since a private
name was already being imported across modules.

`AgentRepairRequest` gained `allow_unproven_autoapply` (default `False`), wired to the *already
existing* `--allow-unproven-autoapply` CLI flag and to MCP; held fixes are reported in `held_fixes`
rather than silently dropped. MCP `dataforge_agent_repair` gained `schema_path` and flipped
`confirm_escalations` to `False`.

**Deliberate narrowing**: the agent enforces only the *strength* dimension, not the
calibration-confidence dimension `partition_auto_apply` also applies. Those are separable -- a proven
fix is correct by construction or schema-verified, whereas the per-class thresholds are a
disabled-by-default policy (every committed threshold is the `1.01` sentinel). Routing the agent
through the full partition held *every* LLM fix even with an authoritative schema, which is a much
larger behavioural change than closing the soundness hole. `run_table_store_repair` does use the full
partition, because it is the warehouse analogue of `run_repair_pipeline` and should match it.

**Reasoning**: the primitives are the only chokepoint every surface must pass through, so enforcement
there converts a convention into a structural property: a future surface inherits the gate, and one
that tries to bypass it raises instead of writing. Mutation-tested, not assumed -- neutering the
primitive gate fails `test_write_primitive_refuses_any_unproven_value`; neutering the controller's
strength filter fails `test_agent_surface_holds` **via the primitive raising**, which demonstrates the
two layers are independently effective rather than one being decorative; neutering
`enforce_plan_proven_only` fails two table-store tests.

**Blast radius**: two existing tests depended on the old behaviour, both incidentally --
`test_apply_then_revert_restores_bytes` (about reversibility) and
`test_custom_policy_runs_through_the_gate` (about policy plumbing). Both now supply the schema that
makes their write legitimate. No test anywhere asserted that an unproven fix *must* reach disk.
Hospital F1 held at 0.7926: deterministic fixes are `proven`, so the gate does not touch them.

**Measured severity (added 2026-08-09, replacing an asserted one)**: I originally justified this
work's priority from reachability-in-principle and never measured realized exposure. Measuring it
deflates the claim, so the claim is rewritten rather than defended.

Committed evidence: `eval/results/agent_gpt56sol_hospital.json` -- the verified agent driven by a 2026
frontier model (gpt-5.6-sol, Azure) over the first 150 rows of RAHA hospital dirty.csv,
`authoritative_schema_present: false`, i.e. exactly the configuration in which the bug was reachable:

| Quantity | Value |
| --- | --- |
| `agent_fix_count` | **0** |
| `agent_proven` / `agent_held` | 0 / 0 |
| agent FIX attempts | 2, both `accepted=false` |
| `applied_fix_provenance` | `{deterministic: 6}` |

So on the only committed real-data agent measurement, **the number of unproven writes the bug could
have produced was zero.** Exposure on the agent surface equals the agent's staged-fix count 1:1 (with
no schema, 100% of agent fixes are `plausibility_only` by definition), and that count was 0 because
the frontier model's residual proposals were refused by the safety/verifier gate before staging.

Two further conditions bound it. An unproven agent write additionally required
`--confirm-escalations`, because `NO_UNCONFIRMED_LLM_WRITE` (`safety/constitution.py:109-115`) covers
`llm_live`; and on MCP it required `--enable-apply`. The reproduction command recorded in that
artifact passes neither.

**Honest limits of this measurement**: n=1 run, 150 rows, one dataset, one model, `dry_run` mode. It
bounds realized exposure at zero *for that configuration*; it does not prove exposure was zero for
every user, and no telemetry exists that could.

**Revised characterization**: a real defect of a serious class -- a reachable path by which an
unproven value could be written -- with **no measured realized harm**. The value of this change is
preventing the class, and making three false claims true, not stopping an ongoing corruption. The
original framing ("live, reachable by default", ranked above everything in the trust register) was
not supported by evidence I had, and asserting severity without measuring it is the same error as
asserting scope without checking it.

**Reviewed with**: nobody; found by auditing write surfaces against the 2026-07-11 claim rather than
trusting it. Noting for the record that I then trusted a subagent's *enumeration* of those surfaces,
which turned out to be incomplete -- it missed `_apply_fixes_to_csv`, `revert_transaction` and the
constraints-artifact rewrite. The lesson is symmetrical: do not trust a claim about scope, including
one produced by your own tooling.

**Reversal criteria**: if the proven-only invariant is ever intentionally relaxed for a surface, that
surface must gain an explicit opt-in field and the relaxation must be recorded here -- not achieved by
skipping the gate. If `enforce_proven_only` shows up as a measurable cost on large batches, cache the
strength verdict per provenance rather than removing the check.

---

## 2026-08-05 - Paired cross-dataset run: the triager does not generalise (confirmed on a 2nd model), and feeding it detector evidence is MEASURED HARMFUL

**Context**: Two things were outstanding. (a) The retracted redundancy claim needed replacing with paired
cross-dataset evidence rather than a caveat. (b) `ReviewRanker`'s prompt discarded every detector signal
(`rank()` took only `(row, column)`), so the model re-derived a judgement the pipeline already held - I
hypothesised this was a handicap and added an opt-in `evidence=` parameter to test it.
`scripts/bench/compare_ranker_arms.py`, artifact `eval/results/ranker_arms_cross_dataset.json`, 300 cells per
dataset, default detector regime, same cells, same label, paired, $0.15.

**MODEL CHANGE, stated first because it bounds everything below.** The original Azure resource began
returning HTTP 401 mid-session. The reachable resource
(`praneshrajan15-9819-resource`, `*.cognitiveservices.azure.com`) has one chat deployment: **`gpt-5-mini`**,
not `gpt-5.6-sol`. These numbers are therefore **not comparable** to the earlier gpt-5.6-sol figures, and
`scripts/bench/repoint_azure_env.py` records the switch. Measured cost fell ~50x, to ~$0.0001/call.

**ROOT CAUSE of the model loss, investigated 2026-08-06 so it is not re-investigated.** `gpt-5.6-sol` is a
genuine, currently-offered model on this resource (version `2026-07-09`, `GlobalStandard` SKU listed by
`az cognitiveservices account list-models`). It cannot be deployed because **quota is 0**:

- `az ...deployment create` fails with `InsufficientQuota: quota limit is 0 for gpt-5.6-sol - GlobalStandard`
  (attempted, so this is not quota-API lag);
- quota is **0.0 in all 26 regions checked**, and across **every** SKU - GlobalStandard, DataZoneStandard and
  Provisioned/PTU;
- the subscription is `quotaId: FreeTrial_2014-09-01` with `spendingLimit: On`, and the pattern is decisive:
  **every premium tier is 0** (gpt-5, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6-sol/luna/terra) while only mini/nano tiers
  carry quota (gpt-5-mini 500, gpt-4.1-mini 200, o4-mini 100, gpt-oss-120b 5000). That is a subscription-tier
  policy, not a misconfiguration, so no CLI action unlocks it;
- `az account list --all` shows only this subscription, so the prior account is not reachable to switch back.

**Consequence for reproducibility, recorded rather than hidden**: the gpt-5.6-sol baselines in this document
(hospital 0.946, rayyan 0.955, flights 0.514, and the arm-sweep AUCs 0.554/0.862/0.948) **cannot be
reproduced on this subscription**. Reproducing them requires either converting the subscription off Free
Trial (a billing action) and being granted premium quota, or access to the original subscription. Until then,
treat every gpt-5.6-sol number here as a historical measurement whose artifact is committed but whose rerun
is blocked - which is precisely why the raw `(score, label)` pairs are persisted.


| dataset | n | positives | evidence-free AUC | with-evidence AUC | paired delta CI |
| --- | --- | --- | --- | --- | --- |
| hospital | 300 | 169 | 0.8717 [0.8304, 0.9092] | 0.9043 [0.8683, 0.9372] | [-0.0138, +0.0827] |
| rayyan | 300 | 101 | 0.6562 [0.5975, 0.7116] | **0.2557** [0.2125, 0.2998] | **[-0.4722, -0.3292]** |
| flights | 300 | 283 | 0.5740 [0.4613, 0.7035] | 0.5458 [0.4579, 0.6498] | [-0.1834, +0.1293] |

**Finding 1: the triager does not generalise, now confirmed across two models.** gpt-5-mini gives hospital
0.872, rayyan 0.656, flights 0.574. The retraction stands, and it strengthens: on gpt-5.6-sol rayyan scored
**0.955**, on gpt-5-mini **0.656**. So triage quality depends on **both the dataset and the model**, and
neither dependence is knowable at runtime without labels. flights has only 17 negatives in 300 cells (base
rate 0.943), so its CI spans chance and no AUC claim should be made there - but the base rate already answers
the product question: there is nothing to triage.

**Finding 2: my "information defect" hypothesis is REFUTED, and the fix is actively harmful.** Supplying
detector findings produced no detectable change on hospital (+0.033, CI crosses zero) and **catastrophic
degradation on rayyan: 0.656 -> 0.256**, below chance, with top-decile precision collapsing **0.567 -> 0.067**.
The cause is anchoring: the model inherits the detectors' false positives instead of checking them. rayyan's
detectors are only 33.7% precise, so trusting them is worse than ignoring them - and a prompt that explicitly
warned the findings were low-precision did **not** prevent it.

**This matters for the trust architecture, not just this feature.** The verifier's value comes from being
*independent* of the detector. Feeding it the detector's opinion converts an independent check into an
amplifier of the detector's errors - which would have quietly undermined `independent_verification`. I
proposed this change as an obvious improvement; it is measurably the opposite.

**Decision**: keep `evidence=` as opt-in, **default off**, with the measured harm documented at the call site
and guarded by a test. The evidence-free prompt stays byte-identical so prior caches and measurements remain
valid. Retained rather than deleted so the negative result stays reproducible - deleting it would invite
someone to re-propose the same idea.

**Not done, and why**: no further spend on flights to narrow its AUC. At a 0.943 base rate only ~5% of cells
are negatives, so ~1,900 cells would be needed for ~100 negatives, and the result would not change any
decision - the base rate already says there is nothing to triage.

**Reviewed with**: the maintainer, who re-authenticated Azure on a second account to unblock this run.

## 2026-08-05 - The flooded queue that justifies the LLM triager is self-inflicted: inferred FD constraints, hospital-only

**Context**: The LLM review triager was approved on 2026-07-25 with a conditional rule - "fire the LLM
triager only when the review-queue base precision is low" - on the premise that some datasets simply arrive
with flooded queues (hospital ~3%, rayyan ~1%) and others do not (flights ~72%). Every paid API-phase
experiment (arm sweep, flagship, review-gate probe, triage comparison) then measured on queues built with
`infer_schema(df).to_schema(include_inferred_constraints=True)`. Nobody had measured the queue **without**
inferred constraints. `scripts/bench/measure_detector_precision.py` does, for free, no provider calls;
artifact `eval/results/detector_queue_composition.json`.

**MEASURED (both regimes, all three datasets, full data):**

| dataset | regime | flagged | true errors | precision | recall | cells reviewed per true error |
| --- | --- | --- | --- | --- | --- | --- |
| hospital | default | 549 | 308 | **0.5610** | 0.6051 | **1.78** |
| hospital | inferred constraints | 10,373 | 455 | **0.0439** | 0.8939 | **22.80** |
| flights | either (identical) | 2,929 | 2,773 | 0.9467 | 0.5636 | 1.06 |
| rayyan | either (identical) | 2,336 | 799 | 0.3420 | 0.8428 | 2.92 |

**The finding: queue flooding is a configuration choice, not a dataset property.** Inferred constraints
change **only hospital** - flights and rayyan are byte-identical between regimes. On hospital they convert a
549-cell queue that is 56% real errors into a 10,373-cell queue that is 4.4% real errors. The exchange rate
is brutal and now quantified: **+147 true errors bought with +9,824 false positives, i.e. ~67 extra false
positives per additional real error found**, and review effort degrades **1.78 -> 22.80 cells per error
(12.8x)**.

**Consequence for the paid feature.** The 2026-07-25 activation condition ("queue base precision is low") is
satisfied on hospital *because of a setting the product controls*. Switch inferred constraints off and
hospital moves into the flights regime - already high-precision, nothing to triage. So the LLM triager's
headline result (ROC-AUC 0.946 on hospital, sorting a 95.6%-noise queue) is substantially **a treatment for a
self-inflicted wound**. The cheapest way to improve review is not to buy ranking of spurious flags; it is to
stop emitting them. This is consistent with, and extends, `docs/trust/constraint-circularity.md` and
`require_declared_fds_for_autoapply`: the project already distrusts inferred FDs enough to bar them from
auto-apply, but still lets them flood the human queue 19x.

**Decision**: treat inferred FD constraints as a **recall/precision dial that must be an explicit, costed
choice, not a default**, and re-scope every triage claim to its regime. Do NOT delete inferred constraints -
they genuinely raise recall 0.61 -> 0.89 on hospital, which matters when a missed error is costlier than a
wasted review. Surface the exchange rate at the point of choice.

**Also retracted here**: my own statement that "~95% of flagged cells are not real errors" as a property of
the detector queue. That is true **only** for hospital under inferred constraints. Under the shipped default
the measured precisions are 0.56 / 0.95 / 0.34 - review is already efficient at 1.06-2.92 cells per real
error, which sharply reduces the headroom any triager can claim.

**Ground-truth objection, closed**: these precisions cannot be an artifact of incomplete annotation.
`real_world._compute_ground_truth` labels every cell where `dirty != clean`, so ground truth is **complete by
construction relative to the clean reference**; a flagged cell absent from it matches the reference. Residual
caveat, recorded not hidden: the reference is itself curated, so 12 real false positives per dataset/regime
are sampled into the artifact for inspection.

**Reviewed with**: the maintainer, who authorised full scope and full remaining budget with judgment on
allocation.

## 2026-08-05 - What the paid triager actually buys is TRANSFER, not information: the free ranker is near-perfect in-sample and collapses out-of-sample

**Context**: The API phase compared two *paid* scorers and called the detector's own sort order a "free
baseline". That control was two coarse fields, and on hospital 10,261 of 10,373 cells share
`confidence=0.95`, so its ROC-AUC 0.488 measured a near-constant feature, not the absence of free signal.
`scripts/bench/compare_free_vs_llm_ranker.py` builds the control that was missing: a logistic regression over
per-cell signals the pipeline **already computes and discards** - detector agreement count and `tier` (both
destroyed by the merge in `run_all_detectors`), row/column flag density, value frequency, blankness, length,
digit ratio, severity, confidence, issue type. Evaluated **leave-one-dataset-out**, never in-sample. Free.
Artifact `eval/results/free_vs_llm_ranker.json`.

**MEASURED (full queues, both regimes):**

| regime | dataset | n | positives | free **transfer** | free *in-sample* | old baseline | LLM (reference) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| default | hospital | 549 | 308 | **0.9316** | 0.9986 | 0.7142 | 0.9459 |
| default | flights | 2,929 | 2,773 | 0.4942 | 0.9964 | 0.5906 | 0.514 |
| default | rayyan | 2,336 | 799 | **0.2722** | 0.9999 | 0.1535 | 0.9545 |
| inferred | hospital | 10,373 | 455 | 0.6958 | 0.9976 | 0.6338 | 0.9459 |
| inferred | rayyan | 2,336 | 799 | 0.1836 | 0.9999 | 0.1535 | 0.9545 |

**The finding, and it reframes the whole feature.** In-sample the free features are **near-perfect
(0.9964-0.9999 everywhere, both regimes)**: essentially all the information needed to identify a true error is
already present, free, in signals the pipeline computes and throws away. But **the weights do not transfer** -
the gap is 0.07 on hospital, 0.50 on flights, **0.73 on rayyan**, where transfer lands at 0.2722, i.e.
*anti-correlated*.

**So what the paid LLM buys is not superior information. It is zero-shot transfer.** It reaches 0.946/0.955 on
hospital/rayyan with no dataset-specific fitting, where a free model that is near-perfect *given labels* fails
completely *without* them. That is a precise statement of the paid feature's value, and a different claim from
the one this project has been making.

**Disposition of the pre-registered decision rule.** The rule was: demote the LLM ranker to opt-in if the free
ranker comes within 0.05 on >= 2 of 3 datasets. Mechanically that is satisfied - hospital (-0.014) and flights
(-0.020). **The rule is NOT honoured, because flights is a tie at chance** (0.494 vs 0.514, on a queue that is
94.7% true errors, so there is almost nothing to discriminate). A degenerate tie where both scorers fail is not
evidence the free scorer suffices. Recorded rather than banked.

**Decision**: **keep the paid LLM ranker**; do **not** promote the free ranker to default. The free ranker is
adequate on hospital (0.93), useless on rayyan (0.27), and neither works on flights - and per the refuted
entropy gate there is **no runtime signal identifying which regime a user's table is in**. Defaulting to a
scorer that is anti-correlated on an unknown fraction of real tables is worse than paying. The free ranker's
legitimate uses are as a **tie-break within the existing ordering** and as an **offline diagnostic**.

**Honest limit**: the LLM column is quoted from 2026-07-25 / `review_gate_probe.json`, which used top-200
candidate slices, row caps of 1,500 on flights and rayyan, and inferred constraints. It is **orientation, not a
paired comparison** - which is precisely the one paid step this finding justifies.

**THE PRODUCT METRIC, finally measured (free, natural rate, full queues, default regime).** ROC-AUC is
base-rate invariant, which makes it right for comparing scorers and useless for the only question a user asks -
*if I review N cells, how many real errors do I find?* Effort curves for the free ranker:

| dataset | unranked precision | review top 5% | review top 20% | verdict |
| --- | --- | --- | --- | --- |
| hospital | 0.561 | **1.000** (27/27) | **1.000** (110/110) | ranking is worth a lot |
| flights | 0.947 | 0.973 | 0.891 | nothing to gain; already precise |
| rayyan | 0.342 | **0.248** | **0.332** | **actively harmful** |

**The rayyan row is decisive.** A ranker with transfer AUC 0.27 does not merely fail to help - it makes review
**worse than no ranking at all**, sending reviewers to the wrong cells first (0.248 precision in the top 5%
against a 0.342 unranked base rate). Combined with the absence of any runtime signal telling you which dataset
you are on, that settles it: **the free ranker must never be a silent default.** Conversely hospital shows what
a working ranker is worth - 0.561 -> 1.000 across the top 20%, i.e. reviewing 110 cells finds 110 real errors
instead of ~62.

**Artifact-design lesson, recorded because it cost the recovery it was meant to enable.** The enriched triage
artifact persisted only summary statistics, not raw `(score, label)` pairs, so it **cannot be reweighted** -
the Horvitz-Thompson recovery that enrichment was supposed to permit is impossible from what was saved. This is
the same mistake already fixed in the arm sweep, where `--reanalyse` works precisely because raw pairs are
persisted. Any future paid measurement must persist raw pairs. The product metric above was obtained free from
full-queue scoring instead of by paying again.

**Reviewed with**: the maintainer (full scope, full remaining budget, allocation at my judgment).

## 2026-08-04 - The API phase: account for the money, then attack calibration with a schema-constrained corrector
**Context**: Directive to focus the phase on live paid inference (Azure OpenAI, up to $50). An audit of
the spend layer found the money was entirely unaccounted for, and an audit of the LLM capabilities found
one measured win that no user could reach. Both were fixed before any new experiment was designed.

**Measured facts established before spending (evidence, not intuition)**:
- The product path was unmeterable: `providers.complete()` returned a bare `str`, so
  `dataforge repair --agent` had no cap of any kind. `cumulative_usd` was computed by the bench clients
  and discarded at process exit; the only dollar figure in the entire repo was a prose "~$1.33".
- The 500-call guard was a *call* guard, not a spend guard. Prices existed, a call estimate existed, and
  they were never multiplied.
- The cost guard was triplicated across three clients; `GeminiBenchClient` had none at all.
- **logprobs are unavailable** on the deployment: `"Unsupported parameter: 'logprobs' is not supported
  with this model."` Verified against Microsoft Learn (reasoning models do not support
  `logprobs`/`top_logprobs`/`temperature`) and then confirmed live for $0.0016
  (`eval/results/azure_capability_probe.json`). This closes the obvious calibration lever.
- **`temperature` is rejected** ("Only the default (1) value is supported"). Consequence: the corrector's
  `temperature=0.4` never took effect on Azure. Sample diversity cannot be tuned by temperature, so `k`
  must be measured.
- **Structured Outputs with an `enum` IS accepted and honoured** (`enum_honoured: true`). This is the one
  open lever.

**Two latent defects found in the corrector while designing the lever**:
1. The pool constraint was a prompt request plus a post-filter, so paid samples were discarded after the
   fact and the `votes / total` agreement denominator was polluted by inadmissible samples.
2. The documented `min(agreement, model_confidence)` "monotonic-safety invariant" **never fired**: both
   system prompts ask for "only the value", never JSON, so `_parse_confidence` returned `None` on every
   production call. A promised safety behavior was unreachable code.

**Alternatives**: (a) logprob-based confidence - CLOSED by measurement, not opinion; (b) raise `k` alone -
widens the grid but leaves the pool a soft constraint and the confidence field dead; (c) schema-constrained
enum + required confidence (CHOSEN); (d) do nothing and keep reporting the calibration wall - rejected, the
wall had a mechanical cause nobody had removed.

**Decision**: ship `corrector_structured` (default `False`, so the default path is byte-identical) which
enforces the candidate pool as a hard decode-time `enum` including an explicit `NONE` abstention member,
and requires the model to emit a confidence. Structured mode implies pool-constrained, because a
decode-time enum *is* the pool constraint and two independent definitions of "admissible" would disagree.

**Measured effect (hospital, live gpt-5.6-sol, 90 issues, pre-registered sweep)**:
| Arm | proposals | confidence-grid size | ECE |
| --- | --- | --- | --- |
| free-text k=3 (baseline) | 20 | 3 | 0.700 |
| structured k=3 | 17 | 7 | 0.577 |
| structured k=5 | 17 | 8 | 0.565 |
| structured k=9 | 18 | 11 | 0.536 |

The grid widening is the point: `conformal.certify_threshold` searches only *observed* confidences, so a
3-point grid leaves almost nowhere to place a certifiable threshold - the mechanical reason prior attempts
certified 0.0 coverage. ECE also improves on the historical 0.80-0.96. Directly observed on four cells:
with the schema enforced, three cells that were *already clean* returned `NONE` instead of proposing
corruptions (one had previously proposed `hexrt attxck` for a correct `heart attack` at confidence 0.667),
while the one real error was fixed at confidence 1.0.

**Honesty controls**: the certification attempt is pre-registered
(`eval/preregistration/api_phase_certification.md`) with `alpha`, `delta`, `min_support`, `calib_fraction`,
the split seed, the arm-selection rule, and the primary endpoint all fixed in advance, and with the
arm-selection slice **disjoint** from the certification set. A null is a pre-registered valid outcome.
Amendment 1 records enlarging the sweep because the primary metric was *undefined* (support-limited) on
every arm rather than zero - a precondition of the rule, not a response to any arm's result. Amendment 2
records overriding the tie-break (which had selected the free-text arm) after measurement showed the two
arms are not equivalent. Amendment 3 records the outcome.

**OUTCOME (Amendment 3, corrected 2026-08-05): the primary endpoint is a NULL. The diagnosis is sharper
than before; the earlier reframing of it was wrong and is retracted.**
Not certified. Certification needs >= 59 all-correct accepted samples
(`min_samples_for_certification(0.05, 0.05)`). Every arm returned `precision_below_target` - a measured
refusal, not `insufficient_support`. Two results, measured by **ROC-AUC of confidence against correctness**
(threshold-free, so it cannot be gamed by choosing a favourable cut):
- **Free-text confidence has no usable discriminating signal**: AUC **0.554**, bootstrap 95% CI
  **[0.500, 0.617]** - the lower bound sits exactly at the 0.5 "no information" convention. That is a
  sharper diagnosis than "the model is confidently wrong": the *score* was the problem too.
- **The structured enum creates real ordering**: AUC **0.948**, CI **[0.885, 0.990]**, non-overlapping with
  free-text, same 11 positives, paired on the same issues. k=3 gives 0.862 and k=5 0.879.

**RETRACTED (was in the first version of this entry):** "the binding constraint is no longer calibration
quality but accepted-set sample size", and the citation of **ECE 0.68 -> 0.46** as evidence.
- The sample-size claim is refuted by the precision gradient: top-6 1.000 (CP95 upper error 0.393), top-10
  **0.800**, top-17 **0.647**. The "n=6 at 100%" figure was a *selected extremum* (largest all-correct
  prefix) whose own bound admits up to 39% true error. Precision decays as the slice grows, so more data
  would most likely produce a **firmer NO**. The constraint is still the achievable **precision level**
  (~0.80 top-tier vs a 0.95 bar) - the same conclusion prior work reached for the pool-constrained
  corrector (0.85, propose-only). This work refines the diagnosis; it does **not** overturn the finding.
- ECE is confounded here: it is a weighted mean of `|mean_confidence - accuracy|`, so when accuracy is low
  *any* uniformly-lower score improves it with zero improvement in ordering. Retained as a secondary
  observation only.

**UNPLANNED RESULT, the most useful one**: AUC 0.948 is a *triage* result, and DataForge already ships a
review-queue consumer. Bounding caveat: the AUC is measured only over cells where the corrector chose to
propose (~18% of attempts), so it describes ranking *proposals*, not an unfiltered detector queue.

**FOLLOW-UP (2026-08-05) - the redundancy question, answered on hospital only.** The corrector's 0.948 and the
ranker's 0.946 were **not comparable**: different populations (proposals vs all flagged cells) and different
labels ("was the fix correct" vs "is this cell really an error"). Rerun matched - same cells, same label,
paired (`scripts/bench/compare_triage_scorers.py`, $2.10): `ReviewRanker` **0.958** CI [0.912, 0.996] at
**1 call/cell**; structured corrector **0.979** CI [0.946, 0.998] at **3 calls/cell**; **paired delta CI
[-0.029, +0.074] straddles zero**. No detectable difference - which is a failure to detect a difference, not
a proof of equivalence. Two methodological points that changed the result: (a) the natural base rate is only
~4.5% under **inferred FD constraints** (371 real errors in 8,299 flagged cells; the shipped default detector
path is 56% precise on hospital), so the first run caught **3 positives** and gave
deceptively narrow CIs - the sample was enriched to 40 positives, valid because ROC-AUC is base-rate
invariant, with `precision@k` suppressed since it is not; (b) the corrector abstains on 67% of cells, all
tied at 0.0, so tie handling dominates - `roc_auc` uses average-rank Mann-Whitney (an all-tied input returns
0.5), so the tie block earns no spurious credit. Abstentions are scored, not dropped, since dropping them
restores the survivor bias the experiment exists to remove. **Decision (hospital only): for ranking alone keep the ranker (same power, one third the calls); for ranking
plus a candidate value the corrector alone (3 calls) beats ranker-then-corrector (4 calls).**

**RETRACTED SAME DAY (2026-08-05) - the redundancy claim was generalised from ONE dataset.** The sentence
that stood here ("the features are substantially redundant as rankers, so the choice is a product decision")
is withdrawn as a general claim. The comparison ran on **hospital only**, and the disconfirming evidence was
already committed in this same session in `eval/results/review_gate_probe.json`: LLM ranker ROC-AUC is
0.9459 on hospital and 0.9545 on rayyan but **0.514 - chance - on flights** (queue 1,941). Redundancy holds
only where both scorers work. Corrected claim: **the LLM ranker's value is dataset-dependent and the
dependence is not predictable at runtime.**

Three consequences worth more than the retracted claim:
1. **There was never an honest free control.** The free baseline is the detector's own sort order, which on
   hospital is a near-constant feature - 10,261 of 10,373 cells share `confidence=0.95` (normalised entropy
   0.0263) - so its 0.488 measures a missing *feature*, not missing *signal*. On flights the baseline is
   0.0201, strongly anti-correlated, inverting to ~0.98, i.e. **better than the LLM**. Two paid options were
   compared against each other with no fair cheap control.
2. **No guard test caught it.** The honesty guards added earlier the same day check artifact fields and
   numbers, not claim **scope**. A claim can be arithmetically correct about its sample and false about the
   world. Scope is now guarded explicitly.
3. **The regime problem is the real binding constraint.** Per the probe's own conclusion, baseline
   informativeness depends on whether confidence *correlates with correctness*, which needs ground truth the
   product lacks at runtime. Dispersion was tested as a proxy and **refuted**: rayyan has entropy 0.641 yet a
   chance baseline (0.540) that the LLM beats at 0.955. So on a user's own table there is currently **no known
   way to predict whether paid triage will help**. Conformal exchangeability, free-ranker weights and LLM
   value all rest on this one unsolved problem. Free corollary: **~95% of
flagged cells are not real errors**, which is the strongest argument for having a triager at all and
independently corroborates the `NO_GO` on auto-firing from the review queue.

**Operational failure, recorded not hidden**: the authorised flagship run produced **no data**, never
reaching its first checkpoint. **Corrected cause (the earlier "throttled" was wrong)**: `max_retries=5`
against `DATAFORGE_AZURE_TIMEOUT_S=180`, with `min(2*(attempt+1),120)` backoff, means one hung request
consumes `5*180 + 20 = 920s ~= 15.3 min` - matching the observed silent window exactly. So real billable
calls were ~5, not ~540, and the original ~$2.90 receipt overstated by up to ~100x; it has been reissued.
Corrected root causes: unbounded per-issue retry wall-time, no per-issue progress logging, and a 180s
timeout applied to ~30-token enum-constrained answers.

**Spend accounting, stated honestly**: campaign total **$17.81 = $10.48 measured (9 receipts) + $7.34
reconstructed (3 receipts)**, i.e. **59% measured** (plus 1 no-op receipt from a run whose every request was
rejected). The certification work alone was $13.51 at only 46%
measured; the 2026-08-05 triage follow-up added $4.15 fully measured, and the gpt-5-mini cross-dataset run
$0.15. **41% of recorded spend is still a
reconstruction rather than a measurement** - an uncomfortable result for a phase whose thesis was that spend
must be accountable, and the direct consequence of building the accountability layer *during* the runs it
was meant to measure. Every run after it landed is fully measured. (Earlier versions of this entry said
"$15.20" and "two of five receipts"; the total included a ~$2.90 estimate that assumed throttling at ~540
calls, now superseded by a $1.21 rigorous upper bound derived from the run never reaching index 25, i.e.
fewer than 25x9=225 calls.) The three reconstructions are the evidence for why receipts are now written at
every checkpoint and why `dataforge.spend.ledger_summary` reports measured and estimated separately, so no
future report can present a partly-reconstructed total as fact.

**Reviewed with**: the maintainer, who also ratified that certified auto-apply stays **opt-in** even on
success (authoritative schema **and** an explicitly loaded calibration artifact), who chose to enlarge
the sweep rather than deviate from the selection rule, and who authorised the flagship at k=9 knowing
certification was arithmetically out of reach.

**Reversal criteria**: if a deployment later accepts `logprobs`, revisit continuous logprob confidence as a
better-conditioned signal than self-consistency agreement (guarded by
`tests/unit/test_azure_capability_probe.py`, which fails loudly if the refusal stops holding). If
Structured Outputs support is withdrawn, fall back to prompt-instructed JSON with a strict local validator.

---

## 2026-08-04 - Review triager shipped as an explicit opt-in; the auto-fire gate is a measured NO-GO
**Context**: `llm_review_ranker` measured a decisive win (hospital review-queue precision 5.0% -> 40.7%,
ROC-AUC 0.95 vs the free detector-confidence baseline's 0.49; rayyan ~50x queue-precision lift) yet was
reachable only through `dataforge bench`: not exported, no CLI flag, no tool, no HTTP field. Measured value
no user can obtain is not a capability.

**Attempted**: an automatic firing rule, so the tool spends money only where it helps. The hypothesis was
that a runtime-observable property of the detector-confidence distribution (normalized entropy) predicts
whether the free baseline ranking is informative - low entropy meaning the free ranking cannot discriminate.

**Measured (free, detectors only, `eval/results/review_gate_probe.json`)**:
| dataset | entropy | gate fires? | LLM actually helps? | correct? |
| --- | --- | --- | --- | --- |
| hospital | 0.026 | yes | yes | yes |
| flights | 0.604 | no | no (lift 0.84) | yes |
| rayyan | 0.641 | no | **yes (lift 50x)** | **NO** |

**Decision**: ship the triager as an explicit opt-in on the Python API (`dataforge.ReviewRanker`), the CLI
(`dataforge repair --review-rank`), and MCP (`dataforge_review_rank`). Do **not** ship the auto-fire gate.

**Reasoning**: rayyan has a well-spread confidence distribution *and* a chance-level baseline that the LLM
beats decisively. Dispersion therefore does not imply baseline informativeness - that property depends on
whether confidence *correlates with correctness*, which requires ground truth the product does not have at
runtime. Shipping the gate would silently withhold a ~50x lift on rayyan-like data, recreating exactly the
"measured value nobody can reach" failure being fixed. A plausible-looking heuristic that is wrong a third
of the time is worse than an honest user choice.

**Safety**: the ranker is passed as a separate argument to `run_repair_pipeline`, never as a field on
`RepairPipelineRequest`, so there is no path from a triage score to a mutation. `RankedCellResult` carries
no candidate value by design. Locked by `tests/unit/test_review_triage_surface.py`.

**Reversal criteria**: a *free* runtime signal that predicts baseline informativeness on all three datasets
(including rayyan) would justify revisiting automatic firing.

---

## 2026-07-27 - Candidate-constrained correction: deterministic NO-GO, LLM select-from-pool GO (propose-only)
**Context**: Asked to elevate hospital candidate-constrained correction to the highest standard. First-
principles re-derivation + a measure-first Phase 0 gate on hospital (the only categorical-heavy RAHA set:
beers is out of the registry, rayyan clean-in-pool 1.4%, flights = time values, tax 97.9% numeric).
**Measured (hospital, 509 errors; existing FD path fixes 451, misses 58)**:
- Deterministic nearest-valid-in-pool corrector (additive to FD): fixes 23 but CORRUPTS 25 correct cells
  (precision 0.47). The corruptions are rare-but-correct values that edit-distance cannot tell apart from
  typos. Same over-correction that got format/categorical repairers withheld. NO-GO.
- Precision-guard re-enable of the withheld repairers: 60 proposals, 0 correct -> pool-constraining them
  yields 0 net benefit. NO-GO.
- Provable-membership promotion: 99.78% of correct fixes are high-support members, but those are FD-
  deterministic and ALREADY proven/auto-applying -> promotion unlocks nothing on hospital. Redundant.
- LLM SELECT-FROM-POOL (live gpt-5.6-sol, 58 FD-missed cells): recovered 29, wrong 5, abstained (NONE) 24
  -> recall 0.50, PRECISION 0.853; false-selection on correct cells 7.5%. The candidate-pool CONSTRAINT
  lifts LLM correction precision from ~0.08-0.16 (free-text) to 0.85 - a 5-10x gain.
**Alternatives**: (a) ship the deterministic corrector - rejected (corrupts); (b) auto-apply the LLM
select-from-pool - rejected (0.85 << the 0.95 promotion bar, 7.5% false-select, the ECE/conformal wall
stands); (c) propose-only pool-constrained LLM corrector (CHOSEN); (d) document-only - rejected, the lever
is real.
**Decision**: Add a flag-gated, propose-only `pool_constrained` mode to `LLMCorrectorRepairer`: it builds
the column's support-graded frequent-value pool, injects it into the prompt, and enforces membership in
`_candidate_ok` (non-members and NONE are rejected -> abstain). Threaded through build_repairers ->
propose_fixes/propose_repairs -> RepairPipelineRequest and exposed as `dataforge repair
--corrector-pool-constrained` (requires --allow-llm). Corrector output stays plausibility_only and is held
for review by default (never auto-applied), so this only improves proposal quality. Default OFF -> the
free-text corrector path is byte-identical.
**Reasoning**: This is the one lever the measurement validated. It turns the uncalibratable free-text
corrector (8-16% precision) into a usable 85%-precision proposer that recovers half of hospital's FD
residual and abstains when unsure - the same human-in-the-loop role the frontier LLM earns elsewhere
(review-ranker), never auto-apply. The deterministic and promotion levers were honest NO-GOs, documented
rather than shipped.
**Reviewed with**: Pranesh K R (funded the live probe; chose to productize the propose-only lever).
**Reversal criteria**: if a broader dataset shows the pool constraint degrades recall without the precision
gain, or if confidence can be calibrated enough for conformal to certify auto-apply (needs ~59 flawless
samples at 0.95/0.95), revisit. Scope caveat: hospital is the only applicable benchmark dataset; the
mechanism generalizes to any categorical-heavy table but is unmeasured beyond hospital.

---

## 2026-07-26 - Cross-row entity consensus: fixing flights from 0.0, provably-gated
**Context**: The product must not merely be honest about what it cannot fix - it must actually
fix the user's data to the highest level, or no one adopts it. Measured reality: hospital
correction F1 was 0.7926 (beats cited SOTA), but flights correction F1 was 0.0000 - it detected
all 2370 missing values and filled none. The residual RAHA errors are semantic, and the LLM
free-text corrector is uncalibratable (precision 8-16%). But SOTA (Raha+Baran, flights F1 0.729)
uses a lever DataForge never built: cross-row consensus. Flights is multi-source (100 flights x
~24 source rows each, zero singletons; cols tuple_id,src,flight,4x time), so the correct value for
a cell already exists in its sibling rows - a candidate-constrained, evidence-grounded fix.
**Alternatives**:
- (a) Free-text LLM correction: measured uncalibratable (never certifiable); rejected as the fix path.
- (b) Entity consensus as PROVEN -> auto-apply by default: rejected - a wrong majority yields a wrong
  consensus, so it is not proof; auto-applying by default would break the proven-only corruption
  invariant (the corruption oracle) and could corrupt correct minority values.
- (c) Entity consensus as plausibility_only, held by default, opt-in auto-apply (CHOSEN).
- (d) Always-register the repairer: rejected - scoring its proposals in the default heuristic path
  added +1 false positive on hospital (0.7926 -> 0.7919), unacceptable for the locked anchor.
**Decision**: New `EntityConsensusDetector` (precision-controlled key discovery) + `EntityConsensusRepairer`,
gated behind `allow_entity_consensus` (default OFF -> baseline byte-identical). The consensus value is
classified `plausibility_only`: held as a pre-filled one-click review suggestion by default, auto-applied
only under the explicit `allow_unproven_autoapply` opt-in (or when a declared schema proves it), honestly
recorded as not-proven in the certificate, and fully reversible. The precision crux is a consensus-value
DIVERSITY guard: a support bar alone cannot separate a true key->attribute (flight -> its own time, each
entity a distinct value, diversity ~1.0) from a categorical CORRELATION (rayyan issue -> "mostly English",
tiny shared vocabulary) whose differing cells are correct minorities. Without it, rayyan wrongly flagged
322 correct cells (precision 0.0); with it, rayyan and tax abstain.
**Measured (flag ON, proposals scored)**: flights correction F1 **0.0000 -> 0.4467** (P 0.841, R 0.304,
1496 correct fixes) - fully automatic, from fixing nothing; SOTA Raha+Baran is 0.729 but semi-supervised.
hospital 0.7919 (opt-in only, +1 fp); rayyan/tax abstain. Baseline (flag OFF): hospital F1 0.7926
tp451/fp178/fn58 EXACT, flights 0.0 - byte-identical.
**Reasoning**: This raises the real fix rate on the worst dataset by the largest possible margin while
preserving the never-corrupt-by-default guarantee: the default engine still auto-applies only proven
fixes (corruption oracle intact), and the huge practical win (1496 pre-filled 89%-correct one-click
suggestions, or full auto-apply under an informed opt-in) is delivered honestly. It reuses the existing
inferred-FD / verify_and_apply plausibility machinery rather than inventing a new proof class.
**Reviewed with**: Pranesh K R (directed "fix to the highest level, user-centric"; chose Phase 3 +
aggressive opt-in auto-apply, never default).
**Reversal criteria**: if the diversity/governance guards prove too permissive on a new dataset (spurious
auto-applies under opt-in) or too strict (misses a genuine multi-source key), retune the guards; if a
dataset needs higher recall, the sub-0.95-support propose tier already surfaces it for review. The locked
hospital anchor (0.7926) is the regression tripwire.

---

## 2026-07-25 - LLM review-queue ranker: GO, but conditional on a flooded queue
**Context**: The detector triage finding (gpt-5.6-sol lifts hospital review-queue precision
5%->41% @ 96% recall) was productized as a review-queue RANKER (sort likely-true-first, never drop,
never auto-apply). Built measurement-first: `dataforge/bench/ranking_metrics.py` (pure precision@k /
recall@k / ROC-AUC / queue_precision_lift, 12 tests), `dataforge/review/ranker.py` (ReviewRanker - a
pure scorer that never mutates; cache + self-consistency + injected completion_fn, 4 tests), and a
`llm_review_ranker` bench method that scores the LLM ordering AGAINST the FREE detector-confidence
baseline over the SAME top-M candidates. The honest question: does the LLM beat the free baseline
(detector severity/confidence order), or is the lift already free?
**Live result (top-200/dataset, single seed, $15-guarded; artifacts eval/results/llm_review_ranker_*.json)**:
- hospital: LLM ROC-AUC **0.946** vs free-baseline **0.488** (near-random); R-precision 0.333 vs 0.0.
- rayyan:   LLM ROC-AUC **0.955** vs baseline **0.540**; R-precision 0.5 vs 0.0 (thin: 2 true in top-200).
- flights:  LLM ROC-AUC **0.514** vs baseline **0.020**; queue already 71.5% true in top-200 -> nothing
  to triage; LLM adds no lift at the operating point.
**Decision**: GO (2/3 clear the gate), with a CONDITIONAL product rule: **fire the LLM triager only
when the review-queue base precision is low** (hospital ~3%, rayyan ~1% -> big lift; flights ~72% ->
skip). The free detector order is NOT a good ranker where the queue floods (AUC ~0.5), so the LLM's
lift is real and not already free; but where detectors are already high-precision there is nothing to
rank. The gating signal (queue base precision) is measurable for free per run.
**Reasoning**: This is "build what matters": a real, measured capability with a cheaply-measurable
activation condition, that never touches the auto-apply gate (the ranker is a pure scorer; a human
disposes). It also correctly refuses to spend LLM credit where it adds nothing (flights).
**Reviewed with**: Pranesh K R (asked to build the ranker end to end, accepted ongoing credit).
**Built (Phase 2 core, opt-in)**: `RepairReceipt.review_ranking: list[ReviewRankedCell]` (default
empty) + a `review_ranker` opt-in kwarg on `run_repair_pipeline` (bounded by `review_ranker_max_cells`,
default 200). When no ranker is supplied the receipt field is empty and behavior is byte-identical; when
supplied it attaches a presentation-only human-review ordering that NEVER enters the verified apply path.
Reliability: `_is_retryable_provider_error` now retries transient server errors (500/502/503/504), not
just 429/503 - a stray 500 had aborted a long paid run. Playground UI surface deferred by choice.
**Reversal criteria**: if a larger-positive re-measure (hospital/rayyan with more true errors in the
candidate set) drops the ROC-AUC lift below ~+0.15, or if the free detector-confidence order can be
made competitive by better severity calibration, prefer the free ranker and drop the LLM cost.
Honest caveat: rayyan positives are thin (n=2); hospital (n=6 here, n=25 in the prior natural-sample
confirmation) is the robust anchor.

---

## 2026-07-25 - gpt-5.6-sol as a review-queue triager (found role): A NO-GO, B confirmed GO
**Context**: Every prior LLM attempt (corrector, agent, teacher, grounded-rationale) targeted
CORRECTION / auto-apply, where the verified gate rejects LLMs by design. A first-principles
re-derivation asked the question none of them did: the project's own thesis is "detection is the easy
half; maximize detection, refuse to guess corrections" (DECISIONS 2026-06-30), it scores detection
independently (`ClassScore.detection_recall`), and it has a human-review path (`suggested_fixes`) and a
defined DETECTABLE-ONLY class (`docs/trust/accuracy-frontier.md`). Yet no LLM had ever been evaluated
as a DETECTOR / review-ranker (Verified). Step 1 (offline) profiled the deterministic ensemble's two
failure modes: flights = high precision, low recall (misses 2424 errors, 49%); hospital = high recall
(89%) but ~4% detection precision (flags 10372 cells to surface 455 real errors).
**Alternatives measured (bounded, USD-guarded live probes; `scripts/bench/probe_llm_detector.py`)**:
- (A) flights recall-booster - does the LLM flag the errors detectors MISS? NO-GO: on the residual,
  LLM recall 4.7% at 82% precision (50 of 55 true flags were already-detected cells). It re-flags known
  errors; it does not find the missed ones. Artifact `eval/results/llm_detector_probe.json`.
- (B) hospital precision-filter - can the LLM triage a flagged cell as truly-wrong? Probe (balanced
  150): precision 89%, recall 97%. CONFIRMED on the natural distribution (uniform 500 flagged cells,
  no reweighting): baseline queue precision 5.0% -> post-filter 40.7% (95% CI [29.1, 53.4]); recall
  retained 96% (95% CI [80.5, 99.3]). Artifact `eval/results/llm_detector_confirm.json`.
**Decision**: A is a NO-GO. B is a confirmed GO as a REVIEW-QUEUE TRIAGER (not an error-finder, not a
corrector): gpt-5.6-sol lifts hospital review-queue precision ~8x (5% -> 41%) while keeping ~96% of real
errors. Documented; product build deferred to a dedicated follow-up (recommended integration: a top-N
review-queue RANKER that sorts likely-true-first and never drops, feeding `suggested_fixes` only, never
auto-apply - preserves 100% recall and bounds runtime credit).
**Reasoning**: This is the one frontier-model role that fits the project's own thesis and was never
tested. It stays inside the guardrail model (LLM proposes suspicion; human/verifier disposes) and never
touches the auto-apply gate or the 0.7926 correction floor. It corrects the over-scoped prior claim
"a stronger LLM adds ~0": true for correction/auto-apply, FALSE for review-queue triage.
**Reviewed with**: Pranesh K R (chose: probe both A and B; confirm-first on B; document and defer build).
**Reversal criteria**: if the triager's precision lift does not generalize beyond hospital (e.g. on
rayyan/flights review queues) or its runtime credit/latency at top-N is uneconomic, keep it a
measured capability and do not ship. Follow-up reliability note: Azure HTTP 500 is not retried (only
429/503 + timeouts are) - a transient 500 aborted one probe run; consider extending the retry set.

---

## 2026-07-25 - gpt-5.6-sol teacher: reasoning-preserving SFT is a NO-GO (spurious grounding)
**Context**: The "unlock teacher -> training" phase first tried a grounded-rationale SFT track and,
on adversarial re-review, was retracted. The honest chain: (1) the v9 action envelope distils every
completion to a reason-free `(row, column, new_value)` action (`completion_reason_text` is a validator
blocker), and teacher repairs are kept only when GT-verified, so they are a strict SUBSET of what
`build_oracle_sft_trajectories.py` already mints for free from clean/dirty diffs -> feeding them into
that objective adds no new signal. (2) The one distinctive teacher contribution is its rationale, so
the track proposed to supervise the rationale ONLY when the repair is FD-grounded. (3) DECISIVE: the
grounding gate I wrote (`fd_grounding_determinant`) was a hand-rolled single-column window-unanimity
check with NONE of the project's anti-spurious guards. Re-measured against the single source of truth
(`dataforge.verifier.inferred.fd_consensus_violation` over `infer_verification_schema`, which enforces
near-key rejection at 0.9, `_MIN_FD_SUPPORT_GROUPS=2`, confidence >=0.9, full-table scope): only
**3/43 (7%)** and **29/216 (13%)** of teacher repairs are robustly grounded, versus **84% / 79%** by
the naive check - a 6-11x over-count. Of the cells that DO have a robust unanimous group, most
(15/18 smoke, 77/106 full) DISAGREE with the teacher's value: the teacher followed local-window
coincidence, not a robust FD. Reproducible: `scripts/data/measure_teacher_grounding.py`.
**Alternatives**:
- (A) Ship grounded-rationale SFT gated on the naive window check. REJECTED - it distils exactly the
  coincidental low-cardinality FDs (`f_name->gender`) that DECISIONS 2026-07-19 ruled
  in-table-indistinguishable from genuine ones (`zip->city`) and refused to mine; a parallel unguarded
  FD notion also violates the `inferred.py` single-source-of-truth invariant. Also moot in production:
  the strict v3 decoder rejects a `rationale` key, so a rationale-trained model cannot even emit it.
- (B) Salvage by gating on the guarded FD check. REJECTED - the robust set (7-13%) overlaps the cells
  the deterministic FD repairer already handles, leaving ~0 marginal repairs to supervise.
- (C) Retract. Keep only the honest negative result plus the verified-abstention adapter.
**Decision**: C. Removed `fd_grounding_determinant` + `render_grounded_rationale_completion` +
`parse_repair_rationales` from `dataforge/repair_contract.py`, deleted
`scripts/data/build_grounded_rationale_curriculum.py` and its two test files. Kept
`scripts/data/promote_expert_v1_to_v4.py`, reframed as a producer of *verified-abstention
hard-negative* v4 records (uses the real `inferability_for_record`; on this data everything abstains).
Committed `scripts/data/measure_teacher_grounding.py` as the reproducible evidence.
**Reasoning**: FD semantics must live only in `inferred.py`; a second, unguarded notion is precisely
the spurious-FD trap the project already closed. This is the fourth NO-GO with the same root cause
(DECISIONS 1282-1293): the RAHA residual is semantic, and in-table local signal for it is
indistinguishable from coincidence. gpt-5.6-sol adds no safely-verifiable repair signal; its durable
role is the legible-guardrail demo (see the Phase C playground proposer), not teacher distillation.
**Reviewed with**: Pranesh K R (asked to transcend the prior draft; the adversarial re-review surfaced
that the earlier "84% grounded" was spurious).
**Reversal criteria**: a grounding notion that PROVABLY separates genuine from coincidental local FDs
on held-out data (not a threshold overfit to two datasets) would reopen reasoning distillation. Until
then, the deterministic floor + declared/external knowledge are the only paths to more certified
coverage.

---

## 2026-07-25 - Azure API reliability: retry transient timeouts, honour the timeout env
**Context**: Long paid teacher/corrector runs died mid-flight on a single slow chunk.
`AzureBenchClient._post` (and the sibling clients) re-raised `httpx.TimeoutException` with no retry
(only 429/503 retried), and the agent-path `_complete_azure` hardcoded a 60s timeout, ignoring
`DATAFORGE_AZURE_TIMEOUT_S` (which the bench client already honoured).
**Alternatives**:
- (A) Leave it; run smaller batches. REJECTED - fragile; a transient timeout still aborts the run.
- (B) Retry timeouts with bounded backoff (mirroring the 429/503 path) and honour the timeout env on
  both paths.
**Decision**: B. All four paid bench clients retry `httpx.TimeoutException` with bounded backoff
capped at `max_retry_after_s`, raising only after the retry budget is exhausted; `_complete_azure`
now reads `DATAFORGE_AZURE_TIMEOUT_S` via `_azure_timeout_s()`.
**Reasoning**: A transient network timeout is retryable exactly like a rate limit; a single slow
reasoning chunk should not waste a run. Behaviour is unchanged when timeouts do not occur.
**Reviewed with**: Pranesh K R (chose end-to-end A->B->C).
**Reversal criteria**: none expected; if a provider makes timeouts non-idempotent, gate the retry per
provider.

---

## 2026-07-24 - gpt-5.6-sol: correct the corrector verdict; leverage is teacher/agent, not auto-apply
**Context**: An earlier pass onboarded gpt-5.6-sol (first-party Azure OpenAI) and reported its
LLM-corrector result as "F1 0.0038 -> REJECT, moat confirmed." Re-examination showed that verdict
cited INVALID evidence: F1 is a denominator artifact (30 sampled corrections scored against
full-class support 423/77 caps recall ~0.06 even for a perfect corrector). The real signal, read
from calibration_samples_by_class: the corrector makes ~63 proposals at ~5% accuracy and is
CONFIDENTLY WRONG (mean confidence 0.89 on wrong answers), so conformal certifies ZERO auto-apply
coverage - and a 5-issue raw-sample probe confirmed the prompt is clean and grounded; the residual
fd_violation cells are genuinely underivable. Benchmarking any LLM at the auto-apply gate answers a
predetermined question (the gate rejects all LLMs by design).
**Alternatives**:
- (A) Keep the F1-based corrector verdict. REJECTED - invalid metric, misattributes the cause.
- (B) Re-run the corrector at larger N to "get a real F1". REJECTED - spends credit to reconfirm the
  moat; still the wrong surface.
- (C) Correct the record with the project's own distribution-free certified-coverage metric, and
  apply gpt-5.6-sol where a frontier model actually helps: as an agent policy the gate vets, and as
  an SFT/GRPO teacher (verified against ground truth, gate-exempt).
**Decision**: C. Corrected artifact eval/results/corrector_gpt56sol_certified_coverage.json
(supersedes the F1 read). Measured leverage: (1) agent-through-gate on hospital -> floor 6 proven,
agent_fix_count 0, 2/2 FIX rejected by SMT (eval/results/agent_gpt56sol_hospital.json) - the
guardrail holds a frontier proposer, exactly the STRATEGY thesis; (2) teacher -> 32 F1=1.000 verified
trajectories in ~97s (data/sft_traj/expert_v1_gpt56sol.jsonl) - the honest frontier-model win.
**Reasoning**: Consistent across corrector + agent: a stronger model does not clear the
verified+calibrated gate (calibration on hard residual, not capability, is the bottleneck). The
model's value is upstream (teacher data) and as a vetted proposer, not silent auto-apply.
**Reviewed with**: Pranesh K R (chose: keep azure default with cost guards; generate teacher data
now, train later).
**Reversal criteria**: a model whose residual proposals are calibrated enough that conformal
certifies non-zero auto-apply coverage at alpha<=0.05 on a held-out split - then revisit auto-apply.
Known blocker for training on fresh teacher data: the v1->v4 curriculum transforms are out-of-repo
(Kaggle); build_repair_curriculum requires expert_v4 + CONTRACT_VERSION_V2.

---

## 2026-07-24 - Earned-Salience perceptual language (color + motion + agent-state + text twin)
**Context**: The playground/CLI/MCP grew a capable but incoherent perceptual layer:
10 of 12 agent states were legend-only (never rendered live), `independent_verification`
was footer text with no perceptual weight, proven vs plausibility differed only by hue,
glow tokens were latent, motion timing was duplicated across TS and CSS, `confidence-high`
reused proof-green, and the CLI/MCP leaked raw machine tokens (`floor_cannot_verify: 3`)
with no humanizer and no `NO_COLOR` handling. For a product whose core property is trust
calibration and whose stated deadly failure is overtrust, an interface that can make an
unproven value look proven is a correctness bug, not a cosmetic one.
**Alternatives**:
- (A) "Refined palette": retune hues/motion for polish. REJECTED — cosmetic; leaves every
  honesty seam intact; still lets confidence-green sit on unproven values.
- (B) "Activity theater": richer thinking/streaming animation to feel alive. REJECTED —
  directly manufactures the overtrust the doctrine names as the deadly failure.
- (C) "Earned Salience": perceptual intensity (chroma, motion amplitude, glow, weight,
  form-completeness) is a strictly monotonic function of epistemic strength, so an unproven
  value physically cannot wear the treatment reserved for proof.
**Decision**: C. Codified in [docs/design/perceptual-language.md](docs/design/perceptual-language.md):
an epistemic-strength ladder (proven / corroborated / plausibility-only / held / downgraded /
rejected / idle), color=semantics, motion=causality (settle/hover/resolve/pause/recoil/still/
downgrade), stillness=punctuation, a redundancy law (no meaning by color alone), and a
non-visual CLI/MCP/API text twin. Opinionated semantic breaks (user-approved): confidence
recolored to a neutral magnitude (never a verdict color); the 10 dead agent states retired;
glow reserved for proven/command.
**Reasoning**: It makes the product's central correctness property the literal grammar of
perception and converts every seam into a derivation. Overtrust becomes unrenderable rather
than merely discouraged.
**Reviewed with**: Pranesh K R (approved full phased implementation, playground + CLI/MCP
twin, and freedom to retire dead/dishonest signals).
**Reversal criteria**: user comprehension testing shows the language is not predictable, or
`hover`/dashed plausibility reads as "broken/loading" rather than "uncommitted". Tracked as
prioritized validation work in the spec (§10).

---

## 2026-07-19 - Accepted-inferred FD contract: informed review by default, declared-FD-only opt-in
**Context**: Measuring sampled tax exposed that FD mining surfaces spurious FDs.
The near-key + minimum-support guards fixed the vacuous cases, but tax FPs only
fell 708 -> 696: the residual are low-cardinality coincidental approximate FDs
(e.g. f_name -> gender) that are IN-TABLE INDISTINGUISHABLE from genuine approximate
FDs (hospital zip -> city) - both hold ~0.9-1.0 with violations; one set is errors,
the other legitimate variation. The product default is already safe (inferred FDs
are pending-until-reviewed; the corruption oracle now proves it). The residual
surface is a user ACCEPTING a coincidental inferred FD, after which its majority
repair auto-applies and overwrites legitimate variation.
**Alternatives**:
- (A) Informed review only: rely on the mining guards + enriched candidate evidence
  (support, informativeness, approximate-FD warning). Acceptance = the user's
  authoritative declaration. Simplest; no fix-path change.
- (B) Provenance-weighted: a correction backed only by an accepted-INFERRED FD is
  never auto-applied. Strongest; adds a fix-path distinction and review friction.
- (C) Tune the confidence threshold to separate hospital from tax - REJECTED: the
  two are in-table indistinguishable, so any separating threshold is overfitting to
  two datasets and violates the honesty doctrine.
**Decision**: A as the default, plus B behind an explicit opt-in
`RepairPipelineRequest.require_declared_fds_for_autoapply` (default False). Under
the flag, an fd_violation correction auto-applies only when its dependent column is
covered by a HAND-DECLARED FD (from the schema), not merely an accepted-inferred
one; otherwise it is held with review_reason `inferred_fd_not_declared`. Declared
FDs and all non-FD corrections are unaffected; default behavior is byte-identical.
**Reasoning**: You cannot mine your way out of coincidental approximate FDs, so the
defense must be architectural. A keeps the low-friction path for everyday use with
honest, informed review; B gives strict/regulated deployments a switch to demand
hand-declared FDs for any auto-applied FD repair. C was rejected as dishonest
overfitting.
**Reviewed with**: user (chose "A now + B as a flag").
**Reversal criteria**: if evidence shows users still accept coincidental FDs under
A, make B (or a confidence-gated variant) the default; if the flag proves unused,
retire it.

---

## 2026-07-18 - Canonize the product constitution (PRODUCT.md) as single source of truth

**Context**: Purpose, philosophy, vision, and mission were scattered and partially
restated across README.md, ARCHITECTURE.md, CLAUDE.md, META_CONTEXT.md,
CURSOR_MASTER.md, and docs/. Drift between them made it unclear which statement of
intent was authoritative, and made honest-claim wording harder to enforce
consistently.
**Alternatives**:
- (a) Leave intent distributed across existing docs - status quo; keeps drifting.
- (b) Put the thesis in README - README is already large and is a
  `readme_truth` truth-doc, so philosophical prose competes with claim-boundary
  checks and command truth.
- (c) A dedicated canonical `PRODUCT.md` constitution that other docs defer to via
  short cross-links, and that states principle only (no numbers/claims that belong
  to README/evidence).
**Decision**: (c). Added `PRODUCT.md` (thesis, purpose, philosophy, first
principles, honesty doctrine, vision, mission, safety invariant, positioning,
wrong-tool scope). README, ARCHITECTURE, and CLAUDE now point to it; PRODUCT.md is
authoritative on *why*, README/BENCHMARK_REPORT/evidence on *numbers*, ARCHITECTURE
on *how*.
**Reasoning**: A single, claim-free constitution ends intent drift without touching
the measured-claim surface. Keeping numbers out of it means it never competes with
`readme_truth`/`benchmark_truth`, and the honesty doctrine now has one home to cite.
**Reviewed with**: user (plan `dataforge-definitive-standard`, Task 1).
**Reversal criteria**: if maintaining a separate constitution proves to duplicate
README intent in practice, fold it back into a README preamble.

---

## 2026-07-11 - Enforce provable-only auto-apply; deep self-verifiable certificate
**Context**: The Corruption Oracle proved the default deterministic auto-apply path
never corrupts, and that the known verifier-floor gaps are LATENT (they live in the
advisory inferred guard, reachable only by an LLM value with no authoritative schema).
But a permissive `corrector_policy` could still auto-apply such a plausibility-only
fix, silently activating a gap. Separately, `verify_certificate` only checked hashes
and structure, not the constraints themselves.
**Alternatives**:
- (a) Never auto-apply plausibility-only fixes at all - maximally strict, removes a
  legitimate power-user escape hatch.
- (b) Opt-in, honestly recorded - off by default; explicit `allow_unproven_autoapply`
  permits it and the certificate records those cells as `plausibility_only`.
- (c) Leave as-is and rely on the default policy - fragile (a permissive policy
  re-opens the gap) and not an enforced guarantee.
- Re-verification: (d) hashes only (status quo); (e) re-run the real verifier per
  applied cell against the certified data; (f) a second, diverse re-implementation
  of constraint semantics.
**Decision**: (b) for enforcement + (e) for re-verification. A fix is `proven`
(deterministic OR authoritative-schema-verified) or `plausibility_only`; only proven
fixes auto-apply unless `allow_unproven_autoapply` is set, and then the receipt's
`applied_fixes[].verification_strength` records it truthfully. `reverify_certificate`
reconstructs the applied fixes and re-runs `SMTVerifier` per cell (mirroring the
engine's guard selection) plus a truthfulness check on the recorded labels.
**Reasoning**: (b) makes "the gaps stay latent" an ENFORCED, tested invariant under
any policy while keeping an honest escape hatch; the certificate never lies. (e)
gives independence in data and execution (catches tampering, drift, receipt/data
mismatch) at low cost; (f) was rejected as itself-unverified and disproportionate.
**Reviewed with**: user (chose "opt-in, honestly recorded").
**Reversal criteria**: if a real deployment needs zero unproven auto-applies, drop
the flag (option a); if a diverse second checker becomes worth its cost, add (f).

---

## 2026-07-07 - Reframe to verified+calibrated gate; conformal risk control; 1.5B is no-go for auto-apply
**Context**: A prior plan pursued "better teacher data + bigger base model" to lift
repair quality. The measured evidence collected while executing it refutes that
premise as the highest-leverage path:
- LLM corrector on the strongest models is rejected by its own promotion gate:
  Gemini precision@auto-apply 0.16 / ECE 0.79; Azure gpt-5-mini 0.077 / ECE 0.82
  (`eval/results/corrector_gpt5mini_hospital.json`). Gate needs >= 0.95 precision,
  <= 0.10 ECE.
- Deterministic correction F1 is 0.00 on flights and 0.039 on beers
  (`BENCHMARK_REPORT.md`); correction is unsolved on 2 of 3 datasets.
- Root cause of untrustworthy auto-apply is NOT model capability. It is (a)
  calibration - models do not know when they are wrong (ECE 0.8), and (b)
  statistical rigor - `dataforge.calibration.fit_thresholds` fit the auto-apply
  threshold IN-SAMPLE (no held-out split, no distribution-free guarantee), so a
  "0.95" threshold silently drops below 0.95 on new data. A bigger local model
  fixes neither.
**Alternatives**:
- Continue scaling the local model to 1.5B/3B. Cost: paid GPU + hours. Expected
  gain: tiny (0.5B already near ceiling per the training note; a 1.5B cannot clear
  a gate GPT-5-mini fails by 12x). Rejected as the primary thrust.
- Invest in the gate: distribution-free guaranteed auto-apply precision (conformal
  risk control) + detection breadth + an honest calibration benchmark. Chosen.
**Decision**:
1. Add `dataforge/conformal.py`: class-conditional (Mondrian) selective-risk
   control via fixed sequential testing + exact Clopper-Pearson bounds, giving a
   distribution-free finite-sample guarantee that an auto-applied class's error
   <= alpha w.p. >= 1 - delta; plus a calibration/test split, a certified-coverage
   report, and a PSI distribution-shift monitor. Refs: Bates et al. RCPS (2021);
   Angelopoulos et al. Learn-then-Test (2021); Conformal Risk Control
   (arXiv:2208.02814); Angelopoulos & Bates gentle intro (arXiv:2107.07511).
2. Wire `conformal_corrector_policy` + `guard_policy_for_drift` behind the
   unchanged `AbstentionPolicy` seam. The SMT verifier and safety constitution
   remain the hard floor; conformal only ever narrows what may auto-apply.
3. 1.5B / teacher-scaling is NO-GO for improving auto-apply. The shipped
   `sft_15b_v10.yaml` + Azure teacher pipeline remain documented, ready-to-run
   rungs, not the roadmap. Any future model work is scoped to exactly the error
   classes conformal can certify at >= 0.95 precision - never a blanket auto-apply.
**Reasoning**: Decision theory over sunk cost. The defensible moat is the verified,
now statistically-guaranteed, honest-abstention gate - not proposer capability
(proposers are commodities). Value is measured (Monte-Carlo validity test proves
the guarantee), never assumed.
**Reviewed with**: (solo) - primary sources verified (the two arXiv papers);
measured corrector reports and the `bench --quick` coverage matrix.
**Reversal criteria**: If a certified-coverage run shows a non-empty corrector
auto-apply slice at >= 0.95 precision on the held-out test split, scope a model
upgrade to those classes. If a specific deployment needs on-prem/offline correction
where hosted models are unavailable, revisit local-model scaling for that context.
**Measured status**: conformal machinery shipped, 1043+ tests, ruff+mypy --strict
clean. Detection (`bench --quick`): flights text_normalization detection = 0.00
(n=1729) is the top winnable-half gap; beers value_format detection is already
1.00 and text_normalization 0.87 (the old 0.40 floor was stale/conservative).

## 2026-07-06 - Azure OpenAI (GPT-5.5) teacher/measurement; 1.5B base; broaden datasets
**Context**: The trained repair policy (Qwen2.5-0.5B) is near its ceiling on
exact-value correction. The two highest-impact levers are better teacher data
and a bigger base model. A $200 Azure free-trial credit is available and the
user wanted "the best model (Opus/GPT-5.5)".
**Alternatives**:
- Use Anthropic Claude (Sonnet 5 / Opus) on Azure Foundry. Pros: strongest.
  Cons: Microsoft docs exclude Claude (a third-party Marketplace SaaS offer)
  from "free trial" and "credit-only" subscriptions - it will NOT run on the
  trial credit. Dishonest to promise it.
- Use Azure OpenAI GPT-5.5 (first-party, "sold directly by Azure"). Pros: billed
  against the subscription so it works on trial credit; strongest usable model;
  OpenAI-compatible surface. Cons: GPT-5 rejects `temperature != 1` and needs
  `max_completion_tokens`.
- Skip Azure; keep oracle-only teacher and 0.5B. Cons: forgoes both levers.
**Decision**: Add a first-party Azure OpenAI provider (product `complete()` +
bench `AzureBenchClient`) with a hard USD cost guard; wire it as a selectable
teacher provider (F1=1.0 verified filter) and corrector-benchmark backend.
Author `sft_15b_v10.yaml` (Qwen2.5-1.5B, bf16, paid GPU) as a structural mirror
of the proven `sft_05b_v9`. Add `rayyan` + `tax` RAHA datasets (verified pinned
SHAs). Keep propose-not-apply and the SMT+safety verifier gate for all
LLM-origin fixes.
**Reasoning**: Honest about what the trial credit can actually run (GPT-5.5, not
Claude); the provider fails fast with an actionable message if a Claude
deployment is requested. Value stays measured, never assumed - the promotion
gates and corrector_promotion_verdict decide "enough".
**Reviewed with**: (solo) - authenticity cross-checked against Microsoft Foundry
docs (models-from-partners, models-sold-directly-by-azure) and Anthropic model
overview.
**Reversal criteria**: If the user moves to pay-as-you-go, enable the Claude
path via the Anthropic Messages endpoint. If GPT-5.5's corrector precision fails
the 0.95 auto-apply gate, the corrector stays propose-not-apply.
**Gating (honest status)**: The Azure provider, teacher wiring, datasets, and
1.5B config are shipped and offline-verified (1010 tests, ruff+mypy --strict
clean). The live teacher-data run and corrector benchmark are gated on a
configured Azure endpoint (see docs/azure-teacher-setup.md); the 1.5B SFT->GRPO
training is gated on the user's paid GPU. Coverage floors for rayyan/tax are to
be seeded from the first measured `dataforge bench --quick` run.

## 2026-06-30 - Reposition as verified+calibrated repair; ensemble + honest coverage
**Context**: A per-error-class instrument (built first) exposed that the
deterministic stack scored F1 0.79 on hospital but 0.00 on flights and 0.04 on
beers - three single-strategy detectors missed the dominant error classes
(missing values, formatting/normalization) on two of three datasets. The field
(Raha/Baran) shows coverage comes from an ensemble of heterogeneous detectors,
not one strategy.
**Alternatives**:
- Keep narrow scope, tighten claims. Pros: honest, no work. Cons: leaves real
  coverage on the table; "detects common CSV issues" stays barely true.
- Broaden with naive detector sprawl. Pros: coverage. Cons: false positives
  regress precision (observed: format/categorical correction tanked hospital/beers).
- Ensemble + calibrated abstention + honest detection-vs-correction reporting,
  with new detectors strictly additive (tier 1) on top of the proven tier-0
  floor, and correction withheld where it cannot be proven safe. Pros: broad
  detection, no precision regression, defensible "trust" thesis. Cons: more
  surface; correction for fuzzy classes deferred behind calibration.
**Decision**: reposition DataForge as "the data-repair engine where every fix is
formally verified, reversible, and calibrated, with honest per-class coverage."
Ship the ensemble (8 detectors), measure detection and correction separately,
keep tier-0 detectors authoritative over their cells, and auto-apply only
provably-safe corrections (decimal-shift, FD, FD-derivable missing-value fill).
Format and categorical correction remain detection-only until calibration-gated.
**Reasoning**: the moat is safety/verifiability/reversibility; the honest move is
to maximize *detection* coverage while refusing to *guess* corrections. Measured
result: flights missing_value detection 0.00 -> 1.00 (2370 cells), beers/hospital
detection broadly up, with correction F1 unchanged (hospital 0.7926, beers
0.0391, flights 0.00) - zero regression. The detection/correction split makes the
limits visible rather than hidden.
**Reviewed with**: `dataforge bench --quick` on full RAHA, the per-class
instrument (`dataforge/bench/error_classes.py`), and `eval/thresholds/coverage_floors.json`.
**Reversal criteria**: enable a detection-only class's correction in
`build_repairers` only when a `dataforge bench --quick` run shows it does not drop
any committed per-class floor and clears the calibrated precision target.

---

## 2026-06-30 - Make agent backend user-selectable; default to hosted, fail fast
**Context**: The verified agent shipped with `local` (trained Qwen) as the
default policy, but that model currently underperforms the deterministic baseline
(F1 ~0.14 vs ~0.79). Users asked for all backends to be first-class, explicit
choices, with the strongest option as the default.
**Alternatives**:
- Keep `local` default. Pros: free/offline. Cons: weakest accuracy now; the
  default agent adds little over the floor.
- Hosted default, silent fallback to deterministic when no key. Pros: never
  errors. Cons: the agent silently does nothing; users cannot tell which backend
  ran.
- Hosted default, fail fast on missing key; `local`/`deterministic`/`custom:<name>`
  all selectable; provider via `--provider` with env fallback. Pros: best
  accuracy by default, explicit and honest, custom plug-in path. Cons: `--agent`
  with no key errors until the user picks a backend or sets a key.
**Decision**: hosted is the default policy across CLI/MCP/controller; selectable
kinds are `hosted`, `local`, `deterministic`, and `custom:<name>` (registry via
`register_policy`); `--provider groq|gemini` chooses the hosted provider with
`DATAFORGE_LLM_PROVIDER`/key autodetect fallback. Hosted and local both fail fast
with an actionable `PolicyUnavailableError` rather than silently degrading.
**Reasoning**: the default should be the most accurate option available today,
and failures should be loud and actionable, not silent. Determinism and the SMT +
constitution + transaction gates still bound every backend, including custom, so
selection never weakens safety. Supersedes the 2026-06-29 "local default" choice.
**Reviewed with**: full repo test suite, `dataforge.agent.available_policies`,
and manual CLI checks (`--policy hosted` no key -> clear error;
`--policy deterministic` offline OK).
**Reversal criteria**: flip the default back to `local` once a local/trained
policy passes `agent_promotion_verdict` against the deterministic baseline.

---

## 2026-06-29 - Make DataForge truly agentic via a verified agent, not LLM-YOLO
**Context**: The product (`dataforge repair`) was a deterministic detect ->
propose -> safety -> SMT -> transaction pipeline. The agent substrate (OpenEnv
env, typed tool actions, scratchpad) and the RL-trained policy existed but were
disconnected from the product, and the trained Qwen-0.5B underperformed the
deterministic heuristic baseline (F1 ~0.14 vs ~0.79). "Make it truly agentic"
risked a regression if it meant handing writes to a stochastic LLM.
**Alternatives**:
- Maximal autonomy: let the LLM drive detection AND repair end to end, with the
  gates as advisory. Pros: most "agentic". Cons: non-deterministic, slower, and
  currently far less accurate than the rules; weakens the safety/verifiability
  moat.
- Integrate the trained model as-is. Pros: ships the RL work. Cons: F1 0.14 is
  below baseline; would degrade the product.
- Verified agent: an autonomous LLM controller that seeds with the deterministic
  floor and works only the residual, where EVERY write is gated by the existing
  safety constitution + SMT verifier + reversible transaction journal, and
  rejections feed back for self-correction. Pros: autonomy in reasoning,
  determinism + proof in what is written; additive on top of the floor so it can
  never ship below baseline; unifies CLI/MCP behind one controller. Cons: more
  surface to maintain; LLM value on the residual still needs training work.
**Decision**: ship the verified agent as an opt-in mode (`dataforge repair
--agent`, MCP `dataforge_agent_repair`), local trained policy by default and
pluggable to hosted/deterministic, gated by a benchmark that blocks promotion to
default until the agent beats the baseline F1 with zero safety regressions.
**Reasoning**: DataForge's moat is safety, verifiability, and reversibility. The
highest-quality interpretation of "agentic" preserves that moat by keeping the
verified floor as both the agent's most-trusted tool and its safety net.
Autonomous agent fixes are additionally soft-escalation gated
(`NO_UNCONFIRMED_LLM_WRITE`), so live-LLM writes require explicit operator
confirmation (`--confirm-escalations`).
**Reviewed with**: full repo test suite (unit/integration/property/adversarial),
`dataforge.release.agent_gate.check_agent_release_gate`, and
`dataforge.bench.agent_promotion_verdict`.
**Reversal criteria**: revisit the deterministic-first ordering only if a trained
policy demonstrably beats the deterministic baseline on hospital/beers/flights
with no safety regression, at which point the promotion gate may flip the default.

---

## 2026-06-03 - Treat Workers as the canonical playground and harden external evidence
**Context**: The full original DataForge vision still depends on external
state: PyPI/TestPyPI trusted publishing, public package publication, live deployment
verification, real dbt-duckdb proof, real design-partner evidence, and a public
Hugging Face model family. A custom domain adds DNS ownership and routing risk
without improving the product proof because the Workers URL is already the
stable hosted playground surface.
**Alternatives**:
- Require a custom domain. Pros: shorter branded URL. Cons: introduces a DNS
  gate unrelated to the product's safety, package, or model claims.
- Keep both a custom domain and Workers. Pros: optional brand path. Cons:
  doubles deployment truth surfaces and invites stale docs.
- Make the Workers playground canonical. Pros: removes DNS ambiguity and keeps
  the external gate focused on package publication, hosted behavior, evidence,
  and model quality. Cons: less polished URL.
**Decision**: use `https://dataforge.praneshrajan15.workers.dev/playground` as
the canonical public playground and make the full-vision gate require hard,
file-backed evidence for PyPI, dbt-duckdb, design partners, and the HF model
family.
**Reasoning**: the project should optimize for falsifiable proof, not vanity
surface area. The Workers URL is sufficient for the hosted playground claim;
the release risk belongs in package publication, reversible repair behavior, and
model/eval evidence.
**Reviewed with**: `dataforge release full-vision --json`, PyPI trusted
publishing guidance, dbt data-test guidance, and Hugging Face model-card
guidance.
**Reversal criteria**: revisit only if a custom domain is already controlled,
monitored, and useful to users without weakening the existing Workers gate.

---

## 2026-05-16 - Use GRPO before GiGPO on the free-tier training path
**Context**: Week 12 needs a post-SFT reinforcement learning step that can run
on Kaggle or Colab free GPUs without adding a second distributed RL stack.
The original prompt named TRL v0.11 and GiGPO as adjacent possibilities, but
the free-tier release path needs a stable trainer, local reward scoring, and
small rollout batches.
**Alternatives**:
- Use TRL GRPO. Pros: ships in the existing TRL family, supports callable
  reward functions, and can run with LoRA/QLoRA on small models. Cons:
  rollout count and prompt length must be conservative on P100/T4 memory.
- Use GiGPO through verl-agent. Pros: closer to newer agentic RL research.
  Cons: heavier setup, larger memory footprint, and more moving parts than the
  current free-tier path can honestly support.
- Skip RL and refresh only SFT. Pros: lowest operational risk. Cons: does not
  test the environment/reward path that Week 12 is meant to validate.
**Decision**: implement GRPO first with TRL, local stateless exact-repair
rewards, and a hard F1 gate before publishing.
**Reasoning**: GRPO is the smallest credible RL step after SFT that can be
reproduced by maintainers without paid infrastructure. GiGPO remains future
work until the project has either paid compute or an HF compute grant.
**Reviewed with**: `specs/SPEC_grpo_training.md`.
**Reversal criteria**: if GRPO cannot clear the +0.03 F1 gate after reward
diagnostics and rollout-count tuning, or if GiGPO gains a lightweight
single-GPU implementation, revisit the RL method choice.

---

## 2026-05-15 - Treat canonical human docs as the documentation source of truth
**Context**: The repository now contains generated Hugging Face staging mirrors,
local logs, cache directories, and canonical human-authored docs. A full docs
refresh needs to update the real source documents without hand-editing generated
deployment copies that can be recreated by scripts.
**Alternatives**:
- Edit every Markdown and text file. Pros: every visible copy can be updated in
  one sweep. Cons: generated mirrors drift from their staging scripts and create
  noisy churn.
- Update only the files named in the prompt. Pros: smallest edit set. Cons:
  leaves stale claims in adjacent docs that readers actually use.
- Update canonical human-facing docs and leave generated/staged mirrors alone.
  Pros: keeps documentation truthful while preserving reproducible deployment
  artifacts. Cons: generated mirrors need regeneration when their canonical
  source changes.
**Decision**: refresh canonical human-facing docs only; do not hand-edit
`.hf-space-repo/`, `.hf-space-stage/`, `.hf-space-stage-plan/`, caches, logs, or
other generated mirrors.
**Reasoning**: documentation should have one source of truth per surface.
Deployment mirrors are outputs, not places to make product decisions.
**Reviewed with**: 2026-05-15 documentation refresh plan.
**Reversal criteria**: if a staging directory becomes the only source consumed
by a deployment and cannot be regenerated from canonical files, promote that
file to documented source status and update this decision.

---

## 2026-05-15 - Package DataForge MCP as a nested standalone distribution
**Context**: Week 11 needs `dataforge-mcp` to be installable by MCP clients
without folding MCP transport concerns into the core `dataforge` package.
**Alternatives**:
- Add MCP commands to the root `dataforge` package. Pros: fewer package files.
  Cons: adds transport dependencies to the core runtime and weakens integration
  package evidence.
- Create a sibling repository immediately. Pros: mirrors the long-term target.
  Cons: harder to test atomically with the current dirty worktree.
- Create `dataforge-mcp/` as a nested standalone package. Pros: keeps a separate
  PyPI artifact while letting CI test it against the local DataForge source.
  Cons: release workflow must build from a subdirectory.
**Decision**: create `dataforge-mcp/` inside this repository as a standalone
package that relies on `dataforge` and `mcp`.
**Reasoning**: this is the narrowest path to a real integration package without
polluting the core dependency graph or requiring a repo split before the
implementation is proven.
**Reviewed with**: `specs/SPEC_mcp_server.md`.
**Reversal criteria**: if the integration gains independent release cadence or
external contributors, split `dataforge-mcp/` into its own repository while
preserving the same package metadata and tool contracts.

---

## 2026-05-15 - Correct ZeroGPU docs for the model demo Space
**Context**: The Week 11 prompt referred to stale ZeroGPU infrastructure details
and an unsupported README field for hardware selection, but current Hugging
Face documentation describes Gradio-only ZeroGPU with dynamic shared GPU
allocation and supported Space config keys such as `sdk` and `app_file`.
**Alternatives**:
- Repeat the original prompt literally. Pros: minimal editing. Cons: commits
  stale or unsupported deployment claims.
- Omit ZeroGPU specifics entirely. Pros: avoids drift. Cons: users need to know
  queue and quota behavior before trying the demo.
- Document the current supported contract and instruct maintainers to select
  ZeroGPU in Space settings. Pros: accurate and actionable. Cons: slightly less
  terse than the original prompt.
**Decision**: use valid Gradio Space frontmatter and document ZeroGPU selection,
queueing, quota, and model-loading behavior in prose.
**Reasoning**: DataForge documentation should not claim infrastructure details
that official upstream docs no longer support.
**Reviewed with**: `specs/SPEC_model_space.md`.
**Reversal criteria**: if Hugging Face adds a supported README configuration key
for accelerator selection or changes ZeroGPU allocation behavior again, update
the Space README and spec
together.

---

## 2026-05-15 - Expand environment action space to include ROOT_CAUSE
**Context**: Week 10 adds causal root-cause analysis for cascading data-quality
errors. The Week 6 environment spec locked seven typed actions, but root-cause
analysis is a distinct read-only diagnostic operation rather than a hypothesis,
diagnosis, or fix.
**Alternatives**:
- Reuse `HYPOTHESIS`. Pros: no action-space change. Cons: mixes free-form
  scratchpad claims with analyzer-backed observations and makes reward credit
  ambiguous.
- Add `ROOT_CAUSE` as an eighth typed action. Pros: explicit interface,
  structured observations, and a narrow reward hook. Cons: supersedes the
  previous seven-action assumption.
- Fold root cause into `DIAGNOSE`. Pros: fewer action types. Cons: row/column
  diagnosis and causal minimization have different inputs and semantics.
**Decision**: add `ROOT_CAUSE(error_indices: list[int])` as the eighth typed
environment action.
**Reasoning**: cascading errors need a first-class read-only analyzer result
without pretending the agent authored the causal explanation. The explicit
action also lets training distinguish "found an issue" from "found the minimal
upstream cause."
**Reviewed with**: `specs/SPEC_causal_root_cause.md` and the Week 10 plan.
**Reversal criteria**: if training shows the eighth action materially worsens
exploration without improving downstream fix quality, fold it into a richer
`DIAGNOSE` observation while preserving the analyzer API.

---

## 2026-05-10 - Add a hard SFT readiness gate before Kaggle
**Context**: The Kaggle notebook can fail late or publish incomplete artifacts
when the HF dataset repo is missing, the local trajectory JSONL is empty, chunk
keys are duplicated, package pins drift, or evaluation fails after an early
upload.
**Alternatives**:
- Trust the notebook alone. Pros: fewer files. Cons: failures happen inside a
  scarce GPU runtime and are harder to diagnose.
- Add notebook-only assertions. Pros: catches some problems. Cons: still
  burns Kaggle startup time and does not protect local handoff quality.
- Add a local preflight gate plus notebook checks. Pros: catches bad handoffs
  before Kaggle, keeps run-all behavior, and prevents incomplete model cards.
  Cons: one more command in the workflow.
**Decision**: validate `expert_v1` locally with
`scripts/data/validate_sft_readiness.py`, enforce exact pins and non-empty
train/held-out split assumptions, and publish from the notebook only after
numeric evaluation metrics exist.
**Reasoning**: the Kaggle step should be a compute execution step, not the
first place basic data and packaging invariants are discovered. A local gate is
the cheapest way to make failures deterministic and actionable.
**Reviewed with**: SPEC_sft_warmup.md and the 2026-05-10 Kaggle failure audit.
**Reversal criteria**: if the workflow moves to a managed trainer with its own
artifact validation and atomic publishing, collapse the local gate into that
system while preserving the same checks.

---

## 2026-05-02 - Collect Week 9 SFT data as chunk-level trajectories
**Context**: Week 9 needs a Kaggle-free-tier SFT warmup dataset from Groq ReAct
teacher runs. Treating each full benchmark episode as one "trajectory" would
make the stated 2,000-trajectory target incompatible with the free-tier request
budget because each episode spans many row chunks.
**Alternatives**:
- Full-episode records. Pros: simple naming. Cons: budget math does not close
  and one record contains too much heterogeneous context for SFT.
- Chunk-level records keyed by `(task_id, seed, chunk_index)`. Pros: matches
  the existing ReAct chunk loop, supports idempotent resume, and yields compact
  chat examples. Cons: episode-level quality filtering must be applied before
  writing chunk records.
- Synthetic fixture-only records. Pros: cheap and deterministic. Cons: misses
  the real-world Hospital / Flights / Beers distribution targeted by Week 9.
**Decision**: collect chunk-level `expert_v1` JSONL records from real-world
DataForge-Bench-light windows and retain only chunks from episodes with F1 >=
0.6.
**Reasoning**: chunk-level records are the only way to honor the Groq request
budget, keep examples trainable on a 0.5B model, and preserve auditable
tool-use provenance.
**Reviewed with**: SPEC_sft_warmup.md and the Week 9 implementation plan.
**Reversal criteria**: if later training shows chunk-local examples do not
teach cross-chunk repair strategy, introduce a second hierarchical dataset
format while keeping `expert_v1` for warmup SFT.

---

## 2026-05-02 - Resolve Week 9 HF repos from the authenticated user
**Context**: The original prompt used a placeholder owner namespace for the
model repo, which is not run-all reproducible in Kaggle and invites users to
edit notebook cells.
**Alternatives**:
- Hardcode a maintainer namespace. Pros: simple for one maintainer. Cons:
  breaks forks and external readers.
- Ask the notebook user to edit a placeholder owner. Pros: obvious. Cons: violates the
  run-all without modification requirement.
- Resolve `HF_TOKEN` with `whoami` and derive dataset/model repo names. Pros:
  reproducible, fork-friendly, and scriptable. Cons: requires a write-capable
  HF token.
**Decision**: use `HF_TOKEN` plus `HfApi.whoami()` to derive
`<hf_user>/dataforge-sft-trajectories` and `<hf_user>/DataForge-0.5B-SFT`.
**Reasoning**: automatic repo resolution is the narrowest way to make the
notebook self-contained while still publishing into the runner's namespace.
**Reviewed with**: SPEC_sft_warmup.md.
**Reversal criteria**: if HF changes token introspection semantics or the
workflow moves to organization-owned releases, add an explicit `--repo-id`
override while keeping `auto` as the default.

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

## 2026-04-21 - Cloudflare Workers Static Assets + HF Docker Spaces for the hosted playground
**Context**: the playground needs a free-tier host for both a static frontend
and a Python backend (FastAPI + pandas + dataforge). The choice must survive
indefinitely on zero-cost infrastructure without maintenance burden.
**Alternatives**:
- Railway. Pros: great Docker support, generous free tier. Cons: free tier
  has a monthly credit cap ($5/month) that can be exhausted by sustained
  traffic; the project would need to monitor credits or risk downtime.
- Render. Pros: Docker support, free tier. Cons: free-tier containers spin
  down after 15 minutes and cold-start takes ~30 s; the free plan has limited
  RAM (512 MB) which is tight for pandas + z3.
- Cloudflare Workers Static Assets (frontend) + HF Docker Space (backend).
  Pros: Workers Static Assets gives the static React/Vite app a global edge
  host on the existing Cloudflare account; HF Spaces support Docker SDK with
  auto-sleep and no monthly credit cap; the combination survives indefinitely
  at zero cost. Cons: HF free-tier Spaces have ~15 min sleep timeout and
  ~30 s cold-start; the frontend must handle this gracefully.
**Decision**: Cloudflare Workers Static Assets (frontend) + HF Docker Space
(backend).
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

---

## 2026-04-27 - Align the frontend deploy path with Cloudflare Workers Static Assets
**Context**: the deployed Cloudflare project is running Workers Builds with
`wrangler deploy`, but the repo still documents Cloudflare Pages and a
build-time `sed` mutation of `playground/web/config.js`. This drift caused the
latest frontend build to fail because Wrangler was not given an explicit assets
directory for the static site.
**Alternatives**:
- Move the frontend back to Cloudflare Pages. Pros: matches the older repo docs.
  Cons: requires reworking the connected Cloudflare project and keeps two
  deployment models in play.
- Keep the current Cloudflare Worker project and add explicit static-assets
  configuration (this choice). Pros: matches the existing Cloudflare build
  system, makes the assets directory explicit, and lets the repo own the
  frontend deployment contract through `wrangler.toml`. Cons: requires doc and
  metadata updates from Pages wording to Workers Static Assets wording.
**Decision**: keep the existing Cloudflare Worker project and standardize the
frontend on Cloudflare Workers Static Assets.
**Reasoning**: the codebase already ships a pure static frontend, so the
minimal durable fix is to add an assets-only Wrangler config, replace the
runtime config mutation with a validated Python renderer, and keep backend CORS
owned by explicit deployment configuration.
**Reviewed with**: `playground/web/DEPLOY.md`, `specs/SPEC_playground.md`, and
the Cloudflare Workers static-assets / Pages configuration docs.
**Reversal criteria**: if Cloudflare deprecates assets-only Worker deploys for
repo-connected builds, or if Pages regains a clear operational advantage for
this static site, revisit the frontend hosting model.

---

## 2026-05-01 - Expand action space from 4 to 7 typed tool-use actions
**Context**: the legacy `data_quality_env` uses 4 untyped actions (`inspect`,
`diagnose`, `fix`, `finalize`). Week 6 migrates to a typed tool-use interface.
The question is whether to preserve the legacy action vocabulary or expand it.
**Alternatives**:
- Keep 4 actions (port legacy vocabulary). Pros: minimal migration risk.
  Cons: blocks richer agent strategies; `inspect` conflates row viewing,
  column stats, and secondary table access into one overloaded action.
- Expand to 7 typed actions. Pros: each action has clear semantics and
  field-level Pydantic validation; enables SQL queries, statistical tests,
  pattern matching, and hypothesis recording that are essential for a
  production-grade data-quality agent. Cons: agent code and training
  pipelines must adapt to the larger action space.
- Expand to 10+ actions (fine-grained per detector). Pros: maximum
  specificity. Cons: combinatorial explosion makes RL exploration harder;
  many actions would be rarely used.
**Decision**: expand to 7 typed actions with discriminated Pydantic union.
**Reasoning**: 7 actions hits the sweet spot between expressiveness and
learnability. Each action maps to a distinct cognitive operation (explore,
analyze, hypothesize, diagnose, repair). The legacy `finalize` is replaced
by automatic step-budget termination, which eliminates the pathological
case where an agent wastes a step by finalizing prematurely and simplifies
the episode lifecycle. The discriminated union pattern prevents cross-model
field pollution that plagued the legacy `DataQualityAction` monolith.
**Reviewed with**: SPEC_openenv_env.md and the Week 6 implementation.
**Reversal criteria**: if RL training shows the 7-action space is too sparse
for exploration (> 2× sample complexity vs 4 actions on equivalent tasks),
consider collapsing SQL_QUERY + STAT_TEST + PATTERN_MATCH into a single
`ANALYZE` action with a sub-type discriminator.

---

## 2026-05-01 - INSPECT_ROWS returns up to 20 rows, not 20 cells
**Context**: the Week 6 prompt says "up to 20 cells total, not 20 rows."
With a 10-column dataset, that allows only 2 rows per inspection — severely
limiting information gain per step compared to the legacy 10-row limit.
**Alternatives**:
- 20 cells (literal prompt). Pros: minimal data leakage per step; forces
  the agent to use SQL_QUERY for broader views. Cons: with 10 columns, only
  2 rows visible per action; the agent needs 5 inspections to see what one
  legacy inspection showed, wasting precious step budget on data access
  instead of reasoning.
- 20 rows (relaxed cap). Pros: each inspection returns enough rows for the
  agent to spot patterns across multiple records; matches the scale at which
  detectors operate (row-level issues); compatible with the exploration bonus
  formula which rewards coverage breadth. Cons: slightly more data per step.
- 10 rows (legacy parity). Pros: direct backward compatibility. Cons:
  arbitrary number with no principled justification.
**Decision**: 20 rows per INSPECT_ROWS action.
**Reasoning**: the cell-level interpretation creates a perverse incentive:
the agent must spend its finite step budget on data access rather than
analysis. With 20 rows × 10 columns, the agent sees ~200 cells per
inspection — enough to identify multi-row patterns (e.g., FD violations,
systematic decimal shifts) that are architecturally invisible in a 2-row
window. The agent retains fine-grained column filtering via the optional
`column_names` field for targeted queries, and SQL_QUERY provides
unrestricted read access for complex analysis.
**Reviewed with**: SPEC_openenv_env.md, REWARD_DESIGN.md exploration bonus.
**Reversal criteria**: if agents learn to request maximum rows on every step
(ignoring the exploration bonus decay), consider reducing the cap to 10 or
adding a diminishing-returns penalty for large inspections.

---

## 2026-05-01 - Use hospital fixture as default, support configurable datasets
**Context**: the environment needs a default dataset for `reset()`. Options
include the existing `fixtures/hospital_10rows.csv`, a purpose-built fixture,
or the legacy JSON datasets in `datasets/`.
**Alternatives**:
- Hospital fixture only. Pros: immediate usability; already has a schema
  YAML. Cons: limited diversity for training.
- Purpose-built fixture. Pros: can be tailored to test all detector types.
  Cons: delays ship; may not represent real-world data characteristics.
- Support both via task configuration. Pros: extensible architecture;
  default fixture for quick-start, configurable loading for BYOD (bring
  your own data) scenarios. Cons: slightly more code surface.
**Decision**: use `fixtures/hospital_10rows.csv` with its schema YAML as
the default episode dataset, with the architecture supporting future
configurable task loading.
**Reasoning**: the hospital fixture is the canonical test dataset already
used by the detector suite and benchmark pipeline. Using it as the default
ensures the ground truth generated by `run_all_detectors()` produces
meaningful issues (type_mismatch on `phone_number`, decimal_shift on
`rating`, fd_violation on `provider_number → hospital_name`). The
architecture's `_load_fixture()` path is trivially extensible to accept
arbitrary CSV+schema pairs in future milestones.
**Reviewed with**: SPEC_openenv_env.md §3 (IN scope).
**Reversal criteria**: if the hospital fixture proves too small or too
repetitive for meaningful RL training, add a larger purpose-built fixture
(~100 rows, 15 columns, all detector types represented) as the default.

---

## 2026-05-01 - Port legacy noise model verbatim (ε=0.15, seed-based RNG)
**Context**: the legacy environment implements stochastic observation noise
with 15% probability per row, using seed-based `random.Random`. The Week 6
prompt asks whether to refine the noise model.
**Alternatives**:
- Port verbatim. Pros: tested, simple, deterministic for same seed, and
  already validated by the legacy test suite. Cons: noise is row-level
  only (no column-correlated noise, no systematic bias).
- Refine with column-correlated noise. Pros: more realistic; mimics
  real-world pipeline errors that affect entire columns. Cons: increased
  complexity; requires new calibration; risks breaking determinism
  guarantees expected by RL training scripts.
- Remove noise entirely. Pros: simplest. Cons: loses the POMDP training
  capability that forces agents to be robust to observation uncertainty.
**Decision**: port the legacy noise model verbatim.
**Reasoning**: the legacy model is simple, deterministic, and effective for
its purpose (partial observability training). Refining the noise model is a
research concern that belongs in a future training experiment, not in the
environment architecture. The ε=0.15 parameter and seed-based RNG ensure
reproducible episodes across training runs, which is more important than
noise realism at this stage.
**Reviewed with**: SPEC_openenv_env.md §4 (constraints).
**Reversal criteria**: if agent training shows the current noise model is
either too easy (agents trivially learn to ignore it) or too hard (agents
can't converge), tune ε or switch to column-correlated noise.

---

## 2026-05-01 - Hypothesis root-cause matching on issue_type (closed vocabulary)
**Context**: the HYPOTHESIS action awards root-cause credit when the agent's
claim matches hidden ground truth. The matching criteria must be defined.
**Alternatives**:
- Match on `issue_type` only. Pros: deterministic, testable; uses the
  closed vocabulary (`IssueTypeLiteral`) which is machine-readable and
  already present in detector output. Cons: coarse; doesn't validate the
  causal reasoning in the `claim` text.
- Match on `issue_type` + `reason` field. Pros: validates richer reasoning.
  Cons: `reason` is free-form text; fuzzy matching is unreliable, requires
  an LLM judge, and violates the "no LLM calls in environment" constraint.
- Match on `issue_type` + `row` + `column`. Pros: precise location-aware
  matching. Cons: this is equivalent to DIAGNOSE; removes the
  strategic value of HYPOTHESIS as a "broader claim" action.
**Decision**: match on `issue_type` (from `IssueTypeLiteral`) plus row and
column membership in `affected_rows` and `affected_columns` respectively.
**Reasoning**: this provides meaningful credit granularity without requiring
text analysis. The agent gets credit for correctly identifying that "rows
[5, 6] in column 'rating' have a `decimal_shift` issue" — which is the
actionable insight a root-cause analysis should produce. The `claim` text
is recorded in the scratchpad for observability but not scored, preserving
the "no LLM calls" invariant. The per-issue credit of `R_EXPLORE = 0.01`
is intentionally small: HYPOTHESIS is a planning action, not a scoring
shortcut, and its primary value is helping the agent organize its
investigation strategy.
**Reviewed with**: SPEC_openenv_env.md §6.5, detector base.py `Issue` model.
**Reversal criteria**: if future work adds a lightweight offline NLI model
for claim verification (no runtime LLM call), consider upgrading hypothesis
matching to validate the `claim` text against the ground-truth `reason`.


---

## 2026-05-01 - Exact-origin CORS and `dataforge-playground` Space naming
**Context**: Week 5 hardening found two deployment risks: the backend accepted
any `*.workers.dev` / `*.pages.dev` origin in production, and docs/config drifted
between the repo name (`data-quality-env`) and the product playground target.
**Alternatives**:
- Keep wildcard Cloudflare CORS and the existing Space slug. Pros: no deploy
  churn. Cons: another Cloudflare-hosted site could call the API, and public
  URLs do not match the product name.
- Revert to Cloudflare Pages and subtree push. Pros: matches the original Week 5
  prompt literally. Cons: contradicts the reviewed Workers Static Assets flow
  and the staged Docker build context already verified in CI.
- Keep Workers Static Assets, require exact production origins, and standardize
  the Hugging Face Space as `dataforge-playground`. Pros: preserves the tested
  deploy path, tightens API exposure, and aligns the public demo URL with the
  product name. Cons: maintainers must set `DATAFORGE_PLAYGROUND_ORIGINS`
  explicitly after deploy.
**Decision**: keep Workers Static Assets, remove production wildcard CORS, allow
localhost only under `DATAFORGE_PLAYGROUND_DEV=1`, and standardize the Space slug
as `dataforge-playground`.
**Reasoning**: exact-origin CORS is the narrowest free-tier-safe contract, while
the product-named Space avoids a confusing public URL without changing API
behavior.
**Reviewed with**: Week 5 playground hardening plan, `SPEC_playground.md`, and
the existing playground smoke/contract tests.
**Reversal criteria**: if Cloudflare changes preview host behavior in a way that
makes exact-origin previews unmanageable, add a narrowly-scoped preview-origin
configuration mechanism rather than restoring broad platform wildcards.


---

## 2026-07-01 - Verified LLM corrector: contract-bound, propose-not-apply, calibrated
**Context**: the measured correction bottleneck is the classes with no derivable
canonical value (missing-value fills, free-text normalization, context-dependent
typos) - the bulk of flights (0.00) and beers (0.04) correction F1. Deterministic
repair cannot invent these values. An LLM can, but a naive LLM writer would
violate the project's verified/reversible/calibrated/honest ethos.
**Alternatives**:
- Naive LLM repairer that writes its best guess. Pros: highest raw coverage.
  Cons: silent, unverifiable writes; corrupts data on hallucination; abandons
  the "prove what you touch" design center.
- Keep those classes detection-only forever. Pros: zero correction risk. Cons:
  leaves the hardest, most valuable half of repair permanently unaddressed.
- Grounded, contract-bound, self-consistent corrector gated by the existing
  verifier/constitution plus a calibrated propose-not-apply policy.
**Decision**: ship the third option. (1) Close the schema-less verification gap
first: when no authoritative schema exists, infer constraints and check any
LLM-origin value against them (`dataforge/verifier/inferred.py`), so corrections
can no longer be structurally auto-accepted. (2) Bind every candidate to a
`CorrectionContract` (detector finding + inferred type/domain/regex/FD) and to
the same inferred guard the verifier enforces, so the corrector can only propose
values the verifier would also accept. (3) Confidence = self-consistency
agreement across k samples. (4) Propose-not-apply by default: corrector output
surfaces as `suggested_fixes`; auto-apply requires both an operator-confirmed LLM
write (constitution `NO_UNCONFIRMED_LLM_WRITE`, now also covering `llm_cache`)
and a per-class threshold fit to a >= 0.95 precision floor. (5) The
`llm_corrector` benchmark method measures per-class correction F1, ECE, and a
fixed-0.95-agreement `precision_at_auto_apply`; `corrector_promotion_verdict`
refuses promotion until the precision floor and a calibration bound are met.
**Reasoning**: deterministic runs stay byte-identical (the guard and corrector
engage only for LLM-origin fixes when `allow_llm` is set), so no regression is
possible when the corrector is off (hospital held at 0.7926). When on, the tool
gains reach on the hard classes without ever making an unverified or silent
write. `precision_at_auto_apply` uses a fixed, pre-committed agreement bar rather
than an in-sample fit to avoid a circular, self-flattering metric.
**Reviewed with**: verified-llm-corrector plan, RAHA detection/correction split,
`dataforge/repairers/contract.py`, `dataforge/repairers/llm_corrector.py`,
`dataforge/calibration.py`, `dataforge/engine/repair.py`.
**Reversal criteria**: if measured `precision_at_auto_apply` for a class clears
the floor with a calibrated confidence signal on held-out data, raise that
class's default threshold so it auto-applies; if the corrector cannot beat the
deterministic stack on any class, keep it suggestion-only.

## 2026-07-12 - N-version differential verification: a second, independent constraint checker

**Decision**: Add a second, independently-written constraint checker,
`DirectVerifier` (`dataforge/verifier/direct.py`), that evaluates the same
authoritative-schema specification as the primary z3-backed `SMTVerifier` but
by DIRECT Python table evaluation -- set membership, comparison, enumeration --
sharing none of its checking logic and importing no z3. The result contract
(`VerificationVerdict`/`VerificationResult`) was relocated to a
dependency-free `dataforge/verifier/result.py` so the diverse checker's import
graph is genuinely z3-free. `differential_verify`
(`dataforge/verifier/differential.py`) runs both and combines their verdicts
FAIL-CLOSED: a fix auto-applies only when BOTH accept; any disagreement (or a
non-accept from either) holds the fix and is recorded. The engine gate is
default-on for the authoritative-schema path
(`RepairPipelineRequest.require_independent_agreement=True`,
`RepairReceipt.independent_verification`), and `reverify_certificate` now
re-derives ACCEPT through the differential pair on that path (recording
`reverify_independent_agreement`), removing the prior "not a diverse
re-implementation" caveat.
**Reasoning**: "trust the verifier" was a single point of failure -- a bug in the
one checker would pass at repair time AND at reverify time. Two implementations
built from different mechanisms make a common-mode logic bug very unlikely; when
they diverge, fail-closed means the diverse checker can only ever REDUCE
auto-applies (hold a fix for review), never wave through a corrupting one. A
Hypothesis equivalence suite (`tests/property/test_verifier_equivalence.py`)
generates random schemas/tables/fixes and asserts the two agree; a 1500-example
stress showed 919 accept/accept + 581 reject/reject and ZERO disagreements. The
gate engages only when an authoritative schema is present, so schema-less
deterministic runs (hospital 0.7926) are byte-identical and untouched.
**Honest boundary**: N-version targets checking LOGIC diversity. The two share
the specification (`Schema`), the output contract, and table I/O; a defect in
the shared spec itself, or in a shared dependency, is not covered. The advisory
inferred guard (heuristic, schema-less) is intentionally single-implementation
because it only ever gates non-auto-applying plausibility fixes.
**Reviewed with**: nversion-independent-constraint-checker plan,
`dataforge/verifier/{direct,differential,result,smt,gate}.py`,
`dataforge/engine/repair.py`, `dataforge/certificate.py`,
`tests/unit/test_direct_verifier.py`, `tests/unit/test_differential_verifier.py`,
`tests/property/test_verifier_equivalence.py`.
**Reversal criteria**: if the equivalence suite ever surfaces a persistent
divergence rooted in the shared spec (not an implementation bug), promote the
spec itself to the object of verification; if per-fix dual verification proves
too slow at scale, batch it or gate it behind a size threshold, never behind
correctness.

## 2026-07-12 - Dataset scope: exclude beers; focus hospital + flights

**Decision**: The `beers` benchmark dataset is removed from the project's active,
forward-looking surfaces and is not used in any new work. Removed from
`dataforge/datasets/registry.py` (registry entry), `dataforge/cli/bench.py`
(the `--quick` default expansion, now `hospital,flights`),
`dataforge/release/model_family.py` (required/eval dataset lists),
`eval/thresholds/coverage_floors.json` (the `heuristic/beers` floor block), and
the README benchmark docs. Live bench tests were repointed to hospital/flights.
The durable rule is recorded in `CLAUDE.md` (DATASET SCOPE RULE) so every future
session follows it. The remaining RAHA datasets are NOT ranked by a fixed
priority: they are one canonical suite of equal provenance that differ by error
profile, not quality. Dataset selection for new work is capability-based:
`hospital` is the flagship and hard regression anchor (heuristic F1 must never
regress below 0.7926 — the one measured SOTA win); `tax` for provable
FD/rule-violation repair at scale; `rayyan` for datetime/format canonicalization;
`flights` for the not-inferable-in-table frontier. `tax`/`rayyan` must be measured
before being prioritized for accuracy work (`tax` = 200k rows, needs a
scale-aware/sampled bench and has no floors yet; `rayyan` has only detection
floors, no measured correction baseline).
**Reasoning**: the product's effort should concentrate on the capability a change
is meant to prove, not on a dataset popularity ranking; `beers` added surface area
without being a focus, so it is removed. Framing the others by capability (not a
rigid hospital>flights>others order) keeps the roadmap honest: `tax`'s
denial-constraint/rule-violation errors align with DataForge's provable FD stack
and are a plausible SECOND place to beat SOTA once measured.
**Honest boundary**: frozen historical artifacts (past SFT/GRPO training curricula
such as `training/grpo_config.py` and the `expert_v*` trajectories, archived
`eval/results/` run snapshots, and released-model tokenizer vocab) still reference
beers because that is a factual record of what past runs did; they were
deliberately left untouched. Only forward-looking use of beers is prohibited.
**Reviewed with**: CLAUDE.md DATASET SCOPE RULE, dataforge/datasets/registry.py,
dataforge/cli/bench.py, dataforge/release/model_family.py,
eval/thresholds/coverage_floors.json, tests/unit/test_bench_real_world.py,
tests/unit/test_bench_core.py, tests/unit/test_bench_runner.py, README.md.
**Reversal criteria**: if a future benchmark need requires beers, re-add its
registry entry (RAHA revision + SHA-256s are preserved in git history) and update
the DATASET SCOPE RULE accordingly.

---

## 2026-07-14 - The honest frontier: deterministic in-table correction is maxed; add post-hoc calibration for the LLM path

**Context**: A fixing-elevation push aimed to raise correction accuracy (beat
Raha+Baran F1 where provable, raise safe auto-apply coverage) WITHOUT weakening
the no-corruption guarantee. Three candidate slices were measured first (offline,
deterministic, no code shipped until proven). All three turned out to be NOT
in-table-provable, for one shared reason worth recording.

**What was measured** (each an offline read-only measurement against the pinned
RAHA revision `7be1334b8c7bbdac3f47ef514fb3e1e8c5fc181c`):

- **flights value_format (time-in-cruft), Phase 1A = NO-GO.** `TimeFormatCruftDetector`
  flags 126 cells with 0 false positives, but stripping the date/timezone cruft
  reproduces `clean.csv` on only **57/126 (precision 0.452)**. The 57 matches are
  the `value_format` class; the 69 misses are `other` cells whose embedded time is
  an ESTIMATE, not the actual time (`'6:47 p.m. (Estimated runway)'` -> clean
  `'6:30 p.m.'`). Byte-identical residues (`'(Estimated)'`, `'(Estimated runway)'`,
  `'12/2/11'`) occur on BOTH correct and wrong cells, so no function of the dirty
  cell separates them. The only 1.0-precision rule ("residue is exactly a zero
  offset +/-00:00") has support 4. Auto-applying the strip would write ~69
  confidently-wrong values.
- **tax FD/rule-violation, Phase 1B = NOT VIABLE near-term.** tax is 200k x 15 with
  121,219 errors, but **97.9% are `numeric`** (rate 87,342; zip 31,311), not
  cross-column FD; the genuine FD-repairable slice (city+state) is ~800 cells.
  Schema inference is super-linear (2k 0.7s -> 20k 9.8s) and does not finish on
  200k in >8 min; the FD repairer is O(issues x rows). The 90%-confidence
  single-column FD heuristic invents spurious dependencies (`zip->salary`,
  `zip->rate`, `f_name->gender`), so on a 3k sample **detection precision is 0.0317**
  (263 real of 8305 flagged; fd_violation alone: 19 real of 7808). Only
  `decimal_shift` is useful (244/375). No SOTA win without exact-FD/denial-constraint
  mining + precision control + a vectorized scale rewrite. `sota_comparison.json`
  also has no tax row, so any tax claim needs a new sourced citation.
- **rayyan datetime_format, Phase 1C = NO-GO for auto-apply (but exact reviewed fix).**
  722 cells, all in `article_jcreated_at`, are a systematic `Y/M/D` -> `M/D/YY`
  transposition. A deterministic left-rotation reproduces `clean` on **722/722
  (correction precision 1.0000)** and the value sets are perfectly disjoint (0
  collisions). BUT every error value is ALSO a syntactically valid `M/D/YY` date,
  so no single-cell validity rule fires; the best genuine structural detector is
  **0.944 precision (27 would-be corruptions)** and the verifier cannot catch it
  (a rotated date is still a valid date). Worse, the corrupted `Y/M/D` form is the
  column MAJORITY (79%), so "canonicalize to dominant" points the wrong way.

**Alternatives**:
- (A) Ship one/all of the above as auto-apply corrections to raise headline F1.
  Rejected: each would auto-apply confidently-wrong values (flights/rayyan) or
  massive false positives (tax), violating "fix only what you can prove / never
  corrupt". The whole thesis forbids it.
- (B) Ship the tiny provably-safe microslices (flights zero-offset = 4 cells;
  rayyan rotation as a REVIEWED, never-auto-applied suggestion). Deferred: near-zero
  accuracy impact; the rayyan rotation is a good future propose-not-apply feature.
- (C) Accept the deterministic frontier and invest in the path that CAN fix
  semantic errors -- the calibrated LLM corrector -- starting with the one piece
  buildable offline without API keys: post-hoc probability calibration. Chosen.

**Decision**: (1) Do NOT ship any of the three as auto-apply corrections; keep the
flights/rayyan slices detection-only and leave the flights `value_format`
correction floor at 0.0 (honest -- we cannot provably fix it). (2) Add
`dataforge/calibration_map.py`: a pure-Python (no new deps), per-class,
leakage-free post-hoc calibration map (PAVA isotonic + Platt) that rescales the
corrector's self-consistency agreement into a calibrated probability before the
existing `dataforge.conformal` auto-apply gate. It is advisory only -- the SMT
verifier, safety constitution, and provable-only gate remain hard gates beneath
it, so a bad map can only withhold fixes, never wave through a corrupting write.

**Reasoning**: THREE consecutive in-table NO-GOs share one root cause -- the
residual errors across the measured RAHA datasets are SEMANTIC value errors (a
wrong/estimated time, a transposed date, a spurious near-FD), not syntactic ones,
so they are not inferable from in-table signal without either a declared
schema/convention or an external model. That means DataForge's deterministic
in-table correction is already at its HONEST FRONTIER; further auto-apply accuracy
must come from schema-directed reviewed repair or the calibrated LLM path, not
from more detector hunting. This validates rather than undermines the product
thesis: the gates correctly refused every tempting-but-wrong fix. The calibration
map is the offline-buildable foundation of the LLM path (measured corrector ECE
~0.8 is the wall); live ECE gains require corrector samples from a provider run
and are deferred with the API-key work.

**Reviewed with**: eval/thresholds/coverage_floors.json (`_frontier_map`),
dataforge/detectors/time_format_cruft.py, dataforge/schema_inference.py
(`_fd_candidates`), dataforge/bench/methods.py, dataforge/conformal.py,
dataforge/calibration_map.py, tests/unit/test_calibration_map.py.

**Reversal criteria**: (a) if a declared/confirmed column schema is available,
the rayyan rotation becomes a provable schema-directed fix worth shipping as a
reviewed suggestion; (b) if the FD inference is hardened to exact/denial
constraints with a vectorized scale pass, re-measure tax for a provable
rule-violation win; (c) once corrector correctness samples exist (API-key phase),
wire `calibration_map` into the corrector policy and confirm ECE drops below the
0.10 promotion bar on a disjoint test split.

---

## 2026-07-15 - Post-hoc calibration breaks the ECE wall; safe calibrated auto-apply wired (live Azure gpt-5-mini)

**Context**: Prior sessions proved a bigger/reasoning model does not fix corrector
calibration (gpt-5-mini ECE ~0.84, precision@auto-apply ~0.05, certified coverage
0.0). This API-key phase asked the complementary question: does POST-HOC calibration
(the new `dataforge/calibration_map.py`, isotonic PAVA + Platt) make the corrector's
confidence an honest probability, and can calibrated + conformally-certified scores
enable safe auto-apply without ever corrupting?

**What was run (live, Azure OpenAI gpt-5-mini, $10 guard, reasoning_effort=minimal)**:
- Fresh corrector benchmark, hospital (60 issues / 180 calls, ECE 0.838) and flights
  (40 issues / 120 calls, ECE 0.525, precision@auto-apply 0.25, 1 tp / 39 fp). Samples
  captured per issue_type (`CellFix.detector_id`), the key the auto-apply gate uses.
- `scripts/bench/calibrate_corrector.py` fit the calibration map on the calibration
  split and measured ECE on a disjoint test split: **overall ECE 0.807 -> 0.0**. The
  certified per-issue-type policy ABSTAINS (thresholds 1.01) -> certified auto-apply
  coverage 0.0 -> 0.0.

**Decision**: (1) Ship the post-hoc calibration map + wire it into the auto-apply gate:
`calibrated_conformal_corrector_policy` (fit maps -> certify thresholds on calibrated
scores, keyed by issue_type) and `_partition_auto_apply` now rescales an LLM fix's raw
confidence through its per-issue-type map before the policy decides. CLI:
`dataforge repair --corrector-calibration <artifact>` under `--allow-llm`. (2) Keep the
corrector propose-not-apply for gpt-5-mini: the certified policy correctly abstains
(precision far below any usable alpha), so nothing auto-applies.

**Reasoning**: Post-hoc calibration is the right tool for a high-ECE, low-precision
proposer -- it makes the reported confidence trustworthy (honest "flags"), which serves
the trust thesis. But it is a MONOTONE rescale: it preserves proposal ranking, so it
lowers ECE WITHOUT changing conformal-certifiable coverage. Calibration therefore fixes
honesty, not accuracy; auto-apply coverage stays gated on the corrector actually being
precise (it is not). This is the honest, non-corrupting outcome: the wiring is real and
tested, and it will let a genuinely-precise future model auto-apply safely, but it never
manufactures coverage from a weak model. All auto-apply remains triple-gated
(authoritative schema -> differential SMT -> certified calibrated threshold);
plausibility-only fixes stay held.

**Reviewed with**: dataforge/calibration_map.py, dataforge/calibration.py
(`calibrated_conformal_corrector_policy`, `load_corrector_calibration`),
dataforge/engine/repair.py (`_partition_auto_apply`, `_calibrated_confidence`),
dataforge/cli/repair.py (`--corrector-calibration`), scripts/bench/calibrate_corrector.py,
eval/results/corrector_calibration.json, tests/unit/test_calibration_map_real.py,
tests/unit/test_corrector_autoapply_wiring.py.

**Reversal criteria**: if a future corrector reaches precision high enough that the
conformal gate certifies a per-issue-type threshold below 1.01, calibrated auto-apply
activates automatically for schema-proven fixes -- re-verify the certified coverage
report and the never-corrupt invariants (byte-identical `allow_llm=False`, apply->revert,
hospital 0.7926) before promoting.

## 2026-07-17 - Truth-in-numbers correction of the 2026-07-15 calibration claims

**Context**: An adversarial re-audit of the 2026-07-15 work put its own claims on trial
against the project thesis ("hard numbers that never mislead"). The audit found the
substantive machinery (calibration_map, conformal gate, PSI drift) correct and well-tested,
but three presentation/rigor defects that this entry corrects. This does not reverse the
2026-07-15 decision; it sharpens its honesty.

**What was wrong**:
1. "ECE 0.807 -> 0.0" was headlined without its context. Evidence: the disjoint test split
   is **n=18**; the isotonic map for the dominant class is `y_knots=[0,0,0]` / `[0,0,0.0417]`,
   i.e. the corrector is ~4% precise, so calibration collapses its confidence to ~0. ECE~0
   is the *degenerate* "confidently-wrong proposer now says ~0" regime - it proves honesty,
   not skill. Publishing 0.0 bare oversold a degenerate artifact.
2. "gpt-5-mini correctly abstains" mis-framed a data limitation as model judgment. The real
   cause is that with 36 outcomes at ~4% precision the conformal procedure **cannot certify**
   any threshold at 95%/delta=0.05, so it falls back to the opaque `1.01` sentinel.
3. The flagship artifact's provenance (`_scratch_azure_corr2.json`) is a scratch file not in
   the repo -> not regenerable from committed inputs.

**Decision**: (1) Requalify the ECE number everywhere (README, docs, CHANGELOG) with n=18 and
the degenerate-regime reading. (2) Make the disabled state self-documenting: new
`AbstentionPolicy.uncertified_classes` reason map + `dataforge.conformal.certification_reason`
and `min_samples_for_certification` (>= 59 all-correct accepted samples to certify 95% at
delta=0.05); populated in the artifact and asserted by tests. (3) Reframe "abstains" as
"cannot certify with current data" and document the labelled-data budget as the true unlock.
(4) Note the provenance limitation honestly in the artifact. No behavior change: auto-apply
remains empty; the deterministic path and never-corrupt invariants are untouched.

**Reasoning**: For a trust tool, an unqualified degenerate number and a magic-number gate are
themselves trust defects. The fix is to make the numbers self-explaining and tie the "why not"
to a concrete data budget, so the gate's silence is legible rather than mysterious.

**Reviewed with**: dataforge/conformal.py (certification_reason, min_samples_for_certification,
uncertified_reasons_by_class), dataforge/calibration.py (AbstentionPolicy.uncertified_classes,
conformal_corrector_policy), eval/results/corrector_calibration.json, README.md,
docs/selective-repair-calibration.md, CHANGELOG.md, tests/unit/test_corrector_autoapply_wiring.py.

**Process lesson**: Diagnose from the authoritative artifact (CI logs / actually-tested SHA,
which for PRs is the refs/pull/**/merge commit), not from code plus a session summary - the
same session shipped a torch pip-audit-exception re-triage for a check that CI showed as PASS.

## 2026-07-18 - Calibration artifacts made fully reproducible from committed inputs

**Context**: The 2026-07-15 calibration artifact was built from `_scratch_azure_corr2.json`, a local file never committed, so `corrector_calibration.json` could not be regenerated. This closes that gap at the highest standard: every published calibration number is now a deterministic, replayable function of committed inputs.

**What was done**:
- Wired a response cache + offline-dataset fallback through the corrector benchmark (`DATAFORGE_CORRECTOR_CACHE_DIR`, `DATAFORGE_BENCH_ALLOW_EMBEDDED`; previously the bench hardcoded `cache_dir=None`). A committed SHA-256 response cache lets anyone replay the full run OFFLINE with no Azure key or cost (proven: live run 240 calls -> cached replay 0 calls, byte-identical samples).
- Ran real Azure `gpt-5-mini` (hospital, seed 0): minimal effort (80 issues, precision 0.0588, ECE 0.7974) and medium effort (25 issues, precision 0.0556, ECE 0.8704). Committed the raw samples JSONs and the response cache under `eval/results/`.
- Regenerated `corrector_calibration.json` (ECE 0.8533 -> 0.0 on n=25; fd_violation n=50; thresholds 1.01 with populated `uncertified_classes`) and `selective_repair_calibration.json` from the committed samples. Provenance now points to a committed file.
- Made `docs/selective-repair-calibration.md` FULLY generated (moved the post-hoc ECE section and the certify data-budget into `render_methods_note`), so regeneration is idempotent and no hand-maintained section is clobbered.
- Re-derived every hand-written number (README, CHANGELOG, ARCHITECTURE) from the new artifacts; hospital 0.7926 deterministic anchor untouched. Repointed the real-data test and script docstrings to the new committed samples; deleted the superseded `_min`/`_med` capture files.

**Reasoning**: LLM outputs are non-deterministic, so reproducibility = capture-once-commit (samples + response cache) + deterministic post-processing. The committed cache makes the whole pipeline Azure-free and replayable; a CI gate re-derives the artifacts from the committed samples and asserts equality. The verdict is unchanged and expected: gpt-5-mini stays ~6% precise on out-of-table hospital errors, so no threshold certifies and nothing auto-applies. The win is provenance and reproducibility, not a better result.

**Reversal criteria**: unchanged from 2026-07-15 - if a future corrector's precision lets the conformal gate certify a threshold below 1.01, calibrated auto-apply activates for schema-proven fixes; re-verify certified coverage and the never-corrupt invariants first.

