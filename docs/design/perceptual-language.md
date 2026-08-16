# The Earned-Salience Perceptual Language

> The perceptual language of DataForge is the nervous system of a working
> relationship between a human and a verification engine. Its one law: **every
> perceivable signal is a claim the system makes about how much it can prove, and
> every claim must be true.** A confident color on an unproven value is not a
> style choice; it is the interface equivalent of data corruption, because the
> human decides — to trust, to accept, to interrupt, to verify — *through* these
> signals.

This document is the constitution the token generator, motion system, agent-state
rendering, trust surfaces, and the CLI/MCP text twin all implement. It is
descriptive of intent and prescriptive of law. Where an implementation and this
document disagree, the implementation is the bug.

---

## 1. Why this language exists (the product truth it must tell)

DataForge is a trust layer, not a data cleaner: it "fixes only what it can prove
correct, honestly flags everything it cannot, never corrupts data, and proves all
three with a reproducible, reversible, self-verifying certificate"
([PRODUCT.md](../../PRODUCT.md)). Its central correctness property is **trust
calibration**, and its own doctrine names the asymmetry that governs this design:

> a confidently-wrong fix is worse than no fix, because it silently corrupts the
> source of truth and is discovered late, if ever.

Therefore the deadlier failure of this interface is **overtrust**, not undertrust.
A beautiful interface can manufacture unearned confidence; this language is
engineered so that it *cannot*. The system must be exactly as reassuring as it
deserves to be, and not one lumen more.

---

## 2. The one law: Earned Salience

**No signal may wear a treatment that a stronger rung has earned.**

Warrant — how much the system can prove about the claim a signal represents — is
carried by *warrant channels* and is strictly monotonic across the rung ladder:
fill, stroke, ground contact, glow, depth, and weight. The more DataForge can
prove, the more *settled, solid, and complete* the signal. The less it can prove,
the more *provisional, outlined, and unresolved* the signal.

The consequence is the property the product needs: **an unproven value physically
cannot wear the treatment reserved for proof.** Overtrust is not discouraged by
guidelines; it is made unrenderable.

### 2.0 Three quantities, disjoint channels (amended 2026-08-14)

This law previously read "perceptual intensity is a strictly monotonic function of
epistemic strength", with intensity defined as "the sum of a signal's **chroma**,
motion amplitude, glow, weight, and form-completeness". That version was wrong,
and measurement proved it: chroma violated the requirement on **five of six**
adjacent rung pairs in the light theme and four of six in the dark theme, with
`rejected` carrying 2.27x the chroma of `proven`. The full measured tables are in
[SPEC_perceptual_verification.md](../../specs/SPEC_perceptual_verification.md) §2.

The palette was not the bug. The law was, in two independent ways. It contradicted
§3.1 of this document — "Color answers 'what is this?', never 'how loud is this?'"
— because a quantity cannot be pure semantics *and* a monotonic summand of
intensity. And it contradicted good practice: a calm proof and a loud rejection is
correct, since attention belongs where a human is needed. Enforcing the old
wording would have required making failures quieter than proofs.

"Intensity" conflated three questions a trust surface must answer separately:

| Quantity | Question | Kind | Channels |
| --- | --- | --- | --- |
| **Warrant** | How much is proven? | ordered, strictly monotonic | fill, stroke, ground contact, glow, depth, weight |
| **Identity** | What kind of claim is this? | nominal, unordered | hue |
| **Urgency** | Must a human act? | ordered on its own axis | chroma, and only channels declared for it |

The channel sets are disjoint, and that disjointness is gated. Signalling urgency
through a warrant channel — giving a rejected claim ground contact or glow so it
"stands out" — is the precise overtrust this language exists to prevent, and it
now fails the build.

The split follows the standard channel taxonomy, in which identity channels are
nominal and magnitude channels are ordered (Bertin's levels of organisation;
Tamara Munzner, *Visualization Analysis and Design*, A K Peters, 2014).

### 2.1 The epistemic-strength ladder

Every signal in the product resolves to exactly one rung. The rung determines the
complete perceptual sentence (color + motion + form + text). See §4 for the sentences.

| Rung | Meaning (product vocabulary) | Salience |
|---|---|---|
| **Proven** | `verification_strength = proven`: deterministic, or verified against an authoritative declared/reviewed schema. Applied and byte-for-byte reversible. | Highest. Solid fill-weight, may glow, motion *settles*. |
| **Corroborated** | Proven **and** `independent_verification = agreed`: two independent verifiers concurred. | Proven + a distinct witness accent. The only intensifier above proven. |
| **Plausibility-only** | `verification_strength = plausibility_only`: an LLM/`external` value with no authoritative schema. Never silently written. | Deliberately low. Provisional hairline/dashed form, **never glows**, motion *hovers* and never settles. |
| **Held / Abstention** | Detected but not applied, with a `review_reason`. Includes honest abstention ("the correct value is not derivable — refusing to guess is correct behavior"). | Warm, arrested. Motion *pauses*. Framed as a first-class outcome, not a failure. |
| **Downgraded** | Was proven/auto-applied, then relaxed to review by drift (PSI) — "the guarantee is never claimed outside its scope." | A settled thing visibly *relaxing* back to held. |
| **Rejected / Unknown** | SMT `REJECT`, or `UNKNOWN` (fails closed), or audit `tampered`/`malformed`. | Decisive. Motion *recoils* once, never loops. |
| **Idle / Not-run** | No claim is being made. `not_run`, queued, unstarted. | Zero. Perfectly still. Stillness is punctuation. |

Ladder invariant (enforceable, see §7): for any two rungs A above B, A's rendered
salience ≥ B's on every channel. No plausibility signal may out-glow, out-move, or
out-weight a proven one.

---

## 3. The three channels and their grammar

### 3.1 Color is semantics — *what a thing means*

Color answers "what is this?", never "how loud is this?". The seven audit-locked
OKLCH seeds ([generate_color_system.mjs](../../playground/web/scripts/generate_color_system.mjs))
are reassigned to epistemic roles:

| Seed | Hue | Epistemic role |
|---|---|---|
| success (viridian, c≤0.04) | 152 | **Proven.** Reserved for verified, applied, reversible outcomes. |
| data (teal-steel) | 188 | **Evidence / verification-in-progress / corroboration accent.** Facts and the act of checking. |
| agent (ultraviolet) | 298 | **Plausibility.** Model cognition that has *not* been proven. Uncommitted. |
| warning (antique brass) | 78 | **Held / abstention / downgraded.** Honest not-applied. |
| danger (carmine) | 18 | **Rejected / unknown / tampered.** Fails closed. |
| brand (aurelian cinnabar) | 34 | **Human command.** The operator's authoritative acts: accept-constraint, confirm-escalation, revert. Not a claim about the agent. |
| neutral (warm mineral) | 92 | **Structure / idle / magnitude.** Surfaces, text, and confidence-as-magnitude. |

Economy is power. Each additional color-meaning devalues the rest. New meanings are
added only when they predict a distinct user action (see §8 forbidden patterns).

**Confidence is a magnitude, not a verdict.** The `confidence` scale
(high/medium/low) is rendered in **neutral tones only** — it may never borrow
proof-green or danger-carmine, because a confidence number is not a proof. Post-hoc
calibration collapses a weak proposer's confidence toward zero (README: ECE
0.85 → 0.0 while the corrector stays ~6% precise) — "calibration fixes honesty, not
accuracy." A green "high confidence" chip on an unproven LLM value is precisely the
lie this language forbids. Confidence therefore appears **only inside a
plausibility container**, as a monochrome magnitude, always beside the words "not
written."

### 3.2 Motion is causality and time — *what is happening*

Motion must explain, or it must not exist. Attention is borrowed, never owned.
Seven primitives, each mapped to a real causal event and each with a
meaning-preserving reduced-motion twin (§6):

| Primitive | Answers | Physical grammar |
|---|---|---|
| `settle` | "this is proven and committed" | Converges decisively to rest (high-damping spring). Done, reversible. |
| `hover` | "this is a proposal, uncommitted" | Low-amplitude, **never resolves to rest**. The honest face of plausibility. |
| `resolve` | "verification is in progress" | Determinate, directional progress. Honest — no fake progress, no theatrical delay. |
| `pause` | "held / abstained deliberately" | Motion arrested mid-gesture and held. |
| `recoil` | "rejected / failed" | One decisive counter-motion, **no loop**. |
| `still` | "idle, no claim" | No motion at all. |
| `downgrade` | "drift relaxed a proof back to review" | A settled element visibly loosening from solid to provisional. A truth event that earns motion. |

**Time-honesty.** Latency may be *masked* (waiting made legible, progress made
truthful) but never *lied about*. `resolve` animates only while work is genuinely
happening. Nothing loops on an idle or finished agent. A loop is a claim of ongoing
activity and must be true.

### 3.3 Stillness and silence are punctuation — *what does not demand attention*

Stillness is a first-class material. Idle is perfectly still; a finished proof
settles and then rests. An interface that is always alive is always shouting, and
shouting cannot be calibrated. The supreme state of this system is quiet: the
intelligence is visible and truthful precisely because most of the surface is not
moving.

---

## 4. Unified state sentences (color + motion + form + text)

Every meaningful state has exactly one sentence. No two may contradict (uncertain
must never move like confident; completed must never wear pending's hue). Text is
mandatory on every sentence — the redundancy law (§5) requires meaning to survive
color and motion removal.

| State | Color token family | Motion | Form | Text label (visual / CLI twin) |
|---|---|---|---|---|
| Proven, applied | `--df-proven-*` (viridian) | `settle` | Solid fill-weight border; glow-eligible | "Proven — applied, reversible" / `proven` |
| Corroborated | `--df-corroborated-*` (viridian + teal witness accent) | `settle` | Proven + second witness mark | "Proven — independently verified" / `proven (independent: agreed)` |
| Plausibility-only | `--df-plausibility-*` (ultraviolet) | `hover` | Hairline/dashed, unfilled; no glow | "Plausibility-only — not written" / `plausibility-only (not written)` |
| Held / abstention | `--df-held-*` (brass) | `pause` | Solid but warm; explicit reason | "Held for review — <reason>" / `held: <humanized reason>` |
| Downgraded (drift) | `--df-downgraded-*` (brass) | `downgrade` | Was-solid, now provisional | "Downgraded by drift — now review" / `downgraded: distribution drift` |
| Rejected / unknown | `--df-status-danger-*` (carmine) | `recoil` | Struck / blocked | "Rejected — <verifier reason>" / `rejected: <reason>` |
| Verifying (in progress) | `--df-proof-*` (teal) | `resolve` | Determinate progress | "Verifying…" / `verifying` |
| Idle / not-run | neutral | `still` | Quiet | "Idle" / (omitted) |
| Human command | `--df-action-*` (cinnabar) | `settle` on commit | Filled command | "Accept" / "Confirm" / "Revert" |

### 4.1 Agent states → real events (no words nobody speaks)

The prior 12-value `AgentMotionState` had 10 states that were colored in a legend
but never rendered on any live surface. A language with words nobody speaks
communicates nothing. Every surviving agent state must map to a **real** pipeline
or trace event and render live. The legible set, each tied to an event source and a
sentence above:

| Agent state | Real event source | Rung / sentence |
|---|---|---|
| `verifying` | stage running (schema_inference, detectors, smt_verifier) | Verifying — `resolve`, teal |
| `proposing` | agent trace FIX attempt, pre-verdict | Plausibility — `hover`, ultraviolet |
| `proven` | fix accepted, deterministic/authoritative | Proven — `settle`, viridian |
| `held` | fix held with review_reason | Held — `pause`, brass |
| `rejected` | SMT REJECT / safety denied | Rejected — `recoil`, carmine |
| `asking` | requires_human / escalation | Held (needs a human) — `pause`, brass |
| `done` | receipt complete, residual resolved | Proven/settled at rest, then `still` |
| `idle` | queued / not started | Idle — `still`, neutral |

States with no real event (`recovered`, `delegated`, `interrupted` as distinct
colors, the unused `attention`/`isLooping` metadata, and `workflowEventToMotion`)
are retired. If a future real event needs one of them, it is re-added with a live
render path, not as a legend entry.

---

## 5. The redundancy law (accessibility is correctness)

**No meaning may ever be carried by color alone.** Every state sentence encodes its
meaning redundantly across at least two of: text, form, position, motion. A
semantic distinction invisible to a colorblind user is a broken feature for that
user, not a lesser aesthetic.

Consequences enforced downstream:
- Proven vs plausibility differ in **form and motion**, not only hue (solid+settle
  vs dashed+hover). Under grayscale the distinction survives.
- `independent_verification` gets a **mark and words** ("independently verified"),
  not just a color.
- Reduced-motion twins preserve *meaning*, never merely delete movement (§6).
- The CLI/MCP text twin (§9) is the fully non-visual rendering of the same ladder,
  so a screen-reader user and a `NO_COLOR` terminal user receive the identical
  claims.

---

## 6. Reduced-motion, high-contrast, colorblind — degrade together, stay honest

The combined system must remain honest when any single channel is unavailable.

- **Reduced motion:** each primitive has a twin that keeps the claim. `hover`
  (never-settles) → a persistent dashed, unfilled *static* state (still reads
  "provisional"). `settle` → an instant solid state (still reads "done"). `resolve`
  → a determinate static progress value, updated stepwise (still honest about
  progress). `recoil` → an instant struck state. No twin may upgrade a rung (a
  reduced-motion plausibility signal must not look proven).
- **High contrast:** inherits the audited high-contrast token overrides; form and
  text carry meaning regardless.
- **Colorblind:** guaranteed by the redundancy law — every distinction survives the
  removal of hue because form + text + position + motion carry it independently.
- **Vestibular / photosensitive safety:** amplitudes are small, no large-area
  flashing, `resolve` is directional not strobing, and the global
  `prefers-reduced-motion` guard remains absolute.

---

## 7. Verification and falsification

This language is a set of claims and must itself be falsifiable. Treat each as a
test (implemented in Phase 6):

1. **Overtrust-impossibility (the core property).** Assert that no
   `plausibility_only` value can render with proven form, proven glow, or `settle`
   motion. If any code path can produce it, the language has failed and the build
   must fail.
2. **Ladder monotonicity.** For any two rungs, the higher rung's salience is at
   least the lower's on every **warrant** channel (fill, stroke, ground contact,
   glow, depth, weight). Hue and chroma are excluded: they carry identity and
   urgency, not warrant (§2.0). Warrant computations may not read a hue or chroma
   value at all, so the exclusion is structural rather than a rule to remember.
3. **Redundancy survival.** Every state distinction is still readable under (a)
   grayscale, (b) dichromacy — deuteranopia, protanopia, tritanopia, (c) reduced
   motion, (d) `NO_COLOR` text. **Verified by simulation over every ordered rung
   pair in both themes, not asserted.** This item read "Automated where possible,
   audited otherwise" until 2026-08-14, and it had never been automated; the
   colourblind-safety claim in §5 was an argument, never an execution.
4. **Honest motion.** No loop exists on an idle/finished element; `resolve` runs
   only while work is happening. (Audited in the motion source, §6.)
5. **Predictability (the real definition of "intelligent-feeling").** A user who has
   seen the language for a week can predict a signal's meaning *before* reading its
   text. Validated with real users (§10); until then it is an explicit unknown.

Falsifiers for the whole direction: if users cannot predict signals; if any
unproven value can be made to look proven; or if "provisional" motion reads as
"broken/loading" rather than "uncommitted." Each has a validation task in §10.

---

## 8. Forbidden patterns

These are lies or attention-taxes and are prohibited (and, where possible, blocked
by the audit):

- **Glow on anything unproven.** Glow is earned salience reserved for proven/applied
  (and legitimate human command). Never on plausibility, held, or failure.
- **Proof-green or danger-carmine on a confidence indicator.** Confidence is a
  neutral magnitude only.
- **Confident hue/motion on a plausibility value.** No `settle`, no solid fill, on
  `plausibility_only`.
- **A loop on an idle or finished agent.** Looping is a claim of ongoing activity.
- **Fake or theatrical progress.** No animation that performs "thinking" the system
  is not doing; no smoothness that hides a failure.
- **Meaning carried by color alone.** Every distinction needs a non-color twin.
- **A pulsing glow to signal "aliveness."** Aliveness bought with the user's calm
  is noise.
- **New color-meanings that predict no distinct user action.** Decoration wearing
  the costume of meaning.

---

## 9. The non-visual twin (CLI / MCP / API text)

Color and motion are the playground's channels; the same ladder must be legible with
neither. The CLI, MCP, and playground API render the identical epistemic vocabulary
as text through one shared humanizer (Phase 5), so that a `NO_COLOR` terminal, a
screen reader, and the browser all receive the same claims:

- `verification_strength` → the words "proven" vs "plausibility-only — not written"
  (never inferred silently from a color).
- `review_reason` → a humanized phrase, not a raw machine token
  (`floor_cannot_verify` → "the deterministic floor could not verify a correct
  value").
- `independent_verification` → "independently verified (two verifiers agreed)" vs
  "single verifier".
- Disposition → one vocabulary everywhere: applied / held / rejected / escalated.
- Color is never the sole channel: severity and provenance always carry a glyph or
  label, and `NO_COLOR`/non-TTY output stays fully meaningful.

---

## 10. Open unknowns → prioritized validation work

Honest unknowns are deliverables; hidden ones become a user's confusion.

1. **Predictability (P1).** Run a lightweight comprehension test: after brief
   exposure, can users name a signal's rung before reading text? Falsifies §7.5.
2. **"Provisional vs broken" (P1).** Verify `hover`/dashed reads as *uncommitted*,
   not *loading/error*, with real users. If it reads as broken, revise the plausibility
   form.
3. **Corroboration legibility (P2).** Confirm the independent-verification accent is
   noticed and understood as "two verifiers", not decoration.
4. **Long-session calm (P2).** Confirm that in hour eight the moving surface (only
   `resolve` while working) does not fatigue; audit that stillness dominates.
5. **Downgrade comprehension (P3).** Confirm `downgrade` reads as "the guarantee was
   withdrawn because the world changed", not "something broke".

---

## 11. Where this is implemented

- Color tokens & audit: [generate_color_system.mjs](../../playground/web/scripts/generate_color_system.mjs),
  [audit_colors.mjs](../../playground/web/scripts/audit_colors.mjs).
- Motion source & primitives: [motion.ts](../../playground/web/src/motion.ts) (single
  source; CSS motion vars generated from it).
- Agent states & trust surfaces: [App.tsx](../../playground/web/src/App.tsx),
  [observatory.ts](../../playground/web/src/observatory.ts), [styles.css](../../playground/web/src/styles.css).
- Text twin: shared humanizer in [dataforge/ui/](../../dataforge/ui/), consumed by the
  CLI, [playground/api/app.py](../../playground/api/app.py), and
  [dataforge-mcp](../../dataforge-mcp/dataforge_mcp/tools.py).
