# The Quantitative Grammar

> A companion to [the Earned-Salience Perceptual Language](perceptual-language.md),
> under its authority. Where this document and that one disagree, that one wins.
>
> The perceptual language governs **what kind** of claim a signal makes. It has no
> vocabulary for **how much**. This document supplies it — and closes a hole the
> one law leaves open: a constitution that governs single signals but not their
> superposition is enforceable only on an empty screen.

---

## 1. Why this document exists

The perceptual language is a complete grammar for the *rung* of a claim: proven,
corroborated, plausibility-only, held, downgraded, rejected, idle. Every law in it
answers "is this signal's claim true?".

None of them answers "how much?". There is no law for scale, axis, comparison,
distribution, or aggregation. The one magnitude token the system has —
`confidence` — is deliberately neutralised to neutral tones by the
`auditEarnedSalience` build gate, and correctly so: a green "high confidence" chip
on an unproven value is the overtrust lie.

The consequence is observable. Before this document, the product rendered **no
quantity graphically at all**. Every number — flagged-cell counts, severity
distributions, certificate check totals, per-issue counts, agent step budgets —
was text. The three graphics that existed were a CSS `scaleX` rail sweep, a
fixed-width loading sweep, and categorical border styles. The
`--df-data-glow` / `--df-action-glow` / `--df-proof-glow` tokens, the sanctioned
strongest-salience channel, had zero consumers.

That was not an oversight of craft. It was the absence of a law. A designer working
honestly under the perceptual language alone has no sanctioned way to draw a
magnitude, so the safe move is to draw nothing.

This document makes drawing quantity legal, bounded, and falsifiable.

---

## 2. L1 — The quantity law

**Magnitude is encoded by position or length. Never by volume. Never by area for
comparison. Never by hue.**

Cleveland and McGill (1984, *Journal of the American Statistical Association*
79(387):531-544) ranked elementary perceptual tasks by measured accuracy of
magnitude judgement. In decreasing order:

1. Position along a common scale
2. Position along identical, non-aligned scales
3. Length
4. Angle / slope
5. Area
6. **Volume, curvature**
7. Shading, colour saturation

The two channels a "3D chart" recruits — volume and perspective-distorted length —
are 6th and worse. The two channels this grammar permits are 1st and 3rd. This is
not taste; it is the reason the rule is short.

Three corollaries:

- **Hue is never a ramp.** Hue is semantics under the perceptual language §3.1;
  reusing it as magnitude would make a single channel carry two contradictory
  meanings. This independently forbids rainbow and heat colormaps, whose
  perceptual non-uniformity is separately documented (Borland and Taylor 2007;
  Crameri, Shephard and Heron 2020).
- **Confidence stays neutral, and stays contained.** Per perceptual-language §3.1,
  confidence renders in neutral tones only, inside a plausibility container,
  beside the words "not written". This grammar adds nothing and relaxes nothing.
- **Quantised data is shown as a distribution, not a gradient.** Where the
  underlying values are near-degenerate, a continuous encoding implies resolution
  the data does not have. In the measured hospital condition with inferred
  functional dependencies, 10,261 of 10,373 flagged cells carry confidence exactly
  0.95 (`eval/results/detector_queue_composition.json` records 23 distinct
  confidence values across that queue). A confidence gradient over that field
  would render as flat colour while claiming continuous precision. Show the spike.

---

## 3. L2 — The addressability law

This is the load-bearing law, and it took two wrong formulations to reach.

The one law binds perceptual intensity to epistemic strength. Now consider drawing
the measured hospital-with-FDs queue — 10,373 flagged cells, 52% of all 20,000
cells in the table — into a region 800 by 400 pixels. There are more marks than
pixels. Marks collide.

Under additive or alpha-accumulating blending, forty stacked plausibility-only
cells in one pixel are brighter than one proven cell. **Overplotting silently
converts density into intensity, and intensity is legally bound to epistemic
strength.** No one wrote a lie. The renderer manufactured one out of arithmetic.

The first two attempts both tried to pick a *representative* rung for the bin. Both
were wrong, because a bin is a **set** and every single representative of a set
misreports it. The law is therefore not about which rung to choose:

> **A mark that is not individually addressable may not carry a rung.**
>
> Aggregated marks encode only magnitude, in a neutral channel. Rung — hue, fill,
> stroke form, glow, depth — is rendered only on marks that represent exactly one
> claim. Additive and lighten blend modes remain forbidden everywhere.

A mark is **individually addressable** when it stands for exactly one claim and is
tall enough to be perceived and operated as an object: at least
`addressableMinHeightPx` (16), and reachable by keyboard as its own element.

### 3.1 What this buys, and why it is stronger than either alternative

- **Overtrust becomes structurally impossible rather than carefully avoided.** The
  overview carries no rung, so there is no rung to overstate. No aggregation rule
  is needed at all: minimum-rung, maximum-rung, and the `mixed` marker are not
  chosen between — they are deleted.
- **Within a single rung, density may legally modulate intensity**, because there
  is no other rung to out-shine. That is exactly what a neutral density channel is,
  and it is the only intensity ramp this grammar permits.
- **Depth becomes legible.** An addressable mark is around 24px, so it can have a
  ground for a contact shadow to fall on. See L5.
- **The renderer follows the nature of the mark.** A density field has no
  individual identity, so it is drawn as *pixels*. A claim has identity, so it is a
  *DOM object* — focusable, labelled, selectable — and satisfies the redundancy law
  by its own markup rather than by a duplicated table beside it.

### 3.2 The two rejected formulations, recorded

**Maximum rung** promotes a bin to its strongest member, so forty unproven cells
containing one proven fix render with proven form, fill, and glow. On the measured
hospital FD queue that map would look dramatically more proven than the run was —
precisely the overtrust bias this law exists to prevent. A rule that contradicts its
own motivation is not a rule.

**Minimum rung** cannot overstate, and shipped first. But it was measured to
*suppress* the proven signal in exactly the regime the map exists for: at roughly
seven cells per bin in the FD regime, proven cells are co-binned with held ones and
vanish. Because collisions are per-(column, band), min-rung is near-identity when
the queue is sparse and near-total-erasure when it is dense — a regime dependence
that was never quantified when it was chosen. Understating is the safer direction to
err, but a channel that erases the product's headline claim is not a tradeoff; it is
a broken feature.

Both failures share one root cause: forcing one mark to carry density *and* rung
*and* identity. Shneiderman's *"The eyes have it: A task by data type taxonomy for
information visualizations"* (IEEE Visual Languages, 1996) prescribes **overview
first, zoom and filter, then details-on-demand**. Collapsing those three stages into
a single artifact is what made a lie unavoidable. Separating them removes the
conflation instead of trading against it.

### 3.3 The rung order, still needed

Rung strength is the system's warrant for writing a change to that cell. It no
longer governs aggregation, but it still orders salience (L5) and sorts detail
views:

| Rank | Rung | Warrant |
|---|---|---|
| 6 | corroborated | Proved, and two independent verifiers agreed |
| 5 | proven | Proved once |
| 4 | held | Not proved, deliberately withheld with a reason |
| 3 | downgraded | Was proved; guarantee withdrawn by drift |
| 2 | plausibility_only | Unproven proposal |
| 1 | rejected | Proved *not* safe — zero warrant |
| 0 | idle | No claim |

`downgraded` sits **below** `held`, which is not the intuitive placement and is
deliberate. A proof that drift withdrew is a proof that did not hold; its extra
information is historical, not epistemic, and its present warrant to be written is
nil, exactly like `held`.

`idle` is rank 0 and is **never drawn**. A region with no flagged cell is an L3
absence, not a rung-0 mark.

**Enforcement is architectural, not advisory.** The density encoder emits marks with
no rung field at all, so a rung cannot reach an aggregated mark even by mistake, and
`audit_quantitative.mjs` fails the build if a rung token is referenced from a
density-renderer module.

---

## 4. L3 — The absence law

**Empty is a claim too. Zero, not-measured, and truncated are three different
statements and must render differently, each with text.**

Three states currently all render as nothing, and they mean entirely different
things:

| State | Meaning | Example in this system |
|---|---|---|
| **Zero** | Measured, and none found. A positive result. | A clean column with no flagged cells. |
| **Not measured** | No measurement was taken. Says nothing about the value. | `tax` has no measured issue count in any artifact under `eval/results/`. `receipt.review_ranking` is empty because no ranker was passed, not because nothing ranked. |
| **Truncated** | Measured, but what you are seeing is a subset. | `ISSUE_ROW_DISPLAY_LIMIT = 50` caps `row_indices` per issue group; a group can hold ~1,000 rows. |

Collapsing these is the most dangerous class of quantitative lie available here,
because it is a lie of *omission that looks like completeness*. A cell map built
naively from `issues[].row_indices` would render roughly 50 of 1,000 flagged cells
and look finished. Nothing on screen would be false. The screen as a whole would
be.

**No quantitative component may ship without declaring its absence rendering.**
This is checked by `audit_quantitative.mjs`, not left to review.

A second corollary, applied in this work: where evidence exists in one direction
only, the asymmetry is rendered, not smoothed. Unsatisfiable cores exist for
rejections; an accepted cell has a proof obligation with status `accepted` but no
positive constraint trace. The honest rendering says "we can show you why we
refused, and we have no positive proof trace for what we accepted." Manufacturing
a symmetric-looking display would be an L3 violation.

---

## 5. L4 — The attention-cost law

**Attention is budgeted, not merely borrowed.**

The perceptual language says attention is borrowed and never owned, and lists
long-session calm as an open unknown (§10, item 4). Borrowing without a budget is
how interfaces become exhausting one honest animation at a time: every individual
motion answers a real question, and the sum is noise.

The budget:

- **Stillness dominates.** At rest, the moving fraction of any view is zero.
- **At most one `resolve` in motion per view.** `resolve` is the only primitive
  that may loop during work (`audit_motion.mjs` restricts `infinite` to `hover`
  and `resolve`). Two concurrent determinate progress indicators do not convey
  twice the information; they convey that the designer did not choose.
- **No animation without a state transition that actually occurred.** This
  forbids, specifically, force-directed graph layouts that jitter toward
  equilibrium: the jitter is motion with no referent event, which perceptual
  language §8 already prohibits as "fake or theatrical". Layouts in this grammar
  are deterministic and arrive composed.

---

## 6. L5 — The depth law

**Depth encodes ordinal epistemic strength and nothing else, and only on marks that
are individually addressable.**

Depth is admitted because it is *position* (L1 channel 1), not volume (channel 6),
and because the ladder it encodes is already ordinal. It is admitted under seven
constraints, all of which must hold:

1. **Addressable marks only.** Depth may appear only on a mark representing exactly
   one claim, at least `addressableMinHeightPx` tall. This is the L2 consequence and
   it is the constraint the first version of this law lacked — see §6.3.
2. **Ordinal only.** Depth takes one fixed constant per rung. It is never scaled by
   a data value, a count, a confidence, or a duration.
3. **Ground contact is the signal, not height.** Proven is *in contact* with the
   plane and casts a tight contact shadow. Plausibility-only is *detached* and
   casts none. The perceptual claim is landedness: a proven fix has landed, an
   unproven one is still in the air, and you do not ship what has not landed.
4. **Orthographic projection.** No perspective, therefore no foreshortening, and
   therefore no distortion of any length the viewer might compare. On an addressable
   mark this is satisfied trivially, because the mark is a laid-out DOM element and
   the shadow is a CSS `box-shadow` — there is no projection to get wrong and no
   shader to verify.
5. **No camera control.** No orbit, no rotation, no parallax on scroll. WCAG 2.1
   Success Criterion 2.3.3 (Animation from Interactions, Level AAA) requires that
   interaction-triggered motion animation be disableable unless essential, and its
   Understanding document names parallax specifically as a vestibular trigger with
   reactions including nausea, migraine, and needing bed rest. Camera state also
   could not be persisted here even if wanted: `localStorage` and `sessionStorage`
   are prohibited across the playground.
6. **Bounded amplitude.** At most 6 device-independent pixels of offset. Depth is
   a categorical cue, not a spatial experience.
7. **The reduced-motion and flat twins must not upgrade a rung.** Removing depth
   removes the shadow and the offset; hue, form, and text still carry the rung
   unchanged, per the redundancy law. `box-shadow` is a static property, so no
   keyframe is involved and `audit_motion`'s transform/opacity restriction is
   untouched.

### 6.1 The rung-to-depth table

Ordered strongest to weakest. `ink` is the measured fill plus stroke coverage;
`total` is the measured salience asserted monotonic in §6.2.

| Rung | Offset | Ground contact | Form | Measured total |
|---|---|---|---|---|
| Corroborated | 0 | Tight contact shadow + witness accent | Filled, solid | 4.5 |
| Proven | 0 | Tight contact shadow | Filled, solid | 4.0 |
| Held / abstention | 0 | None (on-plane, arrested) | Filled, solid, warm | 2.0 |
| Downgraded | +3 | Weakening | Filled, dashed — was solid, now loosening | 1.75 |
| Plausibility-only | +6 | **None** | Unfilled, dashed hairline | 0.5 |
| Rejected / unknown | -4 | None (below plane) | Unfilled, struck | 0.25 |
| Idle / not-run | 0 | None | Not drawn | 0 |

The weights that produce these totals live in `salienceWeights` in
`src/design/quantitative-tokens.json`, so the measure and the gate read the same
numbers.

### 6.2 The obvious objection, and how it is settled

A floating mark might read as *more* prominent than a landed one, which would
invert the one law. Two answers:

- **Structural:** floating marks are unfilled dashed hairlines with no shadow.
  Their ink, contrast, and chroma are all strictly lower than a filled, shadowed,
  solid proven mark. Detachment costs salience; it does not buy it.
- **Measured, not asserted:** ladder monotonicity is computed per rung — total ink,
  contrast, and glow — and asserted monotonic in the test suite. If adding depth
  ever raises a lower rung's measured salience above a higher rung's, the build
  fails.

### 6.3 Why constraint 1 exists: this law shipped dead once

The first version of this law had no addressability constraint, and depth was
consequently **unrenderable on every real dataset**. The arithmetic, recorded
because the lesson is the point:

```
hospital: 1000 rows
  surface height = clamp(rows, 180, 420)        = 420px
  binCount       = min(rows, floor(420 / 3))    = 140
  mark height    = 420 / 140                    = 3.0px
  depth legibility floor                        = 8px
  -> depth = 0, always
```

Depth rendered only for tables of 22 rows or fewer — that is, only for the 10-row
sample fixture the tests exercised. The tests passed on a configuration that never
occurs in production, and the feature was a token table, a monotonicity gate, and a
shader path for a channel no user could ever see.

That is exactly the failure the perceptual language names in its §4.1: *"The prior
12-value `AgentMotionState` had 10 states that were colored in a legend but never
rendered on any live surface. A language with words nobody speaks communicates
nothing."* A law with a channel nobody can see is the same defect one level up.

The fix is not a bigger offset. It is to render depth only where a mark is an object
with a ground — which is what L2 already implies, and what constraint 1 now makes
binding. **A perceptual law must state the conditions under which its channel is
perceivable, or it is not a law.**

---

## 6a. L1a — The common-scale corollary

> **A mark encoding magnitude by position or length must render a scale.**

L1 justifies itself with Cleveland and McGill, whose **rank 1 is "position along a
common scale"** and whose rank 2 is the same channel on *identical, non-aligned*
scales. The difference between the top two ranks in the ranking this document
cites is precisely whether the scale is shared and drawn.

None of the four components drew an axis, a tick, or a label. The confidence
histogram was worse than unlabelled: it normalised every class to **its own peak**
(`Math.max(...entry.bins.map((bin) => bin.count), 1)`), so bar heights were not
comparable across classes even in principle. The document was claiming rank 1 for
marks that were at best rank 2, and in one case not on the ranking at all — a
length with no scale is a shape.

`neutral_count` is exempt because the density field encodes binary presence, not a
magnitude: there is nothing to place on a scale, and `PRESENCE_ALPHA` is a constant
for exactly that reason.

The confidence histogram now encodes each bin as a **share of its class on a common
0–100% axis with quarter-step ticks**, and states the class size as text. Share is
the quantity that is comparable across classes of wildly different sizes; a shared
count axis would have rendered a 5-cell class invisible beside a 10,261-cell one.

## 6b. L6 — The interval law

> **No aggregate proportion may be rendered as a point when its denominator is
> known. The interval is the mark; the point estimate is subordinate to it.**

The frontend rendered no uncertainty at all. Searching `src/` for `interval`,
`error bar`, `quantile`, `variance`, `stderr` and `percentile` returned nothing;
the only carrier of uncertainty was an optional free-text string on a workflow
stage, rendered as a table row.

That became a contradiction when the backend's `TrustLedger.as_dict()` began
emitting a Clopper-Pearson bound and a scope caveat **by construction**, precisely
so a consumer could not read the point estimate alone. The frontend was a consumer
that could only read point estimates.

Every bin of the confidence histogram is a count out of a known class total, so it
is a binomial proportion with a computable interval, and there is no missing-data
excuse of the kind that legitimately blocks the calibration plot (§9).

Three implementation constraints, each load-bearing:

- **One definition of "interval" for the product.** `viz/interval.ts` implements
  Clopper-Pearson, the same method as `dataforge/metrics/trust_ledger.py`, and is
  verified against the same numeric goldens rather than against the Python code.
  Two independent implementations agreeing on shared values is the pattern
  established for the attestation verifier.
- **Log space, not binomial coefficients.** The Python version multiplies
  `math.comb(n, k)` directly, which is fine at n = 17 but not at the measured
  n = 10,373: the coefficient overflows a double long before the powers underflow
  to compensate. `viz/interval.ts` sums in log space via Lanczos log-gamma.
- **Not error bars.** Error bars are systematically misread, including a
  "within-the-bar" bias in which values inside a bar read as more likely than
  values outside it (Correll & Gleicher, *Error Bars Considered Harmful*, IEEE
  TVCG 20(12):2142–2151, 2014). The band is the estimate's extent, and the point
  estimate is a 2px line inside it rather than a filled area, so it cannot read as
  the whole claim.

A component that renders no interval must declare an exemption in writing.
Silence is how a point estimate ships as though it were exact; the three exempt
components each say why they have no denominator.

---

## 7. Verification and falsification

Each law is a claim, and each is falsifiable.

| Law | How it is checked | What falsifies it |
|---|---|---|
| L1 quantity | `audit_quantitative.mjs` rejects hue ramps and volumetric encodings; encoder tests assert position/length only | A magnitude a user reads more accurately from volume than from position |
| L2 addressability | `audit_quantitative.mjs` fails if a rung token is referenced from a density-renderer module, or if a non-addressable component declares depth; the density encoder emits no rung field at all | Users read a neutral density field as a verdict about proof anyway |
| L3 absence | `audit_quantitative.mjs` requires every registered component to declare an absence state; tests assert zero / not-measured / truncated are distinguishable | Users cannot tell "none found" from "not measured" |
| L4 attention cost | Audit counts declared looping primitives per view; layouts asserted deterministic. **Guidance, not a gate**: nothing measures actual motion at runtime | Hour-eight fatigue measured despite the budget holding |
| L5 depth | Measured per-rung salience monotonicity, plus an assertion that depth is non-zero only where marks are addressable and >= `addressableMinHeightPx` | Depth measurably raises a lower rung's salience, or users read "floating" as prominence rather than as unlanded |

**Falsifiers for the whole direction.** If users cannot name a rung from ground
contact alone, the depth channel is removed and nothing else in this grammar
changes — it is deliberately the most detachable law here. If users read the neutral
density field as a proof verdict despite it carrying no rung, then aggregated views
must be abandoned entirely in favour of detail-only browsing.

**Which laws are gates and which are guidance**, stated plainly because conflating
them is how a constitution rots: L1, L2, L3 and L5 are enforced by
`audit_quantitative.mjs` and by tests, and every assertion has been mutation-tested.
L4 is **guidance** — the declared `loopingPrimitives` count is checked, but nothing
measures runtime motion, so L4 rests on review. Calling it a law would be a claim the
implementation does not support.

### 7.1 Measured, with the command that reproduces it

Numbers here are measurements, not estimates. Where a figure could not be reproduced
from committed inputs, that is said rather than papered over.

| Quantity | Measured | Reproduce |
|---|---|---|
| Worst-case redraw, 17,920 marks (140 bands x 128 columns) | best **4.10 ms**, median **5.30 ms**, worst **9.70 ms** | `npm run perf:density` |
| Same draw, under six parallel Playwright workers | best **22.20 ms** | `npx playwright test` (why the measurement is isolated) |
| Payload, 10,000 rows / 3,000 flagged cells | **764.8 KiB -> 151.9 KiB (5.0x)**, index 6.6 B/cell | `python scripts/measure_payload_split.py 10000` |
| Payload, 100 flagged cells | **0.9x — slightly worse** | `python scripts/measure_payload_split.py` |
| Bundle, whole quantitative layer | 131.93 KiB -> **142.59 KiB (+10.66 KiB)**, no new runtime dependency | `npm run build` |
| Gate assertions killed by mutation | **13/13** | `npm run audit:quantitative:mutants` |
| Ink actually drawn on the density canvas | non-zero opaque pixels via `getImageData` | `npx playwright test -g "real ink"` |

Two honest limits on the above. The 10,373-cell hospital figure quoted in §2 and §4
comes from prior benchmark records; the full hospital table is not committed, so the
payload measurements use a synthetic table of the same shape and the script says so
when it substitutes. And the payload ratio is **scale-dependent**: below
`FLAGGED_CELL_DETAIL_LIMIT` the split costs a little rather than saving, because every
cell then appears in both the index and the detail set. Quoting only the 5.0x figure
would misrepresent the sparse case.

---

## 8. Forbidden patterns

Additive to perceptual-language §8, which remains in force.

- **Volume, area, or hue as a magnitude.** L1.
- **Additive or lighten blending anywhere.** L2. This is the overplotting lie.
- **A rung on any mark that is not individually addressable.** L2. This replaces the
  earlier minimum/maximum-rung rules, which both attempted to pick a representative
  for a set.
- **Depth on a non-addressable mark, or on a mark below `addressableMinHeightPx`.**
  L5 constraint 1 — the defect that made the first version of this law dead code.
- **Rendering zero, not-measured, and truncated identically.** L3.
- **A quantitative component with no declared absence state.** L3.
- **Force-directed or physics-settling layouts.** L4 — motion with no referent
  event.
- **More than one looping progress indicator per view.** L4.
- **Depth scaled by any data value.** L5.
- **Perspective projection, orbit, free camera, or scroll parallax.** L5, and WCAG
  2.3.3.
- **A colour authored in the renderer.** Colour is read from the audited token
  system at runtime and uploaded as a uniform. No hex literal may exist anywhere
  in the frontend source, which the Python contract test enforces by grep. The GPU
  layer is a token consumer, never a colour author — which is also why the
  visualisation inherits light, dark, high-contrast, and P3 for free and cannot
  drift from the audited palette.

---

## 9. Open unknowns → prioritised validation work

Honest unknowns are deliverables.

1. **(P1) Does ground contact read as "has not landed", or as decoration?**
   Comprehension test: can a user name the rung from contact state with hue
   removed? Falsifies L5. Depth is removable without touching the rest of this
   grammar.
2. **(P1) Does min-rung aggregation read as honest caution or as "nothing works"?**
   L2 deliberately understates good news to avoid overstating proof. Verify the
   `mixed` marker and the neutral count actually restore the lost signal; if a map
   of mostly-unproven bins causes users to discount genuine proven fixes, L2 needs
   a composition channel with more resolution, not a relaxation.
3. **(P2) Renderer availability.** WebGL2 may be absent in headless test browsers
   and on older devices. Mitigated by three tiers, but the fallback must be the
   tested path, not the assumed one.
4. **(P2) Long-session calm.** Inherited unresolved from perceptual-language §10.
   L4 supplies a budget, not evidence.
5. **Not buildable today, recorded rather than faked.** A per-step agent replay
   over the table is not possible: `ActionOutcome.resolved_cell` and
   `.unsat_core` are discarded inside the engine before any view layer sees them.
   Stating the gap is the deliverable.
6. **The calibration plot is not built, and that is a finding.** No calibration,
   conformal, reliability or ECE data appears anywhere in the playground API
   response. Those artifacts come from a calibration session over labelled
   samples, not from a single stateless analyse call. Plotting the committed
   `eval/results/selective_repair_calibration.json` instead would present one
   dataset's measurement as though it described the user's run — the claim-scope
   error this project has already made and corrected three times. Building it
   requires exposing per-run calibration state, which is a product decision, not a
   rendering one.
7. **The review ranking is surfaced but never fired here.** `review_ranking` now
   reaches the browser so a CLI or library caller that opted into a ranker can
   render it. The playground does not supply one: the ranker is an LLM scorer, so
   firing it per request would spend money on every analysis, its auto-fire gate
   is a measured NO-GO, and its headline ROC-AUC is an in-sample single-dataset
   figure that is measured not to generalise. The free ordering the surface does
   use — severity, then detector confidence — makes no accuracy claim at all.

---

## 10. Where this is implemented

Two halves, split along the addressability law: aggregated marks on one side, individual
claims on the other. Nothing is shared between the two renderers, which is the point —
the density path has no access to a rung.

- Grammar gate: [audit_quantitative.mjs](../../playground/web/scripts/audit_quantitative.mjs)
- Mutation harness for that gate: [mutate_quantitative.mjs](../../playground/web/scripts/mutate_quantitative.mjs)
- Single source: `playground/web/src/design/quantitative-tokens.json`
- Typed derivation: `playground/web/src/viz/grammar.ts`
- Shared claim model (L3 absence states): `playground/web/src/viz/model.ts`
- Token bridge (colour discipline): `playground/web/src/viz/tokens.ts`

**Aggregated, non-addressable — carries no rung:**

- Encoder: `playground/web/src/viz/density.ts` (emits no rung, no depth, no mixed flag)
- Painter: `playground/web/src/viz/paintDensity.ts` (2D canvas, `source-over` only)
- View: `playground/web/src/viz/EvidenceOverview.tsx`

**Addressable — one mark, one claim, one rung:**

- Encoder: `playground/web/src/viz/claims.ts`, including the provenance-independent
  overtrust check `claimSetViolations`
- View: `playground/web/src/viz/ClaimDetail.tsx` (DOM buttons, roving tabindex, earned
  depth as `box-shadow`)
- Confidence distribution: `playground/web/src/viz/confidence.ts` and
  `ConfidenceDistribution.tsx` (population statistics from the server histogram)
- Dependency graph (2D, SVG, deterministic): `playground/web/src/viz/DependencyGraph.tsx`

**Supporting:**

- Salience monotonicity measurement: `measureRungSalience` in `viz/grammar.ts`
- Proof attribution: `parseUnsatCore` in `playground/web/src/observatory.ts`
- Payload split: `_flagged_cells_view` and `_confidence_histogram` in
  `playground/api/app.py`
- Measurements: `playground/web/e2e/density-throughput.spec.ts` (isolated via
  `playwright.perf.config.ts`) and `scripts/measure_payload_split.py`
