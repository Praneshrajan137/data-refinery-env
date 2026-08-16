# SPEC: Perceptual Verification of the Design Laws

> Status: Draft
> Owner: @Praneshrajan15
> Last updated: 2026-08-14

## 1. Purpose

The design laws are stated in perceptual terms and verified in symbolic terms.
`audit_colors.mjs`, `audit_motion.mjs` and `audit_quantitative.mjs` check token
names, set membership and counts. Not one of them parses a colour value,
simulates a colour-vision deficiency, or measures a rendered magnitude.

That gap is not theoretical. It allowed the one law of
[perceptual-language.md](../docs/design/perceptual-language.md) to be violated on
its own first-named channel, in both themes, for the entire life of the design
system, while the gate that claims to enforce it reported success.

This spec defines what it means to *measure* a perceptual law, resolves a
contradiction in the law that made the violation possible, and specifies the
verifications that replace assertion with measurement.

## 2. The measured violation

The one law (§2 of perceptual-language.md):

> **Perceptual intensity is a strictly monotonic function of epistemic strength —
> never of activity, novelty, or importance.**
>
> Perceptual intensity is the sum of a signal's chroma, motion amplitude, glow,
> weight, and form-completeness.

§7.2 makes the requirement explicit: monotonicity must hold "on every channel
(color-chroma, glow, motion amplitude, weight)".

Measured chroma of each rung's text token, converted from the committed hex to
OKLCh, walked along `rungOrder` from weakest to strongest warrant:

### Light theme

| rung | palette | hex | L | chroma | vs. previous |
| --- | --- | --- | --- | --- | --- |
| idle | neutral-50 | `#64635f` | 0.4993 | 0.00637 | — |
| rejected | danger-30 | `#50171d` | 0.2993 | 0.08522 | rises |
| plausibility_only | agent-30 | `#312747` | 0.2998 | 0.05769 | **falls** |
| downgraded | warning-20 | `#201300` | 0.1992 | 0.04153 | **falls** |
| held | warning-20 | `#201300` | 0.1992 | 0.04153 | **flat** |
| proven | success-30 | `#1f3324` | 0.2989 | 0.03751 | **falls** |
| corroborated | success-30 | `#1f3324` | 0.2989 | 0.03751 | **flat** |

**Five of six adjacent pairs violate strict monotonicity.** Excluding `idle`,
chroma decreases monotonically as warrant *increases*: the ladder is inverted.
`rejected` carries 2.27x the chroma of `proven`.

### Dark theme

| rung | palette | hex | L | chroma | vs. previous |
| --- | --- | --- | --- | --- | --- |
| idle | neutral-70 | `#a09e9a` | 0.6997 | 0.00621 | — |
| rejected | danger-90 | `#ffd1d1` | 0.9002 | 0.05196 | rises |
| plausibility_only | agent-90 | `#e2d7ff` | 0.9005 | 0.05537 | rises |
| downgraded | warning-95 | `#ffeccf` | 0.9508 | 0.04326 | **falls** |
| held | warning-95 | `#ffeccf` | 0.9508 | 0.04326 | **flat** |
| proven | success-90 | `#cce6d2` | 0.9003 | 0.03855 | **falls** |
| corroborated | success-90 | `#cce6d2` | 0.9003 | 0.03855 | **flat** |

**Four of six adjacent pairs violate strict monotonicity.** `rejected` carries
1.35x the chroma of `proven`.

### Why no gate saw it

`auditEarnedSalience` ([audit_colors.mjs:403-443](../playground/web/scripts/audit_colors.mjs))
is five `String.prototype.startsWith` tests and two `css.includes` tests. It
verifies that `--df-proven-text` resolves to a palette entry whose name begins
with `success-`. It never reads the `oklch` field sitting beside `hex` in the same
JSON object, and it never compares one rung to another. Its own comment reads
"perceptual intensity must track epistemic strength, so overtrust is
unrenderable. These are BUILD GATES, not advice."

A related trap: these figures are **not** the seed chromas. The seeds declare
`success.c = 0.038`, `warning.c = 0.066`, `danger.c = 0.086`, but `mapToGamut`
reduces chroma to fit sRGB and each rung draws a different tone, so `warning-20`
renders at 0.04153 rather than 0.066. A gate that audited seed values would
report a different ladder from the one users see. **Perceptual verification must
read the shipped value, not the authored intent.**

## 3. The contradiction, and its resolution

The violation is not a slip in the palette. The palette is right and the law is
wrong, in two independent ways.

**The law contradicts itself.** §3.1 states: "Color is semantics — *what a thing
means*. Color answers 'what is this?', never 'how loud is this?'". A quantity
cannot simultaneously be pure semantics and a monotonic summand of intensity.

**The law contradicts good practice.** A calm proof and a loud rejection is
correct: attention should go where a human is needed. The law explicitly forbids
intensity tracking "importance" — and a rejected fix is important. Enforcing the
law as written would require making failures quieter than proofs, which would be
a worse product and no safer.

### Resolution: three quantities over disjoint channel sets

The word "intensity" conflates three questions that a trust surface must answer
separately. Splitting them dissolves the conflict rather than trading against it,
following the precedent of the addressability law
([quantitative-grammar.md](../docs/design/quantitative-grammar.md) §3.2).

The split follows the standard channel taxonomy: identity channels are nominal
and magnitude channels are ordered (Bertin's levels of organisation; Tamara
Munzner, *Visualization Analysis and Design*, A K Peters, 2014).

| Quantity | Question | Kind | Channels |
| --- | --- | --- | --- |
| **Warrant** | How much is proven? | ordered, strictly monotonic | fill, stroke, ground contact, glow, depth, weight |
| **Identity** | What kind of claim is this? | nominal, unordered | hue |
| **Urgency** | Must a human act? | ordered on its own axis | chroma, and only channels declared for it |

Three normative laws follow.

### W — the warrant law

> **Warrant is carried only by warrant channels, and is strictly monotonic in
> epistemic strength across the rung ladder. Hue and chroma are not warrant
> channels.**

Warrant monotonicity is already measured over fill, stroke, contact, glow and
accent by `measureRungSalience`. This law adds the exclusion: no hue or chroma
value may enter a warrant computation. Overtrust remains unrenderable because an
unproven rung still cannot obtain the fill, contact or glow that proof earns.

### I — the identity law

> **Hue carries identity, never order. Every ordered pair of rungs must remain
> distinguishable under normal vision, dichromacy and total absence of colour —
> and the distinction must be verified by simulation, not asserted.**

Separability may be satisfied two ways, in this precedence:

1. the rungs are separable in the simulated colour space, or
2. a non-hue twin — fill, stroke form, or the mandatory verdict text — carries
   the distinction independently.

Route 2 is the redundancy law of perceptual-language.md §5, which the docs
already claim guarantees colourblind safety. That claim has never been executed.
This law executes it.

### U — the urgency law

> **Urgency is declared per rung, may track actionability, and its channel set
> must be disjoint from the warrant set.**

This gives the loud carmine of `rejected` a lawful home. It also makes the
dangerous move fail the build: signalling urgency through a warrant channel —
giving a rejected claim ground contact or glow so it "stands out" — is exactly
the overtrust the system exists to prevent.

Normative parts: disjointness across all three channel roles, warrant's declared
channels matching the weights that actually compute warrant, and every rung
declaring a known urgency level.

**Chroma against the declared urgency bands is measured and reported, not gated.**
It was written as a gate and demoted, because the measurement did not support it:

| theme | comparison | result |
| --- | --- | --- |
| light | none 0.0375 (corroborated) vs review 0.0415 (held) | ordered |
| light | review 0.0577 (plausibility_only) vs attention 0.0852 (rejected) | ordered |
| dark | none 0.0386 (corroborated) vs review 0.0433 (held) | ordered |
| dark | review 0.0554 (plausibility_only) vs attention 0.0520 (rejected) | **inverted** |

Two reasons, both about the law rather than the palette. The themes disagree. And
the semantic ordering between those exact two rungs is arguable: `rejected` is
*resolved* — the system evaluated and refused, and nothing is required of the user
— while `plausibility_only` is *unresolved*, not written, waiting for a human
decision. A good argument puts `plausibility_only` above `rejected`, which would
make the dark theme correct and the declaration wrong.

Gating a direction that cannot be defended from first principles would assert a
law the design does not follow; tuning the palette to satisfy it would fit pixels
to a rule nobody can justify. This follows the precedent already set for L4 in
quantitative-grammar.md: "Calling it a law would be a claim the implementation
does not support."

## 4. Measurement procedure (normative)

### 4.1 Colour space

Chroma, lightness and hue are read in **OKLCh**, converted from the committed
`hex` of the semantic token as resolved for the theme under test. Perceptual
difference is Euclidean distance in **OKLab**.

OKLab is chosen because it is the space the system already authors in, so no
second colour appearance model enters the trust boundary. Distances are reported,
never rounded away.

### 4.2 Dichromacy simulation

Simulation MUST use a published model, and the implementation MUST record which
model and cite it. `culori` is already a pinned devDependency; its filter
implementation is to be read and its model identified before use. If it does not
implement a published model, the Machado, Oliveira & Fernandes (2009) matrices
are to be used directly.

Conditions under test, per theme:

- normal trichromatic vision
- deuteranopia (green-cone absent)
- protanopia (red-cone absent)
- tritanopia (blue-cone absent)
- achromatopsia / full greyscale

Protanopia and dark backgrounds are specifically required because
[W3C Understanding SC 1.4.3](https://www.w3.org/WAI/WCAG22/Understanding/contrast-minimum.html)
records that WCAG 2.x luminance contrast is hue-agnostic by construction —
"contrast is calculated in such a way that color (hue) is not a key factor" — and
names its own blind spot: "the use of predominantly long wavelength colors
against darker colors (generally appearing black) for those who have
protanopia". This product renders `rejected` in `danger` at hue 18. Passing WCAG
contrast therefore says nothing about whether a viewer can tell `rejected` from
`proven`.

### 4.3 Thresholds

A threshold set to whatever the current palette happens to pass is not a
threshold; it is a description. This repo has shipped that mistake twice — a
tautological guard that re-derived a rung from the rung, and a perf assertion
that measured its own test runner.

Therefore each threshold MUST be justified from the requirement it encodes,
stated in this spec with its derivation, and fixed **before** the current palette
is measured against it. If the palette fails a justified threshold, the failure
is the finding and is recorded, not tuned away.

#### COLLAPSE_FLOOR = 0.05 OKLab

Two rungs are **colour-collapsed** under a vision condition when their simulated
OKLab distance falls below 0.05.

Derivation, from the system's own committed structure rather than from folklore:
the palette's tone stops are `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 98,
100]`, and `colorForTone` sets `l = tone / 100`. The **smallest step the system
ever authors deliberately** is therefore 2 tones near the extremes and 5 tones in
the upper range, i.e. `ΔL = 0.05` between `-90` and `-95`, a step the design
system already relies on being visible because it assigns those two tones
different semantic roles. Since OKLab distance with only `L` differing equals
`|ΔL|`, the floor is dimensionally consistent with that step.

0.05 is deliberately conservative in the demanding direction: it requires any two
rungs to be at least as different as the smallest difference the system itself
treats as meaningful. It is not a just-noticeable-difference claim, and it is not
asserted to be one.

#### Text does not satisfy separability

Every rung renders a mandatory verdict string, so a text twin exists for every
pair **by construction**. Accepting text as a separability route would make the
identity law vacuous — the failure mode this repo has hit twice, most recently a
guard that re-derived a rung from the rung.

Separability may therefore be satisfied only by:

1. simulated OKLab distance at or above COLLAPSE_FLOOR, or
2. a difference in **form** — the `(fill, stroke, groundContact)` triple.

Mandatory text remains required by the redundancy law and remains the fallback for
total colour loss and for `NO_COLOR` command-line output. It is simply not
evidence that a *visual* ladder is legible, because reading is not pre-attentive.

Pairs that pass by form alone MUST be reported, not merely counted: a distinction
that survives only because two marks have different outlines is a real fact about
the design that its authors should see.

### 4.4 Relationship to WCAG

WCAG 2.x remains the conformance floor, unchanged: 7.0 for body text, 4.5 for
secondary and state text, 3.0 for focus ring and strong lines, in both themes.
These perceptual checks are **additive**. They cover what the W3C states its own
formula does not cover, and they do not relax any existing ratio.

This spec takes no position on APCA or WCAG 3, which are not normative.

## 5. Scope of the verifications

| Verification | Replaces | Tier |
| --- | --- | --- |
| Warrant monotonicity excludes hue and chroma structurally | name-prefix matching | normative |
| Chroma ladder measured and reported per theme | nothing | normative |
| Every rung pair separable under 5 vision conditions x 2 themes | a prose claim in §5 and two CSS comments | normative |
| Urgency declared and disjoint from warrant | nothing | normative |
| Rung order in the design tokens equals the domain `RUNG_ORDER` | nothing | normative |
| High-contrast overrides meet their contrast ratios | identity check against hard-coded strings | normative |
| Keyframes and loops audited across all CSS, not the generated file only | 6 of 8 loopable animations | normative |
| Declared max duration enforced as a ceiling | nothing | normative |
| Forced-colors path exists and the canvas survives it | nothing | normative |
| Magnitude marks render a shared scale | nothing | normative |
| Aggregate proportions render an interval | nothing | normative |
| Smallest rendered text size measured | nothing | advisory until measured |

## 6. What these verifications do NOT claim

- **Not that the palette is beautiful.** They verify that it does not lie about
  warrant and that its distinctions survive impaired vision. Taste is not gated.
- **Not that a simulation equals lived experience.** Dichromacy models are
  approximations of a spectrum of conditions and do not cover anomalous
  trichromacy severity, acquired deficiency, or context effects. A passing
  simulation means a distinction is not *provably* lost, not that it is
  comfortable.
- **Not WCAG conformance by themselves.** They are additive to the existing
  ratios, not a substitute.
- **Not that measured monotonicity implies perceived monotonicity.** OKLab is a
  good uniform space, not a perfect one; small distances near threshold should be
  treated as unresolved rather than passing.

## 7. Outcomes

- [ ] `specs/SPEC_perceptual_verification.md` states the W/I/U split as normative.
- [ ] perceptual-language.md §2 and §7.2 amended; the retired claim recorded.
- [ ] A perceptual kernel exists, unit-tested against hand-checked values.
- [ ] The chroma inversion is pinned by a test before the law changes.
- [ ] Warrant computations cannot read hue or chroma.
- [ ] All 21 rung pairs measured under 5 conditions in 2 themes.
- [ ] Urgency declared per rung; disjointness gated.
- [ ] Design-token rung order verified against the domain vocabulary.
- [ ] All new gates mutation-verified; every mutant killed.
- [ ] DECISIONS.md records that the spec was wrong, with the measured tables.
