# SPEC: quantitative-visualization

> Status: Accepted
> Owner: dataforge
> Last updated: 2026-08-11

## 1. Purpose

DataForge renders no quantity graphically. Every number the engine computes —
flagged-cell counts, per-cell rungs, severity distributions, certificate totals,
functional-dependency structure, calibration curves — reaches the user as text. This
component supplies the missing quantitative layer: a per-cell **Evidence Surface**
over the row-by-column table, a functional-dependency graph, and a calibration plot,
all governed by a new grammar that inherits earned salience.

The user problem: on the measured hospital condition with inferred functional
dependencies, 10,373 of 20,000 cells are flagged. A text list cannot answer "where
are the problems, and which of them are proven?" at that scale.

## 2. Outcomes

- [ ] A per-cell Evidence Surface renders every flagged cell for a playground-legal
      table (up to 10,000 rows x 128 columns, 200,000 cells) without dropping cells
      silently.
- [ ] Rung is carried by form and depth, not hue alone, and survives grayscale.
- [ ] No `plausibility_only` mark can be emitted with proven form, proven glow,
      ground contact, or `settle` motion. Enforced by test, per
      perceptual-language §7.1.
- [ ] Measured per-rung salience is monotonic in rung strength, including the depth
      channel.
- [ ] Collided marks aggregate by minimum rung and cannot overstate.
- [ ] Zero, not-measured and truncated render distinguishably, each with text.
- [ ] Everything the canvas conveys is available as DOM text; axe reports zero
      violations in light and dark.
- [ ] The renderer degrades WebGL2 -> 2D canvas -> DOM table, and the fallback path
      is tested, not assumed.

## 3. Scope

**IN**:

- The quantitative grammar (L1-L5) and its build gate.
- Pure encoder: `AnalyzeResponse` -> per-cell model -> non-overlapping draw list.
- Three renderer tiers plus the accessible DOM twin.
- Additive API changes that surface per-cell truth the view layer currently drops.
- `unsat_core` decoding into legible constraint attribution.
- Functional-dependency graph (2D, deterministic layout) and calibration plot.

**OUT**:

- Perspective projection, orbit/free camera, scroll parallax (L5, WCAG 2.3.3).
- Volumetric or area-based magnitude encodings (L1).
- Per-step agent replay over the table: `ActionOutcome.resolved_cell` and
  `.unsat_core` are discarded in `controller.py` before any view layer sees them.
  Not buildable without an engine change; recorded, not faked.
- `tax` (200,000 x 15): exceeds every playground cap and has no measured issue
  count in any artifact.
- Geographic rendering: would require external geocoding the engine cannot verify.

## 4. Constraints

- Compatibility: Python `>=3.11,<3.13`; Node 22; TypeScript strict via `tsc -b`.
- Safety: this component is read-only over engine output. It introduces no write
  path and must not weaken the `detect -> propose -> SafetyFilter -> differential
  verify -> auto-apply gate` ordering.
- Content-Security-Policy is unchanged: `script-src 'self'` (no WASM), no blob
  workers, no blob images. Canvas export, if any, uses `toDataURL`.
- No colour literal may appear in frontend source; colours are read from the
  audited token system at runtime.
- No `localStorage`/`sessionStorage` anywhere under `playground/web/`.
- CSS keyframes may animate only `transform` and `opacity`; only `hover` and
  `resolve` may loop.
- Layout must satisfy `scrollWidth <= innerWidth + 1` at Pixel 7 width.
- Backward compatibility: API changes are additive only; the OpenAPI snapshot is
  regenerated and the e2e mock plus all frontend fixtures updated in lockstep.

## 5. Prior Decisions

- `DECISIONS.md` 2026-08-11 — the quantitative grammar and earned depth.
- [docs/design/perceptual-language.md](../docs/design/perceptual-language.md) is
  the governing constitution; where it and this spec disagree, it wins.
- [docs/design/quantitative-grammar.md](../docs/design/quantitative-grammar.md)
  supplies L1-L5.
- Invariants this spec must not change: hospital heuristic F1 stays 0.7926; the
  five-runtime-dependency triage records are updated rather than contradicted;
  overtrust-impossibility (§7.1) holds in the renderer as well as the DOM.

## 6. Task Breakdown

### 6.1 Grammar and gate

- Acceptance: `audit_quantitative.mjs` fails on an empty registry, on a non-minimum
  collision aggregate, on a glow-eligible unproven rung, on a depth beyond bound,
  on an undeclared absence state, and on a colour literal.
- Depends on: none
- Estimated complexity: M

### 6.2 Pure encoder

- Acceptance: `buildDrawList` emits non-overlapping marks; a bin of 1 proven + 40
  plausibility renders plausibility and is marked mixed.
- Depends on: 6.1
- Estimated complexity: M

### 6.3 Token bridge and three tiers

- Acceptance: colours resolve from `--df-*` at runtime and re-resolve on theme
  change; forced-fallback test passes with WebGL2 unavailable.
- Depends on: 6.2
- Estimated complexity: L

### 6.4 Evidence Surface

- Acceptance: renders on `/evidence`, selects into `EvidenceDock`, axe-clean in
  light and dark, no horizontal overflow at Pixel 7.
- Depends on: 6.3
- Estimated complexity: L

### 6.5 Additive API surface

- Acceptance: `review_ranking` is computed and reaches the browser; per-cell
  confidence/actual/expected are untruncated; OpenAPI `--check` passes.
- Depends on: none (parallel with 6.2-6.4)
- Estimated complexity: M

### 6.6 Proof attribution

- Acceptance: `parseUnsatCore` decodes all nine label shapes emitted by
  `SchemaToSMT`; the accept/reject asymmetry is rendered explicitly.
- Depends on: 6.5
- Estimated complexity: M

### 6.7 Dependency graph and calibration plot

- Acceptance: deterministic layout (identical output for identical input, no
  simulation); uncertified calibration renders as an explicit absence state.
- Depends on: 6.3
- Estimated complexity: M

### 6.8 Executable laws

- Acceptance: overtrust-impossibility and salience monotonicity are tests that fail
  when violated (mutation-verified); full local gate chain green.
- Depends on: all
- Estimated complexity: M

## 7. Verification

- Frontend unit: `playground/web/src/viz/*.test.ts` (vitest)
- Frontend e2e + a11y: `playground/web/e2e/playground.spec.ts` (Playwright + axe,
  chromium and mobile projects)
- Contract: `tests/unit/test_playground_web_contract.py`
- Gates: `npm run build` (colors:check, motion:check, audit:quantitative, tsc,
  vite, budget), `npm run test`
- Standard gates: `make lint`, `make type`, `make test`
- Contract gates: `python scripts/ci/openapi_contract.py --check`,
  `python scripts/ci/readme_truth.py`, `python scripts/ci/docs_truth.py --check`
- Regression anchor: hospital heuristic F1 unchanged at 0.7926

## 8. Acceptance Gate

- [ ] All Section 2 outcomes are met.
- [ ] All required tests pass.
- [ ] No regression test fails.
- [ ] Public interfaces and docs are updated.
- [ ] `DECISIONS.md` records the grammar and the depth decision.

## Appendix A - Toy Cases

### Case A.1: the overplot lie

Input: one bin containing 1 fix with `verification_strength: "proven"` and 40 with
`"plausibility_only"`.
Expected output: a single mark with rung `plausibility_only`, `mixed: true`,
`count: 41`, `provenCount: 1`; no ground contact; no glow.
Reasoning: catches max-rung aggregation and additive blending, either of which would
render this bin as proven — density becoming epistemic strength.

### Case A.2: entity-consensus overtrust

Input: a fix with `provenance: "entity_consensus"` and `verification_strength`
absent.
Expected output: rung `plausibility_only`.
Reasoning: the frontend `LLM_PROVENANCE` set omitted `entity_consensus`, which the
engine's `_UNTRUSTED_PROVENANCE` includes, so `strengthOf` returned `proven` for an
untrusted value. Catches the drift in the exact function every trust surface routes
through.

### Case A.3: truncation is not zero

Input: an issue group with `count: 1000` and `row_indices` of length 50 with
`row_indices_truncated: true`.
Expected output: absence state `truncated` with text stating coverage is partial;
never a complete-looking map of 50 cells.
Reasoning: catches the lie of omission that looks like completeness (L3).

### Case A.4: depth may not promote

Input: the rung-to-depth table.
Expected output: measured salience (ink, contrast, glow) strictly monotonic in rung
strength, with `plausibility_only` at +6px measuring below `proven` at 0px.
Reasoning: catches the objection that a floating mark reads as more prominent — the
one way earned depth could invert the one law.
