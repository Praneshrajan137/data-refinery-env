# SPEC: DataForge Chromatic System

> Status: Draft
> Owner: @Praneshrajan15
> Last updated: 2026-06-02

## 1. Purpose

Create the perceptual Mineral Intelligence color system for DataForge
surfaces. The system must feel precise, premium, and inevitable: porcelain
field, graphite structure, deep ink text, near-black command controls with a
restrained vermilion signal, and state color used as instrumentation rather
than large decorative fills.

## 2. Outcomes

- [ ] Color tokens are generated from OKLCH seed hues through a build-time
  script and committed as CSS and JSON artifacts.
- [ ] `culori` is a dev-only dependency; no color engine ships in the browser
  runtime bundle.
- [ ] Public UI tokens include `--df-bg`, `--df-surface-*`, `--df-text-*`,
  `--df-line-*`, `--df-action-*`, `--df-focus-*`, `--df-status-*`,
  `--df-agent-*`, and `--df-diff-*`.
- [ ] Light and dark schemes preserve the same semantic meanings.
- [ ] Primary text on every generated surface reaches at least WCAG 2.2 7:1
  contrast; secondary text reaches at least 4.5:1.
- [ ] Non-text affordances and focus indicators reach at least 3:1 against the
  relevant background.
- [ ] P3-enhanced color is limited to non-text-critical glow or atmosphere
  tokens and has sRGB fallbacks.
- [ ] Raw hand-authored hex colors are rejected outside generated artifacts.
- [ ] `--df-action-*` uses graphite command materials plus restrained
  vermilion signal accents and never uses blue, teal, green, `success`,
  `safe`, or legacy `forge` as the primary product identity.
- [ ] Large light-theme state backgrounds remain neutral/platinum; status
  meaning appears through borders, text, icons, rails, and compact badges.
- [ ] Success verdigris remains low-chroma and is reserved for verified
  completion or proved-safe states.

## 3. Domain Semantics

- Porcelain, graphite, ink, and mineral gray: product architecture, table
  reading, and large surfaces.
- Vermilion signal: primary command border, action emphasis, and executive
  product signature without blue dominance.
- Teal-steel: dataset intake, evidence, inspection, and proof records.
- Muted ultraviolet: optional advanced mode, agentic cognition, and orchestration.
- Low-chroma viridian: accepted, verified, completed, and proved-safe signals.
- Brass caution: uncertainty, rate limits, and human review.
- Hematite failure: unsafe, failed, rejected, or destructive possibilities.

## 4. Constraints

- The system follows WCAG 2.2 as the normative accessibility gate. WCAG 3 and
  APCA may inform margins but cannot replace the current WCAG 2.2 checks.
- Color never carries state alone; labels, icons, ARIA state, and copy remain
  available for every critical status.
- The playground remains storage-free, API-key-free, and dry-run only.
- The browser JavaScript gzip budget is 160 KiB. This preserves a fast
  first-load experience while leaving room for future premium AUX interaction,
  richer evidence inspectors, and carefully chosen visual instrumentation.
- Token generation clamps default output to sRGB; wide-gamut P3 output is
  progressive enhancement only.
- Decorative page glows, rainbow rails, saturated color washes, and pastel
  status slabs are not part of the Observatory direction.
- Large light-mode workbench surfaces must read as one premium product
  material, not a collection of colored cards.

## 5. Verification

- `npm --prefix playground/web run colors:check`
- `npm --prefix playground/web run audit:colors`
- `npm --prefix playground/web run typecheck`
- `npm --prefix playground/web run test:unit`
- `npm --prefix playground/web run build`
- `npm --prefix playground/web run test:e2e`
- `python -m pytest tests/unit/test_playground_web_contract.py`
