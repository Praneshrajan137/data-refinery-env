# SPEC: DataForge Chromatic System

> Status: Draft
> Owner: @Praneshrajan15
> Last updated: 2026-06-02

## 1. Purpose

Create the Aurelian Proof Intelligence color system for DataForge surfaces.
The system must feel precise, premium, alive, luminous, and operationally
trustworthy: porcelain work fields, pearl mineral structure, deep warm ink
text, cinnabar executive signal, teal-steel evidence, ultraviolet agent
cognition, viridian proof, brass review, and hematite failure. Color is a
language of system meaning, not decoration.

## 2. Outcomes

- [ ] Color tokens are generated from OKLCH seed hues through a build-time
  script and committed as CSS and JSON artifacts.
- [ ] `culori` is a dev-only dependency; no color engine ships in the browser
  runtime bundle.
- [ ] Public UI tokens include `--df-bg`, `--df-surface-*`, `--df-text-*`,
  `--df-line-*`, `--df-action-*`, `--df-focus-*`, `--df-status-*`,
  `--df-agent-*`, `--df-info-*`, `--df-selection-*`, `--df-disabled-*`,
  `--df-loading-*`, and `--df-diff-*`.
- [ ] Agent-state tokens exist for thinking, acting, waiting, asking,
  uncertain, confident, completed, failed, interrupted, delegated, escalated,
  and recovered states.
- [ ] Light and dark schemes preserve the same semantic meanings.
- [ ] `prefers-contrast: more` strengthens text, lines, focus, and command
  borders without changing semantic hue meaning.
- [ ] Primary text on every generated surface reaches at least WCAG 2.2 7:1
  contrast; secondary text reaches at least 4.5:1.
- [ ] Non-text affordances and focus indicators reach at least 3:1 against the
  relevant background.
- [ ] P3-enhanced color is limited to non-text-critical glow or atmosphere
  tokens and has sRGB fallbacks.
- [ ] Raw hand-authored hex colors are rejected outside generated artifacts.
- [ ] `--df-action-*` uses aurelian cinnabar command materials and never uses
  black, graphite, blue, teal, green, `success`, `safe`, or legacy `forge` as
  the primary product identity.
- [ ] Large light-theme state backgrounds remain neutral/platinum; status
  meaning appears through borders, text, icons, rails, and compact badges.
- [ ] Success verdigris remains low-chroma and is reserved for verified
  completion or proved-safe states.

## 3. Domain Semantics

- Porcelain, pearl, warm ink, and mineral gray: product architecture, table
  reading, and large surfaces.
- Cinnabar signal: primary command fill, action emphasis, and executive
  product signature without blue dominance.
- Teal-steel: dataset intake, evidence, inspection, and proof records.
- Muted ultraviolet: optional advanced mode, agentic cognition, and orchestration.
- Low-chroma viridian: accepted, verified, completed, and proved-safe signals.
- Brass caution: uncertainty, rate limits, and human review.
- Hematite failure: unsafe, failed, rejected, or destructive possibilities.

## 4. Rejected Directions

- Blue/cobalt intelligence: too generic for modern SaaS and specifically
  rejected as the product identity.
- Black/graphite luxury: serious but too heavy for the desired light-first
  product identity.
- Neon/cyber AI: too loud, less trustworthy, and prone to decorative glow.
- Beige luxury: premium but too passive for proof, risk, and supervision.
- Rainbow semantics: expressive but weak for hierarchy, scan, and cognitive
  load.

## 5. Interaction Semantics

- Thinking and cognition use ultraviolet text/line on neutral surfaces.
- Acting and primary progress use cinnabar text/line on neutral surfaces.
- Waiting and delegated inspection use teal-steel text/line.
- Asking, uncertainty, interruption, and review use brass text/line.
- Confidence, completion, proof, and recovery use viridian text/line.
- Failure and escalation use hematite text/line.
- Hover uses neutral material elevation, selection uses neutral field plus
  vermilion line, disabled uses neutral subdued text/line, and loading uses a
  vermilion line plus explicit text/icon state.

## 6. Constraints

- The system follows WCAG 2.2 as the normative accessibility gate. WCAG 3 and
  APCA may inform margins but cannot replace the current WCAG 2.2 checks.
- Color never carries state alone; labels, icons, ARIA state, and copy remain
  available for every critical status.
- The playground remains storage-free, API-key-free, and dry-run only.
- The browser JavaScript gzip budget is intentionally unbounded for the
  playground frontend. Build output still reports gzip sizes, but quality,
  ambition, and future premium UI/AUX work are not blocked by a small hard cap.
- Token generation clamps default output to sRGB; wide-gamut P3 output is
  progressive enhancement only.
- Decorative page glows, rainbow rails, saturated color washes, neon halos,
  and pastel status slabs are not part of the Observatory direction.
- Large light-mode workbench surfaces must read as one premium product
  material, not a collection of colored cards.

## 7. Verification

- `npm --prefix playground/web run colors:check`
- `npm --prefix playground/web run audit:colors`
- `npm --prefix playground/web run typecheck`
- `npm --prefix playground/web run test:unit`
- `npm --prefix playground/web run build`
- `npm --prefix playground/web run test:e2e`
- `python -m pytest tests/unit/test_playground_web_contract.py`
