# SPEC: DataForge Chromatic System

> Status: Draft
> Owner: @Praneshrajan15
> Last updated: 2026-06-02

## 1. Purpose

Create a perceptual Apex Intelligence color system for DataForge surfaces.
The system must feel precise, premium, and inevitable: platinum/graphite
surfaces, deep ink text, sovereign blue for command, and state color used as
instrumentation rather than large decorative fills.

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
- [ ] `--df-action-*` maps to the sovereign-blue `brand` palette and never to a
  green, teal, `success`, `safe`, or legacy `forge` palette.
- [ ] Large light-theme state backgrounds remain neutral/platinum; status
  meaning appears through borders, text, icons, rails, and compact badges.
- [ ] Success verdigris remains low-chroma and is reserved for verified
  completion or proved-safe states.

## 3. Domain Semantics

- Graphite-platinum: product structure, table reading, and large surfaces.
- Sovereign blue: primary commands, active tabs, focus, and command authority.
- Forensic steel: dataset intake, evidence, inspection, and proof records.
- Restrained amethyst: optional advanced mode, agentic cognition, and orchestration.
- Proof verdigris: accepted, verified, completed, and proved-safe signals.
- Antique amber: caution, uncertainty, rate limits, and human review.
- Oxide red: unsafe, failed, rejected, or destructive possibilities.

## 4. Constraints

- The system follows WCAG 2.2 as the normative accessibility gate. WCAG 3 and
  APCA may inform margins but cannot replace the current WCAG 2.2 checks.
- Color never carries state alone; labels, icons, ARIA state, and copy remain
  available for every critical status.
- The playground remains storage-free, API-key-free, and dry-run only.
- The browser JavaScript gzip budget remains 90 KiB.
- Token generation clamps default output to sRGB; wide-gamut P3 output is
  progressive enhancement only.
- Decorative page glows, rainbow rails, saturated color washes, and pastel
  status slabs are not part of the Apex Intelligence direction.

## 5. Verification

- `npm --prefix playground/web run colors:check`
- `npm --prefix playground/web run audit:colors`
- `npm --prefix playground/web run typecheck`
- `npm --prefix playground/web run test:unit`
- `npm --prefix playground/web run build`
- `npm --prefix playground/web run test:e2e`
- `python -m pytest tests/unit/test_playground_web_contract.py`
