# SPEC: DataForge Temporal Intelligence System

> Status: Draft
> Owner: @Praneshrajan15
> Last updated: 2026-06-06

## 1. Purpose

Create a disciplined motion system for DataForge Observatory. Motion must make
agentic proof-and-repair work understandable: what changed, what is running,
what needs review, what was verified, what failed, and what remains under human
control.

## 2. Motion Philosophy

- Motion is evidence, not ornament.
- Motion communicates causality, progress, interruption, recovery, and handoff.
- Motion never carries state alone; text, icons, color tokens, and ARIA state
  remain primary.
- Large product surfaces stay calm. Only local state changes, route continuity,
  selected evidence, workflow events, repair focus, and handoff reveals move.
- Fake intelligence is forbidden. The workflow only pulses when real
  `workflow_event_v1` events or explicit user actions occur.

## 3. Token Language

- Durations: `instant 80ms`, `micro 120ms`, `fast 180ms`,
  `standard 240ms`, `measured 320ms`, `page 360ms`, `max 480ms`.
- Easings: `standard [0.2,0,0,1]`, `emphasized [0.16,1,0.3,1]`,
  `exit [0.4,0,1,1]`, and `linear` for essential progress loops only.
- Springs: `soft { stiffness: 420, damping: 36, mass: .85 }`,
  `snap { stiffness: 620, damping: 42, mass: .7 }`,
  `layout { stiffness: 500, damping: 45, mass: 1 }`.
- Spatial movement is constrained to 6px route continuity and 2-4px component
  settlement. Scale must remain within 0.985-1.015 for workbench surfaces.

## 4. Component Rules

- Product routes use a subtle opacity plus 6px vertical continuity transition.
- Product navigation uses a shared active marker so location changes are visible
  without a full page slide.
- Proof Atlas nodes use event-driven state motion: running nodes pulse locally,
  completed nodes settle once, review nodes hold attention without looping, and
  failed nodes snap into a still warning state.
- Evidence Dock reveals on selection changes and refocuses through opacity plus
  a small vertical settle.
- Review Queue decisions animate as reversible human choices; rerun makes the
  cause-effect handoff clear.
- Repair rows and diff cells use small focus motion only when selected or
  entering. They never resize tables or shift layout.
- Receipt handoff reveals proof summary before local commands.

## 5. Agent States

The vocabulary is the eight live `AgentMotionState` values. The prior twelve-state list is
retired: ten of its states were only ever drawn in a legend and never rendered on a live
surface, and a language with words nobody speaks communicates nothing. Every state below
maps to a real pipeline or trace event and to exactly one motion primitive, so no state can
be legend-only. See `docs/design/perceptual-language.md` section 4.1 for the trust-rung
rationale and `playground/web/src/motion.ts` (`agentStatePrimitive`) for the binding.

| State | Primitive | Motion | Trust rung |
| --- | --- | --- | --- |
| `verifying` | `resolve` | data-tone cadence; may loop while a check is running | claim in progress |
| `proposing` | `hover` | ultraviolet uncommitted hold; may loop | plausibility, not proven |
| `proven` | `settle` | viridian settle, one-shot | proven |
| `held` | `pause` | brass arrested hold, no shaking | held / abstention |
| `asking` | `pause` | brass attention hold, no shaking | held, awaiting a human |
| `rejected` | `recoil` | hematite recoil once, never loops | rejected / unknown |
| `done` | `settle` | viridian settle, one-shot | proven, terminal |
| `idle` | `still` | perfectly still; stillness is punctuation | no claim |

Only `verifying` and `proposing` may loop. Everything else is one-shot or still, so motion
weight never out-claims verification strength.

## 6. Accessibility And Performance

- Use Motion for React via `motion/react` with `MotionConfig reducedMotion="user"`.
- Reduced motion removes spatial transforms and recurring animation; it uses
  instant changes or non-spatial opacity only.
- Prefer `transform` and `opacity`. Avoid animating geometry, blur, filters,
  large shadows, scroll position, or layout-critical table dimensions.
- Infinite animation is allowed only for active blocking states and must stop
  under reduced motion.
- No parallax, confetti, decorative background motion, bouncing, or glow loops.

## 7. Verification

- `npm --prefix playground/web run typecheck`
- `npm --prefix playground/web run test:unit`
- `npm --prefix playground/web run test:e2e`
- `npm --prefix playground/web run build`
- Browser verification for light mode, mobile, route navigation, running stage,
  completed receipt, and reduced-motion rendering.
