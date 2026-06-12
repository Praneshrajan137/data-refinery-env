# RFC-002: PII Detection Expansion

## Problem

The shipped safety constitution blocks obvious risky edits, but broader PII
detection is needed before DataForge can be trusted in regulated datasets.

## Alternatives

- Expand deterministic regex detectors for common identifiers.
- Integrate a dedicated PII library as an optional dependency.
- Require users to mark PII columns in schema files and keep automatic PII
  inference minimal.

## Decision

Use a layered approach: schema-declared PII remains authoritative, deterministic
patterns cover high-confidence identifiers, and optional third-party detectors
can be added behind an explicit extra. The default write path stays fail-closed.

## Rollout Plan

1. Add schema examples for PII column annotations.
2. Add deterministic detectors for email, phone number, SSN-like identifiers,
   and common account-number shapes.
3. Add adversarial tests for false negatives and false positives.
4. Evaluate optional library integration only after deterministic coverage is
   measured.

## Open Questions

- Which jurisdictions and identifier families are in scope for v0.2?
- How should teams tune sensitivity without weakening default safety?
- Should PII findings be separate detector issues or safety-only annotations?
