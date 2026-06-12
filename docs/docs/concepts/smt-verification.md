# SMT Verification

The SMT verifier encodes proposed repairs and schema constraints into Z3 checks.
It is the final deterministic guard before DataForge writes an applied repair.

## What it checks

- Target rows and columns exist.
- Domain and schema constraints remain valid after the proposed edit.
- Functional dependencies are not made worse by the fix.
- Unsupported or ambiguous encodings return `UNKNOWN` instead of silently
  accepting a risky repair.

## Result contract

Verification returns one of three outcomes:

- `ACCEPT`: mutation may proceed if safety also passed.
- `REJECT`: mutation is blocked with a reason.
- `UNKNOWN`: mutation is blocked because DataForge could not prove the fix safe.
