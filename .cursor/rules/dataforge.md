# DataForge - Always-Applied Rules

This file is a POINTER, deliberately. It was reduced on 2026-09-01 from a copy of the
project's facts to a map of where those facts live.

The previous version restated the CLI surface, the public interface inventory, the safety
invariant, and a verification command list. Every one of those has an owner elsewhere, and
the copy had drifted: it listed **4** CLI commands when **13** were registered, and it
stated the safety invariant as `SafetyFilter -> SMTVerifier -> transaction log`, omitting
both the differential verifier and the provable-only auto-apply gate that PRODUCT.md
section 8 makes central. It also required Conventional Commits with a 72-character subject,
which no commit in this repository follows.

Nothing polices this file. That is the whole argument for it holding no facts: an
auto-injected instruction file with its own copy of the truth is drift by construction, and
the drift is invisible because it reads as authoritative to every session that loads it.

## Read these first, in this order

| Question | Owner |
| --- | --- |
| What is this project for, what may it never do | [PRODUCT.md](../../PRODUCT.md) — the constitution; it wins on purpose, philosophy and principle |
| Session conventions, gotchas, and the DATASET SCOPE RULE | [CLAUDE.md](../../CLAUDE.md) — auto-injected, and the `beers` exclusion is mandatory every session |
| The safety invariant in depth, system design | [ARCHITECTURE.md](../../ARCHITECTURE.md) |
| Measured numbers, commands, release status | [README.md](../../README.md), [BENCHMARK_REPORT.md](../../BENCHMARK_REPORT.md), `docs/evidence/` |
| Why a decision was made, and what would reverse it | [DECISIONS.md](../../DECISIONS.md) |
| Public interface contracts | `specs/` |
| Available gates and their exact scope | `make help`, and the `Makefile` itself |

For anything with a number in it, do not trust this file or any summary — go to the
artifact. `docs/quantitative_claims.yaml` records which numbers are bound to which
evidence, and `scripts/ci/docs_truth.py --check` verifies them.

## Rules that live only here

These are not enforced by `ruff` (which selects `E,F,W,I,N,UP,B,A,C4,PIE,RET,SIM` — note
no `T20` and no `BLE`), not stated in CLAUDE.md, and not derivable from a spec. They are
kept because deleting them would silently drop a convention.

1. Never modify a public API without updating its spec in `specs/`.
2. Never delete or weaken an existing test. If a test is wrong, change the spec and the
   test together, with the rationale written down.
3. Write the failing test before the implementation for feature work.
4. No silent catch-all exception handlers.
5. Avoid global mutable state; inject dependencies where practical.
6. Do not leave TODO/FIXME in merged code. Open an issue, or state the limitation in the
   docstring where a reader will meet it.
7. Public functions, classes and modules carry type hints and a Google-style docstring
   that says why, not what.
8. Commit subjects are a plain imperative sentence describing the change and its reason —
   see `git log`. Not Conventional Commits; this repository does not use them.

## When uncertain

- If documentation and code disagree, the code is what runs: read it and the tests first,
  then fix the document or add the test that explains the new behaviour.
- If a spec is ambiguous, record the real question in `specs/QUESTIONS.md` and proceed with
  the safest documented assumption.
- If a dependency seems useful, justify it in `ARCHITECTURE.md` before adding it.
- Before hardening any component, name its consumer. If nothing reads it, rigour there
  buys the correctness of a report rather than of the product — PRODUCT.md section 1.3
  records what that cost once already.
