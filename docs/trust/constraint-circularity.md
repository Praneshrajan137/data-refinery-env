# Constraint Circularity

A named trust risk in any system that infers constraints from data and then
enforces them: **the tool infers a constraint from dirty data, then "proves" a
corrupting repair against that same inferred constraint.** The proof is real; the
premise is manufactured. This document defines the risk, shows the mechanism, and
states DataForge's layered defenses and their standing proof.

It is the structural analogue of the LLM-corrector risk. There the untrustworthy
input is the *fix source*; here it is the *structure the tool infers about the
data itself*. A verification-first product (see [../STRATEGY.md](../STRATEGY.md))
must be robust to both.

## The mechanism (measured on `tax`)

DataForge mines candidate functional dependencies with
`confidence = 1 - g3-error` (g3 = the minimum fraction of tuples to delete to make
the FD hold — the standard approximate-FD error measure). Two spurious flavors
arise, both grounded in relational theory:

- **Near-(super)key determinant.** A determinant that is (almost) unique per row
  trivially "determines" every other attribute, because almost every group has one
  row (a superkey determines all attributes — classical FD theory). On `tax`,
  `zip` (near-unique) vacuously "determines" `salary`.
- **Low-cardinality coincidence.** A correlation that holds at >= 90% by chance,
  e.g. `f_name -> gender`. Its majority-repair overwrites the legitimate minority.

Measured (sampled 3,000 `tax` rows, `include_inferred_constraints=True`): treating
mined FDs as authoritative produces **696-708 false-positive corrections** with
zero correct ones. See [accuracy-frontier.md](accuracy-frontier.md).

## The honest limit (why you cannot mine your way out)

A genuine approximate FD (hospital `zip -> city`, violated only by *dirty* cells)
and a coincidental one (tax `f_name -> gender`, violated by *legitimate*
variation) have the **same in-table signature**: both hold at ~0.9-1.0 with some
violations. No in-table signal separates "these violations are errors to fix" from
"these violations are correct variation to keep" — that distinction requires
external knowledge. So a confidence threshold cannot separate them; tuning one to
fit two datasets would be overfitting, which the honesty doctrine forbids. The
defense must therefore be **architectural**, not a smarter score.

## DataForge's layered defenses (the proof)

1. **Inferred constraints are pending-until-reviewed.** The product `effective_schema`
   is declared schema + *accepted* reviewed constraints only
   ([engine/repair.py](../../dataforge/engine/repair.py)); the `fd_violation`
   detector fires only on schema FDs ([detectors/fd_violation.py](../../dataforge/detectors/fd_violation.py)).
   With no schema, the product proposes **zero** FD corrections — the 696-708 occur
   only under the bench's `include_inferred_constraints=True`, never in the product
   default.
2. **Mining rejects the vacuous cases + informs the rest.** `_fd_candidates`
   ([schema_inference.py](../../dataforge/schema_inference.py)) rejects near-key
   determinants (`_MAX_DETERMINANT_UNIQUE_FRACTION`) and low-support candidates
   (`_MIN_FD_SUPPORT_GROUPS`), and reports support + informativeness + an
   approximate-FD warning in the candidate evidence so acceptance is informed.
3. **Declared-FD-only opt-in.** `require_declared_fds_for_autoapply` holds any FD
   correction not backed by a hand-declared FD (review_reason
   `inferred_fd_not_declared`), for strict/regulated deployments (see
   [../../DECISIONS.md](../../DECISIONS.md), 2026-07-19).
4. **Accepted constraints are auditable and reversible.** Accepting a constraint is
   the user's authoritative act; any resulting change is recorded in the trust
   certificate and is byte-for-byte reversible, so a mis-accepted FD's effect is
   bounded, provable, and undoable via `audit` / `revert` / `reverify_certificate`.

## The standing proof

`tests/property/test_no_corruption_invariant.py::test_engine_never_corrupts_via_spurious_fd`
generates tables engineered to induce both spurious-FD flavors and asserts the
default engine never overwrites a correct cell (INV1-4) while a genuine
decimal-shift fix still applies. This makes "constraint circularity cannot corrupt
the default path" a proven, continuously-tested invariant — not a claim.

## The rule

Never treat an inferred constraint as authoritative for auto-apply. Inferred
constraints inform review; only a declared schema or an explicitly-accepted
constraint drives a proven repair, and strict mode can require the former. If you
add a new inferred-constraint kind (denial constraints, inclusion dependencies,
etc.), it must enter the same pending-until-reviewed path and gain a corruption-
oracle class before it can affect repair.
