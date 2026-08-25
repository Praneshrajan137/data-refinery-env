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

> ### Amendment, 2026-08-25: this paragraph was too broad, and is narrowed here
>
> The conclusion above survives measurement. The reason given for it does not, and the
> difference matters to anyone who reads this section and concludes the search is futile.
>
> The paragraph conflates two questions:
>
> - **Q1.** Given a dependency that holds on the true data, are these particular violations
>   errors to fix or legitimate variation to keep? **Undecidable in-table. The paragraph is
>   correct about Q1 and nothing below weakens it.**
> - **Q2.** Does the dependency hold on the true data *at all*? `ZipCode -> HospitalName` is
>   not an approximate FD violated by dirty cells — a zip code does not determine a hospital
>   name. It is simply **false**, and it caused 23 of the 25 sampled clean-cell corruptions
>   measured in [bypass-allowlist-evidence.md](bypass-allowlist-evidence.md).
>
> On Q2 the claim "no in-table signal separates" is **refuted**.
> [premise-quality-result.md](premise-quality-result.md) measured that on hospital's 85
> non-vacuous mined candidates, confidence computed on the rows that can actually falsify the
> dependency — excluding singleton determinant groups, which are consistent with any value —
> separates true from false dependencies **perfectly**: false at most 0.9554, true at least
> 0.9599. The shipped `confidence` overlaps and cannot do this at any threshold.
>
> **The threshold is still not shipped, for this section's own reason.** It is fitted to one
> corpus and there is nothing to validate it against: flights and rayyan mine no dependencies
> at all, and tax mines four that are all true. So the honest statement is not that no signal
> exists, but that **the separation is unvalidatable with the corpora available** — a claim
> about evidence rather than about signal. The pre-registered kill criterion
> (`eval/preregistration/premise_quality.md`, K3) forbade introducing the constant, and it was
> not introduced. `tested_confidence` is instead reported to the human who accepts the
> constraint, which is this section's "architectural, not a smarter score" applied literally:
> the score informs a decision, it does not make one.
>
> Two things also changed on the strength of that measurement. The miner no longer emits
> dependencies whose dependent is a **constant column** — a single-valued column is determined
> by everything, so the dependency is vacuous, and 34 of hospital's 119 candidates were of
> that kind. And this document's own tax result is now **vindicated by measurement**: the
> 696-708 false-positive corrections below were produced before
> `_MAX_DETERMINANT_UNIQUE_FRACTION` and `_MIN_FD_SUPPORT_GROUPS` existed. On that same
> 200,000-row corpus the miner today emits **four** candidates and **all four are true**.
> The architectural defense this section argued for on theory has now been shown to work on
> the corpus that motivated it.

## DataForge's layered defenses (the proof)

1. **Inferred constraints are pending-until-reviewed.** The product `effective_schema`
   is declared schema + *accepted* reviewed constraints only
   ([engine/repair.py](../../dataforge/engine/repair.py)); the `fd_violation`
   detector fires only on schema FDs ([detectors/fd_violation.py](../../dataforge/detectors/fd_violation.py)).
   With no schema, the product proposes **zero** FD corrections — the 696-708 occur
   only under the bench's `include_inferred_constraints=True`, never in the product
   default.

   > **CORRECTION (2026-08-06).** The paragraph above is true about *corrections* and
   > misleading about *flags*, and the distinction cost 19x. Accepting a mined FD in
   > `constraints review` folds it into `effective_schema`, and from there it reaches
   > `fd_violation` and raises issues. The playground does exactly this
   > ([playground/api/app.py](../../playground/api/app.py), the accept-candidate path). So
   > while `include_inferred_constraints=True` is bench-only, its *effect on the review
   > queue* is product-reachable in one keystroke.
   >
   > Measured on hospital (`eval/results/detector_queue_composition.json`): accepting the
   > mined FDs takes the queue from **549 cells at 0.561 precision** to **10,373 cells at
   > 0.044** — **+147 true errors bought with +9,824 false positives**, and review effort
   > from **1.78 to 22.80 cells per real error**. Recall genuinely improves (0.605 ->
   > 0.894), so this is a dial, not a defect. What was missing was the ability to *set* it:
   > `require_declared_fds_for_autoapply` runs after detection and filters fixes, so it
   > stopped writes while leaving every flag in the human queue.
   >
   > Now controllable: `fd_detection_source` on `RepairPipelineRequest`
   > (`--fd-detection {declared,accepted,none}`) narrows which FDs may raise issues, and
   > `profile --constraints-out` warns what accepting them would cost before you accept.
   > This document's own framing of "inferred constraints inform review" as the *safe*
   > outcome understated the price of informing review.
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
