# The entailment witness predicts harm exactly, and it does it without ground truth

**Status**: measured 2026-08-29. Pre-registered in
[eval/preregistration/entailment_witness.md](../../eval/preregistration/entailment_witness.md).
Artifacts: [eval/results/entailment_witness_hospital.json](../../eval/results/entailment_witness_hospital.json),
[eval/results/witness_cost_tax.json](../../eval/results/witness_cost_tax.json).
Reproduce with:

```
python scripts/bench/measure_entailment_witness.py --corpus hospital \
    --artifact eval/results/entailment_witness_hospital.json
python scripts/perf/measure_witness_cost.py --corpus tax \
    --rows 5000 25000 100000 200000 --artifact eval/results/witness_cost_tax.json
```

## The question

`PRODUCT.md`:186-190 states the mechanism that determines corruption and says the obvious
metric is the wrong one:

> **Premise precision does not predict corruption.** Two of the four added dependencies are
> equally false and corrupted **nothing**, because a false dependency is inert where its
> determinant group holds no visible disagreement. [...] What determines harm is whether a
> false premise meets a group that disagrees.

No shipped surface exposes that. At the keystroke where a human grants write authority,
`dataforge constraints review` shows them **hospital's** 116 corruptions and 0.2046 harmful
write rate — a published statistic about somebody else's table, at the moment they authorise
unsupervised writes to their own.

The question is whether the two conjuncts of the harm condition can be computed from the
user's own table, with no ground truth, no solver, and no fitted threshold.

## Result: exact reproduction of every oracle number

`dataforge/witness.py::blast_radius` is a groupby. It never runs `FDViolationDetector`, never
runs `FDViolationRepairer`, and never touches a clean column. Against the
`shipped_accept_all` premise on hospital — the miner's full 0.90-floor output through the
real artifact and merge, 85 dependencies — it predicts:

| quantity | witness prediction | measured oracle | source |
| --- | --- | --- | --- |
| writes | **567** | 567 | [shipped-premise-result.md](shipped-premise-result.md):30 |
| repaired a real error | **451** | 451 | same |
| corrupted a clean cell | **116** | 116 | same |
| wrong value on a real error | 0 | 0 | same |

All four exact. Ground truth enters only to *classify* the prediction, never to produce it.

### Per-column decomposition (criterion F2b)

| column | witness | oracle |
| --- | --- | --- |
| `HospitalOwner` | 30 | 30 |
| `ProviderNumber` | 23 | 23 |
| `HospitalName` | 23 | 23 |
| `State` | 20 | 20 |
| `Stateavg` | 20 | 20 |

### Per-dependency attribution, which is new

Previously only a 25-example sample existed (`_write_exposure` caps `corruption_examples` at
25). This is the full attribution of all 116:

| corruptions | dependency |
| --- | --- |
| 29 | `City -> HospitalOwner` |
| 23 | `ZipCode -> HospitalName` |
| 23 | `ZipCode -> ProviderNumber` |
| 20 | `Condition -> State` |
| 20 | `MeasureCode -> Stateavg` |
| 1 | `ZipCode -> HospitalOwner` |

It **refines** the published attribution rather than merely confirming it.
`shipped-premise-result.md`:53 records `HospitalOwner` 30 as coming from
`City -> HospitalOwner`; the witness shows 29 from that dependency and one from
`ZipCode -> HospitalOwner`. The column total is unchanged and the finding is unaffected, but
the 30th corruption came from a different dependency than assumed.

### The decisive criterion: F2d

Pre-registered as the one that separates capturing the mechanism from flagging falseness:

| dependency | false on clean | witness-predicted corruptions |
| --- | --- | --- |
| `ZipCode -> ProviderNumber` | yes | 23 |
| `ZipCode -> Address1` | yes | **0** |
| `ZipCode -> PhoneNumber` | yes | **0** |

Three equally-false dependencies sharing one determinant, and the witness assigns harm to
exactly the one that caused it. Any predictor that scores dependency falseness passes F2a
through F2c on aggregate and fails here. This is the criterion I said in advance I was
genuinely unsure about.

## A per-candidate number is possible after all, if it is marginal

`dataforge/cli/constraints.py`:380-385 records why the shipped acceptance warning carries no
per-candidate figure, and the reasoning is sound:

> `docs/trust/constraint-additivity.md` measures that per-candidate harm **does not
> compose**: summed over hospital's 85 candidates in isolation it is 330, while accepting all
> 85 together yields 116 [...] So a per-candidate number would overstate harm by a factor
> that depends on what else the reviewer accepts, which is worse than giving no per-candidate
> number at all.

That is correct about *isolated* per-candidate harm. It does not rule out a **marginal**
figure — the change in blast radius from accepting a candidate given what is already accepted
— because that quantity conditions on exactly the confound the objection names.

Measured, accepting hospital's 85 dependencies one at a time in canonical order:

| quantity | value |
| --- | --- |
| cells written by the full accepted set | 567 |
| sum of marginal deltas along the acceptance path | **567** |
| sum of each candidate's radius measured in isolation | **2779** |

The marginal decomposition is **exact**. The isolated sum overstates by 4.9x, which
reproduces the additivity finding on the write-count axis and quantifies it: the reason not
to show an isolated number is that it is nearly five times too large.

So the reviewer-facing quantity is well defined: *accepting this candidate, given what you
have already accepted, writes N more cells and destroys M values that currently disagree.*
Both are observable on a table with no ground truth. What must not be shown is a
context-free per-candidate figure.

## The witness is now in the attestation, and it breaks the strength circularity

Added 2026-08-29. Each constraint-derived fix in the predicate carries a `witness`: the
entailing constraint, the determinant group, the value distribution, and the support the
written value had.

**Why this is not the same check as `strength_is_earned`.** `_check_strength` re-derives
`verification_strength` by calling `verification_strength_for` — the *same function object*
the engine calls to stamp the field. Within one language it therefore validates field
consistency, not the rule: a wrong trust model is invisible to it, which is precisely the axis
`decimal_shift` lived on. An attestation from that window would have verified clean.

A witness states arithmetic that can contradict itself. Conformance vector
`reject-witness-without-a-strict-majority` carries a fix with `deterministic` provenance and
`proven` strength whose witness shows 2 votes of 5 across four distinct values. Every other
check in the suite accepts it — including `strength_is_earned`, asserted directly in
`tests/unit/test_attestation_vectors.py`. Only the witness arithmetic refuses it, because the
shipped rule is a strict majority and mutant `M16` measures what plurality costs: 731
corrupted clean cells against 344 on flights.

**Values are hashed, and that preserves rather than weakens verification.** The predicate
deliberately carries no cell values — `build_attestation` projects each fix to row, column,
detector, provenance and strength. A witness stating a group's distribution in plaintext would
reverse that silently and turn a shareable document into a data-disclosure vector. Every value
is published as `sha256(value)[:16]`, the construction
`dataforge/datasets/wild_corrections.py` already uses. A third party **holding the table**
hashes their own group and compares counts — in SQL, in any language, with no DataForge code.
A party **without** the table learns only the shape. A planted-sentinel test asserts no
plaintext value reaches the payload.

**What the normative verifier deliberately does not do.** It does not recompute the
distribution from the data. That would make the verifier a CSV parser and force two
implementations to agree byte-for-byte on quoting, encodings and line endings —
[apply-rewrites-line-endings.md](apply-rewrites-line-endings.md) records that this project has
already been bitten there. So the normative tier stays integer arithmetic and the data check
belongs to whoever holds the table. That is the honest form of the F4 break: the payload
contains enough to check the derivation **without trusting our rule and without running our
code**.

**Where the witness is computed, and why not in the engine.** The attestation is over the
post-repair file, so the pre-repair group has to be recovered. It is recovered rather than
carried: each applied fix records its own `old_value`, so writing those back reconstructs the
input exactly. Computing witnesses during the repair instead would put `dataforge.witness` on
the write path, trip the criterion-F3 tripwire, and oblige a full K4 re-run — hours of
measurement — to establish that a purely additive evidence field changed no verdict. A witness
is evidence *about* a write; it has no business inside one.

An unwitnessed fix is reported **skipped**, never passed. Absence of evidence must not read as
evidence, the same reason `unsigned` and an unchecked `data_identity` are reported separately.

Conformance now stands at **26 vectors, 20 rejections**, with both implementations agreeing on
every one.

## Cost: linear, measured by counted work (criterion F5)

Wall clock cannot gate on this machine — the same verifier code measured 42 to 352 ms/fix in
one afternoon. The deterministic quantity is cell reads.

| rows | cell reads | reads per row | predicted writes |
| --- | --- | --- | --- |
| 5,000 | 40,078 | 8.02 | 4 |
| 25,000 | 220,552 | 8.82 | 420 |
| 100,000 | 881,955 | 8.82 | 507 |
| 200,000 | 1,764,201 | 8.82 | 643 |

Flat at 8.82 reads per row across a 40x scale range, so the implementation is
O(rows x dependencies). The first implementation was O(rows^2 x dependencies) — a per-cell
scan for co-grouped rows — which is 10^10 row comparisons on tax and would have made the
instrument unusable on precisely the corpus that tests its limits. Observed wall clock on the
full 200,000-row tax corpus was 7.15s; that is recorded for context and is **not** a budget.

## Criterion F3: no verdict could have changed

The witness is imported by nothing in the product. Grepping `dataforge/`, `dataforge-mcp/`,
`packages/` and `playground/` for `dataforge.witness` returns only the measurement script.
That is a stronger and cheaper proof than re-running the K4 arms — which cost hours
(`shipped-premise-result.md`:99) — because a module no write path imports cannot have moved a
verdict.

When the witness is wired into a surface, F3 becomes a real obligation and the K4 fence
(FD counts 53/81/85, repairs 393/451/451, corruptions 0/86/116, `replication_mismatches` 0)
must be re-run.

## Criterion F1: honest scoping, and it is narrower than the pre-registration hoped

F1 required the witness to cover every kind in `REPAIR_SUPPORTED_CONSTRAINT_KINDS`
(`column_type`, `domain_bound`, `functional_dependency`) or ship labelled per-kind. **It
ships labelled.**

The scoping argument is about which detectors can *write*, not which constraint kinds exist.
`CONSTRAINT_CHECKABLE_DETECTORS` is `{fd_violation, missing_value}`, so those two are the
only paths a constraint-derived write can take:

- **`fd_violation` — covered.** It accounts for 116 of 116 measured corruptions.
- **`missing_value` — not covered.** It writes only on unanimity, so its group holds no
  disagreement and `destroys` is 0 *by construction*. That is an argument from the
  decision rule, not a measurement, and it is why this path is lower priority rather than
  why it is safe. `PRODUCT.md`:232-236 also records that its mined-premise arm is
  unreachable on every corpus, so there is currently no corpus on which to measure it.
- `column_type` and `domain_bound` constrain what a verifier will *accept*; they do not
  drive a rewrite, so there is no blast radius to enumerate.

A reviewer preview built on this must therefore say what it covers. Presenting an FD-only
blast radius as "the consequence of this acceptance" would be the same over-claim this
document exists to correct.

## What this does not establish

- **Not that an accepted dependency is correct.** The witness shows consequence, not truth.
  A reviewer who accepts after seeing 23 destroyed values has made an informed choice, not a
  verified one.
- **Not that any reviewer will decide differently.** Whether the preview changes a real
  acceptance decision is unmeasured until a design partner uses it. Claiming otherwise would
  be claiming a behavioural result from a code change.
- **Nothing about Q1.** [constraint-circularity.md](constraint-circularity.md):32-41
  forecloses deciding in-table whether a violation of a *true* dependency is an error to fix
  or legitimate variation to keep. Conceded in full. The witness reports what would be
  overwritten; it does not adjudicate whether overwriting is right.
- **Not generalisable beyond hospital.** flights and rayyan mine no candidates, and tax
  mines four true ones, so hospital is the only corpus that can test F2 at all. The cost
  measurement uses tax because scale is the only thing tax can contribute here. Same limit
  as [shipped-premise-result.md](shipped-premise-result.md):129.
- **Not a confidence gate.** `PRODUCT.md`:213-221 refused to ship `tested_confidence` as a
  gate because its separating constant is fitted to 85 candidates from one corpus with
  nothing to validate it against. That refusal stands. The witness enumerates the actual
  consequence rather than scoring the likelihood of one, so it introduces no parameter.

## The mechanism, stated as the witness computes it

A dependency does harm where a determinant group contains disagreement AND the majority
value differs from the cell being examined. `ZipCode -> Address1` is false, but every
ZipCode group in hospital agrees on its Address1, so there is nothing to resolve and nothing
is written. `ZipCode -> ProviderNumber` is false and its groups disagree, so the majority
overwrites the minority 23 times.

One consequence deserves emphasis because it constrains any reviewer interface built on
this: **blast radius is not a per-dependency property.**
`FDViolationRepairer._propose` consults dependencies in canonical determinant order and
returns on the first applicable one — so an earlier-sorted determinant can mask a candidate
entirely, and a candidate can mask an accepted one. The `ZipCode -> HospitalOwner` row above
is that precedence made visible. A per-candidate number computed in isolation would be wrong
in both directions, so the reviewer-facing quantity must be the **marginal** blast radius of
accepting a candidate given the current accepted set
(`dataforge/witness.py::marginal_blast_radius`).
