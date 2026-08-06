# What the certification promises, and what it does not

`docs/trust/accuracy-frontier.md` pointed at this document before it existed. This is the
scope statement for `dataforge.conformal` and the corrector auto-apply gate: what the
guarantee covers, and the four specific places it stops.

Everything here is currently **inert in practice** -- every committed threshold sits at the
`1.01` disabled sentinel and `dataforge/release/corrector_gate.py` reports
`enabled_classes == []`. These are the promises that *would* apply if a class were ever
certified, written down before that happens rather than after.

## What is promised

`certify_threshold` performs selective-risk control by fixed sequential testing (Bates et
al. 2021; Angelopoulos et al. 2021). For a certified threshold `t` in issue class `c`:

> With probability at least `1 - delta` **over the calibration draw**, the true error rate
> of the accepted set `{cells in class c with confidence >= t}` is at most `alpha`.

The bound is an exact one-sided Clopper-Pearson upper bound on a binomial rate --
distribution-free and finite-sample, with no normal approximation and no asymptotics.
Certification is **class-conditional (Mondrian)**: each `issue_type` is certified
independently, so the promise is per class rather than merely averaged across classes.

## Limit 1: the guarantee is an average over accepted cells, not a per-cell promise

This is the limit most likely to be misread, and the one with the sharpest real-world
consequence.

A certified threshold says the accepted *set* has error `<= alpha`. It says **nothing about
any individual cell**, and nothing about any subgroup you did not condition on. On a
hospital table, "95% precision across all accepted `format_violation` fixes" does **not**
mean 95% confidence in the fix applied to one particular diagnosis code. If errors
concentrate in a subpopulation -- one column, one hospital, one date range, one rare
category -- the marginal guarantee is still satisfied while that subpopulation is repaired
badly.

Conditioning axes that exist: `issue_type`. Axes that do **not** exist: per column, per
table, per severity, per row-population, per cell.

This is not an oversight that a bigger sample would fix. Distribution-free *conditional*
coverage is unachievable without further assumptions, which is why the marginal form is
what conformal methods offer. The correct response is not to pretend otherwise but to
refuse silent mutation of high-stakes fields regardless of certification -- which is what
`_verification_strength` and the authoritative-schema requirement already do.

## Limit 2: exchangeability is assumed, and only partially checked

The guarantee holds for data **exchangeable** with the calibration sample. A user's table
may be nothing like the calibration benchmark, which silently voids it. Three defences
exist, and each has a stated hole:

| Defence | What it catches | What it misses |
| --- | --- | --- |
| `guard_policy_for_scope` | An artifact fitted on a structurally different table (column-set fingerprint mismatch), and an artifact with **no** recorded scope, which fails closed | Two tables with identical columns but different populations, units, or eras |
| `guard_policy_for_drift_by_class` | Per-`issue_type` shift in the *confidence* distribution (PSI > 0.2) | Pure covariate shift that leaves the confidence histogram intact |
| `guard_policy_for_drift` | Pooled confidence shift, used only as a fallback when no class has enough live samples | Single-class drift masked by the aggregate |

The scope guard **fails closed on unknown**: an artifact that records no scope cannot be
shown to apply, so auto-apply is downgraded rather than assumed valid. Before it existed,
the loader validated JSON shape and nothing else -- any artifact could be pointed at any
table.

A matching fingerprint is a *necessary*, not sufficient, condition. It rules out the
blatant misapplication; it does not establish exchangeability.

## Limit 3: the candidate grid is data-dependent unless you supply one

Fixed sequential testing requires a **pre-specified** candidate sequence. When
`certify_threshold` is called without an explicit `grid`, it derives candidates from the
observed calibration confidences. That is data-dependent selection, and it means the clean
family-wise `delta` claim is **not strictly earned**.

This was previously documented as a coverage/power limitation. That was wrong: it is a
**validity** caveat. The effect is plausibly second-order -- the grid depends on the
confidence *values* while the tests depend on the *labels* -- but it is not zero, and the
result should not be described as an exact distribution-free guarantee when the grid was
fitted. Pass `grid=` to obtain the guarantee as stated.

## Limit 4: the error classes come from a versioned heuristic labeller

Classes are assigned by the heuristic labeller (`LABELER_VERSION`), not by ground truth.
A class-conditional guarantee is only as meaningful as the class definition, so a labeller
change invalidates prior certificates.

## What sits beneath all of this

Certification is not the safety floor. Even a certified fix passes through the SMT verifier
and the safety constitution, and only **proven** fixes auto-apply at all:

```python
deterministic = fix.provenance not in _LLM_PROVENANCE
if deterministic or policy.action_for(fix.fix.detector_id, confidence) == "auto_apply":
```

Deterministic fixes bypass calibration entirely because they are correct by construction.
A `plausibility_only` fix -- an LLM value with no authoritative schema -- is never
auto-applied unless `allow_unproven_autoapply` is explicitly set, and is then recorded as
unproven. So the practical route to more automation is **expanding what can be proven**,
not raising confidence in what cannot.

## How to check these claims

```
python -m pytest tests/unit/test_conformal.py tests/unit/test_calibration_scope.py -q
python -c "from dataforge.release.corrector_gate import check_corrector_release_gate as c; print(c().enabled_classes)"
```

The second must print `[]` unless a class has been certified *and* has committed passing
promotion evidence.
