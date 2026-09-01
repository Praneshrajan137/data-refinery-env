# SPEC: Premise Quality

> Status: Draft
> Owner: @pranesh
> Last updated: 2026-09-01

## 1. Purpose (2 sentences)

Score a mined functional dependency against the null hypothesis that its determinant
carries no information about its dependent, using a measure whose decision point is zero
rather than a constant fitted to a corpus. This exists so the premise the product repairs
against can be gated without violating `eval/preregistration/premise_quality.md`'s K3, which
forbids introducing a tunable parameter after seeing the data.

## 2. Outcomes (measurable, binary pass/fail)

- [ ] `dataforge.premise_quality.mu_plus()` computes the Piatetsky-Shapiro and Matheus
      corrected measure from determinant groups, with no third-party dependency.
- [ ] `mu_plus` returns exactly `0.0` when every determinant group is a singleton, rather
      than raising `ZeroDivisionError`.
- [ ] `mu_plus` returns exactly `0.0` when the determinant forms a single group, because
      such a determinant carries no information beyond the dependent's own distribution.
- [ ] `mu_plus` returns `1.0` for an exact dependency on a non-constant dependent.
- [ ] `mu_plus` is invariant to row order and to relabelling of values.
- [ ] `g3_prime()` is computed and reported alongside, and is **not** gated on.
- [ ] `ConstraintCandidate` carries `mu_plus` and `g3_prime` as reported fields.
- [ ] `dataforge constraints review` surfaces both, in the machine-readable summary and the
      human table.
- [ ] No new constant is introduced anywhere in the diff. Verifiable by inspection.

## 3. Why not a dependency

`afd-measures` (MIT, `pip install afd-measures`) is the reference implementation from
Parciak et al., ICDE 2024, and it ships `mu_plus`, `g3_prime` and the RFI family. It is
**deliberately not adopted**, for three reasons, in increasing order of weight:

1. **It would be 15 lines of arithmetic behind a dependency.** `mu` is closed-form over
   quantities `_fd_candidates` already computes; there is nothing to import.
2. **Its signature takes a single-column `lhs` over a `pandas.DataFrame`.** The product's
   write path operates on `dataforge.table.Table`. Adopting it would push a DataFrame
   conversion into the product path and **reintroduce the measured-path/shipped-path gap**
   that `docs/trust/fd-repair-scalability.md` documents and that this project's own rule
   forbids: a measurement of something adjacent to the product is not a measurement of the
   product.
3. **Its `mu` divides by `(r_size - domX_size)` with no guard.** When every determinant
   group is a singleton that is a division by zero — and that is exactly the case the gate
   exists to reject, so it is not a rare path. A local implementation can return the
   mathematically correct limit.

The reference implementation remains the right cross-check if a future reviewer wants one,
and the closed form below is stated so the two can be compared without installing anything.

## 4. Definitions

For a candidate `X -> Y` over `N` rows, with determinant groups `g_x` (the rows sharing each
determinant value `x`), `c_xy` the count of dependent value `y` within group `x`, and `c_y`
the count of `y` over the whole column:

```
pdep(X->Y) = (1/N) * SUM_x [ (1/|g_x|) * SUM_y c_xy^2 ]
pdep(Y)    = (1/N^2) * SUM_y c_y^2

mu   = 1 - [ (1 - pdep(X->Y)) / (1 - pdep(Y)) ] * [ (N - 1) / (N - |dom_X|) ]
mu+  = max(mu, 0)
```

`pdep(X->Y)` is the probability that two rows drawn from the same determinant group agree on
the dependent; `pdep(Y)` is the same probability ignoring the determinant. The left bracket
is therefore the proportional reduction in error, and **the right bracket is the correction**:
`(N-1)/(N-|dom_X|)` is the expected inflation from unfalsifiable singleton groups. It
diverges as `|dom_X| -> N`.

`g3_prime` is the combinatorial sibling (Giannella and Robertson), which subtracts the same
floor and rescales:

```
g3'(X->Y) = ( SUM_x max_y c_xy - |dom_X| ) / ( N - |dom_X| )
```

Both are reported. Only `mu+` is gated, because Parciak et al. measure `g3'` as sensitive to
RHS-skew where `mu+` is not.

## 5. Required behaviour at the boundaries

Each row states the value and the reason it is correct, not merely safe.

| Condition | Result | Why |
| --- | --- | --- |
| `N < 2` | `0.0` | Two rows are needed before "agreement" is defined. |
| `|dom_X| == N` (all singletons) | `0.0` | The limit. No group can be falsified, so no evidence exists. This is the case the gate must reject and it is reached in normal use. |
| `|dom_X| == 1` (single group) | `0.0` | `pdep(X->Y) == pdep(Y)` identically, so the bracket is 1 and `mu == 0`. A determinant with one value tells you nothing you did not already know from `Y`. Falls out of the formula; asserted so a future refactor cannot lose it. |
| `pdep(Y) == 1` (constant dependent) | `0.0` | Division by zero in the left bracket. A constant dependent is determined by everything, so the dependency is vacuous. The miner already rejects these; the function must not depend on that. |
| exact FD, non-constant `Y` | `1.0` | `pdep(X->Y) == 1`, numerator zero. |
| `mu < 0` | `0.0` | `X` carries *less* information than the permutation null. Negative values mean "weak evidence", per Parciak et al., and are clamped, not reported as a magnitude. |

### 5.1 The blind spot, found by test rather than by reading

**`mu+` does not penalise an exact dependency for being unfalsifiable.** For an exact
dependency `pdep(X->Y) == 1`, so the numerator `(1 - pdep(X->Y))` is zero and the singleton
correction — however large — is multiplied by zero. `mu+` is 1.0 whether the dependency rests
on two testable rows or two thousand.

This was discovered by writing a test that asserted the opposite and watching it fail, which
is the reason the tests were written before the implementation. `g3'` shares the property for
the same reason: both are error-based measures normalised so that zero error scores 1.

Consequences, stated rather than worked around:

- The correction discriminates among **approximate** dependencies only. That is where the
  measured damage lives — hospital's false dependencies score `confidence` 0.9050 to 0.9620,
  so they are approximate and the correction does bite on them — but the limit is real.
- **The pre-existing near-key guard is not redundant and must not be removed.**
  `_MAX_DETERMINANT_UNIQUE_FRACTION = 0.9` is what rejects an exact dependency on a
  near-unique determinant, and it is a constant that predates C3. C3 does not eliminate it
  and does not claim to.
- Anyone reading `mu+ = 1.0` should read it as "no measured error", not as "well supported".
  Support is a separate quantity and the reviewer is shown it separately.

## 6. Non-goals

- **Q1 is untouched.** Whether the violations of a *true* dependency are errors to fix or
  legitimate variation to keep remains undecidable in-table and conceded in full. `mu+`
  answers only Q2: does the dependency hold at all.
- **No threshold other than zero.** A cut at `mu+ > c` for any other `c` is the fitted
  constant K3 forbids. If `mu+ > 0` proves too permissive, the correct outcome is to report
  that `mu+` does not gate — not to fit one.
- **Multi-column determinants are out of scope here.** `_fd_candidates` mines single-column
  determinants only. The formula generalises by treating the determinant tuple as the group
  key, and the implementation takes pre-computed groups so it already supports that, but no
  caller exercises it and nothing here claims it is measured.

## 7. Verification

- Unit tests with **hand-computed** expected values, derived from the closed form in section
  4 independently of the implementation, so the test is not a restatement of the code.
- Property tests: row-order invariance, value-relabelling invariance, and range `[0, 1]`.
- Every boundary in section 5 has its own test asserting the value **and** that no exception
  is raised.
- The measurement in `eval/preregistration/premise_quality_measure.md` is the outcome test.
  Passing unit tests establishes the formula is implemented; only that measurement
  establishes whether it separates true from false dependencies, and it may refute it.
