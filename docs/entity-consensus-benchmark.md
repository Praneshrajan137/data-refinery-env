# Entity-consensus fixing benchmark (flights)

Reproducible, honest measurement of DataForge's cross-row **entity-consensus**
repairer on the RAHA `flights` dataset - the lever that takes flights correction
from fixing *nothing* to a real, fully-automatic result.

This is the discoverable home for the measured number (the citations-only
`eval/results/sota_comparison.json` is deliberately left pure - it holds *cited*
external SOTA only, never our measured rows).

## Result

`flights` is multi-source: the same flight is reported by ~24 sources, so the
correct value for a cell already exists in its sibling rows. The
`EntityConsensusRepairer` (gated behind `allow_entity_consensus`) proposes that
consensus value.

| Metric | Value |
| --- | --- |
| Correction F1 | **0.4467** (from 0.0000 deterministic baseline) |
| Precision | 0.8414 |
| Recall | 0.3041 |
| TP / FP / FN | 1496 / 282 / 3424 |
| LLM calls | 0 (deterministic) |

Reproduce (deterministic, no credit, ~seconds):

```
dataforge bench --methods entity_consensus --datasets flights --seed-list 0 \
  --output-json eval/results/entity_consensus_flights.json
```

Committed artifact: `eval/results/entity_consensus_flights.json`.

## Honest comparison to published SOTA

| Method | flights F1 | Supervision | Evidence |
| --- | --- | --- | --- |
| DataForge entity_consensus | **0.4467** | **unsupervised, gated, propose-first** | measured (artifact above) |
| Raha+Baran | 0.729 | **semi-supervised** (iteratively asks a user to annotate tuples) | cited, BClean Table 4 |
| HoloClean | 0.477 | weakly-supervised (rules + statistics) | cited, BClean Table 4 |

Read this comparison honestly:

- **0.4467 is below Raha+Baran's 0.729 - and that is expected.** Raha+Baran is
  *semi-supervised*: it asks a human to label/fix example tuples and generalizes
  from them (see the Raha/Baran README: "iteratively asks the user to annotate a
  tuple"). DataForge's number is **fully automatic and unsupervised** - no labels,
  no human in the loop for the measured figure.
- **It is auto-apply-gated, not force-applied.** The consensus value is
  `plausibility_only`: held as a pre-filled, one-click review suggestion by
  default, and auto-applied only under the explicit `allow_unproven_autoapply`
  opt-in. The number above scores the *proposals*; the propose tier surfaces more
  candidates for human review if higher recall is wanted (the lower-support tier
  raises recall toward ~0.52 at ~0.99 precision - see DECISIONS 2026-07-26).
- **The trust guarantee is intact.** A wrong majority yields a wrong consensus, so
  it is evidence, not proof; the never-corrupt-by-default invariant is preserved.

Cited SOTA source: BClean, "A Bayesian Data Cleaning System" (arXiv:2311.06517),
Table 4 - the same source pinned in `eval/results/sota_comparison.json`.

See also: DECISIONS.md (2026-07-26 entry) and `docs/STRATEGY.md` for the full
design and trust rationale.
