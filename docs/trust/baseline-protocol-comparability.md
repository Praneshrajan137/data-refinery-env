# Baseline protocol comparability: why no cited number is a head-to-head result

This document establishes one claim and then dismantles the comparison that claim invites:

**DataForge's hospital correction F1 of 0.8352 is not protocol-comparable with any figure in
`BENCHMARK_REPORT.md`'s citation-only table, and the axis they differ on is not scoring or
tuning. It is where the premise came from.**

Every number below is transcribed from a published table whose PDF bytes are hashed in
[eval/results/sota_comparison.json](../../eval/results/sota_comparison.json). Nothing here was
rerun by this repository. That is the point: this document exists because a comparison drawn
from transcription is worth less than it looks, and saying so precisely is cheaper than
pretending otherwise.

## 1. The baseline is real, and it reproduces

The 0.730 Raha+Baran figure is often the only external number this project quotes. Two things
about it are worth stating plainly.

**It is not a figure Raha or Baran published.** BClean configured and ran them; the original
denial constraints and label sets were never released. Attributing 0.730 to the Raha authors
is a mis-citation, and this repository made it for months.

**It nevertheless holds up.** Two independent papers configured the same baseline and landed
within 0.01 of each other on hospital:

| Baseline | hospital P | hospital R | hospital F1 | Configured by |
| --- | --- | --- | --- | --- |
| Raha+Baran | 0.971 | 0.585 | 0.730 | BClean, Table 4 |
| Raha+Baran | 0.910 | 0.600 | 0.720 | Cocoon, Table 1 |

HoloClean corroborates similarly: 0.626 in BClean Table 4 against 0.63 in Cocoon Table 1.

So the baseline is not a soft target invented by one paper. Any temptation to dismiss 0.730 as
an artifact of BClean's configuration is closed off by Cocoon reproducing it independently.

## 2. Four systems are published above this repository on hospital

| System | hospital F1 | Source |
| --- | --- | --- |
| BClean (PI/PIP) | 0.980 | BClean, Table 4 |
| BClean | 0.976 | BClean, Table 4 |
| PClean | 0.962 | BClean, Table 4 |
| Cocoon | 0.900 | Cocoon, Table 1 |

DataForge does not compete on this axis and this document does not argue that it should.

**One qualification that runs against us and must be stated anyway.** BClean's own abstract
headlines "an F-measure of up to 0.9". The 0.976 is a single Table 4 row. Quoting 0.976 is
therefore the reading *least* favourable to this repository and *most* favourable to BClean,
which is the correct direction to err when citing a competitor.

## 3. Why these numbers are not comparable to ours

### 3.1 Their premises were supplied. Ours was mined.

This is the load-bearing difference, and Cocoon states it about its own experiment without
being asked:

- Of its own pipeline: the detection and cleaning steps are intended to be human-in-the-loop,
  and "we skip these and use the LLM provided ground truth."
- Of HoloClean: it "additionally takes denial constraints as input, for which we provide the
  ground truth."
- Of Baran: it "additionally requires feedback on 20 clean cells. We provide the ground truth."

Every figure in Cocoon Table 1 is therefore measured with a premise handed in from outside the
table. BClean is explicitly a system in which a user supplies or corrects a Bayesian network.

DataForge's 0.8352 was measured with a premise **mined from the dirty table itself**. That is a
strictly harder problem, and it is also the problem where this repository's measured failures
live: all 116 clean-cell corruptions on hospital trace to four false mined dependencies. See
[shipped-premise-result.md](shipped-premise-result.md) and
[premise-quality-measure-result.md](premise-quality-measure-result.md).

**Third axis, added 2026-09-08, and it removes the consolation in the paragraph above.** The
mined-premise framing implies that DataForge is solving a harder problem and would do better if
handed a premise the way these baselines are. **Measured, it would not.** Through the shipped write
path on hospital, a premise authored from the corpus's public data dictionary writes **zero**
cells, and a premise admitted *by ground truth* — strictly more than any baseline is handed —
reaches only **F1 0.1918**. So "supply us a premise too" is not the missing ingredient.

The deeper defect is that 0.8352 is a **proposal-stage** figure: it counts what the detector and
repairer propose, before this project's own verifier and auto-apply gate, while every figure in
Cocoon Table 1 and BClean Table 4 is what those systems *output*. So the three axes on which these
numbers are not comparable are **dataset**, **premise**, and **stage** — and the last is entirely
within this project's control. Full result:
[declared-premise-capability.md](declared-premise-capability.md) and
[capability-measurement-stage.md](capability-measurement-stage.md).

The honest reading is not "we are 0.18 behind." It is that the numbers answer different
questions, and the number that would settle ours has now been measured by us rather than published
by anyone else: **there is no demonstrated end-to-end correction capability on hospital.**

### 3.2 Hospital numbers demonstrably do not transfer between harnesses

Cocoon reports that HoloClean's hospital recall came out below the figure in HoloClean's own
paper, and that "despite experimenting with various threshold values, we were unable to
replicate their results."

This is published, first-party evidence that a hospital correction score is a property of a
harness and not only of a system. It is the strongest available argument for this document's
central claim, and it comes from a paper with no stake in it.

### 3.3 The scoring unit differs

Cocoon's evaluation deliberately excludes three error classes from scoring — case
inconsistency, column-type correctness, and disguised missing values — and credits baselines
as correct even where they perform no such cast. Its Table 3 shows that including those
classes moves Cocoon's hospital F1 to 0.99.

A single dataset name therefore does not fix a scoring unit. See
[scoring-unit-reconciliation.md](scoring-unit-reconciliation.md).

## 4. Independent corroboration of abstention, from a system that outscores us

Cocoon's weakest result is flights: recall 0.420 for F1 0.570. Its explanation is not a bug
report. It argues the benchmark itself is ambiguous on `Flight Number -> Actual Arrival Time`,
observes that one flight's arrival time appears as four different values across rows, and
concludes it is "preferable to preserve these to represent the uncertainty."

An independent LLM-based system, published at SOTA on four of five benchmarks, examined the
exact dependency DataForge refuses to act on and concluded that **not writing is the correct
behaviour**. DataForge writes zero cells on flights because its miner finds no dependency
there, and this repository has described flights as the not-inferable-in-table frontier.

That convergence is worth more than a score. It is also the second time an outside party has
independently reached one of this project's conclusions: the `rwd` annotators, on a 114,919-row
instance of the hospital schema, likewise omitted `ZIPCode -> HospitalName`, which this
repository had argued was false from 25 sampled corruptions.

## 5. What would make a comparison legitimate

Nothing in this document licenses a favourable claim, and no transcription ever will. The only
thing that would is running a baseline here, under this harness, on the same injection, scoring
unit and label set. That is tracked as its own task and is not done.

Until it is, the correct phrasing remains the one PRODUCT.md fixes: competitive with, or in the
range of, the Raha+Baran baseline under our scoring — never "beats", and never SOTA.

## 6. Limitations of this document

- **All rows are transcriptions.** Two of them (0.976, 0.900) are numbers this repository has
  an incentive to get right and no ability to verify beyond the hashed PDF.
- **Cocoon's Table 1 column order is inferred from its prose**, which lists the five benchmarks
  as hospital, flights, beers, rayyan, movies. The HTML rendering of the table drops its
  header row. The hospital and flights assignments are corroborated by the paper's own
  discussion of both datasets, but this is a transcription risk and not a certainty.
- **`beers` and `movies` columns are deliberately not transcribed.** `beers` is out of project
  scope; `movies` is not a corpus this repository measures.
- **No claim here is about detection.** These are correction/repair figures only.
