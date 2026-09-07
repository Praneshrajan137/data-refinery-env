# Benchmark Report

## Reproduction

`dataforge bench --methods random,heuristic --datasets hospital,flights --seeds 3 --seed-list 0,1,2`

## Configuration

- Methods: random, heuristic
- Datasets: hospital, flights
- Seeds: 3
- Exact seed list: 0, 1, 2
- Evidence schema: `dataforge_benchmark_run_v2`
- Git commit: `236df758dbdd3d55bfa99d02eea64928e6dd8979`; dirty worktree: `true`
- Free-tier quota units: `max(llm_calls / 1000, (prompt_tokens + completion_tokens) / 100000)`
- GRPO compute cost is reported as free-tier GPU-hours, not dollars.
- Dataset bytes are pinned to BigDaMa/raha revision `7be1334b8c7bbdac3f47ef514fb3e1e8c5fc181c` for hospital, flights; dirty/clean SHA-256s are recorded in the JSON metadata.

## Cross-Dataset Local Results

| Method | Precision | Recall | F1 | Avg Steps | Quota Units | GPU Hours |
| --- | --- | --- | --- | --- | --- | --- |
| heuristic | 0.3585 | 0.4430 | 0.3963 | 361.50 | 0.0000 | 0.0000 |
| random | 0.0057 | 0.0004 | 0.0008 | 125.50 | 0.0000 | 0.0000 |

## Per-Dataset Local Results

### Hospital

| Method | Precision | Recall | F1 | Avg Steps | Quota Units | GPU Hours |
| --- | --- | --- | --- | --- | --- | --- |
| random | 0.0065 +/- 0.0113 | 0.0007 +/- 0.0012 | 0.0012 +/- 0.0021 | 51.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |
| heuristic | 0.7170 +/- 0.0000 | 0.8861 +/- 0.0000 | 0.7926 +/- 0.0000 | 630.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |

### Flights

| Method | Precision | Recall | F1 | Avg Steps | Quota Units | GPU Hours |
| --- | --- | --- | --- | --- | --- | --- |
| random | 0.0050 +/- 0.0087 | 0.0002 +/- 0.0003 | 0.0004 +/- 0.0007 | 200.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |
| heuristic | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 93.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |

## Citation-Only SOTA Reference

Sources:

- [BClean: A Bayesian Data Cleaning System](https://arxiv.org/abs/2311.06517); Table 4; source SHA-256 `40f85c91e20383131488b758be46fa2aae54e591cd5973824688f301d93c2715`; retrieved `2026-05-25T00:00:00Z`.
- [Data Cleaning Using Large Language Models](https://arxiv.org/abs/2410.15547); Table 1; source SHA-256 `da4b1eaf974f33dc4b4d87964b0b851d5343d0270b99a7847cfb1e021b2f82e5`; retrieved `2026-09-06T00:00:00Z`.

HoloClean rows are transcribed from BClean Table 4; see [HoloClean 2017](https://www.vldb.org/pvldb/vol10/p1190-rekatsinas.pdf) for the original system description.

| Method | Dataset | Precision | Recall | F1 | Source | Note |
| --- | --- | --- | --- | --- | --- | --- |
| BClean | hospital | 0.998 | 0.956 | 0.976 | BClean T4 | Citation-only; the source system of Table 4. Above this repository on hospital. Note the paper's own abstract headlines 'F-measure of up to 0.9'; 0.976 is this single table row, so quoting it is the reading least favourable to us and most favourable to BClean. |
| BClean (PI/PIP) | hospital | 1.000 | 0.960 | 0.980 | BClean T4 | Citation-only; best hospital F1 in Table 4. |
| PClean | hospital | 1.000 | 0.927 | 0.962 | BClean T4 | Citation-only literature result; not rerun by this repository. |
| GARF | hospital | 1.000 | 0.556 | 0.715 | BClean T4 | Citation-only literature result; not rerun by this repository. |
| HoloClean | hospital | 1.000 | 0.456 | 0.626 | BClean T4 | Citation-only literature result; not rerun by this repository. |
| HoloClean | flights | 0.742 | 0.352 | 0.477 | BClean T4 | Citation-only literature result; not rerun by this repository. |
| Raha+Baran | hospital | 0.971 | 0.585 | 0.730 | BClean T4 | Citation-only. NOT a figure Raha or Baran published: BClean configured and ran them. Cocoon re-runs the same baseline independently at 0.72. |
| Raha+Baran | flights | 0.829 | 0.650 | 0.729 | BClean T4 | Citation-only literature result; not rerun by this repository. |
| Cocoon | hospital | 0.870 | 0.930 | 0.900 | Cocoon T1 | Citation-only; LLM-based. Above this repository on hospital. Measured with ground truth supplied to the cleaning step. |
| Cocoon | flights | 0.910 | 0.420 | 0.570 | Cocoon T1 | Citation-only. The paper attributes the low recall to benchmark ambiguity in Flight Number -> Actual Arrival Time and argues it is 'preferable to preserve these to represent the uncertainty' -- an independent SOTA system concluding that abstention is correct here. |
| Raha+Baran (Cocoon re-run) | hospital | 0.910 | 0.600 | 0.720 | Cocoon T1 | Citation-only. Independent re-run of the same baseline BClean reports at 0.730, reproducing it within 0.01 under a different protocol. |
| Raha+Baran (Cocoon re-run) | flights | 0.840 | 0.610 | 0.700 | Cocoon T1 | Citation-only. Independent re-run of the baseline BClean reports at 0.729 on flights. |

## Methodology

Local rows are reproduced from generated JSON. Citation-only SOTA rows are copied from literature and are not rerun in this repository. LLM quota units are free-tier fractions; GRPO compute cost is GPU-hours, not dollars.
