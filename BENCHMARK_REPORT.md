# Benchmark Report

## Reproduction

`dataforge bench --methods random,heuristic,llm_zeroshot,llm_react --datasets hospital,flights,beers --seeds 3`

## Configuration

- Methods: random, heuristic, llm_zeroshot, llm_react
- Datasets: hospital, flights, beers
- Seeds: 3
- Free-tier quota units: `max(llm_calls / 1000, (prompt_tokens + completion_tokens) / 100000)`

Skipped methods in this reproduced run: DATAFORGE_LLM_PROVIDER must be set to groq.

## Cross-Dataset Local Results

| Method | Precision | Recall | F1 | Avg Steps | Quota Units |
| --- | --- | --- | --- | --- | --- |
| heuristic | 0.0000 | 0.0000 | 0.0000 | 134.33 | 0.0000 |
| llm_react | Skipped | Skipped | Skipped | Skipped | Skipped |
| llm_zeroshot | Skipped | Skipped | Skipped | Skipped | Skipped |
| random | 0.0038 | 0.0003 | 0.0005 | 150.33 | 0.0000 |

## Per-Dataset Local Results

### Hospital

| Method | Precision | Recall | F1 | Avg Steps | Quota Units |
| --- | --- | --- | --- | --- | --- |
| random | 0.0065 +/- 0.0113 | 0.0007 +/- 0.0012 | 0.0012 +/- 0.0021 | 51.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |
| heuristic | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 40.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |
| llm_zeroshot | Skipped | Skipped | Skipped | Skipped | Skipped |
| llm_react | Skipped | Skipped | Skipped | Skipped | Skipped |

### Flights

| Method | Precision | Recall | F1 | Avg Steps | Quota Units |
| --- | --- | --- | --- | --- | --- |
| random | 0.0050 +/- 0.0087 | 0.0002 +/- 0.0003 | 0.0004 +/- 0.0007 | 200.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |
| heuristic | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 93.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |
| llm_zeroshot | Skipped | Skipped | Skipped | Skipped | Skipped |
| llm_react | Skipped | Skipped | Skipped | Skipped | Skipped |

### Beers

| Method | Precision | Recall | F1 | Avg Steps | Quota Units |
| --- | --- | --- | --- | --- | --- |
| random | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 200.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |
| heuristic | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 0.0000 +/- 0.0000 | 270.0000 +/- 0.0000 | 0.0000 +/- 0.0000 |
| llm_zeroshot | Skipped | Skipped | Skipped | Skipped | Skipped |
| llm_react | Skipped | Skipped | Skipped | Skipped | Skipped |

## Citation-Only SOTA Reference

Source: [BClean: A Bayesian Data Cleaning System](https://szudseg.cn/assets/papers/vldb2024-qin.pdf)

HoloClean rows are transcribed from BClean Table 4; see [HoloClean 2017](https://www.vldb.org/pvldb/vol10/p1190-rekatsinas.pdf) for the original system description.

| Method | Dataset | Precision | Recall | F1 | Note |
| --- | --- | --- | --- | --- | --- |
| HoloClean | hospital | 1.000 | 0.456 | 0.626 | Citation-only literature result. |
| HoloClean | flights | 0.742 | 0.352 | 0.477 | Citation-only literature result. |
| HoloClean | beers | 1.000 | 0.024 | 0.047 | Citation-only literature result. |
| Raha+Baran | hospital | 0.971 | 0.585 | 0.730 | Citation-only literature result. |
| Raha+Baran | flights | 0.829 | 0.650 | 0.729 | Citation-only literature result. |
| Raha+Baran | beers | 0.873 | 0.872 | 0.873 | Citation-only literature result. |

## Methodology

Local rows are reproduced from generated JSON. Citation-only SOTA rows are copied from literature and are not rerun in this repository. Quota units are reported in free-tier fractions rather than dollars.
