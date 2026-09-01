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

Source: [BClean: A Bayesian Data Cleaning System](https://arxiv.org/abs/2311.06517); Table 4; source SHA-256 `40f85c91e20383131488b758be46fa2aae54e591cd5973824688f301d93c2715`; retrieved `2026-05-25T00:00:00Z`.

HoloClean rows are transcribed from BClean Table 4; see [HoloClean 2017](https://www.vldb.org/pvldb/vol10/p1190-rekatsinas.pdf) for the original system description.

| Method | Dataset | Precision | Recall | F1 | Note |
| --- | --- | --- | --- | --- | --- |
| BClean | hospital | 0.998 | 0.956 | 0.976 | Citation-only; the source system of Table 4. Above this repository on hospital. |
| BClean (PI/PIP) | hospital | 1.000 | 0.960 | 0.980 | Citation-only; best hospital F1 in Table 4. |
| PClean | hospital | 1.000 | 0.927 | 0.962 | Citation-only literature result; not rerun by this repository. |
| GARF | hospital | 1.000 | 0.556 | 0.715 | Citation-only literature result; not rerun by this repository. |
| HoloClean | hospital | 1.000 | 0.456 | 0.626 | Citation-only literature result; not rerun by this repository. |
| HoloClean | flights | 0.742 | 0.352 | 0.477 | Citation-only literature result; not rerun by this repository. |
| Raha+Baran | hospital | 0.971 | 0.585 | 0.730 | Citation-only literature result; not rerun by this repository. |
| Raha+Baran | flights | 0.829 | 0.650 | 0.729 | Citation-only literature result; not rerun by this repository. |

## Methodology

Local rows are reproduced from generated JSON. Citation-only SOTA rows are copied from literature and are not rerun in this repository. LLM quota units are free-tier fractions; GRPO compute cost is GPU-hours, not dollars.
