---
license: apache-2.0
task_categories:
- text-generation
language:
- en
tags:
- dataforge
- data-quality
- supervised-fine-tuning
- tabular
- expert-trajectories
size_categories:
- 1K<n<10K
---

# DataForge SFT Trajectories

This dataset contains chunk-level `expert_v1`, versioned `expert_v2`,
inferability-audited `expert_v3`, and contract-repair `expert_v4`
supervised-fine-tuning records for the DataForge warmup model. The current
milestone is built from
split-safe dirty/clean CSV diffs (`oracle_from_clean_diff`) so model training is
anchored to audited labels rather than teacher guesses.
The v4 handoff may also include a separately identified auxiliary Hospital
format-normalization slice (`synthetic_from_canonical_train_rows`) derived only
from canonical training rows. That slice is repair practice for GRPO/SFT
alignment, not public real-world benchmark evidence.

The earlier `v0-smoke` checkpoint proved the Kaggle-to-Hugging-Face handoff. It
is not a performance-improvement claim.

## Contents

- `expert_v1.jsonl`: original auditable chat-style repair trajectories.
- `expert_v2.jsonl`: repair-contract-aligned trajectories with four-row target
  windows, bounded context, capped repair density, and no-op gates.
- `expert_v3.jsonl`: repair-contract-v2 trajectories with an `inferability`
  label per record. Training handoffs should include only
  `deterministic_normalization` and `context_derivable` records.
- `expert_v4.jsonl`: repair-contract-v2 trajectories for precision repair:
  repair-bearing examples are limited to `deterministic_normalization`, while
  `external_reference_required` and `not_inferable_from_prompt` records are
  explicit `finish` abstentions. Auxiliary synthetic Hospital records, when
  present, use the dataset id `hospital_synthetic_deterministic_v1` and carry
  their base dataset and synthetic policy in provenance.
- `expert_v5_repair_curriculum.jsonl`: split-safe SFT-v5 curriculum generated
  from `expert_v4.jsonl` by oversampling deterministic repair records and
  retaining external-reference/not-inferable hard-negative no-op examples. It
  preserves `schema_version: expert_v4` for parser compatibility and adds
  `curriculum_version: expert_v5_repair_curriculum`.
- `expert_v6_contract_minimal.jsonl`: contract-minimal follow-up curriculum
  generated from SFT-v5 after the private SFT-v5 candidate failed quality
  gates. It removes assistant `reason` fields, caps repair targets per example,
  and keeps no-op abstention examples so the next SFT run attacks parse/schema
  drift before GRPO-v3.
- `split_manifest.json`: deterministic train/eval row manifest containing row
  ids and dirty-row SHA-256 hashes only; it contains no clean labels, suggested
  values, or repair targets.
- `split_manifest_v2.json`: v2 manifest with the same no-label leakage contract.
- `split_manifest_v3.json`: v3 manifest with the same no-label leakage contract
  plus the inferability policy used to build the handoff.
- `split_manifest_v4.json`: v4 manifest with the same no-label leakage contract
  plus the deterministic promotion-slice policy.
- `sft_05b.yaml`: pinned Kaggle training configuration.
- `sft_05b_v2.yaml`: pinned remote-only training/evaluation configuration.
- `sft_05b_v3.yaml`: gated v3 configuration; do not launch it until oracle
  replay, strict parsing, and inferability-count gates pass locally.
- `sft_05b_v4.yaml`: gated v4 configuration; do not launch it until deterministic
  repair, abstention, strict parsing, and split-manifest gates pass locally.
- `sft_05b_v5.yaml`: private repair-curriculum candidate configuration. It is
  preserved as failed diagnostic evidence after strict held-out eval showed
  parse/schema drift and insufficient active repair signal.
- `sft_05b_v6.yaml`: private contract-first candidate configuration over
  `expert_v6_contract_minimal.jsonl`. It uses staged Kaggle modes and may feed
  GRPO-v3 only after `sft_v6_candidate_eval_report.json` has
  `promote_to_grpo: true` and points to an uploaded private checkpoint;
  otherwise GRPO-v3 stays blocked.
- `MODEL_CARD_TEMPLATE.md`: model-card template used by the publishing notebook.

Each JSONL row includes the schema version, trajectory id, task id, dataset,
difficulty, seed, chunk index, observed state, chat messages, tool-call summary,
proposed fixes, teacher/oracle metadata, evaluation metrics, split metadata, and
source provenance.

Chunks with no dirty/clean differences are kept as hard-negative `finish`
examples. They teach the same inference contract as evaluation: do not repair a
cell unless the dirty rows justify an exact replacement.

## Provenance

Primary records are generated from deterministic train rows over the Raha
Hospital, Flights, and Beers benchmark sources. The held-out row split is chosen
before chunking, and held-out rows are excluded from target rows, context rows,
normalization candidates, fixes, and messages.
Regenerated v4 manifests also record dataset-level source revision,
dirty/clean SHA-256, row counts, column counts, and ground-truth cell counts so
GRPO readiness can reject stale caches before GPU training.

Hospital's canonical Raha labels are not converted into repair-bearing v4
examples unless the exact clean value is inferable from the prompt. When the
canonical Hospital slice is abstention-heavy, the auxiliary
`hospital_synthetic_deterministic_v1` slice provides deterministic formatting
repairs from train rows only; it shares the same held-out row split and is
tracked separately from real-world evidence.

Flights is supervised from dirty/clean labels. Schedule and actual-time repairs
are marked as `oracle_from_clean_diff`; they are not Groq, Cerebras, or Gemini
discoveries.

For `expert_v4`, Flights time repairs are trainable only when the corrected
value can be derived by deterministic syntax normalization from the dirty prompt
text. Blank fills, schedule lookup, and real-world flight facts are abstention or
auxiliary evaluation cases, not repair-bearing SFT labels.

Legacy `llm_react_chunk` records may remain for smoke-lineage auditability, but
they are not the source of exact Flights schedule labels.

## Intended Use

- Reproducing the DataForge 0.5B SFT warmup workflow.
- Auditing the exact data handed to the Kaggle notebook.
- Training or debugging small tabular repair agents under DataForge's verifier
  and transaction assumptions.

## Limitations

- This is a supervised training dataset, not a leaderboard.
- It should not be treated as a broad data-cleaning benchmark.
- Legacy teacher outputs can contain mistakes even after filtering.
- The dataset is not a substitute for held-out evaluation; use
  `dataforge-evals` to compare model outputs against exact cell-level ground
  truth before making quality claims.
