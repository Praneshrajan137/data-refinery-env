# SPEC: Week 12 GRPO training

> Status: Draft
> Owner: DataForge maintainers
> Last updated: 2026-06-26

## 1. Purpose

Week 12 adds a free-tier-only GRPO post-training path after the verified SFT
warmup checkpoint. The workflow must train, evaluate, and publish GRPO
checkpoints only when generated evidence proves a real held-out improvement.

## 2. Outcomes

- [ ] `training/configs/grpo_05b.yaml` and `grpo_15b.yaml` encode the corrected
  TRL v1-era GRPO stack, exact package pins, and free-tier memory limits.
- [ ] `training/rewards/dataforge_reward.py` scores completions locally through
  the strict `repair_contract_v2` parser, exact cell-repair metrics, and
  GRPO-only shaping for canonicalized values, precision, and overrepair.
- [ ] `scripts/model/grpo_readiness_report.py` produces a non-claim diagnostic
  report for leak-free expert-v4 prompts, dataset balance, parse/schema stats,
  and reward variance before GPU training.
- [ ] `scripts/data/audit_real_world_sources.py` verifies canonical source
  revision, dirty/clean SHA-256, row counts, column counts, and ground-truth
  cell counts before any regenerated trajectories are considered launchable.
- [ ] `training/kaggle/grpo_kaggle.ipynb` trains with checkpoint resume,
  defaults to a 50-step no-upload smoke stage, and blocks upload unless the
  GRPO release gate passes.
- [ ] `scripts/bench/refresh_benchmark_table.py` merges Week 4 agents with
  trained-model rows and refreshes README/BENCHMARK_REPORT from JSON evidence.
- [ ] `scripts/model/verify_grpo_release.py` rejects incomplete or below-gate
  GRPO model repos before docs cite them.
- [ ] `scripts/remote/kaggle_sft_v5_candidate.py` trains the private SFT-v5
  repair-curriculum predecessor and writes `sft_v5_candidate_eval_report.json`
  before any GRPO-v3 smoke, diagnostic, or candidate run can launch.
- [ ] GRPO-v4 remains blocked until a private SFT-v9-or-later predecessor
  report has `promote_to_grpo: true`; the frozen SFT-v8 smoke report has
  `promote_to_grpo: false` and must not seed GRPO, while the SFT-v9 local
  curriculum preflight is not itself a trained checkpoint.

## 3. Scope

**IN**:

- 0.5B GRPO from `Praneshrajan15/DataForge-0.5B-SFT` with fp16 LoRA.
- 0.5B GRPO-v3/v4 from private, gate-passing SFT predecessor reports only.
- 1.5B GRPO from a verified `DataForge-1.5B-SFT` prerequisite with 4-bit QLoRA.
- Manifest-driven 3B/7B GRPO policy rows that stay blocked until explicit
  HF Jobs or equivalent paid GPU evidence exists.
- DataForge-Bench-light-verified evaluation over seeds `0,1,2`.
- GPU-hour accounting for free-tier compute.
- The fixed sequence: 50-step no-upload smoke, 500-step candidate, then
  1000-step candidate only if the 500-step trend improves but misses the gate.

**OUT**:

- Training or publishing 3B+ models on free tier.
- GiGPO publication before a same-size GRPO predecessor is verifier-passed.
- Reward calls to the mutable OpenEnv HTTP singleton during GRPO rollouts.
- Public quality claims without generated verifier artifacts.
- Launching GRPO from failed SFT-v5/v6/v7/v8 diagnostic evidence or from an
  SFT-v9 curriculum/preflight without a promoted private checkpoint.

## 4. Constraints

- `TRL v0.11` is unsupported for this path. GRPO configs target the repo's
  TRL v1-era stack and fail fast on stale pins.
- `max_prompt_length` is treated as local `prompt_token_budget: 1024` and only
  passed to `GRPOConfig` if the installed TRL signature supports it.
- P100/T4 free-tier runs use `num_generations: 4`, completion length `256`,
  batch size `1`, gradient accumulation `16`, fp16, and no bf16.
- The `0.5B-GRPO` release requires at least `+0.03` absolute macro F1 over
  `0.5B-SFT` on DataForge-Bench-light-verified, plus parse success `>=0.99`
  and zero schema-case errors.

## 5. Prior Decisions

- The Week 9 SFT workflow remains the warmup source and must not be described
  as a quality milestone unless verifier output proves improvement.
- README benchmark rows are generated from JSON artifacts only.
- GRPO is selected before GiGPO for the free-tier path because it ships in TRL;
  GiGPO/verl-agent remains heavier setup and memory work.
- SFT-v8 prompt-completion smoke is failed diagnostic evidence: label-mask
  audit passed, but raw parse stayed at `0.03` and strict macro F1 stayed at
  `0.0`, so more GRPO is the wrong next move.
- SFT-v9 action-envelope work is the next private predecessor path. Its local
  curriculum preflight proves completion parse `1.0`, no held-out leakage, and
  no negative-contrast target leakage, but GRPO-v4 remains blocked until SFT-v9
  strict held-out eval and private upload produce `promote_to_grpo: true`.

## 6. Task Breakdown

### 6.1 Configs and import preflight

- Acceptance: exact pins, no TRL v0.11, fp16/no-bf16, prompt-token budget
  mapping, and `PYTHONUTF8=1` import guidance are covered by tests.
- Depends on: none.
- Estimated complexity: S.

### 6.2 Reward and readiness

- Acceptance: exact repairs, no-op finish records, malformed JSON, duplicate
  repairs, wrong rows, wrong columns, overrepair, canonicalized value matches,
  and schema-case errors are scored deterministically without network. The
  readiness report blocks stale v1 prompts, held-out row leakage, weak dataset
  balance, missing source provenance, low global reward variance, and low
  per-dataset reward variance.
- Depends on: `dataforge.repair_contract`.
- Estimated complexity: M.

### 6.3 Kaggle notebook

- Acceptance: six main cells load config, train with `GRPOTrainer`, resume
  checkpoints, merge adapters, write diagnostics, and upload only after gate.
- Depends on: configs and reward function.
- Estimated complexity: M.

### 6.4 Benchmark and release gates

- Acceptance: trained rows merge with agent rows, GPU-hours render in reports,
  and GRPO Hub repos fail verification without complete metrics and diagnostics.
- Depends on: benchmark report helpers and HF model evidence.
- Estimated complexity: M.

## 7. Verification

- Unit: `tests/unit/test_grpo_configs.py`,
  `tests/unit/test_dataforge_grpo_reward.py`,
  `tests/unit/test_grpo_contract_parity.py`,
  `tests/unit/test_grpo_readiness.py`,
  `tests/unit/test_grpo_notebook_contract.py`,
  `tests/unit/test_grpo_benchmark_refresh.py`,
  `tests/unit/test_grpo_release_verifier.py`.
- Existing benchmark/report tests must continue to pass.
- Documentation gate: `python scripts/ci/readme_truth.py`.
- Before citing a GRPO checkpoint:
  `python scripts/model/verify_grpo_release.py --model-repo <repo> --output eval/results/<repo>.json`.

## 8. Acceptance Gate

- [ ] Section 2 outcomes are met.
- [ ] Focused GRPO tests pass.
- [ ] README contains no trained-model quality claim without verifier evidence.
- [ ] Failed SFT-v5 and GRPO runs write diagnostics and do not push public
  model repos.
- [ ] Failed SFT predecessor reports block GRPO before GPU work.

## Appendix A - Toy Cases

### Case A.1: malformed completion

Input: completion text `not json`.
Expected output: reward `0.0`, parse diagnostic recorded.
Reasoning: prevents rewarding invalid model output.

### Case A.2: clean chunk finish

Input: `{"action":"finish","repairs":[]}` with empty ground truth.
Expected output: reward `1.0`.
Reasoning: clean train chunks must teach no unnecessary edits.

### Case A.3: failed release gate

Input: GRPO F1 only `0.01` above SFT.
Expected output: verifier rejects and notebook upload is blocked.
Reasoning: prevents publishing a worse or meaningless model as progress.
