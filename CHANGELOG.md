# Changelog

## 0.2.0
 - 2026-07-15 (unreleased)

### Features

- Post-hoc probability calibration (`dataforge/calibration_map.py`): per-issue-type
  isotonic (pool-adjacent-violators) and Platt maps; ECE 0.8533 -> 0.0 on a disjoint
  n=25 test split of real Azure gpt-5-mini samples. Honest reading: the corrector is
  ~6% precise, so isotonic collapses its confidence toward 0 (trivially calibrated) -
  this proves the confidence is honest, not that the corrector improved. Reproducible
  from the committed samples + response cache and locked by tests.
- Calibrated, drift-guarded, conformally-certified LLM auto-apply gate: the engine
  rescales corrector confidence through the calibration map, applies a Population
  Stability Index drift guard against the calibration reference, and auto-applies only
  schema-proven fixes that clear the certified per-issue-type threshold; new
  `dataforge repair --corrector-calibration <artifact>` flag. With gpt-5-mini's 50
  fd_violation outcomes at ~6% precision the conformal procedure cannot certify any threshold
  (needs >= 59 all-correct accepted samples), so every class is parked at a disabled
  1.01 sentinel recorded in `policy.uncertified_classes`; nothing auto-applies and the
  gate never manufactures coverage from a weak corrector.
- `date_transposition` detector: flags Y/M/D-vs-M/D/YY transposed dates and surfaces the
  exact rotation as a propose-not-apply reviewed suggestion (no repairer, so no write
  path); measured 0 false positives on hospital/flights, detection 1.0 on rayyan.

### Fixes

- Benchmark-truth consistency: regenerated `eval/results/agent_comparison.json`,
  `sota_comparison.json`, and the generated report/README/docs blocks beers-free so
  `scripts/ci/benchmark_truth.py --check` passes; hospital heuristic F1 preserved at
  0.7926.

## 0.1.0
 - 2026-05-20


### CI

- Ci: add GitHub Action to auto-sync to HuggingFace Space ([b163c73](https://github.com/Praneshrajan15/data_quality_env/commit/b163c73e866f6712949e743fc43b35f87a11d860))



### Changes

- Elevate data quality env to research-grade: 6-phase enhancement ([f8049d5](https://github.com/Praneshrajan15/data_quality_env/commit/f8049d538c50186652d01dbd81754f3792f7adfa))

- ULTRAPLAN: compliance fixes, grader diagnostics, stochastic mode, new corruptions ([e1f5cd8](https://github.com/Praneshrajan15/data_quality_env/commit/e1f5cd8b0908569cb99d1d860d896a386a614515))

- Fix reward score validation bounds ([3ec16fe](https://github.com/Praneshrajan15/data_quality_env/commit/3ec16fedd6e420275beb7d9cd92a077d1f320568))

- Enhance Groq benchmarks and SFT workflow ([68c78b4](https://github.com/Praneshrajan15/data_quality_env/commit/68c78b4fa1629ac21c265b5824e6bc3e0d4473b3))

- Prepare SFT trajectory handoff for Kaggle ([63ce540](https://github.com/Praneshrajan15/data_quality_env/commit/63ce54017971ea958c274d2ae5b86e5c726170ad))

- Fix CI dev dependencies for HF scripts ([2f47737](https://github.com/Praneshrajan15/data_quality_env/commit/2f47737e1d344343cb525b6ab132a8504f7a507f))



### Chores

- Chore: initialize DataForge monorepo ([33aa95a](https://github.com/Praneshrajan15/data_quality_env/commit/33aa95a51cf9b8f89e7a1dfc2111b91c1996e53b))



### Deployment

- Deploy: switch frontend to Cloudflare Workers Static Assets ([3045011](https://github.com/Praneshrajan15/data_quality_env/commit/3045011146b3b556d93c30b1fd3b15b3db6fd299))



### Documentation

- Docs: consolidate reference docs, fix scaffold drift, add Windows bootstrap ([22e4d6d](https://github.com/Praneshrajan15/data_quality_env/commit/22e4d6dba8efd0f08165beb8a93ad964b4dab0a3))



### Features

- Feat: Data Quality RL Environment — production-ready submission ([fc4c35a](https://github.com/Praneshrajan15/data_quality_env/commit/fc4c35ade918d00cbf5015dd58cc95b80fafab1f))

- Feat: add verified baseline scores and deployment validation ([212b0ab](https://github.com/Praneshrajan15/data_quality_env/commit/212b0abb9ffd971aa14ff5df62c920a62b34c5f4))

- Feat: elevate environment to hackathon-winning quality ([b3e45cf](https://github.com/Praneshrajan15/data_quality_env/commit/b3e45cf3238febd871a850a4adc0990b828b52cb))

- Feat: harden RL signal and grader quality for hackathon submission ([3db9e5d](https://github.com/Praneshrajan15/data_quality_env/commit/3db9e5d0689d5afbc7024288b7b79def7efed21b))

- Feat: enforce strict reward bounds (0.0001-0.9999) and add ASGI middleware safety net ([be4b75a](https://github.com/Praneshrajan15/data_quality_env/commit/be4b75add3e1383f4a6924a70009388ea22ded50))

- Feat: ship DataForge quality pipeline and playground ([162d615](https://github.com/Praneshrajan15/data_quality_env/commit/162d615a57a5102523fcde9847d7747bdd992512))

- Feat: add OpenEnv RL environment, agent tool-actions, evals harness, and Dockerfile ([f7d1cd8](https://github.com/Praneshrajan15/data_quality_env/commit/f7d1cd869cd95df58d55368df5d237eb3762805e))

- Feat: add causal root-cause analysis, MCP server, model playground, SFT eval pipeline, and docs updates ([000ea00](https://github.com/Praneshrajan15/data_quality_env/commit/000ea00b6c40be1047caada023c56480248eb0f4))



### Fixes

- Fix: add explicit HF_TOKEN and LOCAL_IMAGE_NAME env var declarations ([513a5c3](https://github.com/Praneshrajan15/data_quality_env/commit/513a5c352f0d51622c804c95f2fc4540984895c2))

- Fix: prevent non-zero exit on LiteLLM proxy 400 errors ([b84c108](https://github.com/Praneshrajan15/data_quality_env/commit/b84c10812e28f59fcada52d85339355c55f39f99))

- Fix: resolve inference.py crash from context overflow and 400 fast-fail ([c8f4dc4](https://github.com/Praneshrajan15/data_quality_env/commit/c8f4dc436e092a00834300c89598b384f7c9a0b5))

- Fix: add root Dockerfile for HuggingFace Spaces deployment ([3d27060](https://github.com/Praneshrajan15/data_quality_env/commit/3d27060a555235f73f2c8d650589a20bb2816956))

- Fix: prevent eval timeout — reduce timeouts, retries, and add deadline ([7017097](https://github.com/Praneshrajan15/data_quality_env/commit/7017097a89745b0376cfaf7be162dc0b1f1bfcf3))

- Fix: handle LiteLLM 400 errors and reduce context for small models ([dc7bac4](https://github.com/Praneshrajan15/data_quality_env/commit/dc7bac43c5520e839caea3b16d9f653c5d14d471))

- Fix: clamp task scores to (0, 1) exclusive — validator rejects 0.0 and 1.0 ([a5544fc](https://github.com/Praneshrajan15/data_quality_env/commit/a5544fc9866a92f3f31dd3d2fdb6f4f4f8d92716))

- Fix: clamp all score paths in inference.py to (0, 1) exclusive ([af7d5e5](https://github.com/Praneshrajan15/data_quality_env/commit/af7d5e5d92246a3334749968707ae9c474dc1c2f))

- Fix: clamp terminal observation scores to (0, 1) exclusive for validator compliance ([f194691](https://github.com/Praneshrajan15/data_quality_env/commit/f1946914c482489cdd872ca4987ace34c1e45587))

- Fix: add defense-in-depth score clamping and push to HF Space ([216ccf9](https://github.com/Praneshrajan15/data_quality_env/commit/216ccf9adf77ab25b755d8e9bd6b8f65b5892a8c))

- Fix: remove invalid working-directory from CI workflow ([5c30f45](https://github.com/Praneshrajan15/data_quality_env/commit/5c30f45498dd35115508bd472e2a9149473955a7))

- Fix: add HTTP POST /reset, /step endpoints with openenv protocol wrapping - Fallback returns {observation: {...}} matching openenv format - Step accepts {action: {...}} (openenv protocol) and flat fields - Dockerfile copies client.py, inference.py, REWARD_DESIGN.md - Dockerfile --extra server ensures fastapi/uvicorn install - uv.lock synced with pyproject.toml ([8e15666](https://github.com/Praneshrajan15/data_quality_env/commit/8e156668c48466bb5e8f69ef9c17a8c0280d25c9))

- Fix: remove generate_datasets.py from .dockerignore + regenerate uv.lock with server extras ([c52be7f](https://github.com/Praneshrajan15/data_quality_env/commit/c52be7f1673f7a2a2d07cbf84967a055d780cc94))

- Fix: clamp scores to strict (0, 1) interval — not 0.0, not 1.0 ([c05b47c](https://github.com/Praneshrajan15/data_quality_env/commit/c05b47c8c8acd20d35660f094454b4f26b71363e))

- Fix: nuclear fix — clamp ALL reward values to strict (0, 1) in ALL observations ([5924dfb](https://github.com/Praneshrajan15/data_quality_env/commit/5924dfb03a6bce74730b7372bb20d3994c734286))

- Fix: correct test expectations and verification for nuclear reward clamping ([6a462bd](https://github.com/Praneshrajan15/data_quality_env/commit/6a462bdfbd9b2c485b47a116ef503f8457b15e52))

- Fix: nuclear score clamping - clamp ALL reward fields (reward, cumulative_reward, reward_delta) at 4 layers ([01a2325](https://github.com/Praneshrajan15/data_quality_env/commit/01a2325cde0dc16e8fdbc6b51cf5aed75f9bbb24))

- Fix: handle None reward from openenv create_app - replace with cumulative_reward ([8c8eb97](https://github.com/Praneshrajan15/data_quality_env/commit/8c8eb97125a2e9f63638ff12b0fd0e3410dd7c73))

- Fix: stop data corruption in nuclear clamper and fix .2f rounding to 0.00/1.00 ([75ec67e](https://github.com/Praneshrajan15/data_quality_env/commit/75ec67e5c82a7887d459d05e8359778b9d34f171))

- Fix: align STEP/END output format with hackathon spec (2dp rewards, rich action strings) ([d02f8d2](https://github.com/Praneshrajan15/data_quality_env/commit/d02f8d2819518e2e771302b27ddbcd4154551828))

- Fix: revert to passing server architecture - remove all middleware and model_serializer ([2b185d5](https://github.com/Praneshrajan15/data_quality_env/commit/2b185d522fc3d988c662f64f51d70ceac761ac13))

- Fix: update tests to expect grader_diagnostics=None after removal ([26a3599](https://github.com/Praneshrajan15/data_quality_env/commit/26a35990f2503d4be42f5173e642ecaa1be9d84c))

- Fix: re-checkout app.py with proper UTF-8 encoding (remove null bytes) ([c8f4b71](https://github.com/Praneshrajan15/data_quality_env/commit/c8f4b71a1c3b6a12864244ca22d6b51e1ce29591))

- Fix: enforce strict (0,1) score range at all layers to pass Phase 2 validation ([a5f25ae](https://github.com/Praneshrajan15/data_quality_env/commit/a5f25ae243e4f26c93964e3ee430f32e878ac244))

- Fix: re-inject reward/done into obs dict so evaluator sees clamped scores ([bbb13fa](https://github.com/Praneshrajan15/data_quality_env/commit/bbb13fae18e8e5036be9794bf138de58e0ce5bcf))

- Fix: triple-layer score clamping to guarantee (0,1) range ([e8f8619](https://github.com/Praneshrajan15/data_quality_env/commit/e8f86190d86a74a7f6c83c6e8176efbee23973a7))

- Fix: harden wire-level score clamping for Phase 2 validation ([4d361f2](https://github.com/Praneshrajan15/data_quality_env/commit/4d361f2f9b49c89eb0df2d06b46c5dec40ced52d))

- Fix: stop fighting framework exclude, add ASGI score-clamp middleware ([7209ed9](https://github.com/Praneshrajan15/data_quality_env/commit/7209ed9954d81d97f32930fd320a7124ae53c43e))

- Fix: raw ASGI middleware for score clamping - removes broken monkey-patch and BaseHTTPMiddleware ([5f66d3a](https://github.com/Praneshrajan15/data_quality_env/commit/5f66d3af8eb059d2236f56a5ef7da4caf4e73f3a))

- Fix: stabilize CI with legacy package compatibility ([636d493](https://github.com/Praneshrajan15/data_quality_env/commit/636d493694ea6fed5ddb023fb4c65441c18819ef))

- Fix: stabilize bench CLI CI contract ([89cb746](https://github.com/Praneshrajan15/data_quality_env/commit/89cb7468629e1aab84e6221eb040d6e8009c0182))

- Fix: sort playground deploy contract imports ([57ff566](https://github.com/Praneshrajan15/data_quality_env/commit/57ff566b93b12f7263328f6a61f4c427a2709783))

- Fix: resolve ruff I001 import sorting and format drift (3 lint + 12 format) ([4e7ac30](https://github.com/Praneshrajan15/data_quality_env/commit/4e7ac30bdf92cb5e0e2009fdd441c8e2bd92268d))

- Fix: resolve all 9 mypy strict errors in tool_actions, environment, server ([0b2db24](https://github.com/Praneshrajan15/data_quality_env/commit/0b2db241f9749c6fd25aeecdf4ba281ea5b3da0b))

- Fix: remove redundant cast and unused type-ignore for CI mypy ([5e88de6](https://github.com/Praneshrajan15/data_quality_env/commit/5e88de6a9f0a4c099f4012fafcf9bb467ac503b9))

- Fix(ci): auto-format 7 files with ruff and add pre-commit hook ([aff0594](https://github.com/Praneshrajan15/data_quality_env/commit/aff0594b9a3c714067d55b70f822d12afd76ba47))

- Fix(ci): skip model-space tests when gradio is not installed ([d7637fe](https://github.com/Praneshrajan15/data_quality_env/commit/d7637fe7af79d291c7fa0e9c30ac9f798c82be86))
