# DataForge

DataForge is a CLI-first data-quality repair toolkit for tabular data. It
detects common CSV issues, proposes deterministic repairs, checks proposed
changes through safety and verification gates, and records applied changes in a
reversible transaction log.

The final public product name is DataForge. The PyPI/TestPyPI distribution
family is `dataforge_07*` because the unqualified `dataforge` project name is
occupied by unrelated packages. Installing `dataforge_07` still provides the
`dataforge` import namespace and `dataforge` CLI. `dataforge15` is only a
temporary staging alias retained for local compatibility.

The current repository is an alpha implementation. It also contains the
OpenEnv-compatible training environment, the SFT warmup workflow, a local MCP
server package, and playground/demo sources. Warehouse integrations and
production model-quality claims remain future work.

Before any public release, review `THREAT_MODEL.md` and `docs/docs/release.md`.
They define the security, supply-chain, and evidence gates that separate the
current alpha from the full original DataForge vision.

## Current Status

Shipped in the current worktree:

- `dataforge profile`, `dataforge repair`, `dataforge revert`,
  `dataforge watch`, `dataforge audit`, and `dataforge bench`
- Three detector families: `type_mismatch`, `decimal_shift`, `fd_violation`
- Reviewable schema inference in `profile --json`, including inferred column
  types, domains, regex candidates, uniqueness, and FD candidates
- Pending constraint review artifacts via `profile --constraints-out`, which
  can feed repair only after individual candidates are marked accepted
- Matching deterministic repairers wired through SafetyFilter -> SMTVerifier
- Backend-neutral `PatchPlan` and `TableStore` contracts for CSV, DuckDB, and
  dry-run-only cloud warehouse boundaries
- Reversible hash-chained transaction journals with immutable source snapshots
- Public backend repair engine at `dataforge.engine.repair`
- Real-world benchmark harness for Hospital, Flights, and Beers
- OpenEnv-compatible HTTP environment with eight typed actions, including
  read-only `ROOT_CAUSE`
- Causal root-cause analyzer for cascading data-quality errors
- Standalone `dataforge-mcp` package exposing DataForge tools over MCP
- Week 9 SFT oracle trajectory workflow, readiness gate, Kaggle notebook, and
  release verifier
- Separate Gradio model-demo Space source for the published 0.5B SFT smoke
  checkpoint

Not shipped yet:

- published `dataforge_07`, `dataforge_07_mcp`, `dataforge_07_evals`,
  `dataforge_07_dbt`, and `dataforge_07_agent_patterns` packages
- committed production verification for the Cloudflare Workers playground
- warehouse-native or external adapter packages
- credentialed Snowflake, BigQuery, or Databricks apply/revert conformance
- design-partner, pilot-user, or customer validation evidence is not yet claimed
- A production-quality trained model family
- Autonomous repair in the playground or model demo

## Quickstart

```bash
python -m pip install -e ".[dev]"
dataforge profile fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml
dataforge profile fixtures/hospital_10rows.csv --constraints-out constraints.json
dataforge constraints review constraints.json
dataforge repair fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --dry-run
dataforge repair fixtures/hospital_10rows.csv --constraints constraints.json --dry-run
dataforge watch fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --once --json
dataforge bench --methods random,heuristic --datasets hospital,flights,beers --seeds 3 --seed-list 0,1,2
```

`dataforge15` remains a temporary staging compatibility alias, but public docs
and release evidence must use `dataforge_07` for PyPI distribution identity and
`dataforge` for the installed CLI/import identity.

To apply repairs, use `--apply`. Applied repairs write a transaction journal and
source snapshot before mutating the CSV, so they can be reverted:

```bash
dataforge repair path/to/file.csv --schema path/to/schema.yaml --apply
dataforge audit <txn-id>
dataforge revert <txn-id>
dataforge revert <txn-id> --search-root path/to --json
```

Warehouse targets use `warehouse://` URIs and always emit a `patch_plan_v1`
contract before any mutation. DuckDB is the local conformance backend; cloud
warehouse adapters are dry-run-only boundaries until credentialed apply,
audit, and rollback suites are enabled:

```bash
dataforge repair "warehouse://duckdb?database=dev.duckdb&relation=main.model&row_id=id" --dry-run --json
dataforge repair "warehouse://snowflake?relation=PUBLIC.MODEL&row_id=ID" --dry-run --json
```

DuckDB `--apply` requires a stable row identity, records the patch plan in the
transaction journal, and can be reverted through the same `audit` and `revert`
commands. Snowflake, BigQuery, and Databricks apply are intentionally refused
until their conformance gates prove reversible transactions.

New transaction logs are local tamper-evident hash chains. `dataforge audit`
verifies the chain head, event order, replayability, and revert prerequisites;
legacy v1 logs remain replayable but are reported as unverified because they do
not contain event hashes.

## Week 9 SFT Warmup

The current SFT workflow builds split-safe `expert_v1` trajectory records from
dirty/clean CSV diffs. Exact repairs in the primary dataset are labeled
`oracle_from_clean_diff`, not inferred from Groq, Cerebras, or Gemini teacher
guesses. Clean train chunks are retained as `finish` examples so the model
learns when no repair is justified.

```powershell
$env:HF_TOKEN="..."
.\.venv\Scripts\python.exe scripts\data\build_oracle_sft_trajectories.py
.\.venv\Scripts\python.exe scripts\data\validate_sft_readiness.py
```

This writes local ignored JSONL at `data/sft_traj/expert_v1.jsonl` and an
auditable row split at `data/sft_traj/split_manifest.json`. Push the dataset
bundle only after the readiness gate passes:

```powershell
$env:HF_TOKEN="..."
.\.venv\Scripts\python.exe scripts\data\build_oracle_sft_trajectories.py --push-to-hub --hf-dataset-repo Praneshrajan15/dataforge-sft-trajectories
```

The current public smoke checkpoint is
`Praneshrajan15/DataForge-0.5B-SFT`, with trajectories at
`Praneshrajan15/dataforge-sft-trajectories`. It proves the dataset, Kaggle
training, merge, evaluation, and Hub upload path; it is not a production
model-quality claim. Verify release artifacts before citing them:

```powershell
.\.venv\Scripts\python.exe scripts\model\verify_sft_release.py --output eval\results\sft_release_v0_smoke.json
.\.venv\Scripts\python.exe scripts\model\verify_sft_release.py --min-dataset-records 272 --require-sha-metrics --output eval\results\sft_release_contract_v2_20260515.json
```

## Week 12 GRPO Path

The repository now contains a gated GRPO post-training path for free-tier
experiments:

- `training/configs/grpo_05b.yaml` targets `DataForge-0.5B-SFT` -> `DataForge-0.5B-GRPO`.
- `training/configs/grpo_05b_v2.yaml` preserves the verified v1 stack while
  starting a balanced-recall improvement cycle toward strict macro F1 `>=0.25`;
  it uses Kaggle OAuth via `C:\Users\Pranesh\.kaggle\credentials.json` and
  keeps public model updates blocked until the v2 gate passes.
- `training/configs/sft_05b_v5.yaml` defines the completed private SFT
  repair-curriculum diagnostic candidate over `expert_v5_repair_curriculum.jsonl`;
  it failed predecessor gates and is not a public model-quality claim.
- `scripts/remote/prepare_kaggle_sft_v5_candidate.py` and
  `scripts/remote/kaggle_sft_v5_candidate.py` package and run the private
  SFT-v5 Kaggle candidate. Its report is preserved as failed diagnostic
  evidence, not a GRPO predecessor.
- `training/configs/sft_05b_v6.yaml` defines the next private contract-first
  candidate over `expert_v6_contract_minimal.jsonl`. The staged Kaggle path
  defaults to a 20-step smoke, then a 60-step no-upload diagnostic, and only
  allows candidate promotion if `sft_v6_candidate_eval_report.json` has
  `promote_to_grpo: true` and the private checkpoint was uploaded.
- `scripts/remote/prepare_kaggle_sft_v6_candidate.py` and
  `scripts/remote/kaggle_sft_v6_candidate.py` package and run the SFT-v6
  candidate. Candidate mode requires a visible HF token before GPU work because
  GRPO-v3 may only consume an uploaded private predecessor.
- `training/configs/grpo_05b_v3.yaml` consumes the SFT-v6 candidate and runs a
  50-step smoke, 250-step no-upload diagnostic, and 500-step gated candidate.
  It targets strict macro F1 `>=0.25`, parse success `>=0.99`, schema-case
  errors `0`, not-inferable F1 `>=0.95`, and deterministic-normalization F1
  `>=0.50`; these are config gates, not achieved evidence.
- `training/configs/grpo_15b.yaml` requires a verified `DataForge-1.5B-SFT`
  prerequisite before attempting `DataForge-1.5B-GRPO`.
- `training/rewards/dataforge_reward.py` scores completions locally through the
  strict `repair_contract_v2` action format: explicit JSON action, exact
  allowed columns, valid rows, and no schema-case drift.
- `scripts/model/grpo_readiness_report.py` writes a local, non-claim diagnostic
  report for dataset balance, held-out leakage, parse/schema stats, and reward
  variance before any Kaggle GPU run.
- `scripts/data/audit_real_world_sources.py` verifies canonical Raha source
  revisions, source hashes, row counts, and ground-truth cell counts before
  trajectory generation; stale local caches and embedded fixtures are refused.
- `training/kaggle/grpo_kaggle.ipynb` defaults to a 50-step no-upload smoke
  stage, then permits 500/1000-step candidates only when readiness and the
  held-out gate allow it. Hub upload is blocked unless GRPO beats SFT by at
  least 3 absolute F1 points on `DataForge-Bench-light-verified`.

`Praneshrajan15/DataForge-0.5B-GRPO` is verified research evidence, not a
production-quality autonomous repair model. The committed verifier report at
`docs/evidence/models/DataForge-0.5B-GRPO.verification.json` records strict
macro F1 `0.1393` versus the 0.5B SFT predecessor at `0.0053`, parse success
`1.0`, schema-case errors `0`, and model SHA
`b5fa9c74d5fbfe1e2f598cb813c7444efebbc601`. Refresh benchmark tables only from
generated JSON:

The subsequent 0.5B-GRPO v2 Kaggle candidate completed 500 steps and strict
eval, then correctly stopped as `quality_gate_failed_no_upload`: strict macro
F1 `0.1212`, SFT F1 `0.0053`, delta `0.1159` (not release evidence), parse success `0.99`, schema-case
errors `0`, with failures on `grpo_f1>=0.25` and
`not_inferable_from_prompt_f1>=0.95`. The evidence is kept under
`eval/results/kaggle_grpo_v2_failed_20260611/` and summarized in
`eval/results/grpo_05b_v2_failed_postmortem.md`; it is diagnostic evidence, not
a release win.

The private 0.5B-SFT-v5 repair-curriculum candidate also completed as
diagnostic evidence and correctly blocked GRPO-v3: strict macro F1 stayed at
`0.002`, parse success was `0.6`, schema-case errors were `108`, and
`promote_to_grpo` is `false`. The next training direction is SFT-v6 on the
`expert_v6_contract_minimal` curriculum, which keeps the held-out eval
unchanged while stripping noisy `reason` fields and capping repair targets to
reduce contract drift before GRPO-v3. No public claim is upgraded unless the
private v6 predecessor and later GRPO gates pass.

The planned HF model-family matrix is now manifest-driven in
`training/configs/model_family.yaml`. It covers the `0.5B`, `1.5B`, `3B`, and
`7B` sizes across gated post-training stages. The public-row report at
`eval/results/model_family_public_verified_20260609.json` verifies only the
0.5B SFT and 0.5B GRPO rows; larger SFT/GRPO/GiGPO rows remain missing or
blocked, and the full model-family claim remains roadmap until every row has
real public Hub repos, model-card metadata, eval reports, verifier reports, and
stage dependencies. The 3B route preserves Qwen's custom `qwen-research`
license metadata instead of rewriting it as Apache.

After GRPO eval evidence exists:

```powershell
.\.venv\Scripts\python.exe scripts\bench\refresh_benchmark_table.py --skip-agent-run --trained-model-json eval\results\grpo_model_comparison.json
```

## MCP Server

The nested `dataforge-mcp/` source directory builds the standalone
`dataforge_07_mcp` distribution. It is not published yet, so install it from
source while release ownership is pending:

```bash
cd dataforge-mcp
python -m pip install -e ".[dev]"
dataforge-mcp serve
```

Tools: `dataforge_profile`, `dataforge_detect_errors`,
`dataforge_verify_fix`, `dataforge_apply_repairs`, and `dataforge_revert`.
The default transport is stdio. MCP reads and writes are sandboxed to configured
allowed roots; dry-run works by default, while apply requires `--enable-apply`.
Streamable HTTP is available for local experiments.

The monorepo `packages/` directory contains the side-package release sources
for `dataforge_07_evals`, `dataforge_07_dbt`, and
`dataforge_07_agent_patterns`.

## Playground And Model Demo

- `playground/api/` is the API backend for the CSV playground. Public Space
  deployments use `dataforge-playground`.
- `playground/web/` is the static browser UI deployed through Cloudflare
  Workers Static Assets. Its primary workflow is `POST /api/analyze`: upload a
  CSV, review categorical risk and pending inferred constraints, inspect
  verified dry-run repairs and non-repairs, then export a receipt with the
  local CLI apply/audit/revert command shape.
- The current verified public playground URL is
  `https://dataforge.praneshrajan15.workers.dev/playground`, backed by
  `https://Praneshrajan15-dataforge-playground.hf.space`.
- That Workers URL is the production playground surface for the full original
  vision; this is the release URL.
- `playground-model/` is a separate Gradio Space demo for the published
  `DataForge-0.5B-SFT` smoke checkpoint. It accepts small CSV snippets and is
  intentionally limited to demo use.

The playground does not persist uploaded files, does not use browser storage,
does not mutate data in the hosted flow, and does not call an LLM unless a
backend provider key is explicitly configured.

## Benchmark Results

<!-- BENCH:START -->
Generated from `eval/results/agent_comparison.json` (schema `dataforge_benchmark_run_v2`, seeds `0, 1, 2`, git `dbd1bed0a03c`, dirty `true`).

| Method | Precision | Recall | F1 | Avg Steps | Quota Units | GPU Hours |
| --- | --- | --- | --- | --- | --- | --- |
| heuristic | 0.3167 | 0.3025 | 0.2772 | 374.33 | 0.0000 | 0.0000 |
| random | 0.0038 | 0.0003 | 0.0005 | 150.33 | 0.0000 | 0.0000 |

See `BENCHMARK_REPORT.md` for per-dataset tables, error bars, and citation-only SOTA rows.

Dataset bytes are pinned to BigDaMa/raha revision `7be1334b8c7bbdac3f47ef514fb3e1e8c5fc181c` for hospital, flights, beers; dirty/clean SHA-256s are recorded in the JSON metadata.
<!-- BENCH:END -->

## Local Setup

```bash
make setup
make lint
make type
make test
make backend-gate
make release-gate
```

Verification works on Linux, macOS, and Windows with Git Bash available for GNU
Make recipes. Python support is `>=3.11,<3.13`.

`profile --constraints-out` writes a strict `constraint_review_v1` JSON artifact.
Every inferred candidate starts as `pending`; repair ignores pending and
rejected candidates. In v1, only accepted `column_type`, `domain_bound`, and
`functional_dependency` candidates affect repair. Accepted regex and uniqueness
candidates remain review evidence until verifier support is added. Use
`dataforge constraints review constraints.json` for the Textual review UI, or
use deterministic CI flags such as `--accept cnd-... --no-tui --json`.

`make backend-gate` is the release-quality backend check: lint, format, strict
mypy, root tests, MCP tests, README truth, benchmark truth, OpenAPI snapshot
drift, secret scan, dependency audit availability, SBOM generation
availability, and package build availability for both `dataforge_07` and
`dataforge_07_mcp`. The gate covers the core `dataforge_07` distribution and
release surfaces; the historical
`data_quality_env` namespace remains source-tree regression coverage, not part
of the `dataforge` wheel or source distribution.

Before release, run `scripts/ci/backend_gate.py --require-optional` so
dependency audit, SBOM generation, and package builds are hard failures rather
than availability checks.

Release doctor scopes:

```bash
dataforge release doctor --core --json
dataforge release doctor --maintainer-deploy --json
dataforge release gate --json
dataforge release full-vision --json
```

`--core` is the default OSS release check. `--maintainer-deploy` additionally
checks maintainer-specific Hugging Face, Kaggle OAuth plus clean-config Kaggle
CLI execution, and Cloudflare state.
`release gate` is the authoritative fresh-user proof: it builds the
distribution, audits wheel contents, creates a dependency wheelhouse, installs
with `pip --no-index --find-links`, then runs profile, repair dry-run, apply,
constraint review, audit, revert, and post-revert audit from outside the source
checkout.

Configure pending trusted publishers for `dataforge_07` on TestPyPI and PyPI
before tagging. The real PyPI workflow refuses pre-release metadata and should
only run after trusted publishing, attestations, and fresh-install evidence are
verified. `dataforge release full-vision --json` is expected to fail until PyPI
publication evidence, dbt-duckdb proof, not yet met design-partner evidence,
and model-family evidence are real.

Windows setup:

```powershell
winget install -e --id Python.Python.3.12
winget install -e --id ezwinports.make
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[all]"
make lint && make type && make test
```

## Environment Variables

Provider keys belong in a root `.env` file, which is gitignored and loaded with
`python-dotenv` where needed.

- `GROQ_API_KEY`
- `GEMINI_API_KEY`
- `CEREBRAS_API_KEY`
- `OPENROUTER_API_KEY`
- `HF_TOKEN`

## When DataForge Is The Wrong Tool

Do not use DataForge for streaming data, very large warehouse tables, regulated
workflows where every fix must be human-authored, strict low-latency SLAs, or
teams already well served by maintained Great Expectations/dbt suites. DataForge
is currently best suited to local CSV profiling, repair experiments, benchmark
runs, and training/evaluation research.

## Repository Docs

- [.cursor/rules/dataforge.md](.cursor/rules/dataforge.md) - always-applied contribution rules
- [ARCHITECTURE.md](ARCHITECTURE.md) - current system architecture and dependencies
- [DECISIONS.md](DECISIONS.md) - technical decision log
- [CONTRIBUTING.md](CONTRIBUTING.md) - workflow and code standards
- [CLAUDE.md](CLAUDE.md) - living gotcha log for agent sessions
- [CURSOR_MASTER.md](CURSOR_MASTER.md) - context and prompt pack
- [META_CONTEXT.md](META_CONTEXT.md) - project meta-context
- [FILE_STRUCTURE.md](FILE_STRUCTURE.md) - current and planned directory map
- [SECURITY.md](SECURITY.md) - vulnerability reporting policy
- [specs/SPEC_TEMPLATE.md](specs/SPEC_TEMPLATE.md) - template for new module specs

## License

Apache-2.0. See [LICENSE](LICENSE).
