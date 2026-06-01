# DataForge

DataForge is a CLI-first data-quality repair toolkit for tabular data. It
detects common CSV issues, proposes deterministic repairs, checks proposed
changes through safety and verification gates, and records applied changes in a
reversible transaction log.

The final public PyPI distribution is `dataforge`, and the import namespace is
`dataforge`. `dataforge15` is only a temporary staging alias retained for local
compatibility; it is not the final public product name. The `dataforge` name is
currently occupied by an unrelated PyPI/TestPyPI project, so package ownership
or maintainer cooperation is a hard release gate before `pip install dataforge`
can truthfully point at this project.

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

- final PyPI/TestPyPI ownership for `dataforge`
- published `dataforge`, `dataforge-mcp`, `dataforge-evals`, `dataforge-dbt`,
  and `dataforge-agent-patterns` packages
- mandatory `https://dataforge.dev/playground` DNS/TLS/Cloudflare route
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
and release evidence must use `dataforge`.

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
- `training/configs/grpo_15b.yaml` requires a verified `DataForge-1.5B-SFT`
  prerequisite before attempting `DataForge-1.5B-GRPO`.
- `training/rewards/dataforge_reward.py` scores completions locally through the
  `repair_contract_v1` exact-repair contract.
- `training/kaggle/grpo_kaggle.ipynb` blocks Hub upload unless GRPO beats SFT
  by at least 3 absolute F1 points on `DataForge-Bench-light-verified`.

No GRPO checkpoint is described as a quality milestone in this README until
`scripts/model/verify_grpo_release.py` produces committed verification
evidence. Refresh benchmark tables only from generated JSON:

After GRPO eval evidence exists:

```powershell
.\.venv\Scripts\python.exe scripts\bench\refresh_benchmark_table.py --skip-agent-run --trained-model-json eval\results\grpo_model_comparison.json
```

## MCP Server

The nested `dataforge-mcp/` source directory builds the standalone
`dataforge-mcp` distribution. It is not published yet, so install it from
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
- `https://dataforge.dev/playground` is a mandatory hard gate for the full
  original vision and is not yet live in this checkout's verified evidence.
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
availability, and package build availability for both `dataforge` and
`dataforge-mcp`. The gate covers the core `dataforge` distribution and
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
CLI execution, Cloudflare, and domain state.
`release gate` is the authoritative fresh-user proof: it builds the
distribution, audits wheel contents, creates a dependency wheelhouse, installs
with `pip --no-index --find-links`, then runs profile, repair dry-run, apply,
constraint review, audit, revert, and post-revert audit from outside the source
checkout.

Configure pending trusted publishers for `dataforge` on TestPyPI and PyPI
before tagging. The real PyPI workflow refuses pre-release metadata and should
only run after package-name ownership, trusted publishing, attestations, and
fresh-install evidence are verified. `dataforge release full-vision --json`
is expected to fail until PyPI ownership, `dataforge.dev`, dbt-duckdb,
not yet met design-partner evidence, and model-family evidence are real.

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
