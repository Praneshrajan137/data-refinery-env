# DataForge - Decisions Log

Format for every entry:

## YYYY-MM-DD - <decision title>
**Context**: what triggered the decision; what problem it solves.
**Alternatives**: 2-4 options considered with honest pros/cons.
**Decision**: the pick.
**Reasoning**: why this over the others.
**Reviewed with**: who (if anyone) sanity-checked it.
**Reversal criteria**: what evidence would make us switch.

---

## 2026-06-30 - Reposition as verified+calibrated repair; ensemble + honest coverage
**Context**: A per-error-class instrument (built first) exposed that the
deterministic stack scored F1 0.79 on hospital but 0.00 on flights and 0.04 on
beers - three single-strategy detectors missed the dominant error classes
(missing values, formatting/normalization) on two of three datasets. The field
(Raha/Baran) shows coverage comes from an ensemble of heterogeneous detectors,
not one strategy.
**Alternatives**:
- Keep narrow scope, tighten claims. Pros: honest, no work. Cons: leaves real
  coverage on the table; "detects common CSV issues" stays barely true.
- Broaden with naive detector sprawl. Pros: coverage. Cons: false positives
  regress precision (observed: format/categorical correction tanked hospital/beers).
- Ensemble + calibrated abstention + honest detection-vs-correction reporting,
  with new detectors strictly additive (tier 1) on top of the proven tier-0
  floor, and correction withheld where it cannot be proven safe. Pros: broad
  detection, no precision regression, defensible "trust" thesis. Cons: more
  surface; correction for fuzzy classes deferred behind calibration.
**Decision**: reposition DataForge as "the data-repair engine where every fix is
formally verified, reversible, and calibrated, with honest per-class coverage."
Ship the ensemble (8 detectors), measure detection and correction separately,
keep tier-0 detectors authoritative over their cells, and auto-apply only
provably-safe corrections (decimal-shift, FD, FD-derivable missing-value fill).
Format and categorical correction remain detection-only until calibration-gated.
**Reasoning**: the moat is safety/verifiability/reversibility; the honest move is
to maximize *detection* coverage while refusing to *guess* corrections. Measured
result: flights missing_value detection 0.00 -> 1.00 (2370 cells), beers/hospital
detection broadly up, with correction F1 unchanged (hospital 0.7926, beers
0.0391, flights 0.00) - zero regression. The detection/correction split makes the
limits visible rather than hidden.
**Reviewed with**: `dataforge bench --quick` on full RAHA, the per-class
instrument (`dataforge/bench/error_classes.py`), and `eval/thresholds/coverage_floors.json`.
**Reversal criteria**: enable a detection-only class's correction in
`build_repairers` only when a `dataforge bench --quick` run shows it does not drop
any committed per-class floor and clears the calibrated precision target.

---

## 2026-06-30 - Make agent backend user-selectable; default to hosted, fail fast
**Context**: The verified agent shipped with `local` (trained Qwen) as the
default policy, but that model currently underperforms the deterministic baseline
(F1 ~0.14 vs ~0.79). Users asked for all backends to be first-class, explicit
choices, with the strongest option as the default.
**Alternatives**:
- Keep `local` default. Pros: free/offline. Cons: weakest accuracy now; the
  default agent adds little over the floor.
- Hosted default, silent fallback to deterministic when no key. Pros: never
  errors. Cons: the agent silently does nothing; users cannot tell which backend
  ran.
- Hosted default, fail fast on missing key; `local`/`deterministic`/`custom:<name>`
  all selectable; provider via `--provider` with env fallback. Pros: best
  accuracy by default, explicit and honest, custom plug-in path. Cons: `--agent`
  with no key errors until the user picks a backend or sets a key.
**Decision**: hosted is the default policy across CLI/MCP/controller; selectable
kinds are `hosted`, `local`, `deterministic`, and `custom:<name>` (registry via
`register_policy`); `--provider groq|gemini` chooses the hosted provider with
`DATAFORGE_LLM_PROVIDER`/key autodetect fallback. Hosted and local both fail fast
with an actionable `PolicyUnavailableError` rather than silently degrading.
**Reasoning**: the default should be the most accurate option available today,
and failures should be loud and actionable, not silent. Determinism and the SMT +
constitution + transaction gates still bound every backend, including custom, so
selection never weakens safety. Supersedes the 2026-06-29 "local default" choice.
**Reviewed with**: full repo test suite, `dataforge.agent.available_policies`,
and manual CLI checks (`--policy hosted` no key -> clear error;
`--policy deterministic` offline OK).
**Reversal criteria**: flip the default back to `local` once a local/trained
policy passes `agent_promotion_verdict` against the deterministic baseline.

---

## 2026-06-29 - Make DataForge truly agentic via a verified agent, not LLM-YOLO
**Context**: The product (`dataforge repair`) was a deterministic detect ->
propose -> safety -> SMT -> transaction pipeline. The agent substrate (OpenEnv
env, typed tool actions, scratchpad) and the RL-trained policy existed but were
disconnected from the product, and the trained Qwen-0.5B underperformed the
deterministic heuristic baseline (F1 ~0.14 vs ~0.79). "Make it truly agentic"
risked a regression if it meant handing writes to a stochastic LLM.
**Alternatives**:
- Maximal autonomy: let the LLM drive detection AND repair end to end, with the
  gates as advisory. Pros: most "agentic". Cons: non-deterministic, slower, and
  currently far less accurate than the rules; weakens the safety/verifiability
  moat.
- Integrate the trained model as-is. Pros: ships the RL work. Cons: F1 0.14 is
  below baseline; would degrade the product.
- Verified agent: an autonomous LLM controller that seeds with the deterministic
  floor and works only the residual, where EVERY write is gated by the existing
  safety constitution + SMT verifier + reversible transaction journal, and
  rejections feed back for self-correction. Pros: autonomy in reasoning,
  determinism + proof in what is written; additive on top of the floor so it can
  never ship below baseline; unifies CLI/MCP behind one controller. Cons: more
  surface to maintain; LLM value on the residual still needs training work.
**Decision**: ship the verified agent as an opt-in mode (`dataforge repair
--agent`, MCP `dataforge_agent_repair`), local trained policy by default and
pluggable to hosted/deterministic, gated by a benchmark that blocks promotion to
default until the agent beats the baseline F1 with zero safety regressions.
**Reasoning**: DataForge's moat is safety, verifiability, and reversibility. The
highest-quality interpretation of "agentic" preserves that moat by keeping the
verified floor as both the agent's most-trusted tool and its safety net.
Autonomous agent fixes are additionally soft-escalation gated
(`NO_UNCONFIRMED_LLM_WRITE`), so live-LLM writes require explicit operator
confirmation (`--confirm-escalations`).
**Reviewed with**: full repo test suite (unit/integration/property/adversarial),
`dataforge.release.agent_gate.check_agent_release_gate`, and
`dataforge.bench.agent_promotion_verdict`.
**Reversal criteria**: revisit the deterministic-first ordering only if a trained
policy demonstrably beats the deterministic baseline on hospital/beers/flights
with no safety regression, at which point the promotion gate may flip the default.

---

## 2026-06-03 - Treat Workers as the canonical playground and harden external evidence
**Context**: The full original DataForge vision still depends on external
state: PyPI/TestPyPI trusted publishing, public package publication, live deployment
verification, real dbt-duckdb proof, real design-partner evidence, and a public
Hugging Face model family. A custom domain adds DNS ownership and routing risk
without improving the product proof because the Workers URL is already the
stable hosted playground surface.
**Alternatives**:
- Require a custom domain. Pros: shorter branded URL. Cons: introduces a DNS
  gate unrelated to the product's safety, package, or model claims.
- Keep both a custom domain and Workers. Pros: optional brand path. Cons:
  doubles deployment truth surfaces and invites stale docs.
- Make the Workers playground canonical. Pros: removes DNS ambiguity and keeps
  the external gate focused on package publication, hosted behavior, evidence,
  and model quality. Cons: less polished URL.
**Decision**: use `https://dataforge.praneshrajan15.workers.dev/playground` as
the canonical public playground and make the full-vision gate require hard,
file-backed evidence for PyPI, dbt-duckdb, design partners, and the HF model
family.
**Reasoning**: the project should optimize for falsifiable proof, not vanity
surface area. The Workers URL is sufficient for the hosted playground claim;
the release risk belongs in package publication, reversible repair behavior, and
model/eval evidence.
**Reviewed with**: `dataforge release full-vision --json`, PyPI trusted
publishing guidance, dbt data-test guidance, and Hugging Face model-card
guidance.
**Reversal criteria**: revisit only if a custom domain is already controlled,
monitored, and useful to users without weakening the existing Workers gate.

---

## 2026-05-16 - Use GRPO before GiGPO on the free-tier training path
**Context**: Week 12 needs a post-SFT reinforcement learning step that can run
on Kaggle or Colab free GPUs without adding a second distributed RL stack.
The original prompt named TRL v0.11 and GiGPO as adjacent possibilities, but
the free-tier release path needs a stable trainer, local reward scoring, and
small rollout batches.
**Alternatives**:
- Use TRL GRPO. Pros: ships in the existing TRL family, supports callable
  reward functions, and can run with LoRA/QLoRA on small models. Cons:
  rollout count and prompt length must be conservative on P100/T4 memory.
- Use GiGPO through verl-agent. Pros: closer to newer agentic RL research.
  Cons: heavier setup, larger memory footprint, and more moving parts than the
  current free-tier path can honestly support.
- Skip RL and refresh only SFT. Pros: lowest operational risk. Cons: does not
  test the environment/reward path that Week 12 is meant to validate.
**Decision**: implement GRPO first with TRL, local stateless exact-repair
rewards, and a hard F1 gate before publishing.
**Reasoning**: GRPO is the smallest credible RL step after SFT that can be
reproduced by maintainers without paid infrastructure. GiGPO remains future
work until the project has either paid compute or an HF compute grant.
**Reviewed with**: `specs/SPEC_grpo_training.md`.
**Reversal criteria**: if GRPO cannot clear the +0.03 F1 gate after reward
diagnostics and rollout-count tuning, or if GiGPO gains a lightweight
single-GPU implementation, revisit the RL method choice.

---

## 2026-05-15 - Treat canonical human docs as the documentation source of truth
**Context**: The repository now contains generated Hugging Face staging mirrors,
local logs, cache directories, and canonical human-authored docs. A full docs
refresh needs to update the real source documents without hand-editing generated
deployment copies that can be recreated by scripts.
**Alternatives**:
- Edit every Markdown and text file. Pros: every visible copy can be updated in
  one sweep. Cons: generated mirrors drift from their staging scripts and create
  noisy churn.
- Update only the files named in the prompt. Pros: smallest edit set. Cons:
  leaves stale claims in adjacent docs that readers actually use.
- Update canonical human-facing docs and leave generated/staged mirrors alone.
  Pros: keeps documentation truthful while preserving reproducible deployment
  artifacts. Cons: generated mirrors need regeneration when their canonical
  source changes.
**Decision**: refresh canonical human-facing docs only; do not hand-edit
`.hf-space-repo/`, `.hf-space-stage/`, `.hf-space-stage-plan/`, caches, logs, or
other generated mirrors.
**Reasoning**: documentation should have one source of truth per surface.
Deployment mirrors are outputs, not places to make product decisions.
**Reviewed with**: 2026-05-15 documentation refresh plan.
**Reversal criteria**: if a staging directory becomes the only source consumed
by a deployment and cannot be regenerated from canonical files, promote that
file to documented source status and update this decision.

---

## 2026-05-15 - Package DataForge MCP as a nested standalone distribution
**Context**: Week 11 needs `dataforge-mcp` to be installable by MCP clients
without folding MCP transport concerns into the core `dataforge` package.
**Alternatives**:
- Add MCP commands to the root `dataforge` package. Pros: fewer package files.
  Cons: adds transport dependencies to the core runtime and weakens integration
  package evidence.
- Create a sibling repository immediately. Pros: mirrors the long-term target.
  Cons: harder to test atomically with the current dirty worktree.
- Create `dataforge-mcp/` as a nested standalone package. Pros: keeps a separate
  PyPI artifact while letting CI test it against the local DataForge source.
  Cons: release workflow must build from a subdirectory.
**Decision**: create `dataforge-mcp/` inside this repository as a standalone
package that relies on `dataforge` and `mcp`.
**Reasoning**: this is the narrowest path to a real integration package without
polluting the core dependency graph or requiring a repo split before the
implementation is proven.
**Reviewed with**: `specs/SPEC_mcp_server.md`.
**Reversal criteria**: if the integration gains independent release cadence or
external contributors, split `dataforge-mcp/` into its own repository while
preserving the same package metadata and tool contracts.

---

## 2026-05-15 - Correct ZeroGPU docs for the model demo Space
**Context**: The Week 11 prompt referred to stale ZeroGPU infrastructure details
and an unsupported README field for hardware selection, but current Hugging
Face documentation describes Gradio-only ZeroGPU with dynamic shared GPU
allocation and supported Space config keys such as `sdk` and `app_file`.
**Alternatives**:
- Repeat the original prompt literally. Pros: minimal editing. Cons: commits
  stale or unsupported deployment claims.
- Omit ZeroGPU specifics entirely. Pros: avoids drift. Cons: users need to know
  queue and quota behavior before trying the demo.
- Document the current supported contract and instruct maintainers to select
  ZeroGPU in Space settings. Pros: accurate and actionable. Cons: slightly less
  terse than the original prompt.
**Decision**: use valid Gradio Space frontmatter and document ZeroGPU selection,
queueing, quota, and model-loading behavior in prose.
**Reasoning**: DataForge documentation should not claim infrastructure details
that official upstream docs no longer support.
**Reviewed with**: `specs/SPEC_model_space.md`.
**Reversal criteria**: if Hugging Face adds a supported README configuration key
for accelerator selection or changes ZeroGPU allocation behavior again, update
the Space README and spec
together.

---

## 2026-05-15 - Expand environment action space to include ROOT_CAUSE
**Context**: Week 10 adds causal root-cause analysis for cascading data-quality
errors. The Week 6 environment spec locked seven typed actions, but root-cause
analysis is a distinct read-only diagnostic operation rather than a hypothesis,
diagnosis, or fix.
**Alternatives**:
- Reuse `HYPOTHESIS`. Pros: no action-space change. Cons: mixes free-form
  scratchpad claims with analyzer-backed observations and makes reward credit
  ambiguous.
- Add `ROOT_CAUSE` as an eighth typed action. Pros: explicit interface,
  structured observations, and a narrow reward hook. Cons: supersedes the
  previous seven-action assumption.
- Fold root cause into `DIAGNOSE`. Pros: fewer action types. Cons: row/column
  diagnosis and causal minimization have different inputs and semantics.
**Decision**: add `ROOT_CAUSE(error_indices: list[int])` as the eighth typed
environment action.
**Reasoning**: cascading errors need a first-class read-only analyzer result
without pretending the agent authored the causal explanation. The explicit
action also lets training distinguish "found an issue" from "found the minimal
upstream cause."
**Reviewed with**: `specs/SPEC_causal_root_cause.md` and the Week 10 plan.
**Reversal criteria**: if training shows the eighth action materially worsens
exploration without improving downstream fix quality, fold it into a richer
`DIAGNOSE` observation while preserving the analyzer API.

---

## 2026-05-10 - Add a hard SFT readiness gate before Kaggle
**Context**: The Kaggle notebook can fail late or publish incomplete artifacts
when the HF dataset repo is missing, the local trajectory JSONL is empty, chunk
keys are duplicated, package pins drift, or evaluation fails after an early
upload.
**Alternatives**:
- Trust the notebook alone. Pros: fewer files. Cons: failures happen inside a
  scarce GPU runtime and are harder to diagnose.
- Add notebook-only assertions. Pros: catches some problems. Cons: still
  burns Kaggle startup time and does not protect local handoff quality.
- Add a local preflight gate plus notebook checks. Pros: catches bad handoffs
  before Kaggle, keeps run-all behavior, and prevents incomplete model cards.
  Cons: one more command in the workflow.
**Decision**: validate `expert_v1` locally with
`scripts/data/validate_sft_readiness.py`, enforce exact pins and non-empty
train/held-out split assumptions, and publish from the notebook only after
numeric evaluation metrics exist.
**Reasoning**: the Kaggle step should be a compute execution step, not the
first place basic data and packaging invariants are discovered. A local gate is
the cheapest way to make failures deterministic and actionable.
**Reviewed with**: SPEC_sft_warmup.md and the 2026-05-10 Kaggle failure audit.
**Reversal criteria**: if the workflow moves to a managed trainer with its own
artifact validation and atomic publishing, collapse the local gate into that
system while preserving the same checks.

---

## 2026-05-02 - Collect Week 9 SFT data as chunk-level trajectories
**Context**: Week 9 needs a Kaggle-free-tier SFT warmup dataset from Groq ReAct
teacher runs. Treating each full benchmark episode as one "trajectory" would
make the stated 2,000-trajectory target incompatible with the free-tier request
budget because each episode spans many row chunks.
**Alternatives**:
- Full-episode records. Pros: simple naming. Cons: budget math does not close
  and one record contains too much heterogeneous context for SFT.
- Chunk-level records keyed by `(task_id, seed, chunk_index)`. Pros: matches
  the existing ReAct chunk loop, supports idempotent resume, and yields compact
  chat examples. Cons: episode-level quality filtering must be applied before
  writing chunk records.
- Synthetic fixture-only records. Pros: cheap and deterministic. Cons: misses
  the real-world Hospital / Flights / Beers distribution targeted by Week 9.
**Decision**: collect chunk-level `expert_v1` JSONL records from real-world
DataForge-Bench-light windows and retain only chunks from episodes with F1 >=
0.6.
**Reasoning**: chunk-level records are the only way to honor the Groq request
budget, keep examples trainable on a 0.5B model, and preserve auditable
tool-use provenance.
**Reviewed with**: SPEC_sft_warmup.md and the Week 9 implementation plan.
**Reversal criteria**: if later training shows chunk-local examples do not
teach cross-chunk repair strategy, introduce a second hierarchical dataset
format while keeping `expert_v1` for warmup SFT.

---

## 2026-05-02 - Resolve Week 9 HF repos from the authenticated user
**Context**: The original prompt used a placeholder owner namespace for the
model repo, which is not run-all reproducible in Kaggle and invites users to
edit notebook cells.
**Alternatives**:
- Hardcode a maintainer namespace. Pros: simple for one maintainer. Cons:
  breaks forks and external readers.
- Ask the notebook user to edit a placeholder owner. Pros: obvious. Cons: violates the
  run-all without modification requirement.
- Resolve `HF_TOKEN` with `whoami` and derive dataset/model repo names. Pros:
  reproducible, fork-friendly, and scriptable. Cons: requires a write-capable
  HF token.
**Decision**: use `HF_TOKEN` plus `HfApi.whoami()` to derive
`<hf_user>/dataforge-sft-trajectories` and `<hf_user>/DataForge-0.5B-SFT`.
**Reasoning**: automatic repo resolution is the narrowest way to make the
notebook self-contained while still publishing into the runner's namespace.
**Reviewed with**: SPEC_sft_warmup.md.
**Reversal criteria**: if HF changes token introspection semantics or the
workflow moves to organization-owned releases, add an explicit `--repo-id`
override while keeping `auto` as the default.

---

## 2026-04-19 - Ship an honest scaffold before feature code
**Context**: the repository needed a clean DataForge monorepo foundation
without pretending the future implementation already exists.
**Alternatives**:
- Port the older hackathon environment directly. Pros: faster apparent progress.
  Cons: mixes product lines and muddies the DataForge architecture.
- Ship a scaffold first. Pros: clean package boundaries, honest README, and
  reproducible Week 0 setup. Cons: little immediate end-user functionality.
- Wait to create the repo until feature code is ready. Pros: fewer visible
  placeholders. Cons: delays CI, packaging, and spec-first workflow discipline.
**Decision**: ship the scaffold first.
**Reasoning**: the scaffold creates a clean baseline for future PRs, keeps the
repository honest about current capabilities, and preserves the spec-first
workflow required by the project rules.
**Reviewed with**: Codex implementation pass.
**Reversal criteria**: if the scaffold blocks incremental delivery or creates
avoidable churn for early feature PRs, collapse unused structure in a follow-up.

---

## 2026-04-20 - Issue severity tiers — 3 levels (SAFE / REVIEW / UNSAFE)
**Context**: the detector subsystem needs a severity classification for
data-quality issues. The choice of how many tiers affects the entire
downstream pipeline: auto-apply logic, UI filtering, and safety gates.
**Alternatives**:
- 2 tiers (safe/unsafe). Pros: simplest possible model. Cons: loses the
  critical "human should look at this" signal — most real issues are ambiguous.
- 3 tiers (SAFE/REVIEW/UNSAFE). Pros: maps to actionable workflows (auto-apply,
  show in table, block). Cons: boundary between REVIEW and UNSAFE requires
  calibration per detector.
- 5 tiers (fine-grained confidence bands). Pros: maximum granularity. Cons:
  creates decision paralysis — where does "probably wrong" end and "suspicious"
  begin? Forces users to configure thresholds.
**Decision**: 3 tiers — SAFE, REVIEW, UNSAFE.
**Reasoning**: 3 tiers match the three fundamental actions a pipeline can take
(auto-apply, present for review, block). The REVIEW tier captures the vast
majority of real-world ambiguous cases without forcing premature classification.
**Reviewed with**: SPEC_detectors.md Section 5.
**Reversal criteria**: if user feedback shows >30% of REVIEW items are
consistently auto-approved or auto-rejected, collapse to 2 tiers.

---

## 2026-04-20 - Transaction-first repair with immutable source snapshots
**Context**: Week 2 adds `dataforge repair --apply` and `dataforge revert`.
The core risk is losing the original file state or claiming byte-identical
revert while relying on a pandas read/write cycle that normalizes formatting.
**Alternatives**:
- Apply file edits first, then write a transaction record. Pros: simplest code path.
  Cons: violates the safety invariant; a crash between write and log loses auditability.
- Write a mutable JSON transaction record and update it in place. Pros: simple to inspect.
  Cons: not append-only; weak audit semantics; higher corruption risk on partial writes.
- Journal first and rely on inverse cell writes for revert. Pros: compact storage.
  Cons: cannot honestly guarantee byte-identical restore for arbitrary CSV formatting.
- Journal first and persist an immutable source snapshot. Pros: true byte-identical
  restore, append-only audit trail, and safe recovery from apply-time failures.
  Cons: more disk usage per transaction.
**Decision**: write the transaction journal and source snapshot before apply,
then use the snapshot as the source of truth for revert.
**Reasoning**: transaction-first ordering preserves the audit trail even when
apply fails, and immutable snapshots are the only honest way to guarantee
byte-for-byte restore after a lossy DataFrame rewrite.
**Reviewed with**: SPEC_transactions.md and the Week 2 implementation pass.
**Reversal criteria**: if snapshot storage cost becomes a real operational
problem and we have a proven patch-based writer that preserves exact bytes on
apply, revisit snapshot-backed revert.

---

## 2026-04-20 - Select Z3 over cvc5 for the Week 3 verifier
**Context**: Week 3 needs a local SMT solver for domain-bound and
functional-dependency verification in the repair path. The solver choice affects
Python integration quality, unsat-core ergonomics, packaging friction, and the
ability to ship a credible local verifier on Windows, macOS, and Linux.
**Alternatives**:
- Use Z3. Pros: mature Python bindings, broad community familiarity, reliable
  support for tracked assertions and unsat cores, already present in the project
  dependency set. Cons: large binary wheel, string-theory ergonomics are not
  always intuitive, quantifiers still require careful handling for performance.
- Use cvc5. Pros: strong SMT support, modern solver implementation, good theory
  coverage. Cons: weaker Python ergonomics for the current repo, higher
  packaging / contributor-friction risk, and less existing team familiarity.
- Avoid an SMT solver and use imperative checks only. Pros: simplest code path,
  easiest to debug. Cons: breaks the architectural promise of SMT-verified
  repairs and weakens explainability / extensibility for future constraints.
**Decision**: use Z3 for Week 3.
**Reasoning**: Z3 is the fastest route to a production-quality local verifier in
this repository because it combines proven Python support, tracked-assertion
APIs, and low adoption friction for contributors. cvc5 remains technically
credible, but the integration overhead is not justified for the Week 3 ship
goal.
**Reviewed with**: SPEC_smt_verifier.md and the Week 3 implementation pass.
**Reversal criteria**: if Z3 fails the benchmark target (`p95 < 200 ms` on the
1,000-row / 2-FD benchmark), proves materially unstable on Windows wheels, or
blocks a needed future theory that cvc5 handles cleanly, revisit the solver
choice.

---

## 2026-04-21 - Separate reproduced local benchmark rows from citation-only SOTA rows
**Context**: Week 4 adds benchmark reporting on the Raha Hospital, Flights, and
Beers datasets. The upstream files reveal dirty/clean header mismatches for
Hospital and Beers, and the external literature rows are not reproduced under
the exact same protocol as the shipped local DataForge runs.
**Alternatives**:
- Force a single mixed table. Pros: compact. Cons: blends reproduced local
  numbers with citation-only literature rows and hides protocol differences.
- Publish only local DataForge rows. Pros: maximal purity. Cons: loses the
  external calibration reviewers expect from benchmark sections.
- Use positional dirty/clean alignment plus dual tables. Pros: preserves honest
  local reproducibility while keeping literature references clearly labeled.
  Cons: slightly more reporting complexity.
**Decision**: align dirty/clean files by column position and report dual tables:
reproduced local rows plus citation-only SOTA rows.
**Reasoning**: positional alignment matches the actual upstream dataset shape,
and dual-table reporting keeps the benchmark section methodologically honest.
**Reviewed with**: SPEC_benchmarks.md and the Week 4 implementation pass.
**Reversal criteria**: if later work reproduces comparable external methods
under the same protocol, collapse the two tables into one fully reproduced
comparison.

---

## 2026-04-21 - Design-partner gate as a Week-4-to-5 go/no-go
**Context**: META_CONTEXT.md §F3 identifies "no design partner" as a
top-three kill risk. The project needs an explicit checkpoint that forces
user-validation work before feature work proceeds. Without a gate, the
playground ships into a vacuum.
**Alternatives**:
- No gate. Pros: maximum velocity on feature code. Cons: ignores the
  highest-probability failure mode; ships a playground nobody asked for;
  the reviewer sees zero external users and reaches judgment 2 or 3.
- Informal gate ("try to find someone"). Pros: low ceremony. Cons: no
  artifact trail; easy to rationalize "I'll do it next week" forever;
  indistinguishable from no gate in retrospect.
- Artifacted gate with bookkeeping (this choice). Pros: committed
  template, issue form, outreach log, and tally table create accountability
  and a visible trail; the gate is pass/fail on concrete criteria (>= 1
  named partner, >= 1 filed issue or verbatim quote). Cons: overhead of
  maintaining the tally table; risk of cargo-culting the form without
  genuine outreach.
**Decision**: artifacted gate with bookkeeping.
**Reasoning**: the overhead is minimal (a template, an issue form, a
progress appendix), and the alternative is pretending user-validation
happened. The artifacts also serve a second purpose: they are themselves
a product-thinking signal for reviewers evaluating the repo.
**Reviewed with**: META_CONTEXT.md §F3, SPEC_playground.md.
**Reversal criteria**: if recruit rate exceeds 1 partner per week sustained,
the gate becomes unnecessary overhead and can be dropped. If recruit rate is
less than 1 per month after 4 weeks of active outreach, pause feature work
further and make outreach the sole Week-6+ activity.

---

## 2026-04-21 - Cloudflare Workers Static Assets + HF Docker Spaces for the hosted playground
**Context**: the playground needs a free-tier host for both a static frontend
and a Python backend (FastAPI + pandas + dataforge). The choice must survive
indefinitely on zero-cost infrastructure without maintenance burden.
**Alternatives**:
- Railway. Pros: great Docker support, generous free tier. Cons: free tier
  has a monthly credit cap ($5/month) that can be exhausted by sustained
  traffic; the project would need to monitor credits or risk downtime.
- Render. Pros: Docker support, free tier. Cons: free-tier containers spin
  down after 15 minutes and cold-start takes ~30 s; the free plan has limited
  RAM (512 MB) which is tight for pandas + z3.
- Cloudflare Workers Static Assets (frontend) + HF Docker Space (backend).
  Pros: Workers Static Assets gives the static React/Vite app a global edge
  host on the existing Cloudflare account; HF Spaces support Docker SDK with
  auto-sleep and no monthly credit cap; the combination survives indefinitely
  at zero cost. Cons: HF free-tier Spaces have ~15 min sleep timeout and
  ~30 s cold-start; the frontend must handle this gracefully.
**Decision**: Cloudflare Workers Static Assets (frontend) + HF Docker Space
(backend).
**Reasoning**: this is the only combination that (a) has no monthly credit cap,
(b) supports a full Python + pandas + z3 stack, (c) survives indefinitely
without human intervention, and (d) provides a global CDN for the static
frontend. The cold-start tradeoff is acceptable for a demo playground.
**Additional design decisions**:
- Stateless by design: no persistence, no sessions, no browser storage. This
  eliminates entire classes of security and privacy concerns and makes the
  playground safe to leave running unattended.
- Heuristic-only default: no LLM call unless the user explicitly opts in AND
  a provider key is configured in Space Secrets. This ensures the playground
  works without any external API dependencies.
**Reviewed with**: SPEC_playground.md, META_CONTEXT.md §0.4 rules 4 and 6.
**Reversal criteria**: if free-tier limits are hit (HF downgrades free Spaces
or Cloudflare changes Pages pricing), or a sponsor donates compute, revisit
the hosting choice. If cold-start UX proves unacceptable in design-partner
feedback, consider a paid tier or a keep-alive cron.

---

## 2026-04-27 - Align the frontend deploy path with Cloudflare Workers Static Assets
**Context**: the deployed Cloudflare project is running Workers Builds with
`wrangler deploy`, but the repo still documents Cloudflare Pages and a
build-time `sed` mutation of `playground/web/config.js`. This drift caused the
latest frontend build to fail because Wrangler was not given an explicit assets
directory for the static site.
**Alternatives**:
- Move the frontend back to Cloudflare Pages. Pros: matches the older repo docs.
  Cons: requires reworking the connected Cloudflare project and keeps two
  deployment models in play.
- Keep the current Cloudflare Worker project and add explicit static-assets
  configuration (this choice). Pros: matches the existing Cloudflare build
  system, makes the assets directory explicit, and lets the repo own the
  frontend deployment contract through `wrangler.toml`. Cons: requires doc and
  metadata updates from Pages wording to Workers Static Assets wording.
**Decision**: keep the existing Cloudflare Worker project and standardize the
frontend on Cloudflare Workers Static Assets.
**Reasoning**: the codebase already ships a pure static frontend, so the
minimal durable fix is to add an assets-only Wrangler config, replace the
runtime config mutation with a validated Python renderer, and keep backend CORS
owned by explicit deployment configuration.
**Reviewed with**: `playground/web/DEPLOY.md`, `specs/SPEC_playground.md`, and
the Cloudflare Workers static-assets / Pages configuration docs.
**Reversal criteria**: if Cloudflare deprecates assets-only Worker deploys for
repo-connected builds, or if Pages regains a clear operational advantage for
this static site, revisit the frontend hosting model.

---

## 2026-05-01 - Expand action space from 4 to 7 typed tool-use actions
**Context**: the legacy `data_quality_env` uses 4 untyped actions (`inspect`,
`diagnose`, `fix`, `finalize`). Week 6 migrates to a typed tool-use interface.
The question is whether to preserve the legacy action vocabulary or expand it.
**Alternatives**:
- Keep 4 actions (port legacy vocabulary). Pros: minimal migration risk.
  Cons: blocks richer agent strategies; `inspect` conflates row viewing,
  column stats, and secondary table access into one overloaded action.
- Expand to 7 typed actions. Pros: each action has clear semantics and
  field-level Pydantic validation; enables SQL queries, statistical tests,
  pattern matching, and hypothesis recording that are essential for a
  production-grade data-quality agent. Cons: agent code and training
  pipelines must adapt to the larger action space.
- Expand to 10+ actions (fine-grained per detector). Pros: maximum
  specificity. Cons: combinatorial explosion makes RL exploration harder;
  many actions would be rarely used.
**Decision**: expand to 7 typed actions with discriminated Pydantic union.
**Reasoning**: 7 actions hits the sweet spot between expressiveness and
learnability. Each action maps to a distinct cognitive operation (explore,
analyze, hypothesize, diagnose, repair). The legacy `finalize` is replaced
by automatic step-budget termination, which eliminates the pathological
case where an agent wastes a step by finalizing prematurely and simplifies
the episode lifecycle. The discriminated union pattern prevents cross-model
field pollution that plagued the legacy `DataQualityAction` monolith.
**Reviewed with**: SPEC_openenv_env.md and the Week 6 implementation.
**Reversal criteria**: if RL training shows the 7-action space is too sparse
for exploration (> 2× sample complexity vs 4 actions on equivalent tasks),
consider collapsing SQL_QUERY + STAT_TEST + PATTERN_MATCH into a single
`ANALYZE` action with a sub-type discriminator.

---

## 2026-05-01 - INSPECT_ROWS returns up to 20 rows, not 20 cells
**Context**: the Week 6 prompt says "up to 20 cells total, not 20 rows."
With a 10-column dataset, that allows only 2 rows per inspection — severely
limiting information gain per step compared to the legacy 10-row limit.
**Alternatives**:
- 20 cells (literal prompt). Pros: minimal data leakage per step; forces
  the agent to use SQL_QUERY for broader views. Cons: with 10 columns, only
  2 rows visible per action; the agent needs 5 inspections to see what one
  legacy inspection showed, wasting precious step budget on data access
  instead of reasoning.
- 20 rows (relaxed cap). Pros: each inspection returns enough rows for the
  agent to spot patterns across multiple records; matches the scale at which
  detectors operate (row-level issues); compatible with the exploration bonus
  formula which rewards coverage breadth. Cons: slightly more data per step.
- 10 rows (legacy parity). Pros: direct backward compatibility. Cons:
  arbitrary number with no principled justification.
**Decision**: 20 rows per INSPECT_ROWS action.
**Reasoning**: the cell-level interpretation creates a perverse incentive:
the agent must spend its finite step budget on data access rather than
analysis. With 20 rows × 10 columns, the agent sees ~200 cells per
inspection — enough to identify multi-row patterns (e.g., FD violations,
systematic decimal shifts) that are architecturally invisible in a 2-row
window. The agent retains fine-grained column filtering via the optional
`column_names` field for targeted queries, and SQL_QUERY provides
unrestricted read access for complex analysis.
**Reviewed with**: SPEC_openenv_env.md, REWARD_DESIGN.md exploration bonus.
**Reversal criteria**: if agents learn to request maximum rows on every step
(ignoring the exploration bonus decay), consider reducing the cap to 10 or
adding a diminishing-returns penalty for large inspections.

---

## 2026-05-01 - Use hospital fixture as default, support configurable datasets
**Context**: the environment needs a default dataset for `reset()`. Options
include the existing `fixtures/hospital_10rows.csv`, a purpose-built fixture,
or the legacy JSON datasets in `datasets/`.
**Alternatives**:
- Hospital fixture only. Pros: immediate usability; already has a schema
  YAML. Cons: limited diversity for training.
- Purpose-built fixture. Pros: can be tailored to test all detector types.
  Cons: delays ship; may not represent real-world data characteristics.
- Support both via task configuration. Pros: extensible architecture;
  default fixture for quick-start, configurable loading for BYOD (bring
  your own data) scenarios. Cons: slightly more code surface.
**Decision**: use `fixtures/hospital_10rows.csv` with its schema YAML as
the default episode dataset, with the architecture supporting future
configurable task loading.
**Reasoning**: the hospital fixture is the canonical test dataset already
used by the detector suite and benchmark pipeline. Using it as the default
ensures the ground truth generated by `run_all_detectors()` produces
meaningful issues (type_mismatch on `phone_number`, decimal_shift on
`rating`, fd_violation on `provider_number → hospital_name`). The
architecture's `_load_fixture()` path is trivially extensible to accept
arbitrary CSV+schema pairs in future milestones.
**Reviewed with**: SPEC_openenv_env.md §3 (IN scope).
**Reversal criteria**: if the hospital fixture proves too small or too
repetitive for meaningful RL training, add a larger purpose-built fixture
(~100 rows, 15 columns, all detector types represented) as the default.

---

## 2026-05-01 - Port legacy noise model verbatim (ε=0.15, seed-based RNG)
**Context**: the legacy environment implements stochastic observation noise
with 15% probability per row, using seed-based `random.Random`. The Week 6
prompt asks whether to refine the noise model.
**Alternatives**:
- Port verbatim. Pros: tested, simple, deterministic for same seed, and
  already validated by the legacy test suite. Cons: noise is row-level
  only (no column-correlated noise, no systematic bias).
- Refine with column-correlated noise. Pros: more realistic; mimics
  real-world pipeline errors that affect entire columns. Cons: increased
  complexity; requires new calibration; risks breaking determinism
  guarantees expected by RL training scripts.
- Remove noise entirely. Pros: simplest. Cons: loses the POMDP training
  capability that forces agents to be robust to observation uncertainty.
**Decision**: port the legacy noise model verbatim.
**Reasoning**: the legacy model is simple, deterministic, and effective for
its purpose (partial observability training). Refining the noise model is a
research concern that belongs in a future training experiment, not in the
environment architecture. The ε=0.15 parameter and seed-based RNG ensure
reproducible episodes across training runs, which is more important than
noise realism at this stage.
**Reviewed with**: SPEC_openenv_env.md §4 (constraints).
**Reversal criteria**: if agent training shows the current noise model is
either too easy (agents trivially learn to ignore it) or too hard (agents
can't converge), tune ε or switch to column-correlated noise.

---

## 2026-05-01 - Hypothesis root-cause matching on issue_type (closed vocabulary)
**Context**: the HYPOTHESIS action awards root-cause credit when the agent's
claim matches hidden ground truth. The matching criteria must be defined.
**Alternatives**:
- Match on `issue_type` only. Pros: deterministic, testable; uses the
  closed vocabulary (`IssueTypeLiteral`) which is machine-readable and
  already present in detector output. Cons: coarse; doesn't validate the
  causal reasoning in the `claim` text.
- Match on `issue_type` + `reason` field. Pros: validates richer reasoning.
  Cons: `reason` is free-form text; fuzzy matching is unreliable, requires
  an LLM judge, and violates the "no LLM calls in environment" constraint.
- Match on `issue_type` + `row` + `column`. Pros: precise location-aware
  matching. Cons: this is equivalent to DIAGNOSE; removes the
  strategic value of HYPOTHESIS as a "broader claim" action.
**Decision**: match on `issue_type` (from `IssueTypeLiteral`) plus row and
column membership in `affected_rows` and `affected_columns` respectively.
**Reasoning**: this provides meaningful credit granularity without requiring
text analysis. The agent gets credit for correctly identifying that "rows
[5, 6] in column 'rating' have a `decimal_shift` issue" — which is the
actionable insight a root-cause analysis should produce. The `claim` text
is recorded in the scratchpad for observability but not scored, preserving
the "no LLM calls" invariant. The per-issue credit of `R_EXPLORE = 0.01`
is intentionally small: HYPOTHESIS is a planning action, not a scoring
shortcut, and its primary value is helping the agent organize its
investigation strategy.
**Reviewed with**: SPEC_openenv_env.md §6.5, detector base.py `Issue` model.
**Reversal criteria**: if future work adds a lightweight offline NLI model
for claim verification (no runtime LLM call), consider upgrading hypothesis
matching to validate the `claim` text against the ground-truth `reason`.


---

## 2026-05-01 - Exact-origin CORS and `dataforge-playground` Space naming
**Context**: Week 5 hardening found two deployment risks: the backend accepted
any `*.workers.dev` / `*.pages.dev` origin in production, and docs/config drifted
between the repo name (`data-quality-env`) and the product playground target.
**Alternatives**:
- Keep wildcard Cloudflare CORS and the existing Space slug. Pros: no deploy
  churn. Cons: another Cloudflare-hosted site could call the API, and public
  URLs do not match the product name.
- Revert to Cloudflare Pages and subtree push. Pros: matches the original Week 5
  prompt literally. Cons: contradicts the reviewed Workers Static Assets flow
  and the staged Docker build context already verified in CI.
- Keep Workers Static Assets, require exact production origins, and standardize
  the Hugging Face Space as `dataforge-playground`. Pros: preserves the tested
  deploy path, tightens API exposure, and aligns the public demo URL with the
  product name. Cons: maintainers must set `DATAFORGE_PLAYGROUND_ORIGINS`
  explicitly after deploy.
**Decision**: keep Workers Static Assets, remove production wildcard CORS, allow
localhost only under `DATAFORGE_PLAYGROUND_DEV=1`, and standardize the Space slug
as `dataforge-playground`.
**Reasoning**: exact-origin CORS is the narrowest free-tier-safe contract, while
the product-named Space avoids a confusing public URL without changing API
behavior.
**Reviewed with**: Week 5 playground hardening plan, `SPEC_playground.md`, and
the existing playground smoke/contract tests.
**Reversal criteria**: if Cloudflare changes preview host behavior in a way that
makes exact-origin previews unmanageable, add a narrowly-scoped preview-origin
configuration mechanism rather than restoring broad platform wildcards.


---

## 2026-07-01 - Verified LLM corrector: contract-bound, propose-not-apply, calibrated
**Context**: the measured correction bottleneck is the classes with no derivable
canonical value (missing-value fills, free-text normalization, context-dependent
typos) - the bulk of flights (0.00) and beers (0.04) correction F1. Deterministic
repair cannot invent these values. An LLM can, but a naive LLM writer would
violate the project's verified/reversible/calibrated/honest ethos.
**Alternatives**:
- Naive LLM repairer that writes its best guess. Pros: highest raw coverage.
  Cons: silent, unverifiable writes; corrupts data on hallucination; abandons
  the "prove what you touch" design center.
- Keep those classes detection-only forever. Pros: zero correction risk. Cons:
  leaves the hardest, most valuable half of repair permanently unaddressed.
- Grounded, contract-bound, self-consistent corrector gated by the existing
  verifier/constitution plus a calibrated propose-not-apply policy.
**Decision**: ship the third option. (1) Close the schema-less verification gap
first: when no authoritative schema exists, infer constraints and check any
LLM-origin value against them (`dataforge/verifier/inferred.py`), so corrections
can no longer be structurally auto-accepted. (2) Bind every candidate to a
`CorrectionContract` (detector finding + inferred type/domain/regex/FD) and to
the same inferred guard the verifier enforces, so the corrector can only propose
values the verifier would also accept. (3) Confidence = self-consistency
agreement across k samples. (4) Propose-not-apply by default: corrector output
surfaces as `suggested_fixes`; auto-apply requires both an operator-confirmed LLM
write (constitution `NO_UNCONFIRMED_LLM_WRITE`, now also covering `llm_cache`)
and a per-class threshold fit to a >= 0.95 precision floor. (5) The
`llm_corrector` benchmark method measures per-class correction F1, ECE, and a
fixed-0.95-agreement `precision_at_auto_apply`; `corrector_promotion_verdict`
refuses promotion until the precision floor and a calibration bound are met.
**Reasoning**: deterministic runs stay byte-identical (the guard and corrector
engage only for LLM-origin fixes when `allow_llm` is set), so no regression is
possible when the corrector is off (hospital held at 0.7926). When on, the tool
gains reach on the hard classes without ever making an unverified or silent
write. `precision_at_auto_apply` uses a fixed, pre-committed agreement bar rather
than an in-sample fit to avoid a circular, self-flattering metric.
**Reviewed with**: verified-llm-corrector plan, RAHA detection/correction split,
`dataforge/repairers/contract.py`, `dataforge/repairers/llm_corrector.py`,
`dataforge/calibration.py`, `dataforge/engine/repair.py`.
**Reversal criteria**: if measured `precision_at_auto_apply` for a class clears
the floor with a calibrated confidence signal on held-out data, raise that
class's default threshold so it auto-applies; if the corrector cannot beat the
deterministic stack on any class, keep it suggestion-only.
