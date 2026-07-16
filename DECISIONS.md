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

## 2026-07-11 - Enforce provable-only auto-apply; deep self-verifiable certificate
**Context**: The Corruption Oracle proved the default deterministic auto-apply path
never corrupts, and that the known verifier-floor gaps are LATENT (they live in the
advisory inferred guard, reachable only by an LLM value with no authoritative schema).
But a permissive `corrector_policy` could still auto-apply such a plausibility-only
fix, silently activating a gap. Separately, `verify_certificate` only checked hashes
and structure, not the constraints themselves.
**Alternatives**:
- (a) Never auto-apply plausibility-only fixes at all - maximally strict, removes a
  legitimate power-user escape hatch.
- (b) Opt-in, honestly recorded - off by default; explicit `allow_unproven_autoapply`
  permits it and the certificate records those cells as `plausibility_only`.
- (c) Leave as-is and rely on the default policy - fragile (a permissive policy
  re-opens the gap) and not an enforced guarantee.
- Re-verification: (d) hashes only (status quo); (e) re-run the real verifier per
  applied cell against the certified data; (f) a second, diverse re-implementation
  of constraint semantics.
**Decision**: (b) for enforcement + (e) for re-verification. A fix is `proven`
(deterministic OR authoritative-schema-verified) or `plausibility_only`; only proven
fixes auto-apply unless `allow_unproven_autoapply` is set, and then the receipt's
`applied_fixes[].verification_strength` records it truthfully. `reverify_certificate`
reconstructs the applied fixes and re-runs `SMTVerifier` per cell (mirroring the
engine's guard selection) plus a truthfulness check on the recorded labels.
**Reasoning**: (b) makes "the gaps stay latent" an ENFORCED, tested invariant under
any policy while keeping an honest escape hatch; the certificate never lies. (e)
gives independence in data and execution (catches tampering, drift, receipt/data
mismatch) at low cost; (f) was rejected as itself-unverified and disproportionate.
**Reviewed with**: user (chose "opt-in, honestly recorded").
**Reversal criteria**: if a real deployment needs zero unproven auto-applies, drop
the flag (option a); if a diverse second checker becomes worth its cost, add (f).

---

## 2026-07-07 - Reframe to verified+calibrated gate; conformal risk control; 1.5B is no-go for auto-apply
**Context**: A prior plan pursued "better teacher data + bigger base model" to lift
repair quality. The measured evidence collected while executing it refutes that
premise as the highest-leverage path:
- LLM corrector on the strongest models is rejected by its own promotion gate:
  Gemini precision@auto-apply 0.16 / ECE 0.79; Azure gpt-5-mini 0.077 / ECE 0.82
  (`eval/results/corrector_gpt5mini_hospital.json`). Gate needs >= 0.95 precision,
  <= 0.10 ECE.
- Deterministic correction F1 is 0.00 on flights and 0.039 on beers
  (`BENCHMARK_REPORT.md`); correction is unsolved on 2 of 3 datasets.
- Root cause of untrustworthy auto-apply is NOT model capability. It is (a)
  calibration - models do not know when they are wrong (ECE 0.8), and (b)
  statistical rigor - `dataforge.calibration.fit_thresholds` fit the auto-apply
  threshold IN-SAMPLE (no held-out split, no distribution-free guarantee), so a
  "0.95" threshold silently drops below 0.95 on new data. A bigger local model
  fixes neither.
**Alternatives**:
- Continue scaling the local model to 1.5B/3B. Cost: paid GPU + hours. Expected
  gain: tiny (0.5B already near ceiling per the training note; a 1.5B cannot clear
  a gate GPT-5-mini fails by 12x). Rejected as the primary thrust.
- Invest in the gate: distribution-free guaranteed auto-apply precision (conformal
  risk control) + detection breadth + an honest calibration benchmark. Chosen.
**Decision**:
1. Add `dataforge/conformal.py`: class-conditional (Mondrian) selective-risk
   control via fixed sequential testing + exact Clopper-Pearson bounds, giving a
   distribution-free finite-sample guarantee that an auto-applied class's error
   <= alpha w.p. >= 1 - delta; plus a calibration/test split, a certified-coverage
   report, and a PSI distribution-shift monitor. Refs: Bates et al. RCPS (2021);
   Angelopoulos et al. Learn-then-Test (2021); Conformal Risk Control
   (arXiv:2208.02814); Angelopoulos & Bates gentle intro (arXiv:2107.07511).
2. Wire `conformal_corrector_policy` + `guard_policy_for_drift` behind the
   unchanged `AbstentionPolicy` seam. The SMT verifier and safety constitution
   remain the hard floor; conformal only ever narrows what may auto-apply.
3. 1.5B / teacher-scaling is NO-GO for improving auto-apply. The shipped
   `sft_15b_v10.yaml` + Azure teacher pipeline remain documented, ready-to-run
   rungs, not the roadmap. Any future model work is scoped to exactly the error
   classes conformal can certify at >= 0.95 precision - never a blanket auto-apply.
**Reasoning**: Decision theory over sunk cost. The defensible moat is the verified,
now statistically-guaranteed, honest-abstention gate - not proposer capability
(proposers are commodities). Value is measured (Monte-Carlo validity test proves
the guarantee), never assumed.
**Reviewed with**: (solo) - primary sources verified (the two arXiv papers);
measured corrector reports and the `bench --quick` coverage matrix.
**Reversal criteria**: If a certified-coverage run shows a non-empty corrector
auto-apply slice at >= 0.95 precision on the held-out test split, scope a model
upgrade to those classes. If a specific deployment needs on-prem/offline correction
where hosted models are unavailable, revisit local-model scaling for that context.
**Measured status**: conformal machinery shipped, 1043+ tests, ruff+mypy --strict
clean. Detection (`bench --quick`): flights text_normalization detection = 0.00
(n=1729) is the top winnable-half gap; beers value_format detection is already
1.00 and text_normalization 0.87 (the old 0.40 floor was stale/conservative).

## 2026-07-06 - Azure OpenAI (GPT-5.5) teacher/measurement; 1.5B base; broaden datasets
**Context**: The trained repair policy (Qwen2.5-0.5B) is near its ceiling on
exact-value correction. The two highest-impact levers are better teacher data
and a bigger base model. A $200 Azure free-trial credit is available and the
user wanted "the best model (Opus/GPT-5.5)".
**Alternatives**:
- Use Anthropic Claude (Sonnet 5 / Opus) on Azure Foundry. Pros: strongest.
  Cons: Microsoft docs exclude Claude (a third-party Marketplace SaaS offer)
  from "free trial" and "credit-only" subscriptions - it will NOT run on the
  trial credit. Dishonest to promise it.
- Use Azure OpenAI GPT-5.5 (first-party, "sold directly by Azure"). Pros: billed
  against the subscription so it works on trial credit; strongest usable model;
  OpenAI-compatible surface. Cons: GPT-5 rejects `temperature != 1` and needs
  `max_completion_tokens`.
- Skip Azure; keep oracle-only teacher and 0.5B. Cons: forgoes both levers.
**Decision**: Add a first-party Azure OpenAI provider (product `complete()` +
bench `AzureBenchClient`) with a hard USD cost guard; wire it as a selectable
teacher provider (F1=1.0 verified filter) and corrector-benchmark backend.
Author `sft_15b_v10.yaml` (Qwen2.5-1.5B, bf16, paid GPU) as a structural mirror
of the proven `sft_05b_v9`. Add `rayyan` + `tax` RAHA datasets (verified pinned
SHAs). Keep propose-not-apply and the SMT+safety verifier gate for all
LLM-origin fixes.
**Reasoning**: Honest about what the trial credit can actually run (GPT-5.5, not
Claude); the provider fails fast with an actionable message if a Claude
deployment is requested. Value stays measured, never assumed - the promotion
gates and corrector_promotion_verdict decide "enough".
**Reviewed with**: (solo) - authenticity cross-checked against Microsoft Foundry
docs (models-from-partners, models-sold-directly-by-azure) and Anthropic model
overview.
**Reversal criteria**: If the user moves to pay-as-you-go, enable the Claude
path via the Anthropic Messages endpoint. If GPT-5.5's corrector precision fails
the 0.95 auto-apply gate, the corrector stays propose-not-apply.
**Gating (honest status)**: The Azure provider, teacher wiring, datasets, and
1.5B config are shipped and offline-verified (1010 tests, ruff+mypy --strict
clean). The live teacher-data run and corrector benchmark are gated on a
configured Azure endpoint (see docs/azure-teacher-setup.md); the 1.5B SFT->GRPO
training is gated on the user's paid GPU. Coverage floors for rayyan/tax are to
be seeded from the first measured `dataforge bench --quick` run.

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

## 2026-07-12 - N-version differential verification: a second, independent constraint checker

**Decision**: Add a second, independently-written constraint checker,
`DirectVerifier` (`dataforge/verifier/direct.py`), that evaluates the same
authoritative-schema specification as the primary z3-backed `SMTVerifier` but
by DIRECT Python table evaluation -- set membership, comparison, enumeration --
sharing none of its checking logic and importing no z3. The result contract
(`VerificationVerdict`/`VerificationResult`) was relocated to a
dependency-free `dataforge/verifier/result.py` so the diverse checker's import
graph is genuinely z3-free. `differential_verify`
(`dataforge/verifier/differential.py`) runs both and combines their verdicts
FAIL-CLOSED: a fix auto-applies only when BOTH accept; any disagreement (or a
non-accept from either) holds the fix and is recorded. The engine gate is
default-on for the authoritative-schema path
(`RepairPipelineRequest.require_independent_agreement=True`,
`RepairReceipt.independent_verification`), and `reverify_certificate` now
re-derives ACCEPT through the differential pair on that path (recording
`reverify_independent_agreement`), removing the prior "not a diverse
re-implementation" caveat.
**Reasoning**: "trust the verifier" was a single point of failure -- a bug in the
one checker would pass at repair time AND at reverify time. Two implementations
built from different mechanisms make a common-mode logic bug very unlikely; when
they diverge, fail-closed means the diverse checker can only ever REDUCE
auto-applies (hold a fix for review), never wave through a corrupting one. A
Hypothesis equivalence suite (`tests/property/test_verifier_equivalence.py`)
generates random schemas/tables/fixes and asserts the two agree; a 1500-example
stress showed 919 accept/accept + 581 reject/reject and ZERO disagreements. The
gate engages only when an authoritative schema is present, so schema-less
deterministic runs (hospital 0.7926) are byte-identical and untouched.
**Honest boundary**: N-version targets checking LOGIC diversity. The two share
the specification (`Schema`), the output contract, and table I/O; a defect in
the shared spec itself, or in a shared dependency, is not covered. The advisory
inferred guard (heuristic, schema-less) is intentionally single-implementation
because it only ever gates non-auto-applying plausibility fixes.
**Reviewed with**: nversion-independent-constraint-checker plan,
`dataforge/verifier/{direct,differential,result,smt,gate}.py`,
`dataforge/engine/repair.py`, `dataforge/certificate.py`,
`tests/unit/test_direct_verifier.py`, `tests/unit/test_differential_verifier.py`,
`tests/property/test_verifier_equivalence.py`.
**Reversal criteria**: if the equivalence suite ever surfaces a persistent
divergence rooted in the shared spec (not an implementation bug), promote the
spec itself to the object of verification; if per-fix dual verification proves
too slow at scale, batch it or gate it behind a size threshold, never behind
correctness.

## 2026-07-12 - Dataset scope: exclude beers; focus hospital + flights

**Decision**: The `beers` benchmark dataset is removed from the project's active,
forward-looking surfaces and is not used in any new work. Removed from
`dataforge/datasets/registry.py` (registry entry), `dataforge/cli/bench.py`
(the `--quick` default expansion, now `hospital,flights`),
`dataforge/release/model_family.py` (required/eval dataset lists),
`eval/thresholds/coverage_floors.json` (the `heuristic/beers` floor block), and
the README benchmark docs. Live bench tests were repointed to hospital/flights.
The durable rule is recorded in `CLAUDE.md` (DATASET SCOPE RULE) so every future
session follows it. The remaining RAHA datasets are NOT ranked by a fixed
priority: they are one canonical suite of equal provenance that differ by error
profile, not quality. Dataset selection for new work is capability-based:
`hospital` is the flagship and hard regression anchor (heuristic F1 must never
regress below 0.7926 — the one measured SOTA win); `tax` for provable
FD/rule-violation repair at scale; `rayyan` for datetime/format canonicalization;
`flights` for the not-inferable-in-table frontier. `tax`/`rayyan` must be measured
before being prioritized for accuracy work (`tax` = 200k rows, needs a
scale-aware/sampled bench and has no floors yet; `rayyan` has only detection
floors, no measured correction baseline).
**Reasoning**: the product's effort should concentrate on the capability a change
is meant to prove, not on a dataset popularity ranking; `beers` added surface area
without being a focus, so it is removed. Framing the others by capability (not a
rigid hospital>flights>others order) keeps the roadmap honest: `tax`'s
denial-constraint/rule-violation errors align with DataForge's provable FD stack
and are a plausible SECOND place to beat SOTA once measured.
**Honest boundary**: frozen historical artifacts (past SFT/GRPO training curricula
such as `training/grpo_config.py` and the `expert_v*` trajectories, archived
`eval/results/` run snapshots, and released-model tokenizer vocab) still reference
beers because that is a factual record of what past runs did; they were
deliberately left untouched. Only forward-looking use of beers is prohibited.
**Reviewed with**: CLAUDE.md DATASET SCOPE RULE, dataforge/datasets/registry.py,
dataforge/cli/bench.py, dataforge/release/model_family.py,
eval/thresholds/coverage_floors.json, tests/unit/test_bench_real_world.py,
tests/unit/test_bench_core.py, tests/unit/test_bench_runner.py, README.md.
**Reversal criteria**: if a future benchmark need requires beers, re-add its
registry entry (RAHA revision + SHA-256s are preserved in git history) and update
the DATASET SCOPE RULE accordingly.

---

## 2026-07-14 - The honest frontier: deterministic in-table correction is maxed; add post-hoc calibration for the LLM path

**Context**: A fixing-elevation push aimed to raise correction accuracy (beat
Raha+Baran F1 where provable, raise safe auto-apply coverage) WITHOUT weakening
the no-corruption guarantee. Three candidate slices were measured first (offline,
deterministic, no code shipped until proven). All three turned out to be NOT
in-table-provable, for one shared reason worth recording.

**What was measured** (each an offline read-only measurement against the pinned
RAHA revision `7be1334b8c7bbdac3f47ef514fb3e1e8c5fc181c`):

- **flights value_format (time-in-cruft), Phase 1A = NO-GO.** `TimeFormatCruftDetector`
  flags 126 cells with 0 false positives, but stripping the date/timezone cruft
  reproduces `clean.csv` on only **57/126 (precision 0.452)**. The 57 matches are
  the `value_format` class; the 69 misses are `other` cells whose embedded time is
  an ESTIMATE, not the actual time (`'6:47 p.m. (Estimated runway)'` -> clean
  `'6:30 p.m.'`). Byte-identical residues (`'(Estimated)'`, `'(Estimated runway)'`,
  `'12/2/11'`) occur on BOTH correct and wrong cells, so no function of the dirty
  cell separates them. The only 1.0-precision rule ("residue is exactly a zero
  offset +/-00:00") has support 4. Auto-applying the strip would write ~69
  confidently-wrong values.
- **tax FD/rule-violation, Phase 1B = NOT VIABLE near-term.** tax is 200k x 15 with
  121,219 errors, but **97.9% are `numeric`** (rate 87,342; zip 31,311), not
  cross-column FD; the genuine FD-repairable slice (city+state) is ~800 cells.
  Schema inference is super-linear (2k 0.7s -> 20k 9.8s) and does not finish on
  200k in >8 min; the FD repairer is O(issues x rows). The 90%-confidence
  single-column FD heuristic invents spurious dependencies (`zip->salary`,
  `zip->rate`, `f_name->gender`), so on a 3k sample **detection precision is 0.0317**
  (263 real of 8305 flagged; fd_violation alone: 19 real of 7808). Only
  `decimal_shift` is useful (244/375). No SOTA win without exact-FD/denial-constraint
  mining + precision control + a vectorized scale rewrite. `sota_comparison.json`
  also has no tax row, so any tax claim needs a new sourced citation.
- **rayyan datetime_format, Phase 1C = NO-GO for auto-apply (but exact reviewed fix).**
  722 cells, all in `article_jcreated_at`, are a systematic `Y/M/D` -> `M/D/YY`
  transposition. A deterministic left-rotation reproduces `clean` on **722/722
  (correction precision 1.0000)** and the value sets are perfectly disjoint (0
  collisions). BUT every error value is ALSO a syntactically valid `M/D/YY` date,
  so no single-cell validity rule fires; the best genuine structural detector is
  **0.944 precision (27 would-be corruptions)** and the verifier cannot catch it
  (a rotated date is still a valid date). Worse, the corrupted `Y/M/D` form is the
  column MAJORITY (79%), so "canonicalize to dominant" points the wrong way.

**Alternatives**:
- (A) Ship one/all of the above as auto-apply corrections to raise headline F1.
  Rejected: each would auto-apply confidently-wrong values (flights/rayyan) or
  massive false positives (tax), violating "fix only what you can prove / never
  corrupt". The whole thesis forbids it.
- (B) Ship the tiny provably-safe microslices (flights zero-offset = 4 cells;
  rayyan rotation as a REVIEWED, never-auto-applied suggestion). Deferred: near-zero
  accuracy impact; the rayyan rotation is a good future propose-not-apply feature.
- (C) Accept the deterministic frontier and invest in the path that CAN fix
  semantic errors -- the calibrated LLM corrector -- starting with the one piece
  buildable offline without API keys: post-hoc probability calibration. Chosen.

**Decision**: (1) Do NOT ship any of the three as auto-apply corrections; keep the
flights/rayyan slices detection-only and leave the flights `value_format`
correction floor at 0.0 (honest -- we cannot provably fix it). (2) Add
`dataforge/calibration_map.py`: a pure-Python (no new deps), per-class,
leakage-free post-hoc calibration map (PAVA isotonic + Platt) that rescales the
corrector's self-consistency agreement into a calibrated probability before the
existing `dataforge.conformal` auto-apply gate. It is advisory only -- the SMT
verifier, safety constitution, and provable-only gate remain hard gates beneath
it, so a bad map can only withhold fixes, never wave through a corrupting write.

**Reasoning**: THREE consecutive in-table NO-GOs share one root cause -- the
residual errors across the measured RAHA datasets are SEMANTIC value errors (a
wrong/estimated time, a transposed date, a spurious near-FD), not syntactic ones,
so they are not inferable from in-table signal without either a declared
schema/convention or an external model. That means DataForge's deterministic
in-table correction is already at its HONEST FRONTIER; further auto-apply accuracy
must come from schema-directed reviewed repair or the calibrated LLM path, not
from more detector hunting. This validates rather than undermines the product
thesis: the gates correctly refused every tempting-but-wrong fix. The calibration
map is the offline-buildable foundation of the LLM path (measured corrector ECE
~0.8 is the wall); live ECE gains require corrector samples from a provider run
and are deferred with the API-key work.

**Reviewed with**: eval/thresholds/coverage_floors.json (`_frontier_map`),
dataforge/detectors/time_format_cruft.py, dataforge/schema_inference.py
(`_fd_candidates`), dataforge/bench/methods.py, dataforge/conformal.py,
dataforge/calibration_map.py, tests/unit/test_calibration_map.py.

**Reversal criteria**: (a) if a declared/confirmed column schema is available,
the rayyan rotation becomes a provable schema-directed fix worth shipping as a
reviewed suggestion; (b) if the FD inference is hardened to exact/denial
constraints with a vectorized scale pass, re-measure tax for a provable
rule-violation win; (c) once corrector correctness samples exist (API-key phase),
wire `calibration_map` into the corrector policy and confirm ECE drops below the
0.10 promotion bar on a disjoint test split.

---

## 2026-07-15 - Post-hoc calibration breaks the ECE wall; safe calibrated auto-apply wired (live Azure gpt-5-mini)

**Context**: Prior sessions proved a bigger/reasoning model does not fix corrector
calibration (gpt-5-mini ECE ~0.84, precision@auto-apply ~0.05, certified coverage
0.0). This API-key phase asked the complementary question: does POST-HOC calibration
(the new `dataforge/calibration_map.py`, isotonic PAVA + Platt) make the corrector's
confidence an honest probability, and can calibrated + conformally-certified scores
enable safe auto-apply without ever corrupting?

**What was run (live, Azure OpenAI gpt-5-mini, $10 guard, reasoning_effort=minimal)**:
- Fresh corrector benchmark, hospital (60 issues / 180 calls, ECE 0.838) and flights
  (40 issues / 120 calls, ECE 0.525, precision@auto-apply 0.25, 1 tp / 39 fp). Samples
  captured per issue_type (`CellFix.detector_id`), the key the auto-apply gate uses.
- `scripts/bench/calibrate_corrector.py` fit the calibration map on the calibration
  split and measured ECE on a disjoint test split: **overall ECE 0.807 -> 0.0**. The
  certified per-issue-type policy ABSTAINS (thresholds 1.01) -> certified auto-apply
  coverage 0.0 -> 0.0.

**Decision**: (1) Ship the post-hoc calibration map + wire it into the auto-apply gate:
`calibrated_conformal_corrector_policy` (fit maps -> certify thresholds on calibrated
scores, keyed by issue_type) and `_partition_auto_apply` now rescales an LLM fix's raw
confidence through its per-issue-type map before the policy decides. CLI:
`dataforge repair --corrector-calibration <artifact>` under `--allow-llm`. (2) Keep the
corrector propose-not-apply for gpt-5-mini: the certified policy correctly abstains
(precision far below any usable alpha), so nothing auto-applies.

**Reasoning**: Post-hoc calibration is the right tool for a high-ECE, low-precision
proposer -- it makes the reported confidence trustworthy (honest "flags"), which serves
the trust thesis. But it is a MONOTONE rescale: it preserves proposal ranking, so it
lowers ECE WITHOUT changing conformal-certifiable coverage. Calibration therefore fixes
honesty, not accuracy; auto-apply coverage stays gated on the corrector actually being
precise (it is not). This is the honest, non-corrupting outcome: the wiring is real and
tested, and it will let a genuinely-precise future model auto-apply safely, but it never
manufactures coverage from a weak model. All auto-apply remains triple-gated
(authoritative schema -> differential SMT -> certified calibrated threshold);
plausibility-only fixes stay held.

**Reviewed with**: dataforge/calibration_map.py, dataforge/calibration.py
(`calibrated_conformal_corrector_policy`, `load_corrector_calibration`),
dataforge/engine/repair.py (`_partition_auto_apply`, `_calibrated_confidence`),
dataforge/cli/repair.py (`--corrector-calibration`), scripts/bench/calibrate_corrector.py,
eval/results/corrector_calibration.json, tests/unit/test_calibration_map_real.py,
tests/unit/test_corrector_autoapply_wiring.py.

**Reversal criteria**: if a future corrector reaches precision high enough that the
conformal gate certifies a per-issue-type threshold below 1.01, calibrated auto-apply
activates automatically for schema-proven fixes -- re-verify the certified coverage
report and the never-corrupt invariants (byte-identical `allow_llm=False`, apply->revert,
hospital 0.7926) before promoting.
