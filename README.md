# DataForge

DataForge is a CLI-first verification layer for changes to tabular data, with a
deterministic repair engine behind it. It detects common CSV issues, proposes
deterministic repairs, proves or refuses every proposed change through safety and
verification gates, and records applied changes in a reversible transaction log.

What makes it different: **every auto-applied fix is formally verified (SMT),
constitution-checked, byte-for-byte reversible, and emits a portable attestation a
third party can verify without running DataForge.** It auto-applies only what it can
prove correct; everything else it detects and flags for review, never silently
changing it. Detection and correction are measured separately (see Coverage),
so the tool's real limits are visible, not hidden behind an aggregate score.

The attestation is the part worth reading first. `dataforge attest verify` checks a
change against the constraints it was proven under, using nothing but the attestation
and the data -- no solver, no schema passed on the side, and no need to trust the tool
that produced it. The format is specified in
[specs/SPEC_repair_attestation.md](specs/SPEC_repair_attestation.md) and has two
independent implementations (Python and TypeScript) that agree on a committed suite of
conformance vectors, including every rejection case.

The final public product name is DataForge. The PyPI/TestPyPI distribution
family is `dataforge_07*` because the unqualified `dataforge` project name is
occupied by unrelated packages. Installing `dataforge_07` still provides the
`dataforge` import namespace and `dataforge` CLI. `dataforge15` is only a
temporary staging alias retained for local compatibility.

The current repository is an alpha implementation. It also contains the
OpenEnv-compatible training environment, the SFT warmup workflow, a local MCP
server package, and playground/demo sources. Warehouse integrations and
production model-quality claims remain future work.

For release work, review `THREAT_MODEL.md` and `docs/docs/release.md`. They
define the security, supply-chain, and evidence gates that separate the current
public package/playground surface from the remaining full original DataForge
vision.

## Verification layer: gate any fix source

DataForge's guarantee is not tied to its own detectors. `verify_and_apply` lets
**any** external actor — an autonomous agent, another tool, or a human — propose
cell fixes and have DataForge prove-or-hold each one through the *same* gate an
internal repair runs: the safety constitution, the differential SMT verifier, a
reversible hash-chained transaction, and a self-verifying `repair_receipt_v1`
certificate. External values are treated as **untrusted**: a fix is auto-applied
only when it (a) clears the unconfirmed-write confirmation and (b) is *proven* —
verified against a schema that actually **constrains the column being written**.
Declaring a column `str` is not a constraint: every CSV cell is already a string, so
such a column confers no authority and its fixes are held. Without a discriminating
premise nothing is applied at all; nothing untrusted is ever silently written, and
every applied change reverts byte-for-byte.

An optional `expected_old_value` per fix is a compare-and-set precondition that
rejects stale writes (lost-update protection). Held and rejected proposals come
back with honest, specific reasons (`verifier_rejected`, `stale_precondition`,
`invalid_target`, `safety_escalation`, `floor_cannot_verify`).

Three surfaces, one write path, one certificate:

```bash
# CLI — an agent writes its proposals to fixes.json
dataforge verify-apply data.csv --fixes fixes.json --schema schema.json \
  --apply --confirm-escalations --proposer my-agent
# fixes.json: [{"row": 0, "column": "score", "new_value": "15",
#               "expected_old_value": "10"}]
```

The `dataforge.verify_and_apply` Python API takes a `VerifyAndApplyRequest`
(`verify_and_apply`, `VerifyAndApplyRequest`, and `ExternalFix` are exported from
the top-level package):

```python
# Python
result = verify_and_apply(
    VerifyAndApplyRequest(
        source_path="data.csv",
        fixes=[ExternalFix(row=0, column="score", new_value="15")],
        mode="apply",
        schema=my_schema,            # proven only where the schema constrains the column
        confirm_escalations=True,
        proposer="my-agent",
    )
)
result.receipt.applied_fixes   # proven and applied
result.receipt.suggested_fixes # held/rejected, each with a review reason
```

The MCP `dataforge_verify_and_apply` tool exposes the same entry to agent
frameworks (apply gated by `--enable-apply` / `DATAFORGE_MCP_ENABLE_APPLY` and the
allowed-root sandbox). The guardrail value is proven end-to-end in
`tests/integration/test_external_agent_guardrail.py`: an untrusted agent's mixed
batch of correct, corrupting, stale, and invalid proposals yields **zero
corruptions** — only the schema-proven fixes apply, the rest are held, and the
applied set re-verifies and reverts.

## Current Status

Shipped in the current worktree:

- `dataforge profile`, `dataforge repair`, `dataforge verify-apply`,
  `dataforge revert`, `dataforge watch`, `dataforge audit`, `dataforge bench`,
  and `dataforge quickstart`
- Eight detector families across an additive ensemble. **Two may auto-apply**:
  `fd_violation` and `missing_value`, and only from a declared functional dependency.
  The other six — `type_mismatch`, `decimal_shift`, `format_violation`,
  `categorical_normalization`, `outlier`, `duplicate_row` — surface issues for review and
  their repairs are calibration-bound. `decimal_shift` and `type_mismatch` were each
  removed from the auto-apply set on measurement, not on taste; see
  [docs/trust/bypass-allowlist-evidence.md](docs/trust/bypass-allowlist-evidence.md).
  Cell ownership is separate from write authority: tier 0 owns its cells in the queue,
  which is why a detector can be high-precision at *detection* and still not permitted
  to write.
- Reviewable schema inference in `profile --json`, including inferred column
  types, domains, regex candidates, uniqueness, and FD candidates
- Pending constraint review artifacts via `profile --constraints-out`, which
  can feed repair only after individual candidates are marked accepted
- Matching deterministic repairers wired through SafetyFilter -> SMTVerifier
- Backend-neutral `PatchPlan` and `TableStore` contracts for CSV, DuckDB, and
  dry-run-only cloud warehouse boundaries
- Reversible hash-chained transaction journals with immutable source snapshots
- Public backend repair engine at `dataforge.engine.repair`
- Real-world benchmark harness for Hospital and Flights
- OpenEnv-compatible HTTP environment with eight typed actions, including
  read-only `ROOT_CAUSE`
- Causal root-cause analyzer for cascading data-quality errors
- Standalone `dataforge-mcp` package exposing DataForge tools over MCP
- Published `dataforge_07`, `dataforge_07_mcp`, `dataforge_07_evals`,
  `dataforge_07_dbt`, and `dataforge_07_agent_patterns` packages on PyPI and
  TestPyPI, with fresh-install smoke logs and Trusted Publishing evidence
- Verified Cloudflare Workers playground backed by the Hugging Face Space API
- Published dbt DuckDB adapter proof from a fresh PyPI install
- Week 9 SFT oracle trajectory workflow, readiness gate, Kaggle notebook, and
  release verifier
- Separate Gradio model-demo Space source for the published 0.5B SFT smoke
  checkpoint

Not claimed yet:

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
# Repairs are earned by a premise that constrains the column. premised_fd_10rows
# declares `state -> city`, so a repair is proven; hospital's schema declares mostly
# `str` columns and therefore proves nothing.
dataforge repair fixtures/premised_fd_10rows.csv \
  --schema fixtures/premised_fd_10rows.schema.yaml --dry-run
dataforge repair fixtures/hospital_10rows.csv --constraints constraints.json --dry-run
# Measure YOUR labelling quality: draw a random sample, label it, get exact intervals.
# Advisory only -- these thresholds are not consumed by `repair`. See
# docs/trust/stratified-label-noise-result.md.
dataforge calibrate fixtures/hospital_10rows.csv --per-class 8
dataforge calibrate fixtures/hospital_10rows.csv --label 3:City=error --label 7:City=correct
# Inferred FDs raise recall but can flood review AND authorize writes; keep the queue
# to declared FDs only.
dataforge repair fixtures/hospital_10rows.csv --constraints constraints.json --fd-detection declared --dry-run
dataforge watch fixtures/hospital_10rows.csv --schema fixtures/hospital_schema.yaml --once --json
dataforge bench --methods random,heuristic --datasets hospital,flights --seeds 3 --seed-list 0,1,2
```

Or run the bundled demo (works from any install, no files needed). It ships a schema,
because nothing in this product repairs without a declared premise:

```bash
dataforge quickstart
```

## Coverage: what DataForge can and cannot safely fix

DataForge reports **detection** (did it flag the error) and **correction** (did
it produce the exact right value) separately, because they are different
problems: detecting a wrong value is often easy, but inventing the correct one
is frequently impossible without external knowledge. The tool refuses to guess.

Measured on the full RAHA benchmark datasets (deterministic stack, no LLM;
reproduce with `dataforge bench --quick`):

| Dataset  | Error provenance | Tier | Correction F1 | Detection coverage (recall by class)                    |
| -------- | ---------------- | ---- | ------------- | ------------------------------------------------------- |
| hospital | injected         | tripwire   | 0.7926  | value_format 1.00, text_normalization 0.87, other 1.00  |
| flights  | contested        | diagnostic | 0.0000  | missing_value 1.00 (2370 cells)                         |

**Provenance note, 2026-08-25.** The hospital correction F1 above was measured with a
deterministic stack that included `type_mismatch` and `decimal_shift` in the auto-apply
set. Both have since been removed from it on measurement, so this figure is a record of
what that stack scored and is **not** currently reproducible by `dataforge bench --quick`.
Per-detector unconditional write measurements now replace it, including how many
already-correct cells each detector would overwrite; they are reported in
[docs/trust/bypass-allowlist-evidence.md](docs/trust/bypass-allowlist-evidence.md) rather
than here.

Neither row is a headline claim, and the two middle columns say why:

- **hospital's errors are injected, and the entire error model is one substituted
  character** -- 509 of 509 corrupted cells contain an `x` (`birminghxm` ->
  `birmingham`). HoloClean's and HoloDetect's own authors describe it in print as an easy
  benchmark, and the field has moved 0.83 -> 0.99 while this number sits at 0.7926. It is
  retained as a fixed regression **tripwire**, which is a real and useful role, and it is
  not evidence of capability on real errors.
- **flights' labels are contested.** The errors are natural, but the same flight's arrival
  time appears upstream as 10:30/10:31/10:28/10:39 and the ground truth picks one, so a
  system that declines to invent a truth is scored identically to one that guesses wrong.
  See [specs/SPEC_abstention_scoring.md](specs/SPEC_abstention_scoring.md).

For measured behaviour on **real** errors -- 2,397 real table columns and 166,387 real
distinct values, scored so that principled abstention is not penalised -- see
[docs/trust/real-error-detection-result.md](docs/trust/real-error-detection-result.md), read
together with its correction in
[docs/trust/frequency-dependence-correction.md](docs/trust/frequency-dependence-correction.md).
On unconstrained columns from tables in the wild, detection precision is roughly an order of
magnitude below the injected-corpus figures above, and no detector holds a safe operating
point at any confidence threshold.

That is the hard case, and it is not the only case. Measured at **cell level** -- the unit a
review queue is actually counted in -- on ordinary operational tables, several detectors reach
perfect precision with no false positives: see
[docs/trust/cell-level-detection-result.md](docs/trust/cell-level-detection-result.md). The
two sets of numbers are **not** comparable, in either direction, and the measured gap between
the units runs up to total
([docs/trust/scoring-unit-reconciliation.md](docs/trust/scoring-unit-reconciliation.md)).

The single most important measured fact for anyone deciding whether to use this: **the same
detector's precision swings by more than an order of magnitude across corpora** -- perfect on
one table, near-useless on another -- and nothing observable at runtime predicts which case a
given table resembles. That is why `dataforge calibrate` measures your table rather than
quoting a benchmark. Read it as a measurement of your labelling, not as a licence: its
thresholds are advisory and are **not** consumed by `repair`, and human-labelled
certification at alpha=0.05 is measured unreachable inside a realistic budget — see
[docs/trust/stratified-label-noise-result.md](docs/trust/stratified-label-noise-result.md).
The argument the swing actually supports is that the write gate must not be relaxed.

How to read this honestly:

- DataForge **detects** a large share of errors on both datasets in the table above,
  including classes (missing values, format/normalization variants, outliers,
  duplicate rows) it deliberately does not auto-correct. `rayyan` is measured for
  detection only and `tax` only on a sample, so neither is an auto-apply target and their
  results are reported in
  [docs/trust/accuracy-frontier.md](docs/trust/accuracy-frontier.md) rather than here.
- It only **auto-corrects** where a value is derivable from a declared dependency and
  provable (FD majority, FD lookup for a missing cell), which is why correction F1 is
  higher on hospital (FD/typo-dominated) and low on flights (dominated by missing
  values and free-form formatting with no derivable canonical).
- Auto-correctable classes pass an SMT proof and the safety constitution before
  being applied inside a reversible transaction. Everything else is calibration-bound:
  `type_mismatch`, `decimal_shift`, `format_violation` and `categorical_normalization`
  repairers exist and are unit-tested, but are withheld from auto-apply until a committed
  measurement earns it. `decimal_shift` (precision 0.0000 on three datasets, 263,428 false
  rewrites on an error-free table) and `type_mismatch` (156 flags and zero proposals across
  three corpora) were each judged against that rule and failed it.

### Optional LLM corrector (opt-in, propose-not-apply)

The correction bottleneck is the classes with no derivable canonical value:
missing-value fills, free-text normalization, and context-dependent typos
(the bulk of flights errors). For these, an opt-in LLM corrector
(`dataforge repair --allow-llm`) proposes an exact value, but it is built to be
trustworthy rather than impressive:

- **Grounded + contract-bound.** Each proposal must satisfy a `CorrectionContract`
  derived from the detector's finding plus inferred constraints (type, numeric
  domain, regex, functional dependency). A value that violates the contract is
  discarded before it is ever considered.
- **Self-consistent.** `k` samples are drawn; the agreement fraction is the
  confidence. Low agreement means low confidence.
- **Verified + reversible.** Surviving values still pass the SMT verifier
  (including the schema-less inferred-constraint guard) and the safety
  constitution, and any applied change is journaled and byte-for-byte reversible.
- **Propose-not-apply by default.** Corrector proposals surface as reviewable
  `suggested_fixes`; they are auto-applied **only** when a per-class threshold is
  cleared *and* the operator confirms LLM writes. Nothing LLM-origin is silently
  written.
- **Distribution-free auto-apply guarantee.** Auto-apply thresholds are no longer
  fit in-sample (which overstates precision on new data). `dataforge/conformal.py`
  certifies each class's threshold with conformal risk control (fixed sequential
  testing + exact Clopper-Pearson bounds): with probability >= 1 - delta, a
  certified class's auto-applied error rate is <= alpha on data exchangeable with
  the calibration split. Two independent guards keep that claim inside its scope: a
  **table-scope check** refuses a certificate whose calibration table has a different
  shape, failing closed when an artifact records no scope at all; and a **Population
  Stability Index monitor** downgrades auto-apply back to review when the live
  confidence distribution drifts. The scope check runs first, because PSI returns early
  when an artifact carries no reference histogram — so an artifact fitted elsewhere and
  carrying no reference would otherwise be guarded by nothing. Every downgrade is
  recorded in `receipt.limitations` rather than printed, so a withdrawn certificate
  leaves durable evidence. The SMT verifier and safety constitution remain the hard
  floor beneath all of this.
- **Locally measurable, and measured to be insufficient.** The reason no class ships
  auto-apply-enabled is not squeamishness: conformal risk control requires the
  calibration data to be exchangeable with the target, and no benchmark can establish
  that against a table it has never seen. `dataforge calibrate` attacks that from the
  other side — you label a small random sample of *your* table, so exchangeability holds
  by construction rather than by assumption. Certification consumes **repair** verdicts
  ("is this proposed value right?"), never detection verdicts ("was this flag right?").
  **What the measurement then showed is that this route does not close.** A human
  labeller's false-accept rate is bounded at 0.8712 on the binding control class, which
  puts alpha=0.05 at 572 error-free labels against a pre-registered budget of ~200, and a
  `SessionCertification` has no consumer: it is printed and discarded, and `repair` reads a
  different artifact whose schema it cannot satisfy. The honest product is the premise-based
  write gate plus reversibility, not a certified threshold. See
  [docs/trust/stratified-label-noise-result.md](docs/trust/stratified-label-noise-result.md).

Cost is explicit: the corrector spends `k` LLM calls per detected issue. The
`llm_corrector` benchmark method reports per-class correction F1, calibration
error (ECE), and `precision_at_auto_apply` (precision among proposals whose
agreement clears a fixed 0.95 bar), and the promotion gate
(`corrector_promotion_verdict`) refuses to promote a class to auto-apply until
that precision floor and a calibration bound are met on measured data.

A first live-provider report is committed and verified at
[`eval/results/corrector_gemini_hospital.json`](eval/results/corrector_gemini_hospital.json)
(Gemini 3.1 Flash-Lite, 200 sampled hospital issues, self-consistency `k=3`).
The verified result is deliberately unflattering and load-bearing: measured
precision 0.14 and `precision_at_auto_apply` 0.16 do not clear the 0.95 floor,
and ECE 0.79 does not meet the 0.1 calibration bound. `corrector_promotion_verdict`
therefore rejects promotion, so the corrector does not auto-apply and stays
propose-not-apply — exactly the intended default. No error class has earned
auto-apply on measured data. (The bench also runs on Bedrock and Groq via
`DATAFORGE_LLM_PROVIDER`.)

A second live report on a frontier model - Azure OpenAI `gpt-5-mini`,
[`eval/results/corrector_gpt5mini_hospital.json`](eval/results/corrector_gpt5mini_hospital.json) -
reinforces the finding rather than overturning it: `precision_at_auto_apply`
0.077 and ECE 0.82 are *worse* than the smaller model's, confirming that frontier
capability does not buy calibration. Auto-apply eligibility is therefore decided
by the distribution-free certified-coverage report (calibrate on a held-out
split, measure on a disjoint test split), not by model size.

This is the design center, not an apology: a data-repair tool you can trust is
one that tells you exactly what it will and will not touch, and proves it.

> **CORRECTION (2026-08-20): those precision numbers were mostly measuring the
> DETECTOR, not the corrector.** The benchmark samples from hospital's
> inferred-FD queue, which is 10,373 cells containing 455 real errors —
> **4.4% queue precision**. A corrector cannot correct a cell that was never
> wrong: if the flag is a false positive, *any* proposed value scores as
> incorrect. So corrector precision measured there is bounded near 0.05
> regardless of model quality, and "frontier capability does not buy
> calibration" was a confounded reading of a denominator artifact — the same
> mistake already documented for F1, never applied to precision.
>
> Measured on a per-table calibration session with the same model
> (`gpt-5.6-sol`) and the same structured-enum mode: **114 correct of 115
> proposals, 0.9913**. The mechanism is abstention — of the 115 cells that
> received a proposal, **114 were genuine errors**. The corrector declines to
> propose on non-errors, acting as a high-precision filter rather than a blind
> rewriter. It is precise but partial (115 proposals from 240 sampled cells),
> which is the correct trade for writing to a user's file.
>
> Two things this does **not** change. The pre-registered global certification
> attempt remains a **NULL** at `alpha = 0.05`: it needs 59 accepted samples
> with *zero* errors (1 error in 60 gives a Clopper-Pearson bound of 0.077),
> and the budget bought ~157 proposals over ~16 hours of serial calling. And
> `reasoning_effort` — the one lever `gpt-5-mini` lacked — does not help:
> `none` and `xhigh` each produced exactly 4 correct proposals on a paired
> 80-issue slice, with `xhigh` costing 47% more. Full analysis in
> [`docs/trust/corrector-queue-contamination.md`](docs/trust/corrector-queue-contamination.md).

#### Selective-Repair Calibration Benchmark

The flagship artifact makes that proof reproducible. Framing the auto-apply gate
as selective classification (Geifman and El-Yaniv, 2017), it certifies per-class
thresholds with conformal risk control on a calibration split and measures them
on a disjoint test split, then reports a risk-coverage curve with its AURC, an
alpha sweep, a 200-way random-split validity check, and a reliability diagram.
Committed at
[`eval/results/selective_repair_calibration.json`](eval/results/selective_repair_calibration.json)
with a methods note at
[`docs/selective-repair-calibration.md`](docs/selective-repair-calibration.md).

Measured on Azure `gpt-5-mini` (hospital), the dominant error class reaches
adequate support (n=48, above the 30-sample floor), so the result is not a
small-sample artifact: certified auto-apply coverage is 0.0 at every tested
error budget from 1% to 20%, AURC is ~0.92 (no safe operating point), and across
200 random splits the gate never once auto-applied a wrong fix. Raising the
model's reasoning effort from minimal to medium does not help (ECE 0.80 vs 0.87),
confirming that calibration - not capability or effort - is the binding
constraint. Propose-not-apply is therefore the provably correct policy, by
measurement rather than by assertion.

#### Post-hoc calibration and safe calibrated auto-apply

Calibration is treated as its own problem, separately from capability.
[`dataforge/calibration_map.py`](dataforge/calibration_map.py) fits a post-hoc,
monotone probability map per issue type (isotonic via pool-adjacent-violators, or
Platt), so a reported confidence reads as an honest probability. On the real
Azure `gpt-5-mini` samples the Expected Calibration Error drops from **0.8533 to
0.0** on a disjoint test split of **n=25**
([`eval/results/corrector_calibration.json`](eval/results/corrector_calibration.json)).
Read that number honestly: the corrector is only ~6% precise here, so isotonic
collapses its confidence toward 0 - which is *trivially* well-calibrated (a
proposer that is almost always wrong, now saying so). It proves the confidence is
honest, not that the corrector improved. The map is monotone, so it preserves
proposal ranking and therefore does **not** change conformal-certifiable
auto-apply coverage (it stays 0.0). Calibration fixes honesty, not accuracy.

The calibrated score is wired into the auto-apply gate end to end. Under an
authoritative schema, an LLM correction auto-applies only when it clears every
gate in order: differential SMT acceptance, a Population Stability Index drift
check against the calibration reference (a shifted live distribution downgrades
the policy to propose-not-apply, so the certificate is never claimed outside its
scope), and a certified per-issue-type threshold on the calibrated confidence.
Pass a certified artifact with `dataforge repair --allow-llm --schema ...
--corrector-calibration <artifact.json>`; without it every LLM correction stays
propose-not-apply. With only 50 labelled fd_violation outcomes at ~6% precision, the
conformal procedure **cannot certify any threshold** at 95%/delta=0.05, so every issue type
falls back to a disabled `1.01` sentinel - recorded with its reason in the
artifact's `uncertified_classes`, not left as a magic number. Certifying even a
perfect corrector would need >= 59 all-correct accepted samples. Nothing
auto-applies today; the wiring lets a genuinely precise, sufficiently-sampled
future model apply safely, and never manufactures coverage from a weak one.

### Bring your own model

The LLM paths (the corrector and the `--agent` policy) are provider- and
model-agnostic. Choose a provider with `DATAFORGE_LLM_PROVIDER` (`groq`,
`gemini`, `bedrock`, or `azure`) and its key, and choose the model with one
environment variable per provider -- the single source of truth used everywhere
(agent policy and repairers), not just the benchmark:

```bash
export DATAFORGE_LLM_PROVIDER=gemini
export GEMINI_API_KEY=...
export DATAFORGE_GEMINI_MODEL=gemini-3.1-flash-lite-preview   # or DATAFORGE_GROQ_MODEL / DATAFORGE_BEDROCK_MODEL / DATAFORGE_AZURE_MODEL
dataforge repair data.csv --agent --dry-run
```

For **Azure OpenAI** (first-party GPT-5 family, works on trial credit; Anthropic
Claude on Foundry needs pay-as-you-go and is rejected fast on credit-only
subscriptions), set `AZURE_API_KEY`, `AZURE_OPENAI_ENDPOINT`, and
`DATAFORGE_AZURE_MODEL` (your deployment name). See
[`docs/azure-teacher-setup.md`](docs/azure-teacher-setup.md) for the full runbook
(teacher-data generation, corrector benchmark, and the hard USD cost guard).

An explicit `--llm-model` (CLI) or `model=` (MCP `dataforge_agent_repair`)
overrides the env var; when neither is set, the provider's default model is
used. For a fully offline / self-hosted model, `--policy local` loads any
Hugging Face causal-LM via `DATAFORGE_AGENT_MODEL`, and `register_policy()`
lets you plug in a custom policy. Every path -- hosted, local, or custom --
still passes the same safety constitution and SMT verifier.

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
- `training/configs/sft_05b_v6.yaml` defines the completed private
  contract-first diagnostic over `expert_v6_contract_minimal.jsonl`. The staged Kaggle path
  defaults to a 20-step smoke, then a 60-step no-upload diagnostic, and only
  allows candidate promotion if `sft_v6_candidate_eval_report.json` has
  `promote_to_grpo: true` and the private checkpoint was uploaded.
- `training/configs/sft_05b_v7.yaml` defines the next private parse-latch
  candidate over `expert_v7_parse_latch.jsonl`. It exists because the
  contract-v3 SFT-v6 diagnostic removed `reason` leakage and schema-case
  errors, but still failed parse/action consistency and active repair.
- `scripts/remote/prepare_kaggle_sft_v7_candidate.py` packages the current
  SFT-v7 parse-latch Kaggle rung on top of the versioned SFT runner. Candidate
  mode requires a visible HF token before GPU work because GRPO-v3 may only
  consume an uploaded private predecessor.
- `training/configs/grpo_05b_v3.yaml` consumes only a promoted SFT-v7
  parse-latch predecessor and runs a
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
`promote_to_grpo` is `false`. The later SFT-v6 contract-first, SFT-v7
parse-latch, and SFT-v8 schema-distill attempts are preserved as failed
diagnostic evidence, not model wins. SFT-v6 removed `reason` leakage but failed
parse/action consistency; SFT-v7 still failed the action envelope; SFT-v8 fixed
the prompt-completion shape and label-mask audit but the 40-step smoke still
failed predecessor gates: strict macro F1 `0.0`, parse success `0.03`,
schema-case errors `26`, and `promote_to_grpo: false`. Do not run GRPO-v4 from
this lineage; the next model work must revise the action-envelope/product
constrained-decoding boundary while keeping raw research metrics separate from
product parse reliability.

That next step is staged as private SFT-v9 action-envelope work, not another
GRPO run. The local SFT-v9 curriculum/preflight at
`eval/results/sft_v9_action_envelope_curriculum_report.json` passes with 3,848
prompt-completion records, completion parse success `1.0`, held-out leakage
`0`, `finish_with_repairs` `0`, and zero negative-contrast target leakage.
This is only a launch preflight; GRPO-v4 remains blocked until a future
`sft_v9_candidate_eval_report.json` has `promote_to_grpo: true` and a private
checkpoint exists.
The private Kaggle smoke has been submitted as kernel version `1` and is
recorded at `eval/results/kaggle_sft_v9_smoke_launch_v1/launch_report.json`;
that run failed before training on an over-strict P100 capability guard and is
preserved at `eval/results/kaggle_sft_v9_smoke_v1_failure/failure_report.json`.
Smoke v2 with the fixed P100-compatible runner is now running and recorded at
`eval/results/kaggle_sft_v9_smoke_v2_relaunch/launch_report.json`. This is
infrastructure/running-job evidence, not completed model-quality evidence.

`docs/evidence/ledger.json` is the canonical claim index for release, product,
model, diagnostic, blocked, and roadmap evidence. Use
`python scripts/evidence/evidence_ledger.py` before changing public claim
wording.

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
`dataforge_07_mcp` distribution. Install the released package with:

```bash
python -m pip install dataforge_07_mcp
dataforge-mcp serve
```

For local development from this checkout:

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
Generated from `eval/results/agent_comparison.json` (schema `dataforge_benchmark_run_v2`, seeds `0, 1, 2`, git `236df758dbdd`, dirty `true`).

| Method | Precision | Recall | F1 | Avg Steps | Quota Units | GPU Hours |
| --- | --- | --- | --- | --- | --- | --- |
| heuristic | 0.3585 | 0.4430 | 0.3963 | 361.50 | 0.0000 | 0.0000 |
| random | 0.0057 | 0.0004 | 0.0008 | 125.50 | 0.0000 | 0.0000 |

See `BENCHMARK_REPORT.md` for per-dataset tables, error bars, and citation-only SOTA rows.

Dataset bytes are pinned to BigDaMa/raha revision `7be1334b8c7bbdac3f47ef514fb3e1e8c5fc181c` for hospital, flights; dirty/clean SHA-256s are recorded in the JSON metadata.
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
mypy, side-package lint/format/type checks, root tests, side-package tests,
README truth, benchmark truth, OpenAPI snapshot drift, secret scan, dependency
audit availability, SBOM generation availability, and package build
availability for the `dataforge_07*` distribution family. The gate covers the
core `dataforge_07` distribution and release surfaces; the historical
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

The `dataforge_07*` packages have PyPI/TestPyPI publication evidence under
`docs/evidence/pypi/`, including Trusted Publishing attestations and
fresh-install smoke logs. The real PyPI workflow refuses pre-release metadata
and should only run after trusted publishing, attestations, and fresh-install
evidence are verified for future releases. `dataforge release full-vision
--json` is expected to fail while design-partner evidence is not met and full
HF model-family evidence remains incomplete.

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

- [PRODUCT.md](PRODUCT.md) - the product constitution (purpose, philosophy, principles, vision, mission); the canonical source other docs defer to
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
