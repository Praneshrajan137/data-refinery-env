# Pre-registration: what does a DECLARED premise actually repair, through the shipped write path?

- **Registered** 2026-09-08, **before** the declared-premise and oracle-pipeline arms were run.
- **Premise under test:** `eval/premises/hospital_declared.yaml`,
  sha256 **`4b2780a74be800808aa013ff2b9ed18b49dfad25e6b6142f2323c4dc119a9228`**
  (over the file's text with line endings normalised to `\n`; see K3 and the note on CRLF).
- **Status at registration:** the `declared` column of the premise/stage matrix is **empty at
  every stage**, and the `oracle x pipeline` cell is empty too. Neither has ever been measured.
- **Amendments are appended, never edited.** Predictions below stand as written even when refuted.

## Why this exists

`eval/preregistration/capability_measurement_stage.md` measured hospital at three points and left
one question explicitly open: *"The obvious missing measurement is the fourth arm — the pipeline
under a **declared** premise — which is where the product's real claim lives and which no
instrument currently reports end-to-end. That needs its own pre-registration."* This is that
document.

The matrix, as of registration:

| premise | proposal stage | pipeline (write path) |
| --- | --- | --- |
| **oracle** — FDs mined from the **clean** frame, admitted only if they hold on ground truth | 393 repairs / 0 corruptions | **NEVER MEASURED** |
| **mined** — from the dirty frame | 451 / 116, F1 0.8352 | 0.0039 legacy, 0.0000 C4 |
| **declared** — authored by a user | **NEVER MEASURED** | **NEVER MEASURED** |

Two facts make the gap urgent rather than tidy-minded, and both are quoted from this
repository's own source.

1. `scripts/bench/measure_deductive_coverage.py::discover_oracle_fds` admits a dependency only
   if it holds exactly on the clean frame, and the module docstring says of that arm: *"No user
   has this. **It is the ceiling.**"* So the widely-quoted **393 repairs / 0 corruptions** is not
   a declared premise. It is an upper bound derived from ground truth.
2. The same docstring says of the mined arm: *"**This is not a default-reachable
   configuration.**"* It models a user who ran `profile --constraints-out`, then
   `constraints review --accept` over a printed queue-cost warning, then `repair --constraints`.

**So the product's central promise — declare a schema and your repairs are proven — has no
measurement at any stage, on any premise a user would actually author.**

### The finding that turned this from valuable into necessary

Verified 2026-09-08, before measuring. Two **shipped** sites credit the oracle ceiling to a
declared premise, and one of them is user-facing output:

- `dataforge/cli/repair.py` — the `--trust-mined-constraints` `--help` text: *"On the reference
  corpus this authorised 451 real repairs and 116 clean-cell corruptions, while **a declared
  premise repaired 393 and corrupted none. Prefer --schema.**"*
- `dataforge/engine/repair.py` — the C4 field docstring: *"the mined premise produced 451 repairs
  with 116 corruptions, while **the declared premise** produced 393 with none."*

Every trust document that *sources* 393 labels it `oracle`
(`docs/trust/deductive-coverage-result.md`, `docs/trust/bypass-allowlist-evidence.md`,
`docs/trust/shipped-premise-result.md`). `DECISIONS.md` states C4's own reversal criterion as
*"if the **declared** arm's numbers move (hospital **oracle** must stay at 393 repairs / 0
corruptions)"* — conflating the two inside a single sentence. That entry is historical and
append-only, and stands; the two code sites state a **current** claim and are corrected as part
of this work.

The consequence is not cosmetic. C4's shipped default, and the advice **"Prefer --schema"** given
to every user who reads that flag, are substantiated by a number produced by a premise **no user
can author**. This measurement supplies the number that claim actually needs. It may support the
advice, or it may not; either outcome is reportable under K4.

## The declared premise, and why it is not the oracle arm relabelled

The premise is `eval/premises/hospital_declared.yaml`: 20 column types (all `str`) and **15
single-column functional dependencies**, loaded by the shipped `dataforge.cli.common.load_schema`
— the same function behind `dataforge repair --schema` — so the arm enters through the door a
real user uses.

The dependencies rest on two warrants, both available on the day the file is written:

- **`cms_documented` (13).** Public semantics of the CMS Hospital Compare release this corpus is
  drawn from. The CCN (`ProviderNumber`) identifies one certified facility, so it determines
  `HospitalName`, `Address1`, `City`, `State`, `ZipCode`, `CountyName`, `PhoneNumber`,
  `HospitalType`, `HospitalOwner`, `EmergencyService`. CMS measure identifiers are a controlled
  vocabulary, so `MeasureCode` determines `MeasureName` and `Condition`. US five-digit ZIPs fall
  inside one state, giving `ZipCode -> State`.
- **`format_evident` (2).** Visible in the dirty frame's own values: `Stateavg` holds
  `al_scip-card-2`, i.e. state code and measure code joined by an underscore. Hence
  `Stateavg -> State` and `Stateavg -> MeasureCode`. A **weaker** warrant, labelled as such so
  the two classes can be reported apart.

**Three mechanical properties make this not an oracle**, and they are the load-bearing content of
this section:

1. **Ground truth is never an admission filter.** The oracle arm admits an FD *only if*
   `fd_holds_on_clean(...)`; ground truth **feeds** it. Here every declared FD stays in the file
   regardless of whether it holds on the clean frame. Ground truth only **grades**. This is the
   repository's own stated principle, at `scripts/bench/validate_measure_on_my_table.py`: *"Now,
   and only now, ground truth enters — to grade the instrument, never to feed it."*
2. **It is frozen by hash before ground truth is touched.** The sha256 above was computed and
   recorded here before any arm ran. K3 voids any run in which the file disagrees. The list
   therefore cannot be pruned after seeing which members embarrass it.
3. **It is falsifiable, and P4 predicts it is wrong.** An oracle premise *cannot* corrupt a clean
   cell — its dependencies hold on ground truth by construction. A declared premise can. The
   harness reports `declared_fds_refuted_by_ground_truth` as an **outcome**; a non-zero value is a
   finding about what declaring costs, not a defect to be fixed by editing the premise.

The premise file additionally records **every dependency considered and rejected, with its
ground** — `index -> *` (a unique-key determinant is vacuous: every group is a singleton, so no
vote is reachable), FDs into the constant columns `Address2`/`Address3`, FDs into the row-level
observations `Score`/`Sample`, the publicly **false** `City -> State`, the publicly
**approximate** `ZipCode -> City` and `ZipCode -> CountyName`, and the reverse-direction
`PhoneNumber -> ProviderNumber` / `HospitalName -> ProviderNumber`. A premise is only as
checkable as the claims it declined to make, and `City -> State` is the sharpest of these: it
would very likely *hold* on this corpus, which spans four states, and it is rejected anyway
because the warrant is public semantics rather than what this table happens to contain.

**The column types grant no authority, and this is verified rather than asserted.**
`dataforge.engine.repair.authoritative_columns` counts a column as covered only when its declared
type is *discriminating*, and `type_discriminates("str")` is `False` — a property established
after a measured defect in which declaring every column `str` let 10 of 14 constraint-violating
attacks be written and stamped `proven`. Measured on this premise: 20 columns declared, **15**
authoritative — exactly the union of the FDs' determinants and dependents, with `index`,
`Address2`, `Address3`, `Score` and `Sample` uncovered. The entire write authority of this
premise comes from its dependencies.

## Hypothesis

**H3.** On hospital, through the shipped write path, a premise a user could plausibly declare
produces materially more correct repairs than the mined premise C4 declines to write from, and
materially fewer than the proposal-stage figure of 0.8352 — and the oracle premise through the
same path bounds how much of the 214.2x stage gap is attributable to the premise rather than to
the repairer's abstain rule.

## Predictions

Fixed before the declared and oracle pipeline arms ran. All arms use hospital, the same
`dataset.ground_truth`, and `dataforge.bench.core.score_repairs` — imported, never reimplemented
— so no arm can differ from another by its scoring.

- **P1.** `pipeline_declared` **writes > 0.** Warrant:
  `tests/unit/test_declared_fd_autoapply.py::test_strict_mode_still_applies_a_declared_fd` proves
  a hand-declared FD auto-applies through `run_repair_pipeline`, and hospital's
  `ProviderNumber` groups have a median of 19 rows, so votes are reachable. If this is refuted,
  the product's central claim is unreachable on its own flagship corpus.
- **P2.** `pipeline_declared` F1 > **0.0039** — strictly better than *both* mined pipeline
  configurations (legacy 0.0039, C4 0.0000). This is C4's implicit promise: authority moves from
  the miner's guess to the user's stated premise.
- **P3.** `pipeline_declared` F1 < **0.8352**. The write path still gates; a declared premise does
  not recover the proposal-stage figure.
- **P4.** `pipeline_declared` **corrupts at least one clean cell**
  (`corrupted_a_clean_cell` > 0). Made deliberately and uncomfortably: hospital's own `ZipCode`,
  `City` and `CountyName` values are corrupted, so a plurality vote inside a determinant group can
  carry a wrong value into a clean cell. **Predicting zero here would be predicting the oracle
  result, which is the exact conflation this document exists to correct.** If P4 is refuted, that
  is a strong positive result for C4 and will be reported as one.
- **P5.** `pipeline_oracle` F1 > `pipeline_declared` F1. The ceiling sits above the achievable.
- **P6.** `pipeline_oracle` F1 < **0.8352**. This is the decisive prediction. If even a
  ground-truth-admitted premise cannot reach the proposal-stage figure through the write path,
  then the 214.2x gap is **not** a premise-quality problem and cannot be closed by better schema
  authoring — it is the repairer's abstain rule and the auto-apply gates. That would mean
  *"Prefer --schema"* is unsupported at **any** premise quality, which is a materially stronger
  claim than the mislabel finding alone.

## Kill criteria

- **K1 — instrument falsification. This outranks every finding below it.** Two referents, each
  independently gated, each read **from its artifact and never from a constant in the harness**:
  1. the `proposal_stage_anchor` arm must reproduce the heuristic/hospital F1 in
     `eval/results/agent_comparison.json` (**0.8352** at registration) to within 0.0001; and
  2. the `pipeline_mined_c4` arm must reproduce `writes`, `tp` and `fp` from
     `eval/results/capability_measurement_stage.json`
     `/arms/pipeline_c4_declared_authority` (**0 / 0 / 0** at registration) exactly.

  If **either** disagrees, **nothing from the run may be reported** and the harness is fixed
  first. Rationale, learned the hard way: AMENDMENT 1 of the stage pre-registration records K2
  firing and blocking a whole result because the harness disagreed with the headline it was
  criticising. Binding to two referents rather than one is strictly stronger, and binding both to
  gated artifacts rather than to documented constants is the correction for the constant that
  rotted for 54 days.
- **K2 — vacuity.** A zero from an unbound premise is an instrument defect, not a refusal. Before
  any arm is scored the harness must assert, against the **dirty** frame: every declared column
  exists in the frame; every declared FD dependent is non-constant; and FD detection raises at
  least one issue on a declared dependent column. If any fails, the arm is reported as
  **`vacuous`** and never as `0.0`. A premise that never bound anything and a premise that bound
  and then refused are different results and must not share a numeral.
- **K3 — anti-oracle freezing.** The sha256 of `eval/premises/hospital_declared.yaml`, over its
  text with line endings normalised to `\n`, must equal the value recorded at the top of this
  document. If it differs, **the run is void.** This is the mechanical guarantee that the declared
  set was authored before ground truth was consulted and not pruned afterwards.
  *Why normalised text rather than file bytes:* `core.autocrlf=true` on a Windows checkout leaves
  CRLF in the worktree while storing LF, so a byte hash would differ between this machine and
  Linux CI and would void every run for a reason unrelated to the premise. The directory also
  carries a `.gitattributes` pinning `eol=lf`; the normalisation is belt to that braces.
- **K4 — anti-motivated-stopping.** The result is published **whatever it shows**, including if
  the declared arm writes zero cells, corrupts clean cells, or scores *below* the mined arm. A
  declared premise that corrupts would undercut the empirical case for C4's default and for
  *"Prefer --schema"*; discovering that is the purpose of running this, not a reason to stop
  running it or to search for a framing that rescues the flag.
- **K5 — no new tunables.** Zero new thresholds, confidence floors or constants that change
  behaviour. This is a measurement, not a mechanism. The premise file is data, not a tunable: it
  is frozen by K3.
- **K6 — scope, stated so it cannot expand quietly.** hospital only: it is the only corpus with a
  published proposal-stage figure to bind K1 against, and the only one with both a dirty/clean
  pair and minable dependencies. `tax` is **out of scope** for the reason in
  `docs/trust/sampling-bias-measured.md` — a head slice is not a sample, and this needs a
  scale-aware sampled bench before it can be measured at all. `beers` remains excluded by the
  dataset-scope rule. No claim is made here about any corpus that was not run.

## Reporting rules fixed in advance

- **A refusal is not a wrong answer.** `score_repairs` returns `precision = 0.0` when
  `tp + fp == 0`, because `0/0` has to be *something*. The artifact therefore reports **both**
  that shared-scorer precision, for cross-arm comparability with the stage result, **and** a
  separate `write_precision` that is **`null` when an arm writes nothing**. Reporting `0/0` as
  `0.0` describes a refusal as an error, and the two must be distinguishable in the artifact
  without reading the harness.
- **Per-detector attribution.** The declared arm's writes are broken down by detector, so a win
  driven by some other detector operating inside the FDs' covered columns cannot be silently
  credited to the declared dependencies.
- **Arm naming.** The stage artifact's arm is called `pipeline_c4_declared_authority`, meaning
  *authority requires declaration*, and it runs a **mined** premise. That name is one word away
  from the conflation under audit, so the arms here are named `pipeline_declared_premise` and
  `pipeline_oracle_premise` — after the premise, never after the authority rule.

## The anchor decision, pre-committed so it cannot become a rationalisation

`docs/trust/capability-measurement-stage.md` leaves open which number should anchor the product's
capability claim, and deliberately did not pick one. That decision is settled by this
measurement, and the **rule is fixed here, before the result is known**, because a rule chosen
after seeing the numbers is a preference wearing a rule's clothes.

- **If `pipeline_declared` F1 is materially non-zero** (>= 0.05, i.e. not a rounding artefact of a
  handful of writes), **it takes the anchor.** It is then the only measured number that is both
  reachable through the shipped write path *and* premised on something a user authors. **0.8352**
  is retained everywhere it appears, explicitly relabelled as a **proposal-stage** measurement of
  the detector-and-repairer stack, with the qualifier `readme_truth` already requires.
- **If `pipeline_declared` F1 is ~0** (< 0.05), the honest anchor is that **DataForge has no
  demonstrated end-to-end correction capability on hospital**, and the product's claim is restated
  as detection plus advisory triage plus reversibility — which is what `DECISIONS.md` already
  records as "the honest product" when human-labelled certification died. **0.8352** is then
  retained *only* as a proposal-stage tripwire and removed from every context that implies
  shipped capability.

In both branches the number that moves is a **label and a scope**, not a measurement. No
committed figure is edited to match a refactor: if any pinned number moves, that is a finding to
write up under `anchor_truth`, not a number to update.

## AMENDMENT 1 (2026-09-08): three of six predictions refuted, and the result stands

**Recorded after the run. Nothing above is edited.** Artifact:
`eval/results/declared_premise_capability.json`.

### All four kill criteria passed, so the result is reportable

| criterion | outcome |
| --- | --- |
| **K1a** anchor reproduced | **PASS.** Proposal stage scored 0.8352 against the artifact's 0.8352, delta 0.000000. |
| **K1b** committed mined-C4 reproduced | **PASS.** 0 writes / 0 tp / 0 fp, exactly the committed stage arm. |
| **K2** premise binds | **PASS.** 8,223 FD issues raised on declared dependents; no absent column, no constant dependent. |
| **K3** premise frozen | **PASS.** `4b2780a7...` matches this document. |

Two independently gated referents both reproduced, so the zero below is a measurement of the
product and not of the instrument.

### The measured result

| arm | premise | writes | tp | corrupted | **F1** | write precision |
| --- | --- | --- | --- | --- | --- | --- |
| proposal stage | mined | 571 | 451 | 120 | **0.8352** | 0.7898 |
| pipeline, mined, C4 | mined | 0 | 0 | 0 | **0.0000** | `null` |
| **pipeline, declared** | **declared** | **0** | **0** | **0** | **0.0000** | **`null`** |
| **pipeline, oracle** | **oracle** | **54** | **54** | **0** | **0.1918** | **1.0000** |

### Predictions, scored honestly

- **P1 REFUTED.** The declared premise writes **0** cells. It was predicted to write more than
  zero, on the warrant of a passing unit test and reachable vote sizes. Both of those facts were
  true and the prediction was still wrong.
- **P2 REFUTED.** 0.0000 does not exceed the mined pipeline's 0.0039. **The declared premise is
  measurably *worse* through the write path than the mined premise C4 declines to trust** — the
  mined arm at least wrote one correct cell under legacy authority. C4's implicit promise, that
  authority moves from the miner's guess to the user's stated premise, does not hold on this
  corpus: it moves to nowhere.
- **P3 HELD.** 0.0000 < 0.8352, trivially.
- **P4 REFUTED, on two independent grounds.** No clean cell was corrupted, because nothing was
  written. And separately, the premise was not fallible at all: **all 15 declared dependencies
  hold exactly on the clean frame** (`fd_set_precision` 1.0000, `refuted_by_warrant` zero in both
  warrant classes). The reasoning offered for P4 — that a plurality vote inside a corrupted
  determinant group would carry a wrong value — was never reached, because no vote produced a
  write.
- **P5 HELD**, but not informatively: 0.1918 > 0.0000 only because the declared arm is zero.
- **P6 HELD, and it is the finding.** The oracle premise — every dependency admitted *by ground
  truth* — reaches **F1 0.1918** through the write path against the proposal stage's 0.8352, a
  ratio of **4.4x**, and writes 54 cells against the 393 the same premise proposes at proposal
  stage. **So the stage gap is not a premise-quality problem and cannot be closed by better
  schema authoring.** Even a premise no user could obtain loses 339 of 393 repairs to the gates.

### What this does to P4's role in the anti-oracle argument

Section *"why it is not the oracle arm relabelled"* offered three properties, the third being
*"it is falsifiable, and P4 predicts it is wrong."* P4 was wrong: the premise turned out to be
exactly right. That outcome must not be allowed to retroactively convert this into an oracle arm,
and the reason is procedural rather than rhetorical:

- Ground truth was **not** an admission filter. It graded a file whose hash predates the grading,
  and it graded 15/15. For the oracle arm `fd_set_precision` is 1.0 **by construction** and, as
  the artifact records, *means nothing*; here 1.0 is a measurement, and what it measures is that
  the CMS data dictionary was read correctly.
- The distinction is nevertheless **weaker empirically than this document assumed**, because on
  this corpus a declared and an oracle premise are not distinguishable by dependency correctness.
  Stating that plainly is the honest position.
- The **reverse mechanism probe below settles it independently**, and more decisively than P4
  could have: at equal size, a declared premise and a ground-truth-admitted premise behave
  identically. The declared arm's zero is not a penalty for being hand-authored.

### Mechanism probes, added POST HOC and labelled as such

Not pre-registered, not hypothesis tests, added because a bare zero is not actionable. Recorded
here rather than folded silently into the harness, because a diagnostic promoted to a finding
without registration is how a mechanism gets asserted instead of measured.

The puzzle they address: the declared premise produces **399** candidate repairs and writes 0,
while the oracle produces **397** — *fewer* — and writes 54. The declared premise's 13 dependent
columns already include all 10 the oracle wrote to. So neither "the user declared too little" nor
"the premise misses the right columns" survives contact with the counts.

| probe | FDs | max determinants per dependent | all hold on clean | writes |
| --- | --- | --- | --- | --- |
| **REVERSE** — oracle thinned to one determinant per dependent | 13 | 1 | yes | **0** |
| **FORWARD** — declared plus the oracle's other two `Condition` determinants | 17 | 3 | yes | **0** |

- The **reverse** probe is load-bearing. Thirteen dependencies, **every one admitted by ground
  truth**, covering the same 13 columns, with only the redundancy removed — and it writes
  **zero**, exactly like the declared arm. **The declared arm's zero is therefore not caused by
  the premise being hand-authored, imperfect, or small.** A user cannot fix it by declaring
  better, and this is the evidence for that claim rather than an inference from the declared arm
  alone.
- The **forward** probe shows per-column redundancy is **not sufficient**: the same three
  `Condition` determinants that yield 11 writes inside the 53-FD oracle premise yield **0**
  inside a 17-FD premise. So whatever gates the write is a property of the premise as a whole, not
  of the column being repaired.

**Redundancy in the FD set is therefore necessary and not sufficient, and the mechanism is not
explained here.** Naming it would require a further pre-registration, and this document does not
claim one. What is established is the negative, which is what the anchor decision needs: the
write path's near-total suppression of repairs is **not** attributable to premise quality.

### The anchor decision rule fires its second branch

Declared F1 is **0.0000**, below the 0.05 materiality floor fixed above before the result was
known. Therefore, by the pre-committed rule and not by preference: **there is no demonstrated
end-to-end correction capability on hospital.** 0.8352 is retained only as a proposal-stage
tripwire, and every surface implying it is shipped capability must say which stage produced it.

The rule was written to be inconvenient, and it was.

