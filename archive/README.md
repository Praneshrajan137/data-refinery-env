# archive/ — frozen, not maintained, not shipped

Nothing in this directory is part of the DataForge product. It is kept because deleting it
would destroy a factual record of work that was done, and PRODUCT.md section 5 forbids
rewriting frozen historical evidence to look better. Keeping it is not an endorsement of it.

**Do not extend anything here. Do not cite anything here as a capability.**

## archive/training/ — the supervised and RL training subsystem

Moved out of the repository root on 2026-09-07.

### Why it was excised

It never passed its own gate, and it never contributed a single write to the product.

| Measure | Value |
| --- | --- |
| Best `sft_f1` ever recorded | **0.0202** |
| v7 candidate `sft_f1` | **0.0** |
| v7 `parse_success` on 576 opportunities | **0.0** — it proposed nothing at all |
| Cells written by the learned corrector on the user-reachable path | **0** |

PRODUCT.md section 1.3 states the rule: *before hardening a component, name its consumer.*
An audit found that rule had been satisfied by **disclosure rather than reallocation** — the
subsystem was documented as unconsumed, in detail, and then kept and maintained anyway. This
directory is that finding acted on instead of restated.

### What was kept, and why the tests still run

The move deleted **no tests and no coverage**. Every test that exercised this code still runs,
now importing `archive.training.*`. That was deliberate: three of those test files are
*parity* tests over product code, and deleting them would have removed product coverage
under cover of a cleanup —

| Test | Product module it pins |
| --- | --- |
| `tests/unit/test_grpo_contract_parity.py` | `dataforge/repair_contract.py` |
| `tests/unit/test_grpo_calibration_reward.py` | `dataforge/repair_contract.py` |
| `tests/unit/test_model_family_manifest.py` | `dataforge/release/model_family.py` |

So the same rule that condemned the subsystem also identified the two modules inside it that
do have a consumer (`grpo_contract.py`, `gigpo_advantage.py`). They are archived alongside the
rest but are still imported, and their contracts are still enforced.

### Integrity of the move

Verified with git's own rename detection rather than by assertion:

- **41 files are byte-identical** (`R100`) — every notebook, config, curriculum, and the
  `expert_v3` trajectories.
- **4 files changed**: `grpo_eval.py`, `grpo_readiness.py`, `rewards/__init__.py`,
  `rewards/dataforge_reward.py`. The only edits are intra-package `import` statements, which
  had to move with the package for the parity tests above to keep running.
- The 14 `kaggle_*_kernel/` files were **never git-tracked** and remain untracked.

Two frozen artifacts were briefly rewritten by an over-broad path replacement during the move
(`docs/evidence/ledger.json` and two `eval/results/**/launch_report.json`). They were restored
and confirmed byte-identical to `HEAD` before anything was committed. That mistake is recorded
here rather than quietly repaired, because a directory whose purpose is preserving evidence is
the worst possible place to be casual about it.

### What is NOT here

`eval/results/` (313.7 MB of run snapshots) stays where it is. It is the frozen evidence
itself, it is referenced by the claim ledger and by `docs/evidence/ledger.json`, and moving it
would break live gates for no benefit. Archiving code is not the same act as archiving
evidence.

### Excluded from every gate, on purpose

`archive/` is outside `ruff`, `mypy --strict`, and the packaged distribution
(`REJECTED_SDIST_PREFIXES` in `dataforge/release/gate.py` refuses it in an sdist;
`pyproject.toml` packages only `dataforge*`). The pinning debt in
`archive/training/configs/*.yaml` is still counted by
`tests/unit/test_fetch_pinning.py`, which is the one gate that deliberately follows it here —
an accepted exception must stay counted or it becomes a permanent one.
