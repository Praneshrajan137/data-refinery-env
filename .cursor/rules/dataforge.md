# DataForge — Always-Applied Rules

You are contributing to DataForge, a production-grade open-source AI product.
Every line you write may be read by a hiring manager evaluating the author.
The bar is: "would a staff engineer at Anthropic/HuggingFace/Databricks be
willing to review this PR without wincing?" If not, raise the bar.

## Non-negotiables

1. NEVER modify a public API without updating its spec in `specs/`.
2. NEVER delete or weaken an existing test. If a test is wrong, open an
   issue, update the spec, update the test — in that order.
3. NEVER write implementation before the failing test exists. Red → Green → Refactor.
4. EVERY public function, class, and module has:
   - A type hint on every parameter and return value.
   - A Google-style docstring with one-line summary, Args, Returns, Raises.
   - An example in the docstring for any non-obvious usage.
5. EVERY change runs through: `make lint && make type && make test-mapped`.
6. COMMIT messages follow Conventional Commits. Short subject (≤ 72 chars),
   optional body explaining why (not what — the diff shows what).
7. NO silent catch-all exception handlers. Catch specific exceptions and either
   handle them or re-raise with added context.
8. NO `print()` in library code. Use the `logging` module. CLI output uses `rich`.
9. NO global mutable state. Dependencies are injected; modules export pure or
   class-scoped behavior.
10. NO TODO or FIXME in merged code. If a follow-up is needed, open an issue
    and reference it with `# See #123`.

## Safety invariants (absolute)

- Every agent-proposed `fix` action MUST pass through: SafetyFilter →
  SMTVerifier → TransactionLog. Never short-circuit this pipeline.
- The SafetyFilter MUST refuse to modify columns declared as PII in the
  constitution unless an explicit `--allow-pii` flag is set AND the user
  has confirmed interactively (or via a documented non-interactive flag).
- Every applied fix MUST produce a `RepairTransaction` object serialized
  to the transaction log BEFORE the underlying data is modified on disk.
- `dataforge revert <txn_id>` MUST restore the byte-for-byte pre-state
  (verified by hash comparison in tests).

## TDD discipline

1. Identify the spec file for the change (`specs/SPEC_<module>.md`).
2. Read the Appendix A toy cases. Those are your initial test cases.
3. Write the failing test in `tests/unit/test_<module>.py`.
4. Run `make test-mapped FILE=<source_file>`. Confirm it FAILS for the
   right reason (not an import error).
5. Implement the minimum code to make the test pass.
6. Run `make test-mapped` again. Confirm PASS.
7. Run `make test` (full suite). Confirm no regressions.
8. Refactor if the implementation has obvious duplication or clarity issues.
9. Commit with a Conventional Commit message.

## When uncertain

- If a spec is ambiguous, write the question into `specs/QUESTIONS.md`
  rather than guessing. Then proceed with the most conservative
  interpretation and note the assumption in the PR description.
- If a test is flaky, do NOT add a retry. Diagnose, then either fix the
  code or strengthen the test. Retries hide bugs.
- If a dependency seems useful, justify it in one sentence in
  `ARCHITECTURE.md § Dependencies` before adding it. If you cannot
  justify it in one sentence, you don't need it.

## Style

- Python 3.11 / 3.12 (`requires-python = ">=3.11,<3.13"`). Use modern syntax (`dict | None` not `Optional[Dict]`).
- `ruff` for linting AND formatting (`ruff format`). `mypy --strict` for types.
- Prefer dataclasses or Pydantic BaseModel over dicts for structured data.
- Prefer composition over inheritance. Small classes, single responsibility.
- Error messages speak to the user, not the developer. "The column
  'discount_pct' has 12 values that appear 10× too large" — not
  "Detected N outliers with z-score > 3".

## Output format expectations

- CLI output: `rich` library. Tables, panels, colored status. Never raw prints.
- Errors: `rich.console.Console.print_exception` for tracebacks. `rich.panel` for
  user-facing error messages with a "what to try" section.
- Long-running operations: `rich.progress` with meaningful stages.

## When writing docs

- Start concrete. A code block or a screenshot in the first 3 paragraphs
  of any README or blog post.
- No "Motivation" section that rehearses how important data is. Start with
  what DataForge does, then show it doing it.
- Benchmark numbers are ALWAYS citable to a committed script. Include the
  seed, the dataset, and the command to reproduce.

## When refusing to do something

If the user's request would violate one of these rules, explain which rule
and propose a compliant alternative. Don't silently comply with bad requests.
