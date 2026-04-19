# CLAUDE.md — DataForge living knowledge base

This file accumulates gotchas, decisions, and context that should survive
across Cursor sessions. Append to the bottom with the date.

## Project conventions

- Python 3.11 / 3.12. `pyproject.toml` pins `requires-python = ">=3.11,<3.13"`.
- Package layout: the top-level `dataforge/` package exports the public API.
  Submodules are internal; tests import from the top level where feasible.
- CLI uses Typer. Each subcommand is its own module in `dataforge/cli/`.
- Rich is used for ALL user-facing output. Never `print()` in library code.

## Known gotchas

- `pandas.read_csv` with `dtype=str` is the safest default for messy CSVs.
  Type inference via pandas is too eager and loses precision on monetary values.
- Z3 `Real` variables do NOT represent floating-point; they are mathematical
  reals. For IEEE-754 behavior, use `FP`.
- TRL v1+ manages `remove_unused_columns` internally in `GRPOConfig` — do NOT
  hand-set it as older tutorials suggest. Access non-prompt reward-function
  columns via the `**kwargs` pattern (every dataset column is passed as a kwarg).
- `causal-learn`'s PC algorithm does NOT accept NaN. Impute or drop rows first.
- OpenEnv's 0.1 spec uses `step() / reset() / state()` as the primary API
  (not `close()` — that appears in later RFCs and is not yet stable). Match the
  current `meta-pytorch/OpenEnv` repo, not blog-post descriptions.

## Performance notes

- Detector pass on a 10k-row CSV should finish in < 2 seconds. If yours is
  slower, profile with `py-spy record` first; don't guess.
- SMT verification scales poorly if you add EVERY FD as a universal quantifier
  over N² row pairs. Use `ForAll` with bound variables, not concrete loops.
- Rich tables render slowly for > 500 rows. Paginate or summarize.

## Append-only from here onward
