# dataforge-agent-patterns

Reusable pure-Python primitives for agent projects:

```python
from dataforge_agent_patterns import ProgressiveToolDisclosure

disclosure = ProgressiveToolDisclosure({"search": 1, "shell": 3})
assert disclosure.visible_tools(task_difficulty=2) == ("search",)
```

The package is intentionally independent from `dataforge`. Runtime imports are
limited to the Python standard library, `pydantic`, and lazily imported `z3`
inside `SMTVerifiedAction`.
It is published as `dataforge_07_agent_patterns`; use the local verification
source install for development changes.

## Primitives

- `ProgressiveToolDisclosure` - reveal tools only when task difficulty warrants them.
- `ConstitutionalFilter` - wrap an agent with a safety-verdict layer.
- `ReversibleTransaction` - decorate side-effect calls with rollback support.
- `SMTVerifiedAction` - check structured actions with Z3 before execution.
- `CausalCascadeDetector` - flag cascading effects in action graphs.

## Local Verification

```bash
python -m pip install -e ".[dev]"
python -m ruff check src tests
python -m ruff format --check src tests
python -m mypy src
python -m pytest
python -m build
```

PyPI publishing should continue to use GitHub Actions Trusted Publishing for
`dataforge_07_agent_patterns`; do not add PyPI API tokens.
