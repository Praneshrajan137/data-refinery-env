# PyPI Name Transfer Runbook

The full original DataForge vision requires `pip install dataforge`. The
`dataforge` name is currently controlled by unrelated PyPI/TestPyPI projects, so
completion requires owner cooperation or a successful PyPI name-transfer request.

## Required Evidence Before Filing

- Public repository URL: `https://github.com/Aegis15/dataforge`.
- Package metadata showing the final project name is `dataforge`.
- TestPyPI/PyPI Trusted Publishing workflow configuration.
- Good-faith contact attempts to the existing project owner where policy
  requires them.
- Explanation that `dataforge15` was a temporary staging name and is not the
  final public artifact.

## Do Not Claim Completion Until

- PyPI and TestPyPI both resolve `dataforge` to this project.
- `python -m pip install dataforge` installs this package in a clean Python 3.12
  environment.
- `dataforge --version`, `profile`, `repair --dry-run`, `repair --apply`,
  `audit`, `revert`, `watch`, and `bench` pass from the installed artifact.
- `docs/evidence/pypi/publish_report.json` records Trusted Publishing,
  attestations, fresh-install proof, and package URLs.
