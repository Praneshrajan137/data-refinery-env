# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in DataForge, **please do not open
a public GitHub issue.** Instead, contact the maintainer privately (use the
email listed in `pyproject.toml`, or open a [draft security advisory](https://docs.github.com/en/code-security/security-advisories/working-with-repository-security-advisories/creating-a-repository-security-advisory)
on GitHub).

Include:

1. A description of the vulnerability and the attack scenario.
2. Steps to reproduce (minimal example preferred).
3. The version(s) affected (or commit hash).
4. Any suggested fix, if you have one.

We will acknowledge receipt within 48 hours and aim to release a patch
within 7 days for critical issues.

## Scope

The following are in scope:

- The `dataforge` CLI and library (`dataforge/` package).
- The playground API (`playground/api/`).
- The safety filter and SMT verifier (`dataforge/safety/`, `dataforge/verifier/`).
- Transaction log integrity (`dataforge/transactions/`).

The following are **out of scope** (but still appreciated):

- Third-party dependencies (report upstream; mention here if relevant).
- The static playground frontend (no secrets, no auth).

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.x.x   | ✅ Latest only |

## Disclosure policy

We follow coordinated disclosure. We will credit reporters in the
`CHANGELOG.md` entry for the fix unless anonymity is requested.
