"""The HF Space front-matter gate must refuse a broken Space config.

This gate spent its whole life as an inline ``python -c`` heredoc in
``.github/workflows/ci.yml``, so it had never been run against a deliberately-broken
input. That is the condition this repo's own rule names: a gate nobody has seen fail on a
case it newly covers has not been shown to cover it. Each test below plants exactly one
defect that would leave the hosted Space dead or unreachable.

The committed-config test is the non-vacuity guard. Without it, every test here could pass
against a validator that returns errors unconditionally.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.ci.hf_space_frontmatter import (
    check,
    dockerfile_exposed_port,
    parse_front_matter,
)

_VALID = """---
title: DataForge Playground
sdk: docker
app_port: 7860
license: apache-2.0
---

# Body
"""

_DOCKERFILE = 'FROM python:3.12-slim\nEXPOSE 7860\nCMD ["uvicorn"]\n'


class TestTheCommittedConfigPasses:
    """Non-vacuity: the validator must accept the real Space config."""

    def test_committed_readme_has_no_errors(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        readme = repo / "playground" / "api" / "README.md"
        dockerfile = repo / "playground" / "api" / "Dockerfile"
        assert readme.exists(), "the Space README is required by this gate"
        errors = check(
            readme.read_text(encoding="utf-8"),
            dockerfile.read_text(encoding="utf-8") if dockerfile.exists() else None,
        )
        assert errors == [], f"committed Space config is invalid: {errors}"

    def test_synthetic_valid_config_has_no_errors(self) -> None:
        assert check(_VALID, _DOCKERFILE) == []


class TestItRefusesEachWayTheSpaceCanDie:
    """One planted defect per test."""

    def test_missing_required_key_is_named(self) -> None:
        broken = _VALID.replace("license: apache-2.0\n", "")
        errors = check(broken, _DOCKERFILE)
        assert any("license" in e for e in errors), errors

    def test_wrong_sdk_is_refused(self) -> None:
        broken = _VALID.replace("sdk: docker", "sdk: gradio")
        errors = check(broken, _DOCKERFILE)
        assert any("sdk must be 'docker'" in e for e in errors), errors

    def test_wrong_port_is_refused(self) -> None:
        broken = _VALID.replace("app_port: 7860", "app_port: 8080")
        errors = check(broken, _DOCKERFILE)
        assert any("8080" in e and "7860" in e for e in errors), errors

    def test_non_integer_port_is_refused(self) -> None:
        broken = _VALID.replace("app_port: 7860", 'app_port: "7860"')
        errors = check(broken, _DOCKERFILE)
        assert any("integer" in e for e in errors), errors

    def test_port_disagreeing_with_the_dockerfile_is_refused(self) -> None:
        """The check the heredoc could not make: it compared against a hardcoded 7860.

        Hardcoding meant the two files could drift together into agreement with the
        constant and away from each other -- or the Dockerfile could move alone and the
        gate would keep asserting the literal it was written with.
        """
        errors = check(_VALID, "FROM python:3.12-slim\nEXPOSE 9000\n")
        assert any("EXPOSE 9000" in e for e in errors), errors

    def test_dockerfile_without_expose_is_reported_not_ignored(self) -> None:
        errors = check(_VALID, "FROM python:3.12-slim\n")
        assert any("no EXPOSE" in e for e in errors), errors


class TestMalformedFrontMatterIsAVerdictNotACrash:
    """A bare ``split('---')[1]`` raised IndexError, which reads as a broken gate."""

    def test_no_front_matter_returns_an_error(self) -> None:
        errors = check("# Just a heading\n", _DOCKERFILE)
        assert errors and "front-matter" in errors[0], errors

    def test_unclosed_fence_returns_an_error(self) -> None:
        errors = check("---\ntitle: x\n", _DOCKERFILE)
        assert errors and "not closed" in errors[0], errors

    def test_non_mapping_front_matter_returns_an_error(self) -> None:
        errors = check("---\n- a\n- b\n---\n", _DOCKERFILE)
        assert errors and "not a mapping" in errors[0], errors

    def test_parse_front_matter_raises_valueerror_not_indexerror(self) -> None:
        with pytest.raises(ValueError):
            parse_front_matter("# no front matter\n")


class TestExposeParsing:
    def test_reads_the_declared_port(self) -> None:
        assert dockerfile_exposed_port(_DOCKERFILE) == 7860

    def test_returns_none_when_absent(self) -> None:
        assert dockerfile_exposed_port("FROM scratch\n") is None

    def test_ignores_a_commented_expose(self) -> None:
        assert dockerfile_exposed_port("FROM scratch\n# EXPOSE 1234\n") is None
