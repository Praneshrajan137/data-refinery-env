"""Tests for the canonical release gate contracts."""

from __future__ import annotations

import io
import subprocess
import sys
import tarfile
import zipfile
from hashlib import sha256
from pathlib import Path

import pytest

from dataforge.release import gate as release_gate
from dataforge.release.gate import (
    REQUIRED_SDIST_MEMBERS,
    REQUIRED_WHEEL_MEMBERS,
    SCHEMA_VERSION,
    ReleaseGateReport,
    _audit_sdist_contents,
    _audit_wheel_contents,
)


def _write_wheel(path: Path, members: set[str]) -> None:
    """Write a minimal wheel-like zip for content-audit tests."""
    with zipfile.ZipFile(path, "w") as archive:
        for member in sorted(members):
            archive.writestr(member, "")


def _write_sdist(path: Path, members: set[str]) -> None:
    """Write a minimal sdist-like tarball for content-audit tests."""
    with tarfile.open(path, "w:gz") as archive:
        for member in sorted(members):
            payload = b""
            info = tarfile.TarInfo(f"dataforge_07-0.1.0/{member}")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


def test_release_gate_report_schema_is_versioned() -> None:
    report = ReleaseGateReport(ok=True, steps=[])

    payload = report.to_dict()

    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["offline_install"] is True
    assert payload["secrets_printed"] is False


def test_wheel_contents_audit_accepts_allowed_surface(tmp_path: Path) -> None:
    wheel_path = tmp_path / "dataforge_07-0.1.0-py3-none-any.whl"
    members = set(REQUIRED_WHEEL_MEMBERS) | {
        "dataforge/__init__.py",
        "dataforge_07-0.1.0.dist-info/METADATA",
        "dataforge_07-0.1.0.dist-info/WHEEL",
        "dataforge_07-0.1.0.dist-info/RECORD",
    }
    _write_wheel(wheel_path, members)

    step = _audit_wheel_contents(wheel_path)

    assert step.ok is True


def test_wheel_contents_audit_rejects_non_package_files(tmp_path: Path) -> None:
    wheel_path = tmp_path / "dataforge_07-0.1.0-py3-none-any.whl"
    members = set(REQUIRED_WHEEL_MEMBERS) | {
        "dataforge/__init__.py",
        "dataforge_07-0.1.0.dist-info/METADATA",
        "tests/test_leaked.py",
        "data_quality_env/legacy.py",
        "root_script.py",
    }
    _write_wheel(wheel_path, members)

    step = _audit_wheel_contents(wheel_path)

    assert step.ok is False
    assert step.metadata["errors"]


def test_sdist_contents_audit_accepts_allowed_surface(tmp_path: Path) -> None:
    sdist_path = tmp_path / "dataforge_07-0.1.0.tar.gz"
    members = set(REQUIRED_SDIST_MEMBERS) | {
        "dataforge_07.egg-info/PKG-INFO",
        "dataforge_07.egg-info/SOURCES.txt",
        "dataforge_07.egg-info/requires.txt",
        "dataforge_07.egg-info/entry_points.txt",
        "dataforge_07.egg-info/top_level.txt",
        "dataforge_07.egg-info/dependency_links.txt",
    }
    _write_sdist(sdist_path, members)

    step = _audit_sdist_contents(sdist_path)

    assert step.ok is True


def test_sdist_contents_audit_rejects_legacy_and_generated_files(tmp_path: Path) -> None:
    sdist_path = tmp_path / "dataforge_07-0.1.0.tar.gz"
    members = {
        "PKG-INFO",
        "README.md",
        "LICENSE",
        "MANIFEST.in",
        "pyproject.toml",
        "dataforge/__init__.py",
        "dataforge/py.typed",
        "dataforge/cli/constraints.py",
        "dataforge/cli/profile.py",
        "dataforge/cli/repair.py",
        "dataforge/fixtures/hospital_10rows.csv",
        "dataforge/fixtures/hospital_schema.yaml",
        "data_quality_env/legacy.py",
        "tests/test_leaked.py",
        "benchmark.py",
        "archive/training/kaggle/sft_warmup.ipynb",
        "dataforge/__pycache__/leaked.pyc",
    }
    _write_sdist(sdist_path, members)

    step = _audit_sdist_contents(sdist_path)

    assert step.ok is False
    assert step.metadata["errors"]


def test_release_gate_command_and_environment_helpers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PYTHONPATH", "must-not-leak")
    env = release_gate._gate_env()

    assert "PYTHONPATH" not in env
    assert env["PIP_DISABLE_PIP_VERSION_CHECK"] == "1"
    assert release_gate._project_root().joinpath("pyproject.toml").is_file()
    assert release_gate._command_text(["python", Path("script.py")]) == "python script.py"
    assert release_gate._tail("abc", limit=3) == "abc"
    assert release_gate._tail("abcdef", limit=3) == "def"
    assert release_gate._output_text(None) == ""
    assert release_gate._output_text(b"caf\xc3\xa9") == "caf\N{LATIN SMALL LETTER E WITH ACUTE}"
    assert release_gate._output_text("text") == "text"

    venv_python = release_gate._python_in_venv(tmp_path)
    venv_script = release_gate._script_in_venv(tmp_path, "dataforge")
    assert venv_python.parent == venv_script.parent
    assert venv_python.name.startswith("python")
    assert venv_script.name.startswith("dataforge")


def test_release_gate_command_runner_reports_success_failure_and_missing_command(
    tmp_path: Path,
) -> None:
    success, result = release_gate._run_command(
        "success",
        [sys.executable, "-c", "print('ready')"],
        cwd=tmp_path,
    )
    failure, _ = release_gate._run_command(
        "failure",
        [sys.executable, "-c", "import sys; print('bad', file=sys.stderr); sys.exit(3)"],
        cwd=tmp_path,
    )
    missing, missing_result = release_gate._run_command(
        "missing",
        [tmp_path / "does-not-exist"],
        cwd=tmp_path,
    )

    assert success.ok is True
    assert result.stdout.strip() == "ready"
    assert success.metadata["returncode"] == 0
    assert failure.ok is False
    assert failure.metadata["returncode"] == 3
    assert failure.metadata["stderr_tail"].strip() == "bad"
    assert missing.ok is False
    assert missing_result.returncode == 127


def test_release_gate_command_runner_reports_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def _timeout(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(args[0], 1, output=b"partial", stderr=b"late")

    monkeypatch.setattr(release_gate.subprocess, "run", _timeout)

    step, result = release_gate._run_command("timeout", ["slow"], cwd=tmp_path)

    assert step.ok is False
    assert step.detail == "Timed out after 120s."
    assert step.metadata["stdout_tail"] == "partial"
    assert step.metadata["stderr_tail"] == "late"
    assert result.returncode == 124


def test_release_gate_artifact_and_json_helpers(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.whl"
    artifact.write_bytes(b"dataforge")

    assert release_gate._find_single_artifact(tmp_path, "*.whl") == artifact
    assert release_gate._file_sha256(artifact) == sha256(b"dataforge").hexdigest()
    with pytest.raises(release_gate.ReleaseGateError, match="No artifact"):
        release_gate._find_single_artifact(tmp_path, "*.tar.gz")
    (tmp_path / "second.whl").write_bytes(b"second")
    with pytest.raises(release_gate.ReleaseGateError, match="Expected one artifact"):
        release_gate._find_single_artifact(tmp_path, "*.whl")

    valid_result = subprocess.CompletedProcess(["command"], 0, stdout='{"ok":true}', stderr="")
    matched = release_gate._json_step("json", valid_result, expected={"ok": True})
    mismatched = release_gate._json_step("json", valid_result, expected={"ok": False})
    invalid = release_gate._json_step(
        "json",
        subprocess.CompletedProcess(["command"], 0, stdout="not-json", stderr="detail"),
        expected={"ok": True},
    )

    assert matched.ok is True
    assert mismatched.ok is False
    assert invalid.ok is False
    assert invalid.metadata["stderr_tail"] == "detail"


def test_release_gate_warehouse_contract_and_step_accumulator() -> None:
    payload = {
        "schema_version": "table_store_repair_result_v1",
        "backend": "snowflake",
        "mode": "dry_run",
        "patch_plan": {
            "schema_version": "patch_plan_v1",
            "apply_supported": False,
            "reversible": False,
            "operations": [],
        },
    }
    valid = release_gate._warehouse_dry_run_contract_step(
        subprocess.CompletedProcess(
            ["warehouse"],
            0,
            stdout=release_gate.json.dumps(payload),
            stderr="",
        )
    )
    invalid = release_gate._warehouse_dry_run_contract_step(
        subprocess.CompletedProcess(["warehouse"], 0, stdout="{}", stderr="")
    )
    malformed = release_gate._warehouse_dry_run_contract_step(
        subprocess.CompletedProcess(["warehouse"], 0, stdout="not-json", stderr="")
    )
    steps: list[release_gate.ReleaseGateStep] = []

    assert release_gate._append_step(steps, valid) is True
    assert release_gate._append_step(steps, invalid) is False
    assert valid.ok is True
    assert invalid.ok is False
    assert malformed.ok is False
    assert steps == [valid, invalid]


class TestReleasePreflightVerifiesDocumentTruth:
    """The release gate must verify the product''s numbers, not only its README prose.

    Until 2026-08-29 the preflight ran `release_doctor_core` and `readme_truth` and not
    `docs_truth`, so a release could be cut with every quantitative claim unverified. The
    preflight list was a local variable inside `run_release_gate`, which is why no test
    could see the omission -- the same shape of defect as a gate that hardcodes the
    population it polices.
    """

    def test_docs_truth_is_in_the_preflight(self) -> None:
        names = [name for name, _command, _timeout in release_gate._preflight_commands()]

        assert "docs_truth" in names, (
            "a release must not be cut with quantitative claims unverified"
        )

    def test_the_preflight_runs_before_anything_is_built(self) -> None:
        """Ordering is the guarantee: a false claim must abort, not accompany an artifact."""
        source = Path(release_gate.__file__).read_text(encoding="utf-8")
        body = source[source.index("def run_release_gate(") :]
        preflight_at = body.index("preflight_commands = _preflight_commands()")
        build_at = body.index('"build_sdist_and_wheel"')

        assert preflight_at < build_at

    def test_every_preflight_command_names_a_real_target(self) -> None:
        """A preflight step pointing at a missing script would fail for the wrong reason."""
        root = Path(release_gate.__file__).resolve().parents[2]

        for name, command, _timeout in release_gate._preflight_commands():
            script = next((str(part) for part in command if str(part).endswith(".py")), None)
            if script is not None:
                assert (root / script).is_file(), f"{name} points at a missing script"

    def test_a_planted_false_claim_fails_the_docs_truth_command(self, tmp_path: Path) -> None:
        """The chain that matters, end to end.

        Asserting that `docs_truth` is listed proves wiring, not efficacy. This runs the
        real command against a document whose prose has been edited to contradict its
        ledger entry, and asserts a non-zero exit. `_append_step` returns False on any
        failed step, so that exit aborts the gate before `build_sdist_and_wheel`.

        Honest scope, and it is the reason this test plants a *contradiction* rather than
        an invented number: `docs_truth` is an allowlist. It verifies the claims registered
        in `docs/quantitative_claims.yaml`, bidirectionally, and by construction cannot see
        a number nobody registered. The file says so itself -- "This is not 'every number
        in the docs' -- it is the load-bearing ones". A planted *unregistered* bolded
        numeral passes, which is a coverage direction this gate does not have and this test
        must not pretend it does.
        """
        root = Path(release_gate.__file__).resolve().parents[2]
        target = root / "docs" / "trust" / "premise-quality-result.md"
        original = target.read_text(encoding="utf-8")
        assert "0.8655" in original, "the planted-claim anchor is stale"
        planted = original.replace("0.8655", "0.9999")

        try:
            target.write_text(planted, encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "scripts/ci/docs_truth.py", "--check"],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=180,
            )
        finally:
            target.write_text(original, encoding="utf-8")

        assert result.returncode != 0, (
            "prose contradicting its ledger entry must fail the gate; "
            f"stdout={result.stdout[-2000:]}"
        )
        assert target.read_text(encoding="utf-8") == original, "the test must restore the doc"
