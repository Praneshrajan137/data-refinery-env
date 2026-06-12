"""Produce the fresh-env dbt-duckdb proof required by DataForge full vision."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import asdict, dataclass
from pathlib import Path

SCHEMA_VERSION = "dataforge_dbt_fresh_env_proof_v1"


@dataclass(frozen=True)
class FreshEnvReport:
    """Serializable dbt-duckdb proof report."""

    schema_version: str
    package: str
    install_source: str
    python_version: str
    dbt_core_version: str
    dbt_duckdb_version: str
    dbt_seed_passed: bool
    dbt_run_passed: bool
    dbt_test_passed: bool
    dbt_duckdb_e2e_passed: bool
    dataforge_dbt_dry_run_passed: bool
    dataforge_dbt_refuse_passed: bool
    dataforge_dbt_apply_passed: bool
    dataforge_table_store_audit_passed: bool
    dataforge_table_store_revert_passed: bool
    skipped_tests: int
    audit_artifact_written: bool
    artifact_path: str
    command_log_path: str


def _python_in_venv(venv_path: Path) -> Path:
    directory = "Scripts" if os.name == "nt" else "bin"
    suffix = ".exe" if os.name == "nt" else ""
    return venv_path / directory / f"python{suffix}"


def _script_in_venv(venv_path: Path, name: str) -> Path:
    directory = "Scripts" if os.name == "nt" else "bin"
    suffix = ".exe" if os.name == "nt" else ""
    return venv_path / directory / f"{name}{suffix}"


def _run(command: list[str | os.PathLike[str]], *, cwd: Path, log: Path) -> None:
    result = subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    with log.open("a", encoding="utf-8") as handle:
        handle.write("$ " + " ".join(str(part) for part in command) + "\n")
        handle.write(result.stdout)
        handle.write(result.stderr)
        handle.write(f"\n[exit {result.returncode}]\n\n")
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(str(part) for part in command)}")


def _write_project(project_dir: Path, profiles_dir: Path, database_path: Path) -> None:
    """Write a minimal dbt-duckdb project with real data tests."""
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "seeds").mkdir()
    profiles_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text(
        textwrap.dedent(
            """
            name: dataforge_dbt_fresh_env
            version: 1.0.0
            config-version: 2
            profile: dataforge_dbt_fresh_env

            model-paths: ["models"]
            seed-paths: ["seeds"]

            models:
              dataforge_dbt_fresh_env:
                +materialized: table
            """
        ).lstrip(),
        encoding="utf-8",
    )
    (project_dir / "seeds" / "dirty_decimal_shift.csv").write_text(
        "id,column_x\n1,100\n2,102\n3,98\n4,101\n5,99\n6,1000\n",
        encoding="utf-8",
    )
    (project_dir / "models" / "example_with_dirty_data.sql").write_text(
        "select id, column_x from {{ ref('dirty_decimal_shift') }}\n",
        encoding="utf-8",
    )
    (project_dir / "models" / "schema.yml").write_text(
        textwrap.dedent(
            """
            version: 2
            models:
              - name: example_with_dirty_data
                columns:
                  - name: id
                    data_tests:
                      - not_null
                      - unique
                  - name: column_x
                    data_tests:
                      - not_null
            """
        ).lstrip(),
        encoding="utf-8",
    )
    (profiles_dir / "profiles.yml").write_text(
        textwrap.dedent(
            f"""
            dataforge_dbt_fresh_env:
              target: dev
              outputs:
                dev:
                  type: duckdb
                  path: "{database_path.as_posix()}"
                  threads: 1
              dataforge:
                mode: dry_run
            """
        ).lstrip(),
        encoding="utf-8",
    )


def _version(venv_python: Path, module_name: str) -> str:
    script = "from importlib.metadata import version\nimport sys\nprint(version(sys.argv[1]))\n"
    result = subprocess.run(
        [str(venv_python), "-c", script, module_name],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _evidence_path(output: Path, copied_path: Path) -> str:
    """Return a portable evidence path for the main DataForge evidence root."""
    evidence_root = output.parent.parent if output.parent.name == "dbt_duckdb" else output.parent
    try:
        return copied_path.relative_to(evidence_root).as_posix()
    except ValueError:
        return str(copied_path)


def run_proof(
    *, output: Path, index_url: str | None, extra_index_url: str | None
) -> FreshEnvReport:
    """Run the proof and write a report."""
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError("The dbt-duckdb fresh-env proof must run under Python 3.12.")
    with tempfile.TemporaryDirectory(prefix="dataforge-dbt-proof-") as tmp:
        root = Path(tmp)
        venv_path = root / ".venv"
        project_dir = root / "dbt_project"
        profiles_dir = root / "profiles"
        database_path = root / "dataforge_dbt.duckdb"
        log_path = root / "commands.log"
        _run([sys.executable, "-m", "venv", venv_path], cwd=root, log=log_path)
        venv_python = _python_in_venv(venv_path)
        install_command: list[str | os.PathLike[str]] = [
            venv_python,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "pip",
        ]
        _run(install_command, cwd=root, log=log_path)
        install_command = [
            venv_python,
            "-m",
            "pip",
            "install",
        ]
        if index_url:
            install_command.extend(["--index-url", index_url])
        if extra_index_url:
            install_command.extend(["--extra-index-url", extra_index_url])
        install_command.extend(["dataforge_07_dbt"])
        _run(install_command, cwd=root, log=log_path)

        _write_project(project_dir, profiles_dir, database_path)
        dbt_command = [venv_python, "-m", "dbt.cli.main"]
        common = ["--project-dir", project_dir, "--profiles-dir", profiles_dir]
        _run([*dbt_command, "seed", *common], cwd=project_dir, log=log_path)
        _run([*dbt_command, "run", *common], cwd=project_dir, log=log_path)
        _run([*dbt_command, "test", *common], cwd=project_dir, log=log_path)

        dispatch = _script_in_venv(venv_path, "dataforge-dbt")
        base_dispatch = [
            dispatch,
            "--relation",
            "main.example_with_dirty_data",
            "--column",
            "column_x",
            "--target-path",
            project_dir / "target",
            "--project-dir",
            project_dir,
            "--profiles-path",
            profiles_dir / "profiles.yml",
            "--profile-name",
            "dataforge_dbt_fresh_env",
            "--row-id",
            "id",
        ]
        _run([*base_dispatch, "--mode", "dry_run"], cwd=project_dir, log=log_path)
        _run([*base_dispatch, "--mode", "refuse"], cwd=project_dir, log=log_path)
        _run(
            [*base_dispatch, "--mode", "apply"],
            cwd=project_dir,
            log=log_path,
        )
        artifacts = sorted((project_dir / "target" / "dataforge_txns").glob("*.jsonl"))
        artifact_written = bool(artifacts)
        table_store_logs = sorted(
            (project_dir / "target" / ".dataforge" / "transactions").glob("*.jsonl")
        )
        if not table_store_logs:
            raise RuntimeError("DataForge dbt apply did not write a table-store transaction log.")
        txn_id = table_store_logs[0].stem
        dataforge = _script_in_venv(venv_path, "dataforge")
        _run(
            [dataforge, "audit", txn_id, "--search-root", project_dir / "target", "--json"],
            cwd=project_dir,
            log=log_path,
        )
        _run(
            [dataforge, "revert", txn_id, "--search-root", project_dir / "target", "--json"],
            cwd=project_dir,
            log=log_path,
        )
        evidence_dir = output.parent
        evidence_dir.mkdir(parents=True, exist_ok=True)
        copied_artifact = (
            evidence_dir / artifacts[0].name if artifacts else evidence_dir / "missing.jsonl"
        )
        if artifacts:
            shutil.copy2(artifacts[0], copied_artifact)
        copied_log = evidence_dir / "commands.log"
        shutil.copy2(log_path, copied_log)
        report = FreshEnvReport(
            schema_version=SCHEMA_VERSION,
            package="dataforge_07_dbt",
            install_source="pypi" if not index_url else "testpypi",
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            dbt_core_version=_version(venv_python, "dbt-core"),
            dbt_duckdb_version=_version(venv_python, "dbt-duckdb"),
            dbt_seed_passed=True,
            dbt_run_passed=True,
            dbt_test_passed=True,
            dbt_duckdb_e2e_passed=artifact_written,
            dataforge_dbt_dry_run_passed=True,
            dataforge_dbt_refuse_passed=True,
            dataforge_dbt_apply_passed=True,
            dataforge_table_store_audit_passed=True,
            dataforge_table_store_revert_passed=True,
            skipped_tests=0,
            audit_artifact_written=artifact_written,
            artifact_path=_evidence_path(output, copied_artifact),
            command_log_path=_evidence_path(output, copied_log),
        )
        output.write_text(json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8")
        return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("../data_quality_env/docs/evidence/dbt_duckdb/fresh_env_report.json"),
    )
    parser.add_argument("--index-url", default=None)
    parser.add_argument("--extra-index-url", default=None)
    args = parser.parse_args(argv)
    report = run_proof(
        output=args.output,
        index_url=args.index_url,
        extra_index_url=args.extra_index_url,
    )
    print(json.dumps(asdict(report), indent=2, sort_keys=True))
    return 0 if report.dbt_duckdb_e2e_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
