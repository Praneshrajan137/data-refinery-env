"""End-to-end tests for the DataForge dbt integration."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAIN_DATAFORGE_REPO = PROJECT_ROOT.parents[1]
if str(MAIN_DATAFORGE_REPO) not in sys.path:
    sys.path.insert(0, str(MAIN_DATAFORGE_REPO))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dataforge_dbt.config import load_config  # noqa: E402
from dataforge_dbt.dispatch import (  # noqa: E402
    DataForgeDbtDispatchError,
    DispatchRequest,
    dispatch,
)

DBT_PROJECT_DIR = PROJECT_ROOT / "integration_tests" / "dbt_project"
MACRO_PATH = PROJECT_ROOT / "macros" / "dataforge_repair.sql"


def test_macro_points_to_dispatcher_command() -> None:
    """The dbt macro should advertise the real Python dispatcher boundary."""
    macro = MACRO_PATH.read_text(encoding="utf-8")

    assert "DATAFORGE_DBT dispatch_configured" in macro
    assert "dataforge-dbt --relation" in macro
    assert "wrapper_required" not in macro


@pytest.mark.integration
def test_wrapper_exports_duckdb_relation_and_writes_transaction(tmp_path: Path) -> None:
    """Verify the wrapper inspects a real dbt-produced DuckDB relation.

    Args:
        tmp_path: Pytest temporary directory fixture.
    """
    if find_spec("dbt") is None:
        pytest.skip("dbt is not installed")
    try:
        import dbt.cli.main  # noqa: F401
    except Exception as exc:
        pytest.skip(f"dbt is not importable in this interpreter: {exc}")

    profiles_dir = _write_profiles(tmp_path)
    dbt_project_dir = _copy_dbt_project(tmp_path)
    _install_local_dbt_package(dbt_project_dir)
    _run_dbt(["seed"], profiles_dir, dbt_project_dir)
    _run_dbt(["run"], profiles_dir, dbt_project_dir)

    request = DispatchRequest(
        relation="main.example_with_dirty_data",
        column="column_x",
        mode="apply",
        input_csv=None,
        target_path=dbt_project_dir / "target",
        project_dir=dbt_project_dir,
        profiles_path=profiles_dir / "profiles.yml",
        profile_name="dataforge_dbt_integration",
    )
    issues = dispatch(request)

    assert any(issue.issue_type == "decimal_shift" for issue in issues)
    txn_files = list((dbt_project_dir / "target" / "dataforge_txns").glob("*.jsonl"))
    assert len(txn_files) == 1
    assert "decimal_shift" in txn_files[0].read_text(encoding="utf-8")


def test_dispatch_logs_known_decimal_shift(
    caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    """Verify DataForge detects the seed fixture's decimal-shift issue.

    Args:
        caplog: Pytest log capture fixture.
        tmp_path: Pytest temporary directory fixture.
    """
    request = DispatchRequest(
        relation="main.example_with_dirty_data",
        column="column_x",
        mode="dry_run",
        input_csv=DBT_PROJECT_DIR / "seeds" / "dirty_decimal_shift.csv",
        target_path=tmp_path / "target",
    )

    issues = dispatch(request)

    assert any(issue.issue_type == "decimal_shift" for issue in issues)
    assert "DATAFORGE_DBT issue" in caplog.text
    assert "type=decimal_shift" in caplog.text


def test_dispatch_apply_writes_transaction_file(tmp_path: Path) -> None:
    """Verify apply mode writes an audit artifact under target/dataforge_txns.

    Args:
        tmp_path: Pytest temporary directory fixture.
    """
    target_path = tmp_path / "target"
    request = DispatchRequest(
        relation="main.example_with_dirty_data",
        column="column_x",
        mode="apply",
        input_csv=DBT_PROJECT_DIR / "seeds" / "dirty_decimal_shift.csv",
        target_path=target_path,
    )

    dispatch(request)

    txn_files = list((target_path / "dataforge_txns").glob("*.jsonl"))
    assert len(txn_files) == 1
    assert "decimal_shift" in txn_files[0].read_text(encoding="utf-8")


def test_dispatch_native_duckdb_apply_refuses_without_a_declared_premise(
    tmp_path: Path,
) -> None:
    """Apply mode reaches the table-store path and then correctly refuses to write.

    This test previously asserted that `column_x` '1000' was rewritten to '100' and that an
    apply receipt was recorded. That write came from `decimal_shift`, which was removed from
    every write path after measuring precision 0.0000 on three corpora and a fourth where it
    would have rewritten 263,428 correct monetary values. The write it asserted is one the
    product deliberately no longer makes.

    What is asserted now is the honest behaviour: detection still fires, the dispatch still
    reaches `DuckDBStore`, and the apply is refused because `dispatch` passes `schema=None`
    (`dataforge_dbt/dispatch.py:289`) and "no declared premise, no write" is a product
    invariant. The relation must be byte-for-byte untouched.

    KNOWN LIMITATION, recorded here rather than hidden: the dbt native apply path cannot
    currently write anything, because `DispatchRequest` has no way to carry a premise. dbt
    users already declare constraints as generic tests, and `dataforge/integrations/dbt.py`
    already maps a dbt manifest to a DataForge `Schema` -- that mapper is simply not wired to
    this path. Wiring it is the fix, and it is a feature, not a test change.
    """
    try:
        import duckdb
    except ImportError:
        pytest.skip("duckdb is not installed")

    profiles_dir = _write_profiles(tmp_path)
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()
    project_dir.joinpath("dbt_project.yml").write_text(
        "name: dataforge_dbt_integration\nprofile: dataforge_dbt_integration\n",
        encoding="utf-8",
    )
    database_path = tmp_path / "dataforge_dbt.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("CREATE TABLE example_with_dirty_data (id VARCHAR, column_x VARCHAR)")
        connection.execute(
            "INSERT INTO example_with_dirty_data VALUES "
            "('1', '100'), ('2', '102'), ('3', '98'), ('4', '101'), "
            "('5', '99'), ('6', '1000')"
        )

    request = DispatchRequest(
        relation="main.example_with_dirty_data",
        column="column_x",
        mode="apply",
        input_csv=None,
        target_path=project_dir / "target",
        project_dir=project_dir,
        profiles_path=profiles_dir / "profiles.yml",
        profile_name="dataforge_dbt_integration",
        row_identity_columns=("id",),
    )

    with pytest.raises(DataForgeDbtDispatchError) as excinfo:
        dispatch(request)

    # The refusal must name the real cause. It used to say "until row identity is configured"
    # for a plan whose `id` column was perfectly good, sending the reader to fix correct
    # configuration; the true cause is that no proposal survived the gates.
    message = str(excinfo.value)
    assert "no operations" in message
    assert "row identity" not in message

    with duckdb.connect(str(database_path), read_only=True) as connection:
        unchanged = connection.execute(
            "SELECT column_x FROM example_with_dirty_data WHERE id = '6'"
        ).fetchone()[0]
    assert unchanged == "1000", "a refused apply must not mutate the relation"
    assert not list((project_dir / "target" / "dataforge_txns").glob("*.jsonl")), (
        "a refused apply must not record a transaction"
    )


def test_dispatch_native_duckdb_still_detects_the_issue(tmp_path: Path) -> None:
    """Refusing to repair is not the same as failing to detect.

    Kept separate from the refusal test so that a regression which silences the detector
    cannot hide behind the expected exception above.
    """
    try:
        import duckdb
    except ImportError:
        pytest.skip("duckdb is not installed")

    profiles_dir = _write_profiles(tmp_path)
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()
    project_dir.joinpath("dbt_project.yml").write_text(
        "name: dataforge_dbt_integration\nprofile: dataforge_dbt_integration\n",
        encoding="utf-8",
    )
    database_path = tmp_path / "dataforge_dbt.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("CREATE TABLE example_with_dirty_data (id VARCHAR, column_x VARCHAR)")
        connection.execute(
            "INSERT INTO example_with_dirty_data VALUES "
            "('1', '100'), ('2', '102'), ('3', '98'), ('4', '101'), "
            "('5', '99'), ('6', '1000')"
        )

    issues = dispatch(
        DispatchRequest(
            relation="main.example_with_dirty_data",
            column="column_x",
            mode="dry_run",
            input_csv=None,
            target_path=project_dir / "target",
            project_dir=project_dir,
            profiles_path=profiles_dir / "profiles.yml",
            profile_name="dataforge_dbt_integration",
            row_identity_columns=("id",),
        )
    )

    assert any(issue.issue_type == "decimal_shift" for issue in issues)


def test_explicit_mode_overrides_profile_default(tmp_path: Path) -> None:
    """Profile blocks provide defaults; explicit hook mode is authoritative."""
    profiles_dir = _write_profiles(tmp_path)

    config = load_config(
        mode="apply",
        target_path=tmp_path / "target",
        profiles_path=profiles_dir / "profiles.yml",
        profile_name="dataforge_dbt_integration",
    )

    assert config.mode == "apply"


def _write_profiles(tmp_path: Path) -> Path:
    """Create an isolated DuckDB profiles.yml for dbt integration tests.

    Args:
        tmp_path: Pytest temporary directory fixture.

    Returns:
        Directory containing profiles.yml.
    """
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    database_path = tmp_path / "dataforge_dbt.duckdb"
    profiles_dir.joinpath("profiles.yml").write_text(
        f"""
dataforge_dbt_integration:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{database_path.as_posix()}"
      threads: 1
  dataforge:
    mode: dry_run
""".lstrip(),
        encoding="utf-8",
    )
    return profiles_dir


def _copy_dbt_project(tmp_path: Path) -> Path:
    """Copy the dbt integration project into an isolated test directory.

    Args:
        tmp_path: Pytest temporary directory fixture.

    Returns:
        Path to the isolated dbt project.
    """
    dbt_project_dir = tmp_path / "dbt_project"
    shutil.copytree(
        DBT_PROJECT_DIR,
        dbt_project_dir,
        ignore=shutil.ignore_patterns("dbt_packages", "target", "logs", "package-lock.yml"),
    )
    return dbt_project_dir


def _install_local_dbt_package(dbt_project_dir: Path) -> None:
    """Install the minimal local dbt package fixture needed by the integration project.

    Args:
        dbt_project_dir: Isolated dbt project directory for this test run.

    Returns:
        None.
    """
    dbt_package_dir = dbt_project_dir / "dbt_packages" / "dataforge"
    dbt_package_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(PROJECT_ROOT / "dbt_project.yml", dbt_package_dir / "dbt_project.yml")
    shutil.copytree(PROJECT_ROOT / "macros", dbt_package_dir / "macros", dirs_exist_ok=True)


def _run_dbt(
    args: list[str],
    profiles_dir: Path,
    dbt_project_dir: Path,
) -> subprocess.CompletedProcess[str]:
    """Run a dbt command against the local integration project.

    Args:
        args: dbt subcommand arguments.
        profiles_dir: Directory containing profiles.yml.
        dbt_project_dir: Isolated dbt project directory for this test run.

    Returns:
        Completed dbt process.
    """
    env = os.environ.copy()
    python_path_entries = [str(PROJECT_ROOT), str(MAIN_DATAFORGE_REPO)]
    existing_pythonpath = env.get("PYTHONPATH")
    if existing_pythonpath:
        python_path_entries.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(python_path_entries)
    command = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(dbt_project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    result = subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        env=env,
        text=True,
        capture_output=True,
        timeout=90,
    )
    if result.returncode != 0:
        raise AssertionError(
            "dbt command failed: "
            f"{' '.join(command)}\n\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )
    return result
