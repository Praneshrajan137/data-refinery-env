"""Smoke-test an installed DataForge CLI artifact outside the source tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SmokeStep:
    """One installed-CLI smoke command result."""

    name: str
    command: list[str]
    returncode: int
    stdout_tail: str
    stderr_tail: str

    @property
    def ok(self) -> bool:
        """Return whether the command passed."""
        return self.returncode == 0


@dataclass(frozen=True, slots=True)
class InstalledCliSmokeReport:
    """Serializable installed-artifact smoke report."""

    schema_version: str
    ok: bool
    dataforge_path: str
    workdir: str
    original_sha256: str
    final_sha256: str
    txn_id: str | None
    steps: list[SmokeStep]


def _tail(text: str, limit: int = 4000) -> str:
    """Return a bounded command-output tail."""
    return text[-limit:]


def _sha256_file(path: Path) -> str:
    """Return the SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run(name: str, command: list[str], *, cwd: Path, steps: list[SmokeStep]) -> str:
    """Run a command and append its result."""
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    step = SmokeStep(
        name=name,
        command=command,
        returncode=result.returncode,
        stdout_tail=_tail(result.stdout),
        stderr_tail=_tail(result.stderr),
    )
    steps.append(step)
    if not step.ok:
        raise RuntimeError(f"{name} failed with exit code {result.returncode}: {result.stderr}")
    return result.stdout


def _write_fixture(workdir: Path) -> tuple[Path, Path]:
    """Write a small CSV and schema that trigger safe deterministic repairs."""
    csv_path = workdir / "hospital.csv"
    schema_path = workdir / "schema.yaml"
    csv_path.write_text(
        textwrap.dedent(
            """\
            provider_number,hospital_name,city,state,zip_code,phone_number,rating,mortality_rate,readmission_rate,er_wait_time
            PRV001,General Hospital,Springfield,IL,62701,2175550101,4.2,0.023,0.145,28
            PRV002,St. Mary Medical Center,Chicago,IL,60601,3125550202,3.8,0.031,0.162,35
            PRV001,Springfield Medical,Springfield,IL,62701,2175550303,4.5,0.019,0.138,22
            PRV003,Mercy Hospital,Peoria,IL,61602,3095550404,3.5,0.028,0.158,31
            PRV004,Northwestern Memorial,Chicago,IL,60611,not available,4.1,0.025,0.149,26
            PRV005,Rush University MC,Chicago,IL,60612,3125550606,45.0,0.022,0.141,29
            PRV006,Advocate Christ,Oak Lawn,IL,60453,7085550707,3.9,0.027,0.155,33
            PRV007,Loyola University MC,Maywood,IL,60153,7085550808,4.3,0.020,0.142,25
            PRV008,Presence St. Joseph,Joliet,IL,60435,8155550909,4.0,0.026,0.151,30
            PRV009,Edward Hospital,Naperville,IL,60540,6305551010,3.7,0.029,0.160,34
            """
        ),
        encoding="utf-8",
    )
    schema_path.write_text(
        textwrap.dedent(
            """\
            columns:
              provider_number: str
              hospital_name: str
              city: str
              state: str
              zip_code: str
              phone_number: str
              rating: float
              mortality_rate: float
              readmission_rate: float
              er_wait_time: int

            functional_dependencies:
              - determinant: [provider_number]
                dependent: hospital_name
            """
        ),
        encoding="utf-8",
    )
    return csv_path, schema_path


def _write_benchmark_cache(workdir: Path) -> Path:
    """Write an isolated hospital benchmark cache for installed-artifact smoke."""
    dataset_dir = workdir / "bench-cache" / "real_world" / "hospital"
    dataset_dir.mkdir(parents=True)
    dataset_dir.joinpath("dirty.csv").write_text(
        textwrap.dedent(
            """\
            id,age,admission_date,name
            1,30,2020-01-01,Alice
            2,45,2020-01-02,Bob
            3,N/A,2020-01-03,Carol
            4,29,2020-01-04,Dave
            5,null,2020-01-05,Eve
            6,51,2020-01-06,Frank
            7,40,2020-01-07,Grace
            """
        ),
        encoding="utf-8",
    )
    dataset_dir.joinpath("clean.csv").write_text(
        textwrap.dedent(
            """\
            id,age,admission_date,name
            1,30,2020-01-01,Alice
            2,45,2020-01-02,Bob
            3,30,2020-01-03,Carol
            4,29,2020-01-04,Dave
            5,35,2020-01-05,Eve
            6,51,2020-01-06,Frank
            7,40,2020-01-07,Grace
            """
        ),
        encoding="utf-8",
    )
    return workdir / "bench-cache"


def run_smoke(*, dataforge: str = "dataforge") -> InstalledCliSmokeReport:
    """Run the installed-CLI smoke and return a report."""
    resolved_dataforge = shutil.which(dataforge)
    if resolved_dataforge is None:
        raise RuntimeError(f"Could not find {dataforge!r} on PATH.")
    steps: list[SmokeStep] = []
    txn_id: str | None = None
    with tempfile.TemporaryDirectory(prefix="dataforge-installed-smoke-") as tmp:
        workdir = Path(tmp)
        csv_path, schema_path = _write_fixture(workdir)
        cache_root = _write_benchmark_cache(workdir)
        constraints_path = workdir / "constraints.json"
        bench_path = workdir / "bench.json"
        original_sha256 = _sha256_file(csv_path)

        _run("version", [resolved_dataforge, "--version"], cwd=workdir, steps=steps)
        _run(
            "profile",
            [resolved_dataforge, "profile", str(csv_path), "--schema", str(schema_path), "--json"],
            cwd=workdir,
            steps=steps,
        )
        _run(
            "profile_constraints",
            [
                resolved_dataforge,
                "profile",
                str(csv_path),
                "--schema",
                str(schema_path),
                "--constraints-out",
                str(constraints_path),
                "--json",
            ],
            cwd=workdir,
            steps=steps,
        )
        _run(
            "constraints_review",
            [
                resolved_dataforge,
                "constraints",
                "review",
                str(constraints_path),
                "--no-tui",
                "--json",
            ],
            cwd=workdir,
            steps=steps,
        )
        _run(
            "repair_dry_run",
            [
                resolved_dataforge,
                "repair",
                str(csv_path),
                "--schema",
                str(schema_path),
                "--dry-run",
                "--json",
            ],
            cwd=workdir,
            steps=steps,
        )
        _run(
            "watch_once",
            [
                resolved_dataforge,
                "watch",
                str(csv_path),
                "--schema",
                str(schema_path),
                "--once",
                "--json",
            ],
            cwd=workdir,
            steps=steps,
        )
        apply_stdout = _run(
            "repair_apply",
            [
                resolved_dataforge,
                "repair",
                str(csv_path),
                "--schema",
                str(schema_path),
                "--apply",
                "--json",
            ],
            cwd=workdir,
            steps=steps,
        )
        payload = json.loads(apply_stdout)
        txn_id = str(payload["receipt"]["txn_id"])
        _run(
            "audit_applied",
            [resolved_dataforge, "audit", txn_id, "--search-root", str(workdir), "--json"],
            cwd=workdir,
            steps=steps,
        )
        _run(
            "revert",
            [resolved_dataforge, "revert", txn_id, "--search-root", str(workdir), "--json"],
            cwd=workdir,
            steps=steps,
        )
        _run(
            "bench",
            [
                resolved_dataforge,
                "bench",
                "--methods",
                "heuristic",
                "--datasets",
                "hospital",
                "--seeds",
                "1",
                "--cache-root",
                str(cache_root),
                "--no-verify-dataset-hashes",
                "--output-json",
                str(bench_path),
                "--json",
            ],
            cwd=workdir,
            steps=steps,
        )
        final_sha256 = _sha256_file(csv_path)
        if final_sha256 != original_sha256:
            raise RuntimeError("Revert did not restore the original CSV bytes.")
        return InstalledCliSmokeReport(
            schema_version="dataforge_installed_cli_smoke_v1",
            ok=all(step.ok for step in steps),
            dataforge_path=resolved_dataforge,
            workdir=str(workdir),
            original_sha256=original_sha256,
            final_sha256=final_sha256,
            txn_id=txn_id,
            steps=steps,
        )


def main(argv: list[str] | None = None) -> int:
    """Run the installed-CLI smoke from CI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataforge", default="dataforge")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(argv)
    report = run_smoke(dataforge=args.dataforge)
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    print(payload)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
