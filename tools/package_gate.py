"""Build and clean-install every distribution on Linux, macOS, and Windows.

This checks packaging, entry-point imports, migrations, SQLite write/recall, and
the existing zero-tolerance release scanner. It does not run live PostgreSQL or
model inference, install inference extras, or download weights.
"""

import argparse
import json
import os
import runpy
import subprocess
import sys
import venv
from pathlib import Path

package_inventory = runpy.run_path(str(Path(__file__).with_name("package_inventory.py")))[
    "package_inventory"
]


def environment_python(directory: Path) -> Path:
    return directory / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def checked_json(command, *, cwd):
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    result.check_returncode()
    return json.loads(result.stdout)


def run(source_root: Path, work_dir: Path, *, no_build_isolation=False, build_only=False):
    source_root, work_dir = source_root.resolve(), work_dir.resolve()
    if work_dir.is_relative_to(source_root):
        raise ValueError("Use a fresh work directory outside the source checkout")
    # Never silently reuse a contaminated environment or stale release artifact.
    work_dir.mkdir(parents=True, exist_ok=False)
    artifacts = work_dir / "artifacts"
    inventory = package_inventory(source_root)
    wheels = []
    for package in inventory:
        output = artifacts / package["distribution"]
        command = [sys.executable, "-m", "build", "--outdir", str(output)]
        if no_build_isolation:
            command.append("--no-isolation")
        command.append(str(source_root / package["path"]))
        subprocess.run(command, check=True, cwd=work_dir)
        built_wheels = list(output.glob("*.whl"))
        sdists = list(output.glob("*.tar.gz"))
        if len(built_wheels) != 1 or len(sdists) != 1:
            raise RuntimeError(f"Expected one wheel and one sdist for {package['distribution']}")
        wheels.extend(built_wheels)
    if build_only:
        return {"status": "built_only_unverified", "packages": inventory}

    install_dir = work_dir / "installed"
    venv.EnvBuilder(with_pip=False).create(install_dir)
    python = str(environment_python(install_dir))
    # pip --python works even when the target venv has no pip/ensurepip. Do not
    # use --no-deps: the ordinary install must have satisfiable base dependencies.
    pip = [sys.executable, "-m", "pip", "--python", python]
    subprocess.run([*pip, "install", *(str(path) for path in wheels)], check=True, cwd=work_dir)
    subprocess.run([*pip, "check"], check=True, cwd=work_dir)
    evidence = checked_json([
        python, "-I", str(source_root / "tests/operational/v7_installed_smoke.py"),
        "--source-root", str(source_root),
    ], cwd=work_dir)
    # Run the installed scanner with its default threshold over both source and
    # every produced archive, without scanning the dependency venv.
    report = checked_json([
        python, "-I", "-c",
        "from agent_memory_postgres.release_scan import main; raise SystemExit(main())",
        "--json", "--fail-on-violation", str(source_root), str(artifacts),
    ], cwd=work_dir)
    if report["scanned_archives"] < 2 * len(inventory):
        raise RuntimeError("Not every built wheel and sdist was scanned")
    evidence["release_scan"] = report
    evidence["archive_count"] = 2 * len(inventory)
    (work_dir / "evidence.json").write_text(
        json.dumps(evidence, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--no-build-isolation", action="store_true")
    parser.add_argument("--build-only", action="store_true")
    args = parser.parse_args()
    result = run(args.source_root, args.work_dir, no_build_isolation=args.no_build_isolation,
                 build_only=args.build_only)
    print(json.dumps({"status": result["status"], "package_count": len(result["packages"])}))
