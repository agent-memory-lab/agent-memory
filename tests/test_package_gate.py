"""Metadata and orchestration contracts; real installed smoke runs in the OS matrix."""

import json
import runpy
import subprocess
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
GATE = runpy.run_path(str(ROOT / "tools/package_gate.py"))
inventory = GATE["package_inventory"]


def test_inventory_covers_every_project_and_includes_seventh_local_models():
    packages = inventory(ROOT)
    assert len(packages) == 7
    assert {package["path"] for package in packages} == {
        ".", *(p.parent.relative_to(ROOT).as_posix()
               for p in (ROOT / "packages").glob("*/pyproject.toml")),
    }
    assert next(p for p in packages if p["distribution"] == "agent-memory-local-models") == {
        "path": "packages/local-models", "distribution": "agent-memory-local-models",
        "module": "agent_memory_local_models", "version": "0.1.0",
    }


def test_new_package_is_discovered_without_another_build_or_import_list(tmp_path):
    metadata = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(metadata, encoding="utf-8")
    child = tmp_path / "packages/new-adapter"
    child.mkdir(parents=True)
    (child / "pyproject.toml").write_text(
        metadata.replace('name = "agent-memory"', 'name = "agent-memory-new"')
        .replace('packages = ["src/agent_memory"]', 'packages = ["src/agent_memory_new"]'),
        encoding="utf-8",
    )
    assert {p["module"] for p in inventory(tmp_path)} == {"agent_memory", "agent_memory_new"}
    (child / "pyproject.toml").write_text(metadata, encoding="utf-8")
    with pytest.raises(ValueError, match="Duplicate"):
        inventory(tmp_path)


def test_heavy_local_model_dependencies_are_only_in_explicit_inference_extra():
    metadata = tomllib.loads((ROOT / "packages/local-models/pyproject.toml").read_text())
    project = metadata["project"]
    assert project["dependencies"] == ["agent-memory>=0.1,<0.2"]
    assert len(project["optional-dependencies"]["inference"]) == 4


def test_environment_python_is_portable(tmp_path, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(GATE["os"], "name", "nt")
        windows = GATE["environment_python"](tmp_path)
    assert windows == tmp_path / "Scripts/python.exe"
    with monkeypatch.context() as patch:
        patch.setattr(GATE["os"], "name", "posix")
        unix = GATE["environment_python"](tmp_path)
    assert unix == tmp_path / "bin/python"


def test_gate_rejects_source_internal_or_reused_work_directory(tmp_path):
    with pytest.raises(ValueError, match="outside"):
        GATE["run"](ROOT, ROOT / "dist/gate")
    with pytest.raises(FileExistsError):
        GATE["run"](ROOT, tmp_path)


def test_gate_builds_scans_and_installs_same_inventory_with_normal_dependencies(
    tmp_path, monkeypatch,
):
    commands = []
    packages = inventory(ROOT)

    def command(args, **kwargs):
        commands.append(args)
        if "build" in args:
            output = Path(args[args.index("--outdir") + 1])
            output.mkdir(parents=True)
            (output / "package.whl").touch()
            (output / "package.tar.gz").touch()
        if "--source-root" in args:
            data = {"status": "passed", "packages": packages}
        else:
            data = {"scanned_archives": 2 * len(packages)}
        return subprocess.CompletedProcess(args, 0, json.dumps(data), "")

    monkeypatch.setattr(GATE["subprocess"], "run", command)
    monkeypatch.setattr(GATE["venv"], "EnvBuilder", lambda **kwargs: SimpleNamespace(
        create=lambda directory: directory.mkdir(),
    ))
    work = tmp_path / "new-gate"
    evidence = GATE["run"](ROOT, work, no_build_isolation=True)
    builds = [args for args in commands if "build" in args]
    assert {args[-1] for args in builds} == {str(ROOT / p["path"]) for p in packages}
    install = next(args for args in commands if "install" in args)
    assert len([arg for arg in install if arg.endswith(".whl")]) == len(packages)
    assert "--no-deps" not in install
    assert any("check" in args for args in commands)
    assert all("-I" in args for args in commands if "--source-root" in args or "--json" in args)
    assert commands[-1][-2:] == [str(ROOT), str(work / "artifacts")]
    assert "--fail-on-violation" in commands[-1]
    assert "--max-findings" not in commands[-1]
    assert evidence["archive_count"] == 2 * len(packages)
    assert json.loads((work / "evidence.json").read_text())["status"] == "passed"
