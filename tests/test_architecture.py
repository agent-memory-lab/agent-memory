"""Dependency boundaries and compatibility contracts for the subsystem layout."""

import ast
import importlib
import importlib.util
import pickle
import subprocess
import sys
from pathlib import Path

import pytest

import agent_memory
from agent_memory._compat import LEGACY_MODULES

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src" / "agent_memory"


def imports(path):
    relative = path.relative_to(SOURCE).with_suffix("")
    package = ".".join(("agent_memory", *relative.parts[:-1]))
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                module = importlib.util.resolve_name("." * node.level + module, package)
            yield module
            # Also catch `from agent_memory import legacy_module`.
            yield from (f"{module}.{alias.name}" for alias in node.names)


def test_production_code_uses_owners_instead_of_compatibility_modules():
    legacy = {f"agent_memory.{name}" for name in LEGACY_MODULES}
    paths = list(SOURCE.rglob("*.py"))
    paths += list((ROOT / "packages").glob("*/src/**/*.py"))
    for path in paths:
        if path.is_relative_to(SOURCE):
            dependencies = imports(path)
        else:
            dependencies = (
                node.module
                for node in ast.walk(ast.parse(path.read_text()))
                if isinstance(node, ast.ImportFrom) and not node.level
            )
        assert not (set(dependencies) & legacy), path


@pytest.mark.parametrize("name", ["domain.py", "ports.py", "ontology/model.py"])
def test_domain_contracts_do_not_depend_on_implementation(name):
    allowed = {"agent_memory.domain", "agent_memory.serialization"}
    for module in imports(SOURCE / name):
        if module.startswith("agent_memory."):
            assert any(module == item or module.startswith(item + ".") for item in allowed)


def test_runtime_does_not_depend_on_evaluation():
    for path in SOURCE.rglob("*.py"):
        if path.parent.name == "evaluation" or path.name in {"__init__.py", "_compat.py"}:
            continue
        assert not any(
            module.startswith("agent_memory.evaluation") for module in imports(path)
        ), path


def test_ontology_implementation_does_not_import_legacy_facade():
    for path in (SOURCE / "ontology").glob("*.py"):
        assert "agent_memory.ontology.memory" not in set(imports(path)), path


@pytest.mark.parametrize("legacy,target", sorted(LEGACY_MODULES.items()))
def test_old_and_new_module_paths_share_identity_and_pickle_globals(legacy, target):
    old = importlib.import_module(f"agent_memory.{legacy}")
    new = importlib.import_module(f"agent_memory.{target}")
    assert old is new
    assert getattr(agent_memory, legacy) is new
    # Protocol 0 GLOBAL references reproduce persisted pre-migration class paths.
    for name, value in vars(new).items():
        if not name.startswith("_") and isinstance(value, type):
            reference = f"cagent_memory.{legacy}\n{name}\n.".encode()
            assert pickle.loads(reference) is value


def test_patching_old_import_updates_the_implementation(monkeypatch):
    old = importlib.import_module("agent_memory.capture_queue")
    new = importlib.import_module("agent_memory.capture.queue")
    replacement = object()
    monkeypatch.setattr(old, "SQLiteCaptureQueue", replacement)
    assert new.SQLiteCaptureQueue is replacement


@pytest.mark.parametrize("first", ["agent_memory.recovery", "agent_memory.ontology.store"])
def test_fresh_import_keeps_optional_providers_optional(first):
    script = f"""
import importlib
import sys
importlib.import_module({first!r})
from agent_memory import MemoryKernel, SQLiteOntologyStore
from agent_memory.ontology.store import SQLiteOntologyStore as Store
assert Store is SQLiteOntologyStore
for package in ('psycopg', 'tiktoken', 'graphiti_core', 'mem0', 'langgraph', 'mcp'):
    assert package not in sys.modules, package
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=20
    )
    assert result.returncode == 0, result.stderr
