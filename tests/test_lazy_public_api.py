"""Cold imports stay small while old module identities and pickle remain usable."""

import ast
import subprocess
import sys
from pathlib import Path


def test_plain_import_does_not_load_subsystems():
    script = """
import sys
import agent_memory
assert len(agent_memory.__all__) == 329
for name in ('sqlite', 'kernel', 'runtime', 'evaluation.benchmark', 'ontology.store'):
    assert 'agent_memory.' + name not in sys.modules, name
from agent_memory import MemoryScope, AgentMemory
assert not any(name.startswith('agent_memory.evaluation') for name in sys.modules)
assert MemoryScope('tenant').tenant_id == 'tenant'
assert agent_memory.domain.MemoryScope is MemoryScope
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=20
    )
    assert result.returncode == 0, result.stderr


def test_typed_public_names_resolve_to_their_runtime_owners():
    import agent_memory
    from agent_memory._exports import EXPORTS

    stub = Path(agent_memory.__file__).with_suffix(".pyi")
    owners = {
        item.asname: "." + group.module
        for group in ast.parse(stub.read_text()).body
        if isinstance(group, ast.ImportFrom)
        for item in group.names
        if item.asname == item.name
    }
    assert owners == EXPORTS


def test_lazy_alias_keeps_canonical_spec_and_reload_identity():
    script = """
import importlib
import pickle
import agent_memory
old = importlib.import_module('agent_memory.ontology_memory')
new = importlib.import_module('agent_memory.ontology.memory')
assert old is new is agent_memory.ontology_memory
assert old.__spec__.name == 'agent_memory.ontology.memory'
assert importlib.reload(old) is new
persisted = b'cagent_memory.ontology_memory\\nSQLiteOntologyStore\\n.'
assert pickle.loads(persisted) is new.SQLiteOntologyStore
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=20
    )
    assert result.returncode == 0, result.stderr
