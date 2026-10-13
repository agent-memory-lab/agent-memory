"""Run actual lifecycle functions without installing optional comparison SDKs.

AST loading selects the source functions, rather than copying implementations;
the tests measure settling and resource ownership, not model/framework quality.
"""

import ast
import asyncio
import importlib.util
import inspect
import json
from pathlib import Path
from threading import Event
from time import perf_counter
from types import SimpleNamespace

import pytest

_EXAMPLES = Path(__file__).parents[1] / "examples"
_SPEC = importlib.util.spec_from_file_location(
    "benchmark_protocol_lifecycle", _EXAMPLES / "memory_benchmark_protocol.py"
)
_PROTOCOL = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_PROTOCOL)


def lifecycle_functions(native=None):
    path = _EXAMPLES / "unified_memory_benchmark.py"
    tree = ast.parse(path.read_text())
    selected = ast.Module(
        body=[
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name in ("run_arm", "close_trial_resources")
        ],
        type_ignores=[],
    )
    namespace = {
        "asyncio": asyncio,
        "inspect": inspect,
        "perf_counter": perf_counter,
        "checked_call": _PROTOCOL.checked_call,
        "BenchmarkTimeout": _PROTOCOL.BenchmarkTimeout,
        "summarize_cases": _PROTOCOL.summarize_cases,
        "events_from_fixture": lambda _fixture, _key: (),
        "native": native,
    }
    exec(compile(selected, str(path), "exec"), namespace)
    return SimpleNamespace(**namespace)


@pytest.mark.parametrize("cancel", [False, True])
def test_cleanup_waits_for_synchronous_delete_before_timeout_or_cancellation(cancel):
    started, release = Event(), Event()
    effects, phases = [], []

    class Adapter:
        name = "sync-sdk"
        clears = 0

        async def clear(self):
            self.clears += 1
            if self.clears == 1:
                return

            def delayed_delete():
                started.set()
                assert release.wait(2)
                effects.append("deleted")

            await asyncio.to_thread(delayed_delete)

    functions = lifecycle_functions()
    output = {"arms": []}
    ledger = SimpleNamespace(start_phase=lambda *value: phases.append(value))

    async def run():
        task = asyncio.create_task(
            functions.run_arm(
                None,
                Adapter(),
                ledger,
                {"cases": []},
                1,
                output,
                1 if cancel else 0.005,
            )
        )
        try:
            assert await asyncio.to_thread(started.wait, 1)
            if cancel:
                task.cancel()
            await asyncio.sleep(0.02)
            assert not task.done(), "cleanup must retain the active SDK write"
            assert phases[-1] == (1, "sync-sdk", "cleanup")
        finally:
            release.set()
        expected = asyncio.CancelledError if cancel else _PROTOCOL.BenchmarkTimeout
        with pytest.raises(expected):
            await task
        assert effects == ["deleted"]

    asyncio.run(run())
    row = output["arms"][0]
    assert row["status"] == "aborted"
    assert row["cleanup_error_types"] == ["CancelledError" if cancel else "BenchmarkTimeout"]
    assert row["total_seconds_including_cleanup"] > 0
    assert row["quality"]["planned"] == 0


def test_cleanup_failure_does_not_report_a_completed_trial():
    class Adapter:
        name = "broken-cleanup"
        clears = 0

        async def clear(self):
            self.clears += 1
            if self.clears > 1:
                raise RuntimeError("private provider details")

    output = {"arms": []}
    ledger = SimpleNamespace(start_phase=lambda *_value: None)
    row = asyncio.run(
        lifecycle_functions().run_arm(
            None,
            Adapter(),
            ledger,
            {"cases": []},
            1,
            output,
            1,
        )
    )
    assert row["status"] == "failed" and row["cleanup_error_types"] == ["RuntimeError"]
    assert "private" not in str(row)


def test_nullable_resources_close_only_created_components_and_continue_after_error():
    effects = []

    class Resource:
        def __init__(self, label, *, broken=False):
            self.label, self.broken = label, broken

        async def close(self):
            effects.append(self.label)
            if self.broken:
                raise RuntimeError("private cleanup details")

    graph_resource = Resource("graph", broken=True)
    graph = SimpleNamespace(
        close=graph_resource.close, embedder=SimpleNamespace(client=Resource("embedding"))
    )
    functions = lifecycle_functions()
    errors = asyncio.run(functions.close_trial_resources(None, None, graph, None, None))
    assert effects == ["graph", "embedding"]
    assert errors == [{"resource": "graphiti", "error_type": "RuntimeError"}]
    assert asyncio.run(functions.close_trial_resources(None, None, None, None, None)) == []


def test_local_only_and_partial_mem0_resources_close_without_other_frameworks():
    effects = []

    def closed(label):
        return SimpleNamespace(close=lambda: effects.append(label))

    mem0 = SimpleNamespace(
        close=closed("mem0").close, vector_store=SimpleNamespace(client=closed("qdrant"))
    )
    errors = asyncio.run(
        lifecycle_functions().close_trial_resources(closed("local"), mem0, None, None, None)
    )
    assert errors == [] and effects == ["local", "mem0", "qdrant"]


def test_resource_cleanup_failure_stops_trials_and_persists_failed_report(tmp_path):
    """Run the real orchestration with offline clients and a failed resource close."""
    import importlib.metadata
    import platform
    import statistics
    import subprocess
    import tempfile
    from datetime import UTC, datetime
    from hashlib import sha256
    from uuid import uuid4

    path = _EXAMPLES / "unified_memory_benchmark.py"
    tree = ast.parse(path.read_text())
    selected = ast.Module(body=[
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in ("run", "write_json", "aggregate")
    ], type_ignores=[])
    setups, cleanup_calls, client_closes = [], [], []
    cleanup_error = {"resource": "falkor", "error_type": "RuntimeError"}

    class Client:
        def __init__(self, **_kwargs):
            self._client = self

        async def aclose(self):
            client_closes.append(True)

        async def list(self):
            return SimpleNamespace(models=[SimpleNamespace(
                model="offline", model_dump=lambda **_kwargs: {"model": "offline"},
            )])

        async def get(self, *_args):
            return SimpleNamespace(json=lambda: {"version": "offline"})

        async def chat(self, **_kwargs):
            return None

        async def embed(self, **_kwargs):
            return None

    class Proxy:
        def __init__(self, *_args, **_kwargs):
            self.url = "offline-no-network"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    class Ledger:
        def __init__(self, *_args):
            pass

        def start_phase(self, *_args):
            pass

        def summary(self, **_kwargs):
            return {"calls": 0}

    class Local:
        def __init__(self, *_args, **_kwargs):
            pass

        async def initialize(self):
            pass

    class AgentAdapter:
        name = "agent-memory"
        packing_token_budget = staticmethod(lambda: 100)

        def __init__(self, *_args):
            pass

    async def make_clients(*_args, **_kwargs):
        setups.append(True)
        mem0 = SimpleNamespace(
            llm=SimpleNamespace(client=SimpleNamespace(
                chat=lambda **_kwargs: None, _client=SimpleNamespace(timeout=0),
            )),
            embedding_model=SimpleNamespace(client=SimpleNamespace(
                _client=SimpleNamespace(timeout=0),
            )),
        )
        graph = SimpleNamespace(embedder=SimpleNamespace(client=SimpleNamespace(timeout=0)))
        return mem0, graph, SimpleNamespace(client=SimpleNamespace(pid=None))

    async def run_arm(_client, adapter, _ledger, _fixture, trial, output, _timeout):
        row = {"trial": trial, "name": adapter.name, "status": "completed",
               "cases": [], "total_seconds_including_cleanup": 0}
        output["arms"].append(row)
        return row

    async def close_resources(*_args):
        cleanup_calls.append(True)
        return [cleanup_error]

    async def settled(*_args):
        return True

    def embeddings(*_args):
        return None

    embeddings.retrieval_prefix_version = "offline"
    namespace = {
        "__file__": str(path), "asyncio": asyncio, "importlib": importlib,
        "json": json, "platform": platform, "statistics": statistics,
        "subprocess": subprocess, "tempfile": tempfile, "UTC": UTC,
        "datetime": datetime, "sha256": sha256, "Path": Path,
        "perf_counter": perf_counter, "uuid4": uuid4,
        "native": SimpleNamespace(MODEL="offline", EMBED_MODEL="offline-embed",
                                  OllamaEmbeddings=embeddings, _make_arm_clients=make_clients),
        "ARMS": ("agent-memory", "mem0-oss", "graphiti"), "SETTINGS": {},
        "EXTRACTION_PROMPT": "", "ANSWER_PROMPT": "", "CLAIM_SCHEMA": {},
        "ANSWER_SCHEMA": {}, "CommonAgentAdapter": AgentAdapter,
        "Mem0ComparisonAdapter": lambda *_args: SimpleNamespace(name="mem0-oss"),
        "GraphitiComparisonAdapter": lambda *_args, **_kwargs: SimpleNamespace(name="graphiti"),
        "ModelCallLedger": Ledger, "OllamaAccountingProxy": Proxy, "ControlledClient": Client,
        "UnifiedMemory": Local, "build_local_kernel": lambda *_args, **_kwargs: None,
        "GeneratedTrajectoryClaimExtractor": lambda *_args, **_kwargs: None,
        "ClaimGenerator": lambda *_args: None, "MemoryScope": lambda *_args, **_kwargs: None,
        "SQLiteDeletionJournal": lambda *_args: None,
        "canonical_digest": _PROTOCOL.canonical_digest, "run_arm": run_arm,
        "close_trial_resources": close_resources, "require_model_accounting": settled,
        "summarize_cases": _PROTOCOL.summarize_cases, "BenchmarkTimeout": _PROTOCOL.BenchmarkTimeout,
    }
    exec(compile(selected, str(path), "exec"), namespace)
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps({"dataset_id": "offline", "initial": [],
                                   "updates": [], "cases": []}))
    output = tmp_path / "output"
    args = SimpleNamespace(fixture=fixture, output_dir=output, repeats=2,
                           operation_timeout=1, upstream="offline")
    with pytest.raises(RuntimeError, match="resource cleanup failed"):
        asyncio.run(namespace["run"](args))
    report = json.loads((output / "report.json").read_text())
    assert len(setups) == len(cleanup_calls) == len(client_closes) == 1
    assert report["status"] == "failed"
    assert report["error_type"] == "RuntimeError"
    assert report["resource_cleanup_errors"] == [cleanup_error]
    assert len(report["arms"]) == 3 and all(row["trial"] == 1 for row in report["arms"])
    assert all(row["planned_trials"] == 2 and row["completed_trials"] == 1
               for row in report["summary"])
