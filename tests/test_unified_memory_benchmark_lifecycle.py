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


@pytest.mark.parametrize("failure", [
    "cleanup", "local-construction", "configuration", "client-close",
    "local-construction-client-close", "client-close-cancel", "final-accounting",
    "final-accounting-timeout", "client-construction", "factory-cleanup",
])
def test_setup_and_cleanup_failures_stop_trials_and_persist_failed_report(tmp_path, failure):
    """Run real orchestration with offline clients and setup/cleanup failures."""
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
    setups, cleanup_calls, client_closes, accounting_calls = [], [], [], []
    cleanup_error = {"resource": "falkor", "error_type": "RuntimeError"}

    class Client:
        def __init__(self, **_kwargs):
            if failure == "client-construction":
                raise OSError("offline client construction failure")
            self._client = self

        async def aclose(self):
            client_closes.append(True)
            if failure == "client-close-cancel":
                raise asyncio.CancelledError()
            if "client-close" in failure:
                raise RuntimeError("offline client close failure")

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
            if failure.startswith("local-construction"):
                raise OSError("offline local construction failure")

        async def initialize(self):
            pass

    class AgentAdapter:
        name = "agent-memory"
        packing_token_budget = staticmethod(lambda: 100)

        def __init__(self, *_args):
            pass

    async def make_clients(*_args, **_kwargs):
        setups.append(True)
        if failure == "factory-cleanup":
            error = OSError("offline factory setup failure")
            error.comparison_cleanup_errors = (cleanup_error,)
            raise error
        mem0 = SimpleNamespace(
            llm=SimpleNamespace(client=SimpleNamespace(
                chat=lambda **_kwargs: None, _client=SimpleNamespace(timeout=0),
            )),
            embedding_model=SimpleNamespace(client=SimpleNamespace(
                _client=None if failure == "configuration" else SimpleNamespace(timeout=0),
            )),
        )
        graph = SimpleNamespace(embedder=SimpleNamespace(client=SimpleNamespace(timeout=0)))
        return mem0, graph, SimpleNamespace(client=SimpleNamespace(pid=314))

    async def run_arm(_client, adapter, _ledger, _fixture, trial, output, _timeout):
        row = {"trial": trial, "name": adapter.name, "status": "completed",
               "cases": [], "total_seconds_including_cleanup": 0}
        output["arms"].append(row)
        return row

    async def close_resources(*resources):
        cleanup_calls.append(resources)
        return [cleanup_error] if failure == "cleanup" else []

    async def settled(*_args):
        accounting_calls.append(True)
        if failure == "final-accounting" and _args[1].get("schema_version") == 2:
            raise OSError("offline accounting failure")
        if failure == "final-accounting-timeout" and _args[1].get("schema_version") == 2:
            raise _PROTOCOL.BenchmarkTimeout("unfinished_model_accounting")
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
    expected_error = {"cleanup": RuntimeError, "local-construction": OSError,
                      "configuration": AttributeError, "client-close": RuntimeError,
                      "local-construction-client-close": RuntimeError,
                      "client-close-cancel": asyncio.CancelledError,
                      "final-accounting": OSError,
                      "final-accounting-timeout": _PROTOCOL.BenchmarkTimeout,
                      "client-construction": OSError, "factory-cleanup": OSError}[failure]
    with pytest.raises(expected_error):
        asyncio.run(namespace["run"](args))
    report = json.loads((output / "report.json").read_text())
    completed_run = failure in {
        "client-close", "client-close-cancel", "final-accounting", "final-accounting-timeout",
    }
    client_created = failure != "client-construction"
    assert len(setups) == len(cleanup_calls) == (2 if completed_run else int(client_created))
    assert len(client_closes) == int(client_created)
    assert report["status"] == (
        "aborted" if failure in {
            "client-close-cancel", "final-accounting", "final-accounting-timeout",
        } else "failed"
    )
    assert report["error_type"] == (
        "IncompleteModelAccounting" if failure == "final-accounting-timeout"
        else expected_error.__name__
    )
    expected_cleanup = [cleanup_error] if failure in {"cleanup", "factory-cleanup"} else []
    if "client-close" in failure:
        expected_cleanup.append({"resource": "shared-model-client",
                                 "error_type": expected_error.__name__})
    assert report.get("resource_cleanup_errors", []) == expected_cleanup
    if failure == "local-construction-client-close":
        assert report["original_error_type"] == "OSError"
    if failure.startswith("final-accounting"):
        assert report["accounting_complete"] is False
    if failure == "factory-cleanup":
        assert cleanup_calls[0] == (None, None, None, None, None)
    elif client_created:
        closed_local, *other_resources = cleanup_calls[0]
        assert (closed_local is not None) == (failure == "cleanup" or completed_run)
        assert all(resource is not None for resource in other_resources)
        assert other_resources[-1] == 314
    expected_trials = 2 if completed_run else int(failure == "cleanup")
    assert len(report["arms"]) == 3 * expected_trials
    assert all(1 <= row["trial"] <= expected_trials for row in report["arms"])
    assert len(accounting_calls) == 3 * expected_trials + 1
    assert report["model_usage"] == {"calls": 0}
    assert all(row["planned_trials"] == 2 and row["completed_trials"] == expected_trials
               for row in report["summary"])


@pytest.mark.parametrize("failure", ["driver", "graph-indices", "mem0", "mem0-configure"])
@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_partial_factory_closes_owned_handles_before_reraising_original(
    tmp_path, failure, cleanup_failure,
):
    path = _EXAMPLES / "ollama_memory_comparison.py"
    tree = ast.parse(path.read_text())
    selected = ast.Module(body=[node for node in tree.body
                               if isinstance(node, ast.AsyncFunctionDef)
                               and node.name == "_make_arm_clients"], type_ignores=[])
    effects = []
    original = OSError("private setup details")

    def resource(label):
        async def close():
            effects.append(label)
            if label == "graphiti-embedding" and cleanup_failure:
                raise RuntimeError("private cleanup details")
        return SimpleNamespace(close=close)

    async def indices():
        if failure == "graph-indices":
            raise original

    def driver(**_kwargs):
        if failure == "driver":
            raise original
        return object()

    class Mem0Client:
        def __init__(self):
            self._client = resource("mem0-llm")

        @property
        def chat(self):
            raise original

    def mem0_factory(_config):
        if failure == "mem0":
            raise original
        return SimpleNamespace(
            close=resource("mem0-history").close,
            llm=SimpleNamespace(client=Mem0Client()),
            vector_store=SimpleNamespace(client=resource("qdrant")),
            embedding_model=SimpleNamespace(client=SimpleNamespace(
                _client=resource("mem0-embedding"),
            )),
        )

    async def stop(pid):
        assert pid == 314
        effects.append("falkor-process")

    def graph(**_kwargs):
        return SimpleNamespace(close=resource("graphiti").close,
                               build_indices_and_constraints=indices)

    namespace = {
        "Path": Path, "ollama": SimpleNamespace(AsyncClient=object),
        "asyncio": asyncio, "inspect": inspect, "MODEL": "offline",
        "EMBED_MODEL": "offline", "OLLAMA_URL": "offline-no-network",
        "LLMConfig": lambda **_kwargs: None,
        "NativeOllamaCompletionClient": lambda *_args, **_kwargs: None,
        "OpenAIGenericClient": lambda **_kwargs: None,
        "OpenAIEmbedderConfig": lambda **_kwargs: None,
        "OpenAIEmbedder": lambda **_kwargs: SimpleNamespace(client=resource("graphiti-embedding")),
        "AsyncFalkorDB": lambda **_kwargs: SimpleNamespace(
            close=resource("falkor").close, client=SimpleNamespace(pid=314),
        ),
        "FalkorDriver": driver, "Graphiti": graph,
        "OpenAIRerankerClient": lambda **_kwargs: None,
        "Memory": SimpleNamespace(from_config=mem0_factory), "_stop_falkordblite": stop,
        "uuid4": lambda: SimpleNamespace(hex="offline"),
    }
    exec(compile(selected, str(path), "exec"), namespace)
    with pytest.raises(OSError) as error:
        asyncio.run(namespace["_make_arm_clients"](tmp_path, object(), {}))
    assert error.value is original
    expected = {"graphiti-embedding", "falkor", "falkor-process"}
    if failure != "driver":
        expected.add("graphiti")
    if failure == "mem0-configure":
        expected.update(("mem0-history", "qdrant", "mem0-llm", "mem0-embedding"))
    assert set(effects) == expected and len(effects) == len(expected)
    diagnostics = getattr(error.value, "comparison_cleanup_errors", ())
    assert diagnostics == (({"resource": "graphiti-embedding", "error_type": "RuntimeError"},)
                           if cleanup_failure else ())
    assert "private" not in json.dumps(diagnostics)
