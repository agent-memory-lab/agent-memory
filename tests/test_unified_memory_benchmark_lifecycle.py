"""Run actual lifecycle functions without installing optional comparison SDKs.

AST loading selects the source functions, rather than copying implementations;
the tests measure settling and resource ownership, not model/framework quality.
"""

import ast
import asyncio
import importlib.util
import inspect
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
