"""Protocol tests only: these do not measure real vendor memory quality."""

import asyncio
from functools import wraps

import pytest

from agent_memory.memory_evaluation import (
    EvaluationCase,
    EvaluationQuery,
    EvaluationTurn,
    compare_memory_backends,
)


def run_async(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        return asyncio.run(function(*args, **kwargs))

    return wrapped


class RecordingBackend:
    def __init__(self):
        self.turns = []
        self.operations = []
        self.closed = False

    async def add(self, turn):
        self.turns.append(turn)
        self.operations.append(("add", turn))

    async def search(self, text, limit):
        self.operations.append(("search", text, limit))
        return [turn.text for turn in self.turns[-limit:]]

    async def delete_all(self):
        self.operations.append(("delete",))
        self.turns.clear()

    async def close(self):
        self.closed = True


def sample_case():
    return EvaluationCase(
        case_id="protocol-only",
        initial=(EvaluationTurn("e1", "user", "I prefer tea.", "2026-09-29T00:00:00+00:00"),),
        updates=(EvaluationTurn("e2", "user", "I now prefer coffee.", "2026-09-29T01:00:00+00:00"),),
        initial_queries=(EvaluationQuery("What drink?", ("tea",)),),
        update_queries=(EvaluationQuery("What drink now?", ("coffee",)),),
    )


@run_async
async def test_equal_raw_inputs_isolated_factories_and_cleanup():
    instances = []
    isolation_ids = []

    async def factory(isolation_id):
        assert isinstance(isolation_id, str)
        isolation_ids.append(isolation_id)
        backend = RecordingBackend()
        instances.append(backend)
        return backend

    report = await compare_memory_backends(
        (sample_case(),), {"protocol-a": factory, "protocol-b": factory}
    )

    assert isinstance(report, dict)
    assert len(instances) == 2
    assert len(set(isolation_ids)) == 2
    assert instances[0].operations == instances[1].operations
    expected = list(sample_case().initial + sample_case().updates)
    for backend in instances:
        assert [op[1] for op in backend.operations if op[0] == "add"] == expected
        assert backend.closed
        assert backend.turns == []
        assert sum(op[0] == "delete" for op in backend.operations) >= 1
    # Neither raw text nor expected answers should leak into aggregate output.
    assert "I prefer tea." not in str(report)
    assert "I now prefer coffee." not in str(report)


@run_async
async def test_backend_failure_is_sanitized_and_cleanup_runs():
    class FailingBackend(RecordingBackend):
        async def add(self, turn):
            raise RuntimeError("private-secret-provider-diagnostic")

    backend = FailingBackend()

    async def factory(isolation_id):
        return backend

    report = await compare_memory_backends((sample_case(),), {"failing": factory})
    assert "private-secret-provider-diagnostic" not in str(report)
    assert "failed" in str(report)
    assert backend.closed
    assert ("delete",) in backend.operations


@run_async
async def test_timeout_is_bounded_and_backend_closed():
    import asyncio

    class SlowBackend(RecordingBackend):
        async def add(self, turn):
            await asyncio.sleep(60)

    backend = SlowBackend()

    async def factory(isolation_id):
        return backend

    report = await asyncio.wait_for(
        compare_memory_backends((sample_case(),), {"slow": factory}, timeout_seconds=0.02),
        timeout=2,
    )
    assert "failed" in str(report)
    assert backend.closed


def test_turn_rejects_naive_timestamp():
    with pytest.raises(ValueError):
        EvaluationTurn("bad", "user", "text", "2026-09-29T00:00:00")


def test_case_rejects_duplicate_event_ids():
    original = sample_case()
    with pytest.raises(ValueError):
        EvaluationCase(
            "duplicate", original.initial, original.initial,
            original.initial_queries, original.update_queries,
        )
