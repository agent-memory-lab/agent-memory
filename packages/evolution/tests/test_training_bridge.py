import asyncio
import json

import pytest

from agent_memory import MemoryScope
from agent_memory_evolution.training import MemoryTrainingBridge
from test_feedback_episode_builder import trajectory


SCOPE = MemoryScope("training-test")


class Host:
    allowed = True
    current = True
    evidence = True

    def __init__(self):
        self.calls = []

    async def authorize(self, scope, action, subject):
        self.calls.append(action)
        return self.allowed

    async def is_current(self, value):
        return self.current

    async def verify(self, scope, ids):
        return self.evidence

    def redact(self, row):
        row["decision"]["action"] = "redacted"
        return row

    async def submit(self, payload, *, idempotency_key):
        self.calls.append(("submit", idempotency_key))
        return "job-" + idempotency_key

    async def status(self, job_id):
        return "running"

    async def cancel(self, job_id):
        return "cancelled"

    def bridge(self, **kwargs):
        return MemoryTrainingBridge(authorize=self.authorize, is_current=self.is_current,
            redact=self.redact, evidence=self, trainer=self, **kwargs)


def test_training_export_redaction_idempotency_and_job_lifecycle(tmp_path):
    async def scenario():
        host = Host()
        bridge = host.bridge()
        rows = [trajectory(SCOPE, 1)]
        batch = await bridge.prepare(SCOPE, rows)
        assert batch == await bridge.prepare(SCOPE, rows)
        assert json.loads(batch.payload)["decision"]["action"] == "redacted"
        path = tmp_path / "dataset.jsonl"
        assert await bridge.export(SCOPE, rows, path) == batch
        assert path.read_bytes() == batch.payload
        with pytest.raises(FileExistsError):
            await bridge.export(SCOPE, rows, path)
        first, second = await bridge.submit(SCOPE, rows), await bridge.submit(SCOPE, rows)
        assert first == second
        assert await bridge.status(first) == "running"
        assert await bridge.cancel(first) == "cancelled"
        host.allowed = False
        with pytest.raises(PermissionError):
            await bridge.cancel(first)
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["authorization", "stale", "evidence", "scope", "duplicate", "budget"])
def test_training_rejects_untrusted_or_unbounded_inputs(failure):
    async def scenario():
        host = Host()
        rows = [trajectory(SCOPE, 1)]
        scope = SCOPE
        kwargs = {}
        if failure == "authorization":
            host.allowed = False
        elif failure == "stale":
            host.current = False
        elif failure == "evidence":
            host.evidence = False
        elif failure == "scope":
            scope = MemoryScope("foreign")
        elif failure == "duplicate":
            rows *= 2
        elif failure == "budget":
            kwargs["max_bytes"] = 1024
            host.redact = lambda row: {"large": "x" * 2048}
        with pytest.raises(PermissionError if failure == "authorization" else ValueError):
            await host.bridge(**kwargs).submit(scope, rows)
        assert not any(isinstance(c, tuple) and c[0] == "submit" for c in host.calls)
    asyncio.run(scenario())


def test_training_rechecks_feedback_after_redaction():
    async def scenario():
        host = Host()
        def redact(row):
            host.current = False
            return row
        host.redact = redact
        with pytest.raises(ValueError, match="changed"):
            await host.bridge().prepare(SCOPE, [trajectory(SCOPE, 1)])
    asyncio.run(scenario())


def test_training_rechecks_evidence_after_redaction():
    async def scenario():
        host = Host()
        def redact(row):
            host.evidence = False
            return row
        host.redact = redact
        with pytest.raises(ValueError, match="evidence"):
            await host.bridge().prepare(SCOPE, [trajectory(SCOPE, 1)])
    asyncio.run(scenario())
