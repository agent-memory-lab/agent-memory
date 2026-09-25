import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory import (
    BackfillGatedOntologyAuthorizer, MemoryScope, OntologyBackfillCheckpoint,
    OntologyClass, OntologyProperty, OntologySchema, OntologySwitchRequest,
    SQLiteOntologyCheckpointSink, SQLiteOntologyRegistry, ontology_schema_digest,
)


SCOPE = MemoryScope("readiness", user_id="alice")
SCHEMA = OntologySchema(
    "readiness.test", "1.0.0", (OntologyClass("person", "Person"),),
    (OntologyProperty("name", "Name", "person"),),
    created_at=datetime(2026, 9, 23, tzinfo=UTC),
)
DIGEST = ontology_schema_digest(SCHEMA)
COMPLETE = OntologyBackfillCheckpoint(SCOPE, "snapshot-1", DIGEST, processed_claims=5, completed=True)
REQUEST = OntologySwitchRequest(SCOPE, None, SCHEMA, DIGEST, "activate", "target validated")


class Reader:
    def __init__(self, checkpoint):
        self.checkpoint = checkpoint

    async def load(self):
        return self.checkpoint


class Host:
    def __init__(self, decision=True):
        self.calls = []
        self.decision = decision

    async def authorize(self, request):
        self.calls.append(request)
        return self.decision


@pytest.mark.parametrize("checkpoint", [
    None,
    OntologyBackfillCheckpoint(SCOPE, "snapshot-1", DIGEST),
    replace(COMPLETE, scope=MemoryScope("foreign")),
    replace(COMPLETE, snapshot_id="other"),
    replace(COMPLETE, schema_digest="other"),
])
def test_unready_checkpoint_denies_before_host_approval(checkpoint):
    async def scenario():
        host = Host()
        gate = BackfillGatedOntologyAuthorizer(Reader(checkpoint), host, snapshot_id="snapshot-1")
        assert await gate.authorize(REQUEST) is False
        assert host.calls == []
    asyncio.run(scenario())


@pytest.mark.parametrize("decision", [False, None, 1, "approved"])
def test_completed_backfill_does_not_override_host_denial(decision):
    async def scenario():
        host = Host(decision)
        gate = BackfillGatedOntologyAuthorizer(Reader(COMPLETE), host, snapshot_id="snapshot-1")
        assert await gate.authorize(REQUEST) is False
        assert host.calls == [REQUEST]
    asyncio.run(scenario())


def test_checkpoint_change_during_approval_denies_switch():
    async def scenario():
        reader = Reader(COMPLETE)

        class ChangingHost:
            async def authorize(self, request):
                reader.checkpoint = replace(COMPLETE, processed_claims=6)
                return True

        gate = BackfillGatedOntologyAuthorizer(reader, ChangingHost(), snapshot_id="snapshot-1")
        assert await gate.authorize(REQUEST) is False
    asyncio.run(scenario())


def test_forged_target_digest_and_unknown_action_denied():
    async def scenario():
        host = Host()
        gate = BackfillGatedOntologyAuthorizer(Reader(COMPLETE), host, snapshot_id="snapshot-1")
        assert await gate.authorize(replace(REQUEST, target_digest="forged")) is False
        assert await gate.authorize(replace(REQUEST, action="unknown")) is False
        assert host.calls == []
    asyncio.run(scenario())


def test_durable_completion_gates_real_registry_activation(tmp_path):
    async def scenario():
        registry = SQLiteOntologyRegistry(tmp_path / "registry.db")
        await registry.initialize()
        await registry.register(SCOPE, SCHEMA)
        sink = SQLiteOntologyCheckpointSink(
            tmp_path / "checkpoints.db", scope=SCOPE, snapshot_id="snapshot-1", schema_digest=DIGEST,
        )
        await sink.initialize()
        host = Host()
        gate = BackfillGatedOntologyAuthorizer(sink, host, snapshot_id="snapshot-1")
        with pytest.raises(PermissionError):
            await registry.activate(
                SCOPE, SCHEMA.ontology_id, SCHEMA.version, expected_generation=0,
                reason="not ready yet", authorizer=gate,
            )
        assert await registry.active(SCOPE, SCHEMA.ontology_id) is None
        assert await registry.history(SCOPE, SCHEMA.ontology_id) == ()
        await sink.save(COMPLETE)
        activation = await registry.activate(
            SCOPE, SCHEMA.ontology_id, SCHEMA.version, expected_generation=0,
            reason="index validated by host", authorizer=gate,
        )
        assert activation.version == SCHEMA.version
        assert activation.generation == 1
        assert len(host.calls) == 1
        assert len(await registry.history(SCOPE, SCHEMA.ontology_id)) == 1
    asyncio.run(scenario())


def test_checkpoint_storage_failure_propagates():
    class BrokenReader:
        async def load(self):
            raise OSError("checkpoint database unavailable")

    async def scenario():
        host = Host()
        gate = BackfillGatedOntologyAuthorizer(BrokenReader(), host, snapshot_id="snapshot-1")
        with pytest.raises(OSError, match="checkpoint database"):
            await gate.authorize(REQUEST)
        assert host.calls == []
    asyncio.run(scenario())
