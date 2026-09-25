import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory import (
    CallableOntologyEvidenceVerifier, Claim, ClaimStatus, MemoryScope,
    OntologyBackfillCheckpoint, OntologyCheckpointConflict, OntologyClaimPage,
    OntologyClass, OntologyProperty, OntologySchema,
    PluginContext, PluginResourceLimits, Provenance,
    SQLiteOntologyCheckpointSink, SQLiteOntologyStore,
    backfill_ontology_memory, ontology_schema_digest,
)


SCOPE = MemoryScope("checkpoint-test", user_id="alice")


def sink(path, **overrides):
    return SQLiteOntologyCheckpointSink(path, **(
        dict(scope=SCOPE, snapshot_id="snapshot-1", schema_digest="digest-1") | overrides
    ))


def checkpoint(count=2, cursor="page-2", completed=False):
    return OntologyBackfillCheckpoint(
        SCOPE, "snapshot-1", "digest-1", cursor, count, completed,
    )


def test_checkpoint_survives_reopen_and_identical_retries(tmp_path):
    async def scenario():
        path = tmp_path / "checkpoint.db"
        store = sink(path)
        await store.initialize()
        assert await store.load() is None
        await store.save(checkpoint())
        await store.save(checkpoint())
        reopened = sink(path)
        assert await reopened.load() == checkpoint()
        # Empty terminal source page can complete without consuming more claims.
        final = checkpoint(cursor=None, completed=True)
        await reopened.save(final)
        await reopened.save(final)
        assert await store.load() == final
        with pytest.raises(OntologyCheckpointConflict, match="completed"):
            await reopened.save(checkpoint(4, "page-4"))
    asyncio.run(scenario())


@pytest.mark.parametrize("changes", [
    {"scope": MemoryScope("foreign")}, {"snapshot_id": "other"}, {"schema_digest": "other"},
])
def test_job_binding_isolated_on_read_and_write(tmp_path, changes):
    async def scenario():
        path = tmp_path / "checkpoint.db"
        store = sink(path)
        await store.initialize()
        await store.save(checkpoint())
        foreign = sink(path, **changes)
        assert await foreign.load() is None
        with pytest.raises(ValueError, match="bound backfill job"):
            await store.save(replace(checkpoint(), **changes))
        assert await store.load() == checkpoint()
    asyncio.run(scenario())


@pytest.mark.parametrize("invalid", [
    checkpoint(1, "page-1"), checkpoint(2, "other-cursor"), checkpoint(3, "page-2"),
])
def test_invalid_progress_does_not_overwrite_checkpoint(tmp_path, invalid):
    async def scenario():
        store = sink(tmp_path / "checkpoint.db")
        await store.initialize()
        await store.save(checkpoint())
        with pytest.raises(OntologyCheckpointConflict):
            await store.save(invalid)
        assert await store.load() == checkpoint()
    asyncio.run(scenario())


def test_concurrent_saves_cannot_regress_progress(tmp_path):
    async def scenario():
        path = tmp_path / "checkpoint.db"
        store = sink(path)
        await store.initialize()
        results = await asyncio.gather(
            sink(path).save(checkpoint(4, "page-4")),
            sink(path).save(checkpoint(2, "page-2")),
            return_exceptions=True,
        )
        assert all(result is None or isinstance(result, OntologyCheckpointConflict) for result in results)
        assert await store.load() == checkpoint(4, "page-4")
    asyncio.run(scenario())


def test_backfill_resumes_with_reopened_durable_sink(tmp_path):
    now = datetime(2026, 9, 23, tzinfo=UTC)
    schema = OntologySchema(
        "durable.test", "1.0.0", (OntologyClass("person", "Person"),),
        (OntologyProperty("knows", "Knows", "person", "person"),), created_at=now,
    )
    claims = tuple(Claim(
        id=f"claim-{i}", scope=SCOPE, key=f"knows.{i}",
        value={"$ontology": {
            "subject": {"id": "person:alice", "class": "person", "label": "Alice"},
            "predicate": "knows",
            "object": {"id": f"person:p{i}", "class": "person", "label": f"Person {i}"},
        }},
        text=f"Alice knows person {i}", confidence=0.9, importance=0.5,
        status=ClaimStatus.ACTIVE, provenance=Provenance(source_event_ids=(f"event-{i}",)),
        valid_from=now, created_at=now,
    ) for i in range(3))

    class Source:
        def __init__(self):
            self.cursors = []

        async def read_page(self, scope, snapshot_id, *, cursor, limit):
            self.cursors.append(cursor)
            offset = int(cursor or 0)
            page = claims[offset:offset + limit]
            end = offset + len(page)
            return OntologyClaimPage(page, str(end) if end < len(claims) else None)

    async def verify(scope, ids):
        return scope == SCOPE and all(value in {"event-0", "event-1", "event-2"} for value in ids)

    async def scenario():
        path = tmp_path / "checkpoint.db"
        binding = dict(schema_digest=ontology_schema_digest(schema))
        first_sink = sink(path, **binding)
        await first_sink.initialize()
        args = dict(
            source=Source(), snapshot_id="snapshot-1", schema=schema,
            store=SQLiteOntologyStore(tmp_path / "ontology.db"),
            evidence_verifier=CallableOntologyEvidenceVerifier(verify),
            context=PluginContext(SCOPE, PluginResourceLimits(max_batch_size=2)),
        )
        first = await backfill_ontology_memory(**args, checkpoint_sink=first_sink, max_batches=1)
        assert first.processed_claims == 2 and not first.completed
        reopened = sink(path, **binding)
        second_source = Source()
        args["source"] = second_source
        args["store"] = SQLiteOntologyStore(tmp_path / "ontology.db")
        final = await backfill_ontology_memory(
            **args, checkpoint_sink=reopened, resume=await reopened.load(),
        )
        assert second_source.cursors == ["2"]
        assert final.completed and final.processed_claims == 3
        assert await reopened.load() == final
        found = await args["store"].search(
            "Alice knows", SCOPE, ontology_id=schema.ontology_id,
            ontology_version=schema.version, at_time=now, limit=8, max_scan=64,
        )
        assert len(found) == 3
    asyncio.run(scenario())
