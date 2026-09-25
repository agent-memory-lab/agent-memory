import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory import (
    CallableOntologyEvidenceVerifier, Claim, ClaimStatus, MemoryScope,
    OntologyClass, OntologyProperty, OntologySchema, PluginContext,
    PluginResourceLimits, Provenance, SQLiteOntologyStore,
    OntologyClaimPage, backfill_ontology_memory,
)


NOW = datetime(2026, 9, 23, tzinfo=UTC)
SCOPE = MemoryScope("backfill", user_id="alice")
SCHEMA = OntologySchema(
    "backfill.test", "1.0.0", (OntologyClass("person", "Person"),),
    (OntologyProperty("knows", "Knows", "person", "person"),), created_at=NOW,
)


def claim(number):
    return Claim(
        id=f"claim-{number}", scope=SCOPE, key=f"knows.{number}",
        value={"$ontology": {
            "subject": {"id": "person:alice", "class": "person", "label": "Alice"},
            "predicate": "knows",
            "object": {"id": f"person:p{number}", "class": "person", "label": f"Person {number}"},
        }},
        text=f"Alice knows person {number}", confidence=0.9, importance=0.5,
        status=ClaimStatus.ACTIVE, provenance=Provenance(source_event_ids=(f"event-{number}",)),
        valid_from=NOW, created_at=NOW,
    )


class Source:
    def __init__(self, values):
        self.values = values
        self.calls = []

    async def read_page(self, scope, snapshot_id, *, cursor, limit):
        self.calls.append((cursor, limit))
        offset = int(cursor or 0)
        page = self.values[offset:offset + limit]
        end = offset + len(page)
        return OntologyClaimPage(tuple(page), str(end) if end < len(self.values) else None)


class Sink:
    def __init__(self):
        self.checkpoints = []

    async def save(self, checkpoint):
        self.checkpoints.append(checkpoint)


async def verify(scope, ids):
    return scope == SCOPE and all(value.startswith("event-") for value in ids)


def options(tmp_path, source, sink):
    return dict(
        source=source, snapshot_id="snapshot-1", schema=SCHEMA,
        store=SQLiteOntologyStore(tmp_path / "ontology.db"),
        evidence_verifier=CallableOntologyEvidenceVerifier(verify),
        context=PluginContext(SCOPE, PluginResourceLimits(max_batch_size=2)),
        checkpoint_sink=sink,
    )


def test_bounded_backfill_resumes_without_skipping_claims(tmp_path):
    async def scenario():
        source, sink = Source([claim(i) for i in range(5)]), Sink()
        args = options(tmp_path, source, sink)
        first = await backfill_ontology_memory(**args, max_batches=1)
        assert first.processed_claims == 2 and not first.completed
        assert first.cursor == "2"
        final = await backfill_ontology_memory(**args, resume=first)
        assert final.processed_claims == 5 and final.completed
        assert source.calls == [(None, 2), ("2", 2), ("4", 2)]
        found = await args["store"].search(
            "Alice knows", SCOPE, ontology_id=SCHEMA.ontology_id,
            ontology_version=SCHEMA.version, at_time=NOW, limit=8, max_scan=64,
        )
        assert len(found) == 5
        assert await backfill_ontology_memory(**args, resume=final) == final
        assert len(source.calls) == 3
    asyncio.run(scenario())


def test_checkpoint_failure_can_replay_batch_idempotently(tmp_path):
    class FailingSink(Sink):
        async def save(self, checkpoint):
            raise OSError("checkpoint unavailable")

    async def scenario():
        source = Source([claim(1), claim(2)])
        args = options(tmp_path, source, FailingSink())
        with pytest.raises(OSError, match="checkpoint unavailable"):
            await backfill_ontology_memory(**args)
        args["checkpoint_sink"] = Sink()
        final = await backfill_ontology_memory(**args)
        assert final.completed and final.processed_claims == 2
        found = await args["store"].search(
            "Alice knows", SCOPE, ontology_id=SCHEMA.ontology_id,
            ontology_version=SCHEMA.version, at_time=NOW, limit=8, max_scan=64,
        )
        assert len(found) == 2
    asyncio.run(scenario())


def test_resume_rejects_wrong_snapshot_and_schema(tmp_path):
    async def scenario():
        source, sink = Source([claim(1), claim(2), claim(3)]), Sink()
        args = options(tmp_path, source, sink)
        first = await backfill_ontology_memory(**args, max_batches=1)
        for changes in ({"snapshot_id": "other"}, {"schema": replace(SCHEMA, version="1.0.1")}):
            with pytest.raises(ValueError, match="checkpoint does not match"):
                await backfill_ontology_memory(**(args | changes), resume=first)
        assert len(source.calls) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["cross_scope", "over_limit", "stuck"])
def test_invalid_source_batch_never_checkpoints(tmp_path, mode):
    class InvalidSource:
        async def read_page(self, scope, snapshot_id, *, cursor, limit):
            if mode == "cross_scope":
                return OntologyClaimPage((replace(claim(1), scope=MemoryScope("foreign")),), None)
            if mode == "over_limit":
                return OntologyClaimPage(tuple(claim(i) for i in range(limit + 1)), None)
            return OntologyClaimPage((), "stuck")

    async def scenario():
        sink = Sink()
        args = options(tmp_path, InvalidSource(), sink)
        with pytest.raises(ValueError):
            await backfill_ontology_memory(**args)
        assert sink.checkpoints == []
    asyncio.run(scenario())


def test_failed_evidence_does_not_advance_checkpoint(tmp_path):
    async def reject(scope, ids):
        return False

    async def scenario():
        sink = Sink()
        args = options(tmp_path, Source([claim(1)]), sink)
        args["evidence_verifier"] = CallableOntologyEvidenceVerifier(reject)
        with pytest.raises(ValueError, match="evidence"):
            await backfill_ontology_memory(**args)
        assert sink.checkpoints == []
    asyncio.run(scenario())
