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
        # Correctness with real disk IO must not depend on a 1-second SLA.
        context=PluginContext(SCOPE, PluginResourceLimits(max_batch_size=2, timeout_ms=10_000)),
        phase_timeout_ms=10_000,
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


@pytest.mark.parametrize("phase", ["initialize", "read_page", "consolidate", "save_checkpoint"])
def test_timeout_reports_phase_and_retry_preserves_claims(tmp_path, monkeypatch, phase):
    from agent_memory.ontology_memory import OntologyProjectionConsolidatorPlugin

    async def scenario():
        source, sink = Source([claim(i) for i in range(5)]), Sink()
        args = options(tmp_path, source, sink)
        first = await backfill_ontology_memory(**args, max_batches=1)
        original_initialize = OntologyProjectionConsolidatorPlugin.initialize
        original_consolidate = OntologyProjectionConsolidatorPlugin.consolidate
        original_read = source.read_page
        original_save = sink.save

        async def interrupted_initialize(plugin, context):
            await original_initialize(plugin, context)
            raise TimeoutError("injected initialization timeout")

        async def interrupted_read(*a, **kw):
            await original_read(*a, **kw)
            raise TimeoutError("injected source timeout")

        async def interrupted_consolidate(plugin, request, context):
            # Commit the projection before failure to exercise replay safety.
            await original_consolidate(plugin, request, context)
            raise TimeoutError("injected projection timeout")

        async def interrupted_save(checkpoint):
            # Model a durable checkpoint whose acknowledgement was lost.
            await original_save(checkpoint)
            raise TimeoutError("injected checkpoint acknowledgement timeout")

        with monkeypatch.context() as patch:
            if phase == "initialize":
                patch.setattr(OntologyProjectionConsolidatorPlugin, "initialize", interrupted_initialize)
            elif phase == "read_page":
                patch.setattr(source, "read_page", interrupted_read)
            elif phase == "consolidate":
                patch.setattr(OntologyProjectionConsolidatorPlugin, "consolidate", interrupted_consolidate)
            else:
                patch.setattr(sink, "save", interrupted_save)
            with pytest.raises(TimeoutError) as caught:
                await backfill_ontology_memory(**args, resume=first)
        assert any(f"phase={phase};" in note for note in caught.value.__notes__)
        assert first.cursor == "2" and first.processed_claims == 2
        if phase != "save_checkpoint":
            assert sink.checkpoints == [first]
        # Replaying the last acknowledged checkpoint is safe even when the
        # failed operation had already persisted projections/checkpoints.
        final = await backfill_ontology_memory(**args, resume=first)
        assert final.completed and final.processed_claims == 5
        found = await args["store"].search(
            "Alice knows", SCOPE, ontology_id=SCHEMA.ontology_id,
            ontology_version=SCHEMA.version, at_time=NOW, limit=8, max_scan=64,
        )
        assert {match.item.id for match in found}.__len__() == 5
    asyncio.run(scenario())


def test_host_timeout_still_caps_explicit_phase_budget(tmp_path, monkeypatch):
    from agent_memory.ontology_memory import OntologyProjectionConsolidatorPlugin

    async def scenario():
        source, sink = Source([claim(1)]), Sink()
        args = options(tmp_path, source, sink)
        # Isolate timer behavior from disk/worker scheduling and initialization.
        async def initialized(plugin, context):
            return None

        cancelled = asyncio.Event()
        async def blocked_read(*a, **kw):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        args["context"] = PluginContext(SCOPE, PluginResourceLimits(max_batch_size=2, timeout_ms=20))
        with monkeypatch.context() as patch:
            patch.setattr(OntologyProjectionConsolidatorPlugin, "initialize", initialized)
            patch.setattr(source, "read_page", blocked_read)
            with pytest.raises(TimeoutError) as caught:
                await backfill_ontology_memory(**args)
        assert cancelled.is_set()
        assert sink.checkpoints == []
        assert any("phase=read_page; timeout_ms=20;" in note for note in caught.value.__notes__)
    asyncio.run(scenario())


@pytest.mark.parametrize("budget", [True, 0, -1, 300_001, 1.5])
def test_invalid_phase_timeout_rejected_before_io(tmp_path, budget):
    async def scenario():
        source, sink = Source([claim(1)]), Sink()
        args = options(tmp_path, source, sink)
        args["phase_timeout_ms"] = budget
        with pytest.raises(ValueError, match="phase_timeout_ms"):
            await backfill_ontology_memory(**args)
        assert source.calls == [] and sink.checkpoints == []
    asyncio.run(scenario())
