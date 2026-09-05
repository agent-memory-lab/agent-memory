import asyncio

from agent_memory import (
    ArtifactStatus,
    Episode,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryQuery,
    MemoryScope,
    Procedure,
    Provenance,
    build_local_kernel,
)


def run(coroutine):
    return asyncio.run(coroutine)


def test_idempotency_supersession_and_state(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope(
            tenant_id="tenant-a",
            user_id="user-a",
            agent_id="agent-a",
            session_id="session-a",
        )

        first = MemoryEvent(
            scope=scope,
            event_type="preference.changed",
            content="Use email.",
            idempotency_key="event-1",
            metadata={
                "claims": [
                    {
                        "key": "contact.preference",
                        "value": "email",
                        "text": "Use email.",
                        "scope": "user",
                    }
                ]
            },
        )
        result = await kernel.ingest_event(first)
        duplicate = await kernel.ingest_event(first)
        assert not result.duplicate
        assert duplicate.duplicate
        assert duplicate.claim_ids == result.claim_ids

        second = MemoryEvent(
            scope=scope,
            event_type="preference.changed",
            content="Use SMS.",
            idempotency_key="event-2",
            metadata={
                "claims": [
                    {
                        "key": "contact.preference",
                        "value": "sms",
                        "text": "Use SMS.",
                        "scope": "user",
                    }
                ]
            },
        )
        changed = await kernel.ingest_event(second)
        state = await kernel.get_state(scope)
        assert changed.superseded_claim_ids == result.claim_ids
        assert len(state) == 1
        assert state[0].value == "sms"
        assert state[0].version == 2

    run(scenario())


def test_four_channel_bundle_and_evolution_evidence(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")
        event = MemoryEvent(scope, "task.completed", "Published the release notes.")
        await kernel.ingest_event(event)

        episode = Episode(
            scope=scope,
            observation="A release needed documentation.",
            action="Generate and review release notes.",
            outcome="Release notes were accepted.",
            lesson="Review generated release notes before publishing.",
            quality=0.9,
            status=ArtifactStatus.ACTIVE,
            provenance=Provenance(source_event_ids=(event.id,)),
        )
        procedure = Procedure(
            scope=scope,
            name="Publish release notes",
            trigger="A release is ready.",
            steps=("Generate notes", "Review notes", "Publish notes"),
            success_conditions=("Notes are published",),
            status=ArtifactStatus.ACTIVE,
            provenance=Provenance(source_event_ids=(event.id,)),
        )
        await kernel.record_episode(episode)
        await kernel.publish_procedure(procedure)

        bundle = await kernel.retrieve(MemoryQuery(scope, "How do I publish release notes?"))
        assert bundle.episodes
        assert bundle.procedures
        assert bundle.capability_snapshot.procedural_memory
        assert not bundle.capability_snapshot.learned_policy

    run(scenario())


def test_archive_is_not_legal_erase(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", session_id="session-a")
        event = MemoryEvent(scope, "message", "Temporary content")
        await kernel.ingest_event(event)

        archived = await kernel.forget(
            ForgetRequest(scope=scope, memory_ids=(event.id,), mode=ForgetMode.ARCHIVE)
        )
        assert archived.mode == ForgetMode.ARCHIVE
        assert archived.affected_events == 1

        erased = await kernel.forget(
            ForgetRequest(scope=scope, memory_ids=(event.id,), mode=ForgetMode.ERASE)
        )
        assert erased.mode == ForgetMode.ERASE
        assert erased.affected_events == 1

    run(scenario())
