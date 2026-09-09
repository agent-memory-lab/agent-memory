from __future__ import annotations

import asyncio

from agent_memory import (
    ForgetMode,
    ForgetRequest,
    MemoryBlock,
    MemoryEvent,
    MemoryScope,
    build_local_kernel,
)


def run(coroutine):
    return asyncio.run(coroutine)


def test_forget_event_updates_then_removes_dependent_block(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", session_id="session-a")
        first = MemoryEvent(scope, "user.message", "Prefer concise answers.")
        second = MemoryEvent(scope, "user.message", "Keep technical details.")
        await kernel.ingest_event(first)
        await kernel.ingest_event(second)

        block = await kernel.write_block(
            MemoryBlock(
                scope=scope,
                title="Response style",
                content="Use concise answers while retaining technical detail.",
                event_ids=(first.id, second.id),
                token_budget=64,
            )
        )

        partial = await kernel.forget(
            ForgetRequest(
                scope=scope,
                memory_ids=(first.id,),
                mode=ForgetMode.ERASE,
            )
        )
        updated = await kernel.read_block(scope, block.id)
        assert partial.affected_artifacts == 1
        assert updated is not None
        assert updated.event_ids == (second.id,)
        assert updated.provenance.source_event_ids == (second.id,)
        assert updated.version == 2

        final = await kernel.forget(
            ForgetRequest(
                scope=scope,
                memory_ids=(second.id,),
                mode=ForgetMode.ERASE,
            )
        )
        assert final.affected_artifacts == 1
        assert await kernel.read_block(scope, block.id) is None

    run(scenario())


def test_archive_last_source_archives_dependent_block(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", session_id="session-a")
        event = MemoryEvent(scope, "tool.completed", "Release scan passed.")
        await kernel.ingest_event(event)
        block = await kernel.write_block(
            MemoryBlock(
                scope=scope,
                title="Release evidence",
                content="The release scan passed.",
                event_ids=(event.id,),
                token_budget=32,
            )
        )

        result = await kernel.forget(
            ForgetRequest(
                scope=scope,
                memory_ids=(event.id,),
                mode=ForgetMode.ARCHIVE,
            )
        )
        assert result.affected_artifacts == 1
        assert await kernel.read_block(scope, block.id) is None

    run(scenario())
