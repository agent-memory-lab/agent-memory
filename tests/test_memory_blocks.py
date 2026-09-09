from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from agent_memory import (
    ForgetMode,
    MCPMemoryTools,
    MCPRequestContext,
    MCPToolError,
    MemoryBlock,
    MemoryEvent,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
    build_local_kernel,
)


def run(coroutine):
    return asyncio.run(coroutine)


def test_memory_block_crud_search_budget_and_forget(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", session_id="session-a")
        event = MemoryEvent(scope, "user.message", "Use concise technical answers.")
        await kernel.ingest_event(event)

        created = await kernel.write_block(
            MemoryBlock(
                scope=scope,
                title="Response preference",
                content="Prefer concise technical answers.",
                event_ids=(event.id,),
                token_budget=64,
            )
        )
        assert created.version == 1
        assert await kernel.read_block(scope, created.id) == created
        assert (await kernel.search_blocks(scope, "concise", limit=2))[0].id == created.id

        recalled = await kernel.retrieve(MemoryQuery(scope, "concise", token_budget=64))
        assert any(
            item.id == created.id and item.kind == MemoryKind.BLOCK
            for item in recalled.relevant_memories
        )

        updated = await kernel.write_block(
            replace(created, content="Always prefer concise technical answers."),
            expected_version=1,
        )
        assert updated.version == 2
        with pytest.raises(RuntimeError, match="version conflict"):
            await kernel.write_block(created, expected_version=1)

        forgotten = await kernel.forget_block(scope, created.id, ForgetMode.ARCHIVE)
        assert forgotten.affected_artifacts == 1
        assert await kernel.read_block(scope, created.id) is None

    run(scenario())


def test_memory_block_rejects_missing_evidence_and_oversized_content(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", session_id="session-a")

        with pytest.raises(ValueError, match="source evidence"):
            await kernel.write_block(
                MemoryBlock(
                    scope=scope,
                    title="Unsupported",
                    content="No matching event exists.",
                    event_ids=("missing",),
                    token_budget=64,
                )
            )
        with pytest.raises(ValueError, match="token_budget"):
            await kernel.write_block(
                MemoryBlock(
                    scope=scope,
                    title="Oversized",
                    content="x" * 1000,
                    event_ids=("missing",),
                    token_budget=16,
                )
            )

    run(scenario())


def test_mcp_memory_block_tools_keep_scope_and_erase_authority(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", session_id="session-a")
        event = MemoryEvent(scope, "user.message", "Remember the release checklist.")
        await kernel.ingest_event(event)
        tools = MCPMemoryTools(kernel)
        context = MCPRequestContext(scope=scope)

        names = {tool["name"] for tool in tools.list_tools()}
        assert {
            "memory_block_read",
            "memory_block_write",
            "memory_block_search",
            "memory_block_forget",
        } <= names

        written = await tools.call_tool(
            "memory_block_write",
            {
                "title": "Release checklist",
                "content": "Run security scan before publishing.",
                "event_ids": [event.id],
                "token_budget": 64,
            },
            context,
        )
        block_id = written["block"]["id"]
        read = await tools.call_tool("memory_block_read", {"block_id": block_id}, context)
        assert read["block"]["scope"]["tenant_id"] == "tenant-a"

        with pytest.raises(MCPToolError, match="authorized"):
            await tools.call_tool(
                "memory_block_forget",
                {"block_id": block_id, "mode": "erase"},
                context,
            )

    run(scenario())
