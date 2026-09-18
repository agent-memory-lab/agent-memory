"""Explicit AgentMemory integration with a Plugin Protocol v1 retriever."""

import asyncio
from dataclasses import replace

import pytest

from agent_memory import (
    AgentMemory,
    MemoryScope,
    PluginContext,
    PluginError,
    PluginKind,
    PluginLoader,
    PluginResourceLimits,
)
from agent_memory.retriever_plugin import register_sqlite_lexical_retriever
from agent_memory.sqlite import SQLiteMemoryRepository


def test_loaded_sqlite_retriever_is_opt_in_and_scope_checked(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            await memory.remember("orchid migration checkpoint")
            loader = PluginLoader(core_version="0.1.0")
            register_sqlite_lexical_retriever(loader, SQLiteMemoryRepository(database))
            loaded = await loader.load(
                "scoped-lexical",
                PluginKind.RETRIEVER,
                PluginContext(
                    scope=memory.scope,
                    resource_limits=PluginResourceLimits(max_candidates=1, max_batch_size=2),
                    config={},
                    request_id="request-1",
                ),
                required_capabilities=("lexical.search",),
            )
            try:
                candidates = await memory.retrieve_candidates("ORCHID", loaded, limit=5)
                assert len(candidates) == 1
                assert candidates[0].retrieval_method == "lexical"
                assert candidates[0].source_event_ids == (candidates[0].item.id,)
                assert candidates[0].rank == 1

                bundle = await memory.recall("orchid")
                assert bundle is not None

                with pytest.raises(ValueError, match="positive integer"):
                    await memory.retrieve_candidates("orchid", loaded, limit=0)
                async with AgentMemory.local(
                    database, scope=MemoryScope("foreign-tenant")
                ) as foreign:
                    with pytest.raises(ValueError, match="scope"):
                        await foreign.retrieve_candidates("orchid", loaded)
            finally:
                await loader.close()

            with pytest.raises(PluginError, match="not active"):
                await memory.retrieve_candidates("orchid", loaded)

    asyncio.run(scenario())


def test_loaded_retriever_budget_and_timeout_are_enforced(tmp_path) -> None:
    class SlowRetriever:
        async def retrieve(self, query, context):
            await asyncio.sleep(0.05)
            return ()

    class ExcessiveRetriever:
        def __init__(self, candidate):
            self.candidate = candidate

        async def retrieve(self, query, context):
            return (self.candidate, self.candidate)

    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            await memory.remember("orchid migration checkpoint")
            loader = PluginLoader(core_version="0.1.0")
            register_sqlite_lexical_retriever(loader, SQLiteMemoryRepository(database))
            loaded = await loader.load(
                "scoped-lexical",
                PluginKind.RETRIEVER,
                PluginContext(
                    scope=memory.scope,
                    resource_limits=PluginResourceLimits(),
                    request_id="request-2",
                ),
            )
            try:
                candidate = (await memory.retrieve_candidates("orchid", loaded))[0]
                small_context = replace(
                    loaded.context,
                    resource_limits=PluginResourceLimits(
                        timeout_ms=1, max_candidates=1, max_batch_size=1
                    ),
                )
                with pytest.raises(TimeoutError):
                    await memory.retrieve_candidates(
                        "orchid",
                        replace(loaded, context=small_context, instance=SlowRetriever()),
                    )
                with pytest.raises(ValueError, match="excessive"):
                    await memory.retrieve_candidates(
                        "orchid",
                        replace(
                            loaded,
                            context=small_context,
                            instance=ExcessiveRetriever(candidate),
                        ),
                    )
            finally:
                await loader.close()

    asyncio.run(scenario())
