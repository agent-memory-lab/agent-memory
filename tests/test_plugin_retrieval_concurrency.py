"""Loaded retrievers share capacity across requests and orchestrators."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest

from agent_memory import MemoryQuery, PluginKind, PluginLoader, PluginResourceLimits
from agent_memory.retrieval.parallel import ParallelRetrieverOrchestrator
from test_plugin_protocol_loader import FakeRetriever, context, manifest


async def load(plugin, *, host_limits=None, deadline=None):
    loader = PluginLoader(core_version="0.1.2")
    name = plugin.descriptor.name
    loader.register(name=name, kind=PluginKind.RETRIEVER, factory=lambda: plugin)
    entry = await loader.load(
        name, PluginKind.RETRIEVER, context(limits=host_limits, deadline=deadline)
    )
    return loader, entry


def request(entry):
    return MemoryQuery(entry.context.scope, "query")


@pytest.mark.parametrize("host_cap,plugin_cap", [(1, 3), (3, 1), (2, 3)])
@pytest.mark.parametrize("separate_orchestrators", [False, True])
def test_shared_loaded_plugin_honors_effective_capacity(host_cap, plugin_cap, separate_orchestrators):
    async def scenario():
        class CountingRetriever(FakeRetriever):
            active = 0
            peak = 0

            async def retrieve(self, query, ctx):
                self.active += 1
                self.peak = max(self.peak, self.active)
                try:
                    await asyncio.sleep(0)
                    return ()
                finally:
                    self.active -= 1

        plugin = CountingRetriever(manifest(
            "counting", limits=PluginResourceLimits(max_concurrency=plugin_cap)
        ))
        loader, entry = await load(plugin, host_limits=PluginResourceLimits(max_concurrency=host_cap))
        shared = ParallelRetrieverOrchestrator()
        try:
            results = await asyncio.gather(*(
                (ParallelRetrieverOrchestrator() if separate_orchestrators else shared).retrieve(
                    request(entry), (entry,)
                ) for _ in range(6)
            ))
            assert all(not result.degraded for result in results)
            assert plugin.peak == min(host_cap, plugin_cap)
            assert plugin.active == 0
        finally:
            await loader.close()

    asyncio.run(scenario())


def test_independent_loaded_plugins_can_run_at_the_same_time():
    async def scenario():
        both_entered = asyncio.Event()
        entered = []

        class WaitingRetriever(FakeRetriever):
            async def retrieve(self, query, ctx):
                entered.append(self.descriptor.name)
                if len(entered) == 2:
                    both_entered.set()
                await both_entered.wait()
                return ()

        loaders, entries = [], []
        for name in ("first", "second"):
            loader, entry = await load(WaitingRetriever(manifest(
                name, limits=PluginResourceLimits(max_concurrency=1)
            )))
            loaders.append(loader)
            entries.append(entry)
        try:
            result = await ParallelRetrieverOrchestrator().retrieve(request(entries[0]), entries)
            assert not result.degraded
            assert sorted(entered) == ["first", "second"]
        finally:
            for loader in loaders:
                await loader.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["error", "cancel", "timeout"])
def test_execution_failure_releases_capacity(failure):
    async def scenario():
        class FailsOnce(FakeRetriever):
            calls = 0

            async def retrieve(self, query, ctx):
                self.calls += 1
                if self.calls == 1:
                    if failure == "error":
                        raise RuntimeError("retrieval failed")
                    if failure == "cancel":
                        raise asyncio.CancelledError("plugin stopped")
                    await asyncio.Event().wait()
                return ()

        plugin = FailsOnce(manifest(
            "fails-once", limits=PluginResourceLimits(max_concurrency=1, timeout_ms=30)
        ))
        loader, entry = await load(plugin)
        orchestrator = ParallelRetrieverOrchestrator()
        try:
            assert (await orchestrator.retrieve(request(entry), (entry,))).degraded
            assert not (await orchestrator.retrieve(request(entry), (entry,))).degraded
            assert plugin.calls == 2
        finally:
            await loader.close()

    asyncio.run(scenario())


def test_cancelled_waiter_never_dispatches_and_does_not_lose_capacity():
    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        class WaitingRetriever(FakeRetriever):
            calls = 0

            async def retrieve(self, query, ctx):
                self.calls += 1
                entered.set()
                await release.wait()
                return ()

        plugin = WaitingRetriever(manifest("waiting", limits=PluginResourceLimits(max_concurrency=1)))
        loader, entry = await load(plugin)
        orchestrator = ParallelRetrieverOrchestrator()
        first = asyncio.create_task(orchestrator.retrieve(request(entry), (entry,)))
        await asyncio.wait_for(entered.wait(), 1)
        waiter = asyncio.create_task(orchestrator.retrieve(request(entry), (entry,)))
        try:
            # Let the second request reach its capacity wait (or incorrectly dispatch).
            for _ in range(5):
                await asyncio.sleep(0)
            waiter.cancel("caller cancelled queued request")
            with pytest.raises(asyncio.CancelledError):
                await waiter
            release.set()
            assert not (await first).degraded
            assert plugin.calls == 1
            assert not (await orchestrator.retrieve(request(entry), (entry,))).degraded
            assert plugin.calls == 2
        finally:
            release.set()
            await asyncio.gather(first, waiter, return_exceptions=True)
            await loader.close()

    asyncio.run(scenario())


def test_timeout_includes_time_waiting_for_capacity():
    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        class SlowRetriever(FakeRetriever):
            calls = 0

            async def retrieve(self, query, ctx):
                self.calls += 1
                if self.calls == 1:
                    entered.set()
                    await release.wait()
                else:
                    await asyncio.sleep(0.18)
                return ()

        plugin = SlowRetriever(manifest(
            "slow", limits=PluginResourceLimits(max_concurrency=1, timeout_ms=300)
        ))
        loader, entry = await load(plugin)
        orchestrator = ParallelRetrieverOrchestrator()
        first = asyncio.create_task(orchestrator.retrieve(request(entry), (entry,)))
        await asyncio.wait_for(entered.wait(), 1)
        second = asyncio.create_task(orchestrator.retrieve(request(entry), (entry,)))
        timer = asyncio.get_running_loop().call_later(0.18, release.set)
        try:
            assert not (await first).degraded
            result = await second
            assert result.degraded
            assert result.traces[0].reason == "TimeoutError"
        finally:
            timer.cancel()
            release.set()
            await asyncio.gather(first, second, return_exceptions=True)
            await loader.close()

    asyncio.run(scenario())


def test_expired_host_deadline_does_not_dispatch():
    async def scenario():
        plugin = FakeRetriever(manifest("expired"))
        calls = []

        async def retrieve(query, ctx):
            calls.append(query)
            return ()

        plugin.retrieve = retrieve
        loader, entry = await load(plugin)
        entry = replace(entry, context=replace(
            entry.context, deadline=entry.context.clock.now() - timedelta(seconds=1)
        ))
        try:
            result = await ParallelRetrieverOrchestrator().retrieve(request(entry), (entry,))
            assert result.degraded
            assert result.traces[0].reason == "TimeoutError"
            assert calls == []
        finally:
            await loader.close()

    asyncio.run(scenario())
