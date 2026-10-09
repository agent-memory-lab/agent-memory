"""Shared DB reservations across independent repositories, threads and event loops.

Both SQLite and hosted live PostgreSQL use the same tests. No provider, cache,
service, repository lock, connection pool or event loop is shared by runtimes.
"""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_source_embeddings import Port, rows, setup

from agent_memory.derived import ObservationService
from agent_memory.retrieval.model_contracts import ModelError
from agent_memory.retrieval.source_embeddings import (
    HEADER,
    GovernedSourceEmbeddings,
    SourceEmbeddingAuthority,
)

store = base.store


def connection_target(repository):
    if hasattr(repository, "pool"):
        return "postgres", repository.pool.conninfo
    return "sqlite", repository._path


def traced_authority(service, configuration, coordinates, trace):
    trace.update(
        repository=id(service.repository),
        thread=threading.get_ident(),
        loop=id(asyncio.get_running_loop()),
        backend_pids=set(),
    )

    async def guard(uow, current):
        assert uow._repository is service.repository
        if hasattr(service.repository, "pool"):
            trace["backend_pids"].add(uow.connection.info.backend_pid)
        return current == coordinates

    return SourceEmbeddingAuthority(service, configuration=configuration, verify_coordinates=guard)


@asynccontextmanager
async def independent_runtime(target, scope, clock, configuration, coordinates):
    backend, location = target
    if backend == "postgres":
        from agent_memory_postgres.repository import PostgresMemoryRepository

        repository = PostgresMemoryRepository.from_dsn(location, max_size=2)
    else:
        from agent_memory.sqlite import SQLiteMemoryRepository

        repository = SQLiteMemoryRepository(location)
    try:
        await repository.initialize()
        service = ObservationService(
            repository,
            scope,
            base.POLICY,
            clock=lambda: clock[0],
            authority_id="local-host",
            authority_min_version=1,
        )
        trace = {}
        authority = traced_authority(service, replace(configuration), coordinates, trace)
        port = Port(authority.configuration)
        cache = GovernedSourceEmbeddings(authority, port, max_inflight=1, max_cache_entries=1)
        yield cache, port, trace
    finally:
        if backend == "postgres":
            await repository.close()


def distinct_runtimes(primary, other, target):
    for key in ("repository", "thread", "loop"):
        assert primary[key] != other[key]
    if target[0] == "postgres":
        # Distinct still-live pools cannot lend one backend connection to both
        # runtimes. This checks actual server connection IDs, not Python wrappers.
        assert primary["backend_pids"] and other["backend_pids"]
        assert primary["backend_pids"].isdisjoint(other["backend_pids"])


async def primary_runtime(engine, kernel, scope, clock):
    original, coordinates, ids, *_ = await setup(engine, kernel, scope, clock, inputs=2)
    cfg = replace(original.configuration, timeout_seconds=30)
    trace = {}
    authority = traced_authority(original.service, cfg, coordinates, trace)
    port = Port(cfg)
    cache = GovernedSourceEmbeddings(authority, port, max_inflight=1, max_cache_entries=1)
    return authority, coordinates, ids, port, cache, trace


def test_separate_thread_repositories_share_singleflight_and_inflight_limit(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, trace = await primary_runtime(
                engine, kernel, scope, clock
            )
            target = connection_target(engine.repository)
            entered, release = asyncio.Event(), asyncio.Event()
            waiter_observed = threading.Event()

            async def wait_in_provider():
                entered.set()
                await release.wait()

            port.callback = wait_in_provider
            first = asyncio.create_task(cache.embed_source(coords, ids[0]))

            async def contender():
                try:
                    async with independent_runtime(
                        target, scope, clock, authority.configuration, coords
                    ) as (other, other_port, other_trace):
                        with pytest.raises(ModelError, match="embedding_concurrency_capacity"):
                            await other.embed_source(coords, ids[1])
                        claim = other._claim

                        async def observe_wait(sealed):
                            outcome = await claim(sealed)
                            if outcome[0] == "wait":
                                waiter_observed.set()
                            return outcome

                        other._claim = observe_wait
                        result = await other.embed_source(coords, ids[0])
                        return result, other_port.calls, other_trace
                finally:
                    # Unblock the main test on failure too; it will propagate
                    # the contender's exception after releasing its provider.
                    waiter_observed.set()

            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = None
                try:
                    await asyncio.wait_for(entered.wait(), 20)
                    pending = asyncio.get_running_loop().run_in_executor(
                        executor, lambda: asyncio.run(contender())
                    )
                    assert await asyncio.to_thread(waiter_observed.wait, 20)
                finally:
                    release.set()
                own = await first
                other, other_calls, other_trace = await asyncio.wait_for(pending, 40)
            distinct_runtimes(trace, other_trace, target)
            assert own.values == other.values == (0.6, 0.8)
            assert not own.cache_hit and other.cache_hit
            assert port.calls == 1 and other_calls == 0
            headers = await rows(engine.repository, scope, HEADER)
            assert len(headers) == 1 and headers[0]["payload"]["state"] == "ready"

    asyncio.run(run())


def test_separate_thread_replacement_fences_old_publication_and_cleanup(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, trace = await primary_runtime(
                engine, kernel, scope, clock
            )
            target = connection_target(engine.repository)
            sealed = await authority.prepare(coords, ids[0])
            state, slot, old_token = await cache._claim(sealed)
            assert state == "claimed"
            clock[0] += timedelta(seconds=32)
            entered, release = threading.Event(), threading.Event()

            async def replacement():
                try:
                    async with independent_runtime(
                        target, scope, clock, authority.configuration, coords
                    ) as (other, other_port, other_trace):

                        async def wait_in_provider():
                            entered.set()
                            assert await asyncio.to_thread(release.wait, 20)

                        other_port.callback = wait_in_provider
                        result = await other.embed_source(coords, ids[0])
                        return result, other_port.calls, other_trace
                finally:
                    entered.set()

            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = asyncio.get_running_loop().run_in_executor(
                    executor, lambda: asyncio.run(replacement())
                )
                try:
                    assert await asyncio.to_thread(entered.wait, 20)
                    active = (await rows(engine.repository, scope, HEADER))[0]["payload"]
                    assert active["state"] == "reserved" and active["token"] != old_token
                    with pytest.raises(ModelError, match="embedding_execution_fenced"):
                        await cache._publish(sealed, slot, old_token, (0.0, 1.0))
                    await cache._release(slot, old_token)
                    assert (await rows(engine.repository, scope, HEADER))[0]["payload"] == active
                finally:
                    release.set()
                other, other_calls, other_trace = await asyncio.wait_for(pending, 40)
            await cache._release(slot, old_token)
            current = await cache.embed_source(coords, ids[0])
            distinct_runtimes(trace, other_trace, target)
            assert current.values == other.values == (0.6, 0.8)
            assert current.cache_hit and not other.cache_hit
            assert port.calls == 0 and other_calls == 1
            assert (await rows(engine.repository, scope, HEADER))[0]["payload"]["state"] == "ready"

    asyncio.run(run())


def test_simultaneous_cold_claims_in_distinct_threads_dispatch_once(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, trace = await primary_runtime(
                engine, kernel, scope, clock
            )
            target = connection_target(engine.repository)
            start = threading.Barrier(2)
            winner, waiter, release = threading.Event(), threading.Event(), threading.Event()

            async def contender():
                async with independent_runtime(
                    target, scope, clock, authority.configuration, coords
                ) as (other, other_port, other_trace):

                    async def wait_in_provider():
                        winner.set()
                        assert await asyncio.to_thread(release.wait, 20)

                    claim = other._claim

                    async def observe_claim(sealed):
                        outcome = await claim(sealed)
                        if outcome[0] == "wait":
                            waiter.set()
                        return outcome

                    other_port.callback = wait_in_provider
                    other._claim = observe_claim
                    # Neither contender can claim until both independent
                    # repositories, pools, authorities and loops are ready.
                    await asyncio.to_thread(start.wait, 20)
                    result = await other.embed_source(coords, ids[0])
                    return result, other_port.calls, other_trace

            with ThreadPoolExecutor(max_workers=2) as executor:
                pending = [
                    asyncio.get_running_loop().run_in_executor(
                        executor, lambda: asyncio.run(contender())
                    )
                    for _ in range(2)
                ]
                try:
                    assert await asyncio.to_thread(winner.wait, 20)
                    assert await asyncio.to_thread(waiter.wait, 20)
                    with pytest.raises(ModelError, match="embedding_concurrency_capacity"):
                        await cache.embed_source(coords, ids[1])
                finally:
                    release.set()
                    results = await asyncio.wait_for(
                        asyncio.gather(*pending, return_exceptions=True), 40
                    )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            left, right = results
            distinct_runtimes(left[2], right[2], target)
            distinct_runtimes(trace, left[2], target)
            distinct_runtimes(trace, right[2], target)
            assert left[0].values == right[0].values == (0.6, 0.8)
            assert left[0].cache_hit != right[0].cache_hit
            assert left[1] + right[1] == 1 and port.calls == 0
            headers = await rows(engine.repository, scope, HEADER)
            assert len(headers) == 1 and headers[0]["payload"]["state"] == "ready"

    asyncio.run(run())
