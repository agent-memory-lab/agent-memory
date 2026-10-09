"""Injected known vectors on SQLite/live PostgreSQL; no quality/performance claims."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_controls import configured
from test_durable_purge import source_id

from agent_memory.derived import DerivedError, ObservationService, ProcessingGrant
from agent_memory.domain import ForgetMode, ForgetRequest, MemoryEvent
from agent_memory.retrieval.model_contracts import ModelCoordinates, ModelError, digest
from agent_memory.retrieval.source_embeddings import (
    BODY,
    EMBEDDING_KINDS,
    GRANT,
    HEADER,
    EmbeddingConfiguration,
    EmbeddingResponse,
    GovernedSourceEmbeddings,
    SourceEmbeddingAuthority,
)

store = base.store


def configuration(**changes):
    return replace(
        EmbeddingConfiguration(
            provider="injected-test",
            recipient="test-only-local-account-region-policy/1",
            model="known-vector-fixture",
            model_revision="a" * 64,
            runtime_sha256="b" * 64,
            dimensions=2,
            timeout_seconds=2,
        ),
        **changes,
    )


class Port:
    def __init__(self, cfg, callback=None):
        self.configuration, self.callback = cfg, callback
        self.calls = 0
        self.answer = (0.6, 0.8)

    async def embed(self, sealed):
        self.calls += 1
        if self.callback:
            await self.callback()
        return EmbeddingResponse(self.configuration.fingerprint, sealed.key, self.answer)


async def setup(engine, kernel, scope, clock, *, inputs=1, **limits):
    service, _, capture, _, _, _ = await configured(engine, kernel, scope, clock, inputs=inputs)
    cfg = configuration()
    proof = [True]

    async def guard(uow, coordinates):
        return proof[0]

    authority = SourceEmbeddingAuthority(service, configuration=cfg, verify_coordinates=guard)
    ids = tuple(source_id(scope, str(i)) for i in range(1, inputs + 1))
    for identity in ids:
        await authority.allow_processing(
            identity,
            readers=("alice",),
            purposes=("agent_context",),
            expires_at=clock[0] + timedelta(hours=1),
        )
    coordinates = ModelCoordinates(
        scope.partition_key(),
        "alice",
        "p",
        "agent_context",
        "alice",
        "sources",
        "1",
        "{}",
        "rank",
        "c" * 64,
        "d" * 64,
        "e" * 64,
        clock[0].isoformat(),
        clock[0].isoformat(),
    )
    port = Port(cfg)
    cache = GovernedSourceEmbeddings(authority, port, **limits)
    return authority, coordinates, ids, port, cache, proof, capture


async def rows(repository, scope, kind):
    async with repository.unit_of_work() as uow:
        return await uow.derived_records(scope, kind)


def test_exact_source_vector_reused_across_instances_without_text_in_storage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            first = await cache.embed_source(coords, ids[0])
            other = GovernedSourceEmbeddings(authority, port)
            second = await other.embed_source(coords, ids[0])
            assert first.values == second.values == (0.6, 0.8)
            assert not first.cache_hit and second.cache_hit and port.calls == 1
            header = (await rows(engine.repository, scope, HEADER))[0]["payload"]
            body = (await rows(engine.repository, scope, BODY))[0]["payload"]
            assert header["generation_manifest"]["sources"] == [ids[0]]
            assert header["body_sha256"] == digest(body)
            assert "private-offline-marker" not in json.dumps([header, body])
            assert await rows(engine.repository, scope, "model_cache_body") == ()
            assert not hasattr(cache, "_tasks")

    asyncio.run(run())


@pytest.mark.parametrize(
    "field,value",
    [
        ("model", "other"),
        ("model_revision", "c" * 64),
        ("runtime_sha256", "d" * 64),
        ("dimensions", 3),
        ("normalization", "l2"),
        ("provider", "different-provider"),
        ("recipient", "different-recipient"),
    ],
)
def test_pinned_model_variants_never_reuse(store, field, value):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            await cache.embed_source(coords, ids[0])
            cfg = replace(port.configuration, **{field: value})
            other_authority = SourceEmbeddingAuthority(
                authority.service,
                configuration=cfg,
                verify_coordinates=authority.verify_coordinates,
            )
            if field == "recipient":
                await other_authority.allow_processing(
                    ids[0],
                    readers=("alice",),
                    purposes=("agent_context",),
                    expires_at=clock[0] + timedelta(hours=1),
                )
            new_port = Port(cfg)
            if field == "dimensions":
                new_port.answer = (0.6, 0.8, 0.0)
            other = GovernedSourceEmbeddings(other_authority, new_port)
            result = await other.embed_source(coords, ids[0])
            assert not result.cache_hit and new_port.calls == 1 and port.calls == 1

    asyncio.run(run())


@pytest.mark.parametrize(
    "field,value",
    [
        ("principal", "bob"),
        ("purpose", "other"),
        ("scope_key", "other"),
        ("audience", "bob"),
    ],
)
def test_wrong_actor_scope_purpose_audience_refused_before_source_or_vector_read(
    store, monkeypatch, field, value
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            await cache.embed_source(coords, ids[0])
            cls = type(engine.repository.unit_of_work())
            source_get, derived_get = cls.get_source_event, cls.derived_get
            reads = []

            async def spy_source(self, *args):
                reads.append("source")
                return await source_get(self, *args)

            async def spy_body(self, *args):
                if args[1] == BODY:
                    reads.append("vector")
                return await derived_get(self, *args)

            monkeypatch.setattr(cls, "get_source_event", spy_source)
            monkeypatch.setattr(cls, "derived_get", spy_body)
            with pytest.raises((ModelError, DerivedError)):
                await cache.embed_source(replace(coords, **{field: value}), ids[0])
            assert reads == [] and port.calls == 1

    asyncio.run(run())


@pytest.mark.parametrize(
    "field,value",
    [
        ("project", "other"),
        ("context_sha256", "f" * 64),
        ("query_proof_sha256", "a" * 64),
        ("policy_sha256", "a" * 64),
        ("parameters_json", '{"filter":"other"}'),
    ],
)
def test_other_full_proof_coordinates_do_not_reuse(store, field, value):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            await cache.embed_source(coords, ids[0])
            result = await cache.embed_source(replace(coords, **{field: value}), ids[0])
            assert not result.cache_hit and port.calls == 2

    asyncio.run(run())


@pytest.mark.parametrize("when", ["before", "inflight", "published", "cached"])
@pytest.mark.parametrize("change", ["proof", "read", "processing", "authority", "erasure"])
def test_changed_controls_fence_dispatch_publish_and_delivery(store, when, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, proof, _ = await setup(
                engine, kernel, scope, clock
            )

            async def mutate():
                if change == "proof":
                    proof[0] = False
                elif change == "read":
                    await authority.service.grant(
                        ProcessingGrant(ids[0], ("alice",), revoked=True), expected_version=2
                    )
                elif change == "processing":
                    await authority.allow_processing(
                        ids[0],
                        readers=("alice",),
                        purposes=("agent_context",),
                        expires_at=clock[0] + timedelta(hours=1),
                        expected_version=1,
                        revoked=True,
                    )
                elif change == "authority":
                    from agent_memory.derived import HostGrantAuthority

                    await authority.service.set_authority(
                        HostGrantAuthority(
                            "local-host", ("alice",), clock[0] + timedelta(hours=1), revoked=True
                        ),
                        expected_version=1,
                    )
                else:
                    await kernel.forget(ForgetRequest(scope, (ids[0],), mode=ForgetMode.ERASE))

            if when == "cached":
                await cache.embed_source(coords, ids[0])
                await mutate()
            elif when == "inflight":
                port.callback = mutate
            elif when == "published":
                original = cache._publish

                async def after_publish(*args):
                    await original(*args)
                    await mutate()

                cache._publish = after_publish
            else:
                await mutate()
            with pytest.raises((ModelError, DerivedError)):
                await cache.embed_source(coords, ids[0])
            assert port.calls == (0 if when == "before" else 1)
            if change == "erasure":
                for kind in (HEADER, BODY):
                    assert all(
                        r["payload"] == {"state": "erased"}
                        for r in await rows(engine.repository, scope, kind)
                    )

    asyncio.run(run())


def test_revision_replacement_and_content_binding(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, capture = await setup(
                engine, kernel, scope, clock
            )
            await cache.embed_source(coords, ids[0])
            producer, session, *_ = capture
            new = MemoryEvent(scope, "message", "new source revision", actor="alice")
            await producer.receiver.revise(
                new,
                base_event_id=ids[0],
                expected_revision=1,
                request_id="revision",
                producer_id="device",
                configuration_sha256=session.configuration_sha256,
            )
            with pytest.raises(ModelError, match="source_unavailable"):
                await cache.embed_source(coords, ids[0])
            await authority.service.grant(ProcessingGrant(new.id, ("alice",)))
            await authority.allow_processing(
                new.id,
                readers=("alice",),
                purposes=("agent_context",),
                expires_at=clock[0] + timedelta(hours=1),
            )
            fresh = await cache.embed_source(coords, new.id)
            assert not fresh.cache_hit and port.calls == 2

    asyncio.run(run())


@pytest.mark.parametrize(
    "bad",
    [
        None,
        (),
        (0.0, 0.0),
        (float("nan"), 1),
        (float("inf"), 1),
        (True, 1),
        ("0.6", 0.8),
        (1.0,),
        (10**400, 1),
        (1e308, 1e308),
    ],
)
def test_unknown_or_malicious_vectors_never_publish(store, bad):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            port.answer = bad
            with pytest.raises(ModelError, match="invalid_embedding_vector"):
                await cache.embed_source(coords, ids[0])
            assert all(
                "values" not in r["payload"] for r in await rows(engine.repository, scope, BODY)
            )

    asyncio.run(run())


@pytest.mark.parametrize("bad", ["model", "input", "type", "normalization"])
def test_port_response_requires_exact_input_model_and_normalization(store, bad):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            original = port.embed

            async def malicious(sealed):
                value = await original(sealed)
                if bad == "type":
                    return (0.6, 0.8)
                if bad == "model":
                    return replace(value, configuration_sha256="f" * 64)
                if bad == "input":
                    return replace(value, input_sha256="f" * 64)
                return replace(value, values=(2.0, 3.0))

            if bad == "normalization":
                cfg = replace(port.configuration, normalization="l2")
                authority = SourceEmbeddingAuthority(
                    authority.service,
                    configuration=cfg,
                    verify_coordinates=authority.verify_coordinates,
                )
                port.configuration = cfg
                cache = GovernedSourceEmbeddings(authority, port)
            port.embed = malicious
            with pytest.raises(ModelError):
                await cache.embed_source(coords, ids[0])
            assert all(
                "values" not in r["payload"] for r in await rows(engine.repository, scope, BODY)
            )

    asyncio.run(run())


def test_expiry_scrubs_and_reuses_bounded_slots_and_bytes(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, port, cache, _, _ = await setup(
                engine,
                kernel,
                scope,
                clock,
                inputs=3,
                max_cache_entries=2,
                max_cache_bytes=16384,
                cache_seconds=10,
            )
            for key in ids * 3:
                await cache.embed_source(coords, key)
            assert port.calls == 9  # Byte budget only fits one charged slot at this bound.
            assert len(await rows(engine.repository, scope, HEADER)) <= 2
            data = [
                r["payload"]
                for kind in (HEADER, BODY)
                for r in await rows(engine.repository, scope, kind)
            ]
            assert sum(len(json.dumps(r).encode()) for r in data) < 16384
            clock[0] += timedelta(seconds=11)
            assert await cache.sweep_expired() == 1
            assert all(
                "values" not in r["payload"] for r in await rows(engine.repository, scope, BODY)
            )
            assert not (await cache.embed_source(coords, ids[0])).cache_hit

    asyncio.run(run())


def test_durable_singleflight_limits_and_independent_waiter_delivery(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(
                engine, kernel, scope, clock, inputs=2, max_inflight=1
            )
            entered, release = asyncio.Event(), asyncio.Event()

            async def wait():
                entered.set()
                await release.wait()

            port.callback = wait
            first = asyncio.create_task(cache.embed_source(coords, ids[0]))
            await entered.wait()
            other = GovernedSourceEmbeddings(authority, port, max_inflight=1)
            second = asyncio.create_task(other.embed_source(coords, ids[0]))
            await asyncio.sleep(0.03)
            with pytest.raises(ModelError, match="concurrency_capacity"):
                await other.embed_source(coords, ids[1])
            release.set()
            one, two = await asyncio.gather(first, second)
            assert one.values == two.values and one.cache_hit != two.cache_hit and port.calls == 1

    asyncio.run(run())


def test_cancelled_flight_scrubs_reservation_and_allows_retry(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            entered = asyncio.Event()

            async def wait():
                entered.set()
                await asyncio.Event().wait()

            port.callback = wait
            pending = asyncio.create_task(cache.embed_source(coords, ids[0]))
            await entered.wait()
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert all(
                r["payload"] == {"state": "expired"}
                for r in await rows(engine.repository, scope, HEADER)
            )
            port.callback = None
            assert not (await cache.embed_source(coords, ids[0])).cache_hit

    asyncio.run(run())


def test_corrupt_cache_refuses_output(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            await cache.embed_source(coords, ids[0])
            async with engine.repository.unit_of_work() as uow:
                body = await uow.derived_get(scope, BODY, "0")
                body["values"] = [0.0, 1.0]
                await uow.derived_put(scope, BODY, "0", body)
            with pytest.raises(ModelError, match="cache_proof_invalid"):
                await cache.embed_source(coords, ids[0])
            assert port.calls == 1

    asyncio.run(run())


@pytest.mark.parametrize("all_scope", [False, True])
def test_old_backup_replay_scrubs_vectors_and_proofs(store, tmp_path, all_scope):
    from test_purge_restore import backup_copy, erase, replay, restorer

    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            await cache.embed_source(coords, ids[0])
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, ids[0], all_in_scope=all_scope)
                checkpoint = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), checkpoint)
                service = ObservationService(
                    backup,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    authority_id="local-host",
                    authority_min_version=1,
                )
                restored = SourceEmbeddingAuthority(
                    service,
                    configuration=port.configuration,
                    verify_coordinates=authority.verify_coordinates,
                )
                with pytest.raises((ModelError, DerivedError)):
                    await GovernedSourceEmbeddings(restored, port).embed_source(coords, ids[0])
                for kind in (HEADER, BODY):
                    assert all(
                        r["payload"] == {"state": "erased"} for r in await rows(backup, scope, kind)
                    )
                assert all(
                    r["payload"].get("state") == "erased" for r in await rows(backup, scope, GRANT)
                )
                async with backup.unit_of_work() as uow:
                    assert not any(
                        k.startswith("source-embedding:")
                        for k in await uow.derived_reverse(scope, "source:" + ids[0])
                    )
                assert port.calls == 1

    asyncio.run(run())


def test_new_kinds_have_explicit_question_gc_and_erasure_semantics(store):
    from agent_memory.derived.question_gc import KNOWN_KINDS, QuestionRetentionPolicy, plan

    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, _, cache, _, _ = await setup(engine, kernel, scope, clock)
            await cache.embed_source(coords, ids[0])
            assert set(EMBEDDING_KINDS) <= KNOWN_KINDS
            async with engine.repository.unit_of_work() as uow:
                snapshot = await uow.derived_gc_snapshot(
                    scope, max_records=32768, max_edges=131072, max_bytes=67108864
                )
            result = plan(scope, QuestionRetentionPolicy("embedding-test"), snapshot, clock[0])
            assert result["reason"] not in {
                "question_gc_kind_unsupported",
                "question_gc_record_unsupported",
            }
            assert not result["deleted"]

    asyncio.run(run())


def test_generation_processing_grant_cannot_enable_embedding(store, monkeypatch):
    from test_governed_models_v7 import setup as generation_setup

    async def run():
        async with store() as (engine, kernel, scope, clock):
            old, sealed, *_ = await generation_setup(engine, kernel, scope, clock, inputs=1)
            cfg = configuration(recipient=old.configuration.recipient)
            authority = SourceEmbeddingAuthority(
                old.service, configuration=cfg, verify_coordinates=old.verify_coordinates
            )
            port = Port(cfg)
            cache = GovernedSourceEmbeddings(authority, port)
            with pytest.raises(ModelError, match="processing_unauthorized"):
                await cache.embed_source(sealed.coordinates, source_id(scope, "1"))
            assert port.calls == 0

    asyncio.run(run())


def test_stale_process_reservation_is_reclaimed_and_old_publication_fenced(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(
                engine, kernel, scope, clock, max_cache_entries=1
            )
            sealed = await authority.prepare(coords, ids[0])
            state, slot, token = await cache._claim(sealed)
            assert state == "claimed"
            clock[0] += timedelta(seconds=4)
            assert not (await cache.embed_source(coords, ids[0])).cache_hit
            with pytest.raises(ModelError, match="execution_fenced"):
                await cache._publish(sealed, slot, token, (0.0, 1.0))
            assert (await cache.embed_source(coords, ids[0])).values == (0.6, 0.8)
            assert port.calls == 1

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["source", "manifest", "model", "expiry"])
def test_mutated_dispatch_input_and_elapsed_lease_cannot_publish(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            original = port.embed

            async def changed(sealed):
                response = await original(sealed)
                if mutation == "source":
                    object.__setattr__(sealed, "content", "untrusted new source body")
                elif mutation == "manifest":
                    object.__setattr__(sealed, "manifest_json", "{}")
                elif mutation == "model":
                    port.configuration = replace(port.configuration, model_revision="c" * 64)
                else:
                    clock[0] += timedelta(seconds=4)
                return response

            port.embed = changed
            with pytest.raises(ModelError):
                await cache.embed_source(coords, ids[0])
            assert all(
                "values" not in r["payload"] for r in await rows(engine.repository, scope, BODY)
            )

    asyncio.run(run())


def test_provider_errors_are_body_free_and_capacity_failure_never_dispatches(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)

            async def fail(_):
                raise RuntimeError("private source body leaked by test port")

            port.embed = fail
            with pytest.raises(ModelError) as error:
                await cache.embed_source(coords, ids[0])
            assert str(error.value) == "embedding_execution_failed"
            cfg = replace(port.configuration, dimensions=4096)
            new_authority = SourceEmbeddingAuthority(
                authority.service,
                configuration=cfg,
                verify_coordinates=authority.verify_coordinates,
            )
            new_port = Port(cfg)
            small = GovernedSourceEmbeddings(
                new_authority, new_port, max_cache_entries=1, max_cache_bytes=16384
            )
            with pytest.raises(ModelError, match="cache_capacity"):
                await small.embed_source(coords, ids[0])
            assert new_port.calls == 0

    asyncio.run(run())


def test_late_old_worker_cannot_publish_or_scrub_replacement_reservation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(
                engine, kernel, scope, clock, max_cache_entries=1
            )
            sealed = await authority.prepare(coords, ids[0])
            _, slot, old_token = await cache._claim(sealed)
            clock[0] += timedelta(seconds=4)
            entered, release = asyncio.Event(), asyncio.Event()

            async def wait():
                entered.set()
                await release.wait()

            port.callback = wait
            other = GovernedSourceEmbeddings(authority, port, max_cache_entries=1)
            replacement = asyncio.create_task(other.embed_source(coords, ids[0]))
            await entered.wait()
            active = (await rows(engine.repository, scope, HEADER))[0]["payload"]
            assert active["state"] == "reserved" and active["token"] != old_token
            with pytest.raises(ModelError, match="execution_fenced"):
                await cache._publish(sealed, slot, old_token, (0.0, 1.0))
            await cache._release(slot, old_token)
            assert (await rows(engine.repository, scope, HEADER))[0]["payload"] == active
            release.set()
            assert (await replacement).values == (0.6, 0.8)
            await cache._release(slot, old_token)
            assert (await cache.embed_source(coords, ids[0])).cache_hit
            assert port.calls == 1

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["grant", "cache", "lease", "read_grant"])
def test_expiry_and_revocation_across_storage_await_fence_last_delivery(
    store, monkeypatch, boundary
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, port, cache, _, _ = await setup(engine, kernel, scope, clock)
            cls = type(engine.repository.unit_of_work())
            original_get, original_put = cls.derived_get, cls.derived_put
            fired = []
            if boundary != "lease":
                await cache.embed_source(coords, ids[0])

            async def get(self, *args):
                result = await original_get(self, *args)
                if args[1] == BODY and not fired and boundary != "lease":
                    fired.append(True)
                    if boundary == "read_grant":
                        grant = await original_get(self, scope, "grant", ids[0])
                        await original_put(self, scope, "grant", ids[0], {**grant, "revoked": True})
                    else:
                        clock[0] += timedelta(seconds=3601 if boundary == "grant" else 301)
                    await asyncio.sleep(0)
                return result

            async def put(self, *args):
                result = await original_put(self, *args)
                if (
                    args[1] == HEADER
                    and args[3].get("state") == "ready"
                    and not fired
                    and boundary == "lease"
                ):
                    fired.append(True)
                    clock[0] += timedelta(seconds=4)
                    await asyncio.sleep(0)
                return result

            monkeypatch.setattr(cls, "derived_get", get)
            monkeypatch.setattr(cls, "derived_put", put)
            with pytest.raises((ModelError, DerivedError)):
                await cache.embed_source(coords, ids[0])
            assert fired and port.calls == 1
            if boundary == "lease":
                assert all(
                    "values" not in row["payload"]
                    for row in await rows(engine.repository, scope, BODY)
                )

    asyncio.run(run())


def test_erasure_scrubs_paired_unknown_vector_body_without_relying_on_its_metadata(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, coords, ids, _, cache, _, _ = await setup(engine, kernel, scope, clock)
            await cache.embed_source(coords, ids[0])
            async with engine.repository.unit_of_work() as uow:
                await uow.derived_put(
                    scope, BODY, "0", {"schema": "future-or-corrupt/99", "values": [0.6, 0.8]}
                )
            await kernel.forget(ForgetRequest(scope, (ids[0],), mode=ForgetMode.ERASE))
            for kind in (HEADER, BODY):
                assert (await rows(engine.repository, scope, kind))[0]["payload"] == {
                    "state": "erased"
                }

    asyncio.run(run())


@pytest.mark.parametrize("limits", [{"max_cache_entries": 1}, {"max_cache_bytes": 16384}])
def test_smaller_runtime_limits_refuse_oversized_existing_census_even_on_hit(store, limits):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, coords, ids, port, cache, _, _ = await setup(
                engine, kernel, scope, clock, inputs=2
            )
            for key in ids:
                await cache.embed_source(coords, key)
            smaller = GovernedSourceEmbeddings(authority, port, **limits)
            with pytest.raises(ModelError, match="cache_capacity"):
                await smaller.embed_source(coords, ids[0])
            assert port.calls == 2

    asyncio.run(run())
