"""Full-evidence host archival; no external service, provider or fabricated invoice."""

import asyncio
import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_governed_models_v7 import setup

from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.operations.model_authorization_archive import (
    PENDING_KIND,
    STATE_KIND,
    ModelAuthorizationArchive,
    ModelAuthorizationArchivePolicy,
    verify_model_authorization_archive,
)
from agent_memory.retrieval.model_contracts import ModelError, canonical, digest

store = base.store
POLICY = ModelAuthorizationArchivePolicy(
    "audit-test/1", "host-test-archive", minimum_age_seconds=0, retain_recent=1, max_batch=256
)


async def read_rows(engine, scope, kind="model_authorization"):
    async with engine.repository.unit_of_work() as uow:
        return await uow.derived_records(scope, kind)


async def export(archive, policy=POLICY):
    checkpoint = (await archive.status())["state"]["checkpoint"]
    return await archive.export(policy, expected_previous_checkpoint=checkpoint)


async def acknowledge(archive, batch, policy=POLICY):
    # A test-owned durable object store substitute. Its receipt is specific to
    # exact whole-envelope bytes, not a monetary/provider receipt.
    return await archive.acknowledge(
        policy,
        expected_checkpoint=batch["checkpoint"],
        archive_receipt_sha256=digest(["test-storage", canonical(batch)]),
    )


def test_archive_cache_hits_repeated_rotation_preserves_full_evidence_and_cost(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            archive = ModelAuthorizationArchive(authority)
            delivered, stored = [], []
            for _ in range(4):
                delivered.extend([await answers.answer(sealed) for _ in range(8)])
                before = await ledger.export()
                batch = await export(archive)
                assert verify_model_authorization_archive(
                    batch, expected_checkpoint=batch["checkpoint"]
                )
                assert len(await read_rows(engine, scope)) == 9
                stored.append(deepcopy(batch))
                state = await acknowledge(archive, batch)
                assert await ledger.export() == before
                assert len(await read_rows(engine, scope)) == 1
                assert len(await read_rows(engine, scope, STATE_KIND)) == 1
                assert not await read_rows(engine, scope, PENDING_KIND)
                assert await acknowledge(archive, batch) == state
            archived = {
                r["identity"]: r["payload"]
                for b in stored
                for r in b["records"]
                if r["kind"] == "model_authorization" and r["identity"] in b["authorization_ids"]
            }
            live = {r["identity"]: r["payload"] for r in await read_rows(engine, scope)}
            assert len(archived) + len(live) == 33
            assert {a.delivery_id for a in delivered}.issubset(set(archived) | set(live))
            assert state["archived_records"] == 32 and state["sequence"] == 4
            assert (
                state["stages"]["dispatch"]
                + int(any(v["stage"] == "dispatch" for v in live.values()))
                == 1
            )
            assert len(port.calls) == 1
            assert len(await ledger.snapshot()) == 1
            assert all(a.cost_status == "unknown" for a in delivered)
            with pytest.raises(ModelError, match="archive_stale"):
                await acknowledge(archive, stored[0])

    asyncio.run(run())


def test_archive_capacity_recovery_and_multiple_calls(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            first = await answers.answer(sealed)
            async with engine.repository.unit_of_work() as uow:
                template = await uow.derived_get(scope, "model_authorization", first.delivery_id)
                for n in range(4094):
                    key = f"capacity-{n:04d}"
                    await uow.derived_put(
                        scope, "model_authorization", key, {**template, "id": key}
                    )
            before = await ledger.export()
            with pytest.raises(ModelError, match="model_authorization_capacity"):
                await answers.answer(sealed)
            assert len(port.calls) == 1 and await ledger.export() == before
            archive = ModelAuthorizationArchive(authority)
            batch = await export(archive)
            assert len(batch["authorization_ids"]) == 256
            # Export alone cannot permit capacity reuse.
            with pytest.raises(ModelError, match="model_authorization_capacity"):
                await answers.answer(sealed)
            await acknowledge(archive, batch)
            assert (await answers.answer(sealed)).cache_hit
            # Distinct calls can also be archived; only the audit window shrinks.
            clock[0] += timedelta(seconds=301)
            assert not (await answers.answer(sealed)).cache_hit
            batch = await export(archive)
            await acknowledge(archive, batch)
            assert len(port.calls) == len(await ledger.snapshot()) == 2
            assert len(await read_rows(engine, scope)) < 4096

    asyncio.run(run())


def test_archive_requires_matching_pin_receipt_policy_and_keeps_new_events(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            batch = await export(archive)
            before = await read_rows(engine, scope)
            with pytest.raises(ModelError, match="archive_pending"):
                await export(archive)
            with pytest.raises(ModelError, match="archive_stale"):
                await archive.acknowledge(
                    POLICY, expected_checkpoint="0" * 64, archive_receipt_sha256="1" * 64
                )
            with pytest.raises(ModelError, match="invalid_model_digest"):
                await archive.acknowledge(
                    POLICY, expected_checkpoint=batch["checkpoint"], archive_receipt_sha256=""
                )
            with pytest.raises(ModelError, match="archive_stale"):
                await acknowledge(archive, batch, replace(POLICY, archive_id="other-host"))
            assert await read_rows(engine, scope) == before
            new = await answers.answer(sealed)
            # Recreate the host service to simulate restart with persisted export.
            archive = ModelAuthorizationArchive(authority)
            assert (await archive.status())["pending"]["checkpoint"] == batch["checkpoint"]
            await acknowledge(archive, batch)
            assert new.delivery_id in {r["identity"] for r in await read_rows(engine, scope)}

    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_erase_blocks_stale_archive_acknowledgment(store, all_in_scope):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            batch = await export(archive)
            before_money = await ledger.export()
            sources = tuple(json.loads(sealed.manifest_json)["sources"])
            await kernel.forget(
                ForgetRequest(
                    scope,
                    mode=ForgetMode.ERASE,
                    memory_ids=() if all_in_scope else sources[:1],
                    all_in_scope=all_in_scope,
                )
            )
            before = await read_rows(engine, scope)
            # Scope erase may revoke the host authority before the stale fence.
            with pytest.raises((ModelError, ValueError)):
                await acknowledge(archive, batch)
            assert await read_rows(engine, scope) == before
            assert await ledger.export() == before_money
            assert all(r["payload"] == {"state": "erased"} for r in before)
            assert not await read_rows(engine, scope, STATE_KIND)

    asyncio.run(run())


def test_archive_age_recent_window_cancel_and_unsupported_census(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            aged = replace(POLICY, minimum_age_seconds=10)
            with pytest.raises(ModelError, match="archive_empty"):
                await export(archive, aged)
            clock[0] += timedelta(seconds=11)
            batch = await export(archive, aged)
            with pytest.raises(ModelError, match="archive_stale"):
                await archive.cancel(expected_checkpoint="0" * 64)
            await archive.cancel(expected_checkpoint=batch["checkpoint"])
            assert len(await read_rows(engine, scope)) == 2
            with pytest.raises(ModelError, match="archive_census_limit"):
                await export(archive, replace(POLICY, max_records=1))
            with pytest.raises(ModelError, match="archive_census_limit"):
                await export(archive, replace(POLICY, max_bytes=1))
            async with engine.repository.unit_of_work() as uow:
                await uow.derived_put(scope, "unknown-future-kind", "test", {})
            with pytest.raises(ModelError, match="archive_record_unsupported"):
                await export(archive)
            assert len(await read_rows(engine, scope)) == 2
            assert not await read_rows(engine, scope, PENDING_KIND)

    asyncio.run(run())


def test_archive_concurrent_acknowledgment_is_idempotent_and_full_closure_is_exported(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            result = await answers.answer(sealed)
            async with engine.repository.unit_of_work() as uow:
                # Synthetic immutable legacy proof graph; audit root -> proof ->
                # deeper proof, and an explicit reverse edge -> another proof.
                auth = await uow.derived_get(scope, "model_authorization", result.delivery_id)
                auth["parents"] = ["derived:proof-a"]
                await uow.derived_put(scope, "model_authorization", result.delivery_id, auth)
                await uow.derived_put(
                    scope, "revision", "proof-a", {"parents": ["derived:proof-b"]}
                )
                await uow.derived_put(scope, "revision", "proof-b", {"proof": "saved"})
                await uow.derived_put(scope, "revision", "proof-c", {"proof": "edge-only"})
                await uow.derived_edges(scope, "proof-a", (("processing", "derived:proof-c"),))
            archive = ModelAuthorizationArchive(authority)
            policy = replace(POLICY, retain_recent=0)
            batch = await export(archive, policy)
            assert {"proof-a", "proof-b", "proof-c"}.issubset(
                {r["identity"] for r in batch["records"]}
            )
            a, b = await asyncio.gather(
                acknowledge(archive, batch, policy),
                acknowledge(ModelAuthorizationArchive(authority), batch, policy),
            )
            assert a == b and a["sequence"] == 1 and a["archived_records"] == 2
            assert not await read_rows(engine, scope)
            assert len(await read_rows(engine, scope, "revision")) >= 3

    asyncio.run(run())


def test_archive_ack_transaction_rolls_back_every_deletion(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            policy = replace(POLICY, retain_recent=0)
            batch = await export(archive, policy)
            async with engine.repository.unit_of_work() as uow:
                cls = type(uow)
            original = cls.model_authorization_archive_delete
            deleted = []

            async def fail_after_one(self, *args):
                await original(self, *args)
                deleted.append(args)
                if len(deleted) == 2:
                    raise RuntimeError("test rollback")

            monkeypatch.setattr(cls, "model_authorization_archive_delete", fail_after_one)
            with pytest.raises(RuntimeError, match="test rollback"):
                await acknowledge(archive, batch, policy)
            assert len(await read_rows(engine, scope)) == 2
            assert not await read_rows(engine, scope, STATE_KIND)
            assert (await archive.status())["pending"]["checkpoint"] == batch["checkpoint"]
            monkeypatch.setattr(cls, "model_authorization_archive_delete", original)
            await acknowledge(archive, batch, policy)
            assert not await read_rows(engine, scope)

    asyncio.run(run())


def test_archive_uses_independent_prior_checkpoint_and_reopened_backend(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            old_pin = (await archive.status())["state"]["checkpoint"]
            batch = await archive.export(POLICY, expected_previous_checkpoint=old_pin)
            if hasattr(engine.repository, "pool"):
                from agent_memory_postgres.repository import PostgresMemoryRepository

                reopened = PostgresMemoryRepository.from_dsn(
                    engine.repository.pool.conninfo, max_size=2
                )
            else:
                from agent_memory.sqlite import SQLiteMemoryRepository

                reopened = SQLiteMemoryRepository(engine.repository._path)
            await reopened.initialize()
            try:
                from agent_memory.derived import ObservationService
                from agent_memory.retrieval.model_authority import SourceModelAuthority

                service = ObservationService(
                    reopened,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    authority_id="local-host",
                    authority_min_version=authority.service.authority_min_version,
                )
                current = ModelAuthorizationArchive(
                    SourceModelAuthority(
                        service,
                        public_template=authority.template,
                        configuration=authority.configuration,
                        verify_coordinates=authority.verify_coordinates,
                    )
                )
                assert (await current.status())["pending"]["checkpoint"] == batch["checkpoint"]
                state = await acknowledge(current, batch)
                assert state["checkpoint"] != old_pin
                with pytest.raises(ModelError, match="archive_stale"):
                    await current.export(POLICY, expected_previous_checkpoint=old_pin)
                assert (await current.status())["pending"] is None
            finally:
                if hasattr(reopened, "close"):
                    await reopened.close()

    asyncio.run(run())


def test_scope_erase_reauthorization_preserves_checkpoint_and_recovers_pending(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived import HostGrantAuthority

            authority, sealed, ledger, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            first = await export(archive)
            state = await acknowledge(archive, first)
            await answers.answer(sealed)
            pending = await export(archive)
            money = await ledger.export()
            await kernel.forget(ForgetRequest(scope, mode=ForgetMode.ERASE, all_in_scope=True))
            await authority.service.set_authority(
                HostGrantAuthority(
                    "local-host",
                    ("alice",),
                    clock[0] + timedelta(hours=1),
                ),
                expected_version=2,
            )
            assert (await archive.status())["state"] == state
            with pytest.raises(ModelError, match="archive_stale"):
                await acknowledge(archive, pending)
            await archive.cancel(expected_checkpoint=pending["checkpoint"])
            policy = replace(POLICY, retain_recent=0)
            erased = await export(archive, policy)
            assert all(r["payload"] == {"state": "erased"} for r in erased["records"])
            current = await acknowledge(archive, erased, policy)
            assert current["sequence"] == state["sequence"] + 1
            assert current["archived_records"] == 3
            assert current["stages"]["erased"] == 2
            assert await ledger.export() == money
            assert not await read_rows(engine, scope)

    asyncio.run(run())


def test_archive_rejects_changed_selected_record_and_authority_rotation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived import HostGrantAuthority

            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            batch = await export(archive)
            key = batch["authorization_ids"][0]
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "model_authorization", key)
                await uow.derived_put(
                    scope, "model_authorization", key, {**old, "payload_sha256": "f" * 64}
                )
            with pytest.raises(ModelError, match="archive_stale"):
                await acknowledge(archive, batch)
            await archive.cancel(expected_checkpoint=batch["checkpoint"])
            batch = await export(archive)
            await authority.service.set_authority(
                HostGrantAuthority(
                    "local-host",
                    ("alice",),
                    clock[0] + timedelta(minutes=30),
                ),
                expected_version=1,
            )
            with pytest.raises(ModelError, match="archive_stale"):
                await acknowledge(archive, batch)
            assert len(await read_rows(engine, scope)) == 2
            assert not await read_rows(engine, scope, STATE_KIND)

    asyncio.run(run())


def test_pending_delivery_is_never_archived_as_completed_or_deleted_with_wrong_token(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            answer = await answers.answer(sealed)
            key, token = "reserved-delivery", "leader-token"
            row = dict(
                schema="model-delivery-reservation/1",
                state="reserved",
                id=key,
                stage="delivery",
                consumed=False,
                token=token,
                expires_at=(clock[0] + timedelta(minutes=1)).isoformat(),
                call_id=answer.call_id,
                key=sealed.key,
                sources=list(json.loads(sealed.manifest_json)["sources"]),
                parents=authority.parents(sealed),
            )
            async with engine.repository.unit_of_work() as uow:
                await uow.derived_put(scope, "model_authorization", key, row)
                assert not await uow.model_delivery_reservation_delete(scope, key, "other-token")
            archive = ModelAuthorizationArchive(authority)
            assert (await archive.status())["capacity"] == dict(
                limit=4096,
                used=3,
                available=4093,
                consumed=2,
                dispatch=1,
                delivery=1,
                reserved=1,
                erased=0,
                first_dispatch_available=True,
            )
            policy = replace(POLICY, retain_recent=0)
            batch = await export(archive, policy)
            assert key not in batch["authorization_ids"]
            state = await acknowledge(archive, batch, policy)
            assert state["archived_records"] == 2 and state["stages"]["delivery"] == 1
            capacity = (await archive.status())["capacity"]
            assert capacity["used"] == capacity["reserved"] == 1
            assert capacity["consumed"] == 0 and capacity["available"] == 4095
            rows = await read_rows(engine, scope)
            assert len(rows) == 1 and rows[0]["payload"] == row
            async with engine.repository.unit_of_work() as uow:
                assert await uow.model_delivery_reservation_delete(scope, key, token)
                assert not await uow.model_delivery_reservation_delete(scope, key, token)
                completed = next(
                    r["payload"] for r in batch["records"] if r["identity"] == answer.delivery_id
                )
                await uow.derived_put(scope, "model_authorization", key, {**completed, "id": key})
                assert not await uow.model_delivery_reservation_delete(scope, key, token)
            assert len(await read_rows(engine, scope)) == 1

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["selection", "stages", "evidence", "scope"])
def test_pinned_checkpoint_binds_exact_pending_deletion_descriptor(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            batch = await export(archive)
            new = await answers.answer(sealed)
            async with engine.repository.unit_of_work() as uow:
                pending = await uow.derived_get(scope, PENDING_KIND, "scope")
                if mutation == "selection":
                    row = await uow.derived_get(scope, "model_authorization", new.delivery_id)
                    pending["authorizations"] = {new.delivery_id: digest(row)}
                    pending["authorization_ids"] = [new.delivery_id]
                    pending["stages"] = {"delivery": 1}
                elif mutation == "stages":
                    pending["stages"] = {"delivery": 99}
                elif mutation == "evidence":
                    pending["evidence_sha256"] = "f" * 64
                else:
                    pending["scope_key"] = "other-scope"
                await uow.derived_put(scope, PENDING_KIND, "scope", pending)
            before = await read_rows(engine, scope)
            with pytest.raises(ModelError, match="archive_stale"):
                await acknowledge(archive, batch)
            assert await read_rows(engine, scope) == before
            assert not await read_rows(engine, scope, STATE_KIND)

    asyncio.run(run())


def test_full_archive_verifier_rejects_changed_proof_with_pinned_descriptor(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            batch = await export(ModelAuthorizationArchive(authority))
            assert verify_model_authorization_archive(
                batch, expected_checkpoint=batch["checkpoint"]
            )
            original = deepcopy(batch)
            batch["records"] = []
            with pytest.raises(ModelError, match="archive_invalid"):
                verify_model_authorization_archive(
                    batch, expected_checkpoint=original["checkpoint"]
                )
            batch["evidence_sha256"] = digest({"records": [], "edges": batch["edges"]})
            with pytest.raises(ModelError, match="archive_invalid"):
                verify_model_authorization_archive(
                    batch, expected_checkpoint=original["checkpoint"]
                )

    asyncio.run(run())


def test_old_real_backup_cannot_start_archive_against_current_external_chain(store, tmp_path):
    from test_purge_restore import backup_copy

    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            archive = ModelAuthorizationArchive(authority)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                batch = await export(archive)
                current = await acknowledge(archive, batch)
                from agent_memory.derived import ObservationService
                from agent_memory.retrieval.model_authority import SourceModelAuthority

                restored_authority = SourceModelAuthority(
                    ObservationService(
                        backup,
                        scope,
                        base.POLICY,
                        clock=lambda: clock[0],
                        authority_id="local-host",
                        authority_min_version=1,
                    ),
                    public_template=authority.template,
                    configuration=authority.configuration,
                    verify_coordinates=authority.verify_coordinates,
                )
                restored = ModelAuthorizationArchive(restored_authority)
                with pytest.raises(ModelError, match="archive_stale"):
                    await restored.export(
                        POLICY, expected_previous_checkpoint=current["checkpoint"]
                    )
                assert (await restored.status())["pending"] is None
                async with backup.unit_of_work() as uow:
                    assert len(await uow.derived_records(scope, "model_authorization")) == 2

    asyncio.run(run())


def test_archive_includes_real_registered_question_generation_proof(store):
    from test_question_models_v7 import configured_question
    from test_question_runtime_v7 import ACTOR

    async def run():
        async with store() as (engine, kernel, scope, clock):
            questions, models, ledger, _ = await configured_question(
                engine, scope, clock, second=True
            )
            result = await models.answer("project-a:owner", actor=ACTOR)
            original_view = await questions.read("project-a:owner", actor=ACTOR)
            archive = ModelAuthorizationArchive(models.authority)
            batch = await export(archive, replace(POLICY, retain_recent=0))
            kinds = {r["kind"] for r in batch["records"]}
            assert {
                "question_content",
                "question_certificate",
                "model_cache_header",
                "model_cache_body",
            } <= kinds
            rows = [r["payload"] for r in batch["records"] if r["kind"] == "model_authorization"]
            assert any(r["id"] == result["delivery_id"] for r in rows)
            assert all({"source", "private-unused"} == set(r["sources"]) for r in rows)
            money = await ledger.export()
            await acknowledge(archive, batch, replace(POLICY, retain_recent=0))
            assert await ledger.export() == money
            assert await questions.read("project-a:owner", actor=ACTOR) == original_view

    asyncio.run(run())
