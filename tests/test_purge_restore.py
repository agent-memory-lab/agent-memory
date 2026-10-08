"""Pinned current deletion history over real old backups; finite atomic host recovery."""

import asyncio
import json
import re
import shutil
import sqlite3
import subprocess
from contextlib import asynccontextmanager, closing
from dataclasses import replace
from uuid import uuid4

import pytest
import test_atom_admission as base
from test_durable_indexing import service, status
from test_durable_purge import envelope, source_id
from test_index_recovery import seed

from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.kernel import MemoryKernel
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.purge_restore import PurgeRestore, digest
from agent_memory.operations.resource_refresh import ResourceRefreshQueue
from agent_memory.operations.retention import DurableReceiver, RetentionError
from agent_memory.operations.worker_tasks import WorkerQueueError
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)

store = base.store
SECRET = b"test-journal-integrity-key-32-bytes-minimum"


def restorer(repo, scope, clock, **changes):
    return PurgeRestore(repo, scope, authority_id="authority", secret=SECRET,
                        actor="operator", clock=lambda: clock[0], **changes)


async def replay(operator, snapshot, **changes):
    return await operator.replay(snapshot, expected_checkpoint=snapshot["checkpoint"],
                                 restore_id="restore", reason="offline-backup", **changes)


@asynccontextmanager
async def backup_copy(repository, tmp_path):
    """Actual SQLite backup / consistent pg_dump, restored into an independent store."""
    schema = None
    if hasattr(repository, "pool"):
        import psycopg
        from agent_memory_postgres.repository import PostgresMemoryRepository
        from psycopg.conninfo import conninfo_to_dict, make_conninfo

        options = conninfo_to_dict(repository.pool.conninfo)
        original = options["options"].split("search_path=")[1]
        assert re.fullmatch(r"atom_behavior_[a-f0-9]+", original)
        schema = "purge_backup_" + uuid4().hex
        path = tmp_path / (schema + ".sql")
        pg_dump = shutil.which("pg_dump")
        if pg_dump is None:
            pytest.fail("Live PostgreSQL backup tests require pg_dump on PATH")
        await asyncio.to_thread(
            subprocess.run,
            [pg_dump, "--dbname",
             repository.pool.conninfo, "--schema", original, "--inserts",
             "--no-owner", "--no-privileges", "--file", str(path)],
            check=True, capture_output=True,
        )
        # Test-only schema relocation; the actual dump includes all tables, history and indexes.
        sql = "\n".join(line for line in path.read_text().splitlines()
                        if not line.startswith("\\")).replace(original, schema)
        connection = await psycopg.AsyncConnection.connect(
            repository.pool.conninfo, autocommit=True
        )
        async with connection:
            await connection.execute(sql, prepare=False)
        options["options"] = "-csearch_path=" + schema
        clone = PostgresMemoryRepository.from_dsn(make_conninfo(**options), max_size=4)
    else:
        from agent_memory.sqlite import SQLiteMemoryRepository

        path = tmp_path / ("backup-" + uuid4().hex + ".db")
        with closing(repository._connect()) as connection, closing(sqlite3.connect(path)) as target:
            connection.backup(target)
        clone = SQLiteMemoryRepository(path)
    kernel = MemoryKernel(clone, MetadataClaimExtractor(), TrustedMemoryPolicy(),
                          ReciprocalRankFusionReranker())
    try:
        await kernel.initialize()
        yield clone, kernel
    finally:
        await kernel.close()
        if schema:
            async with repository.pool.connection() as connection:
                await connection.execute(psycopg.sql.SQL("DROP SCHEMA {} CASCADE").format(
                    psycopg.sql.Identifier(schema)
                ))


async def erase(kernel, scope, identity, *, mode=ForgetMode.ERASE, all_in_scope=False):
    await kernel.forget(ForgetRequest(scope, memory_ids=(identity,), mode=mode,
                                     all_in_scope=all_in_scope))


@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
def test_old_backup_replay_preserves_other_sources_and_cleans_all_index_streams(
    store, tmp_path, mode
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, index_recovery = await seed(engine, kernel, scope, clock)
            _, session, client, _, _, _, queue, _ = h
            await index_recovery.rollover(
                recovery_id="new", expected_generation=0, reason="operator"
            )
            target = await client.durable_freeze_target(session, [1])
            refresh = ResourceRefreshQueue(engine.repository, scope, clock=lambda: clock[0])
            refresh_request = await refresh.submit(
                dedupe_key="refresh", serialization_key="view", definition_sha256="d" * 64,
                units={"one": [source_id(scope, "1")]},
            )
            lease = await refresh.claim("stale-worker", lease_seconds=30)
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, source_id(scope, "1"), mode=mode)
                await erase(kernel, scope, source_id(scope, "never-submitted"))
                snapshot = await restorer(engine.repository, scope, clock).export()
                operator = restorer(backup, scope, clock)
                receipt = await replay(operator, snapshot)
                assert receipt["replayed_entries"] == 2 and receipt["affected"]["events"] == 1
                assert await replay(operator, snapshot) == receipt
                assert (await operator.export())["checkpoint"] == snapshot["checkpoint"]
                clone_h = await service(base.AdmissionEngine(backup), restored_kernel, scope, clock)
                producer, clone_session, clone_client, _, _, _, clone_queue, _ = clone_h
                assert clone_session == session
                assert (await status(clone_client, clone_session, target))["state"] == "blocked"
                assert (await producer.cursor(scope, session, actor="alice"))["acked_through"] == 2
                async with backup.unit_of_work() as uow:
                    assert await uow.get_source_event(scope, source_id(scope, "1")) is None
                    assert await uow.get_source_event(scope, source_id(scope, "2")) is not None
                    assert await uow.source_erased(scope, source_id(scope, "never-submitted"))
                    for key in (queue.channel.key, await clone_queue.active_key(uow)):
                        for job in await uow.index_jobs(scope, key, 0):
                            if job["event_id"] == source_id(scope, "1"):
                                assert job["status"] == "cancelled" and "applied" not in job
                                for item in job["dispositions"]:
                                    assert await uow.index_document_get(
                                        scope, key, item["candidate_id"]
                                    ) is None
                if mode == ForgetMode.ERASE:
                    tables = ["events", "claims", "artifacts", "admission_records",
                              "admission_versions", "claim_versions", "retention_entries",
                              "resource_refresh", "index_jobs"]
                    async with backup.unit_of_work() as uow:
                        for table in tables:
                            if hasattr(backup, "pool"):
                                cursor = await uow.connection.execute(
                                    "SELECT * FROM agent_memory_" + table
                                )
                                rows = await cursor.fetchall()
                            else:
                                rows = [dict(r) for r in uow.connection.execute(
                                    "SELECT * FROM " + table
                                ).fetchall()]
                            assert "private-offline-marker 1" not in json.dumps(rows, default=str)
                old_refresh = ResourceRefreshQueue(backup, scope, clock=lambda: clock[0])
                refreshed = await old_refresh.status(refresh_request["request_id"])
                assert refreshed["state"] == "blocked"
                with pytest.raises(WorkerQueueError):
                    await old_refresh.heartbeat(lease, lease_seconds=30)
                from agent_memory_sdk import MemoryClientError

                with pytest.raises(MemoryClientError, match="source_identity_erased"):
                    await clone_client.durable_append(
                        envelope(scope, "never-submitted", clock), session, 3
                    )
                with pytest.raises(RetentionError, match="idempotency_conflict"):
                    await operator.replay(snapshot, expected_checkpoint=snapshot["checkpoint"],
                                          restore_id="restore", reason="different")

    asyncio.run(run())


def test_scope_erase_replay_fences_old_ticket_and_offline_device_but_allows_new_authorized_input(
    store, tmp_path
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, _ = await seed(engine, kernel, scope, clock, count=1)
            _, session, _, _, _, _, _, _ = h
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, "", all_in_scope=True)
                snapshot = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), snapshot)
                receiver = DurableReceiver(backup, clock=lambda: clock[0])
                from agent_memory.capture.producer import DurableProducer

                producer = DurableProducer(receiver)
                with pytest.raises(RetentionError, match="producer_revoked"):
                    await producer.cursor(scope, session, actor="alice")
                async with backup.unit_of_work() as uow:
                    assert await uow.retention_epoch(scope) == 1
                    assert await uow.purge_head(scope) == 1
                    assert await uow.get_source_event(scope, source_id(scope, "1")) is None
                fresh = await producer.open(
                    scope, producer_id="new-device", actor="alice", configuration_sha256="a" * 64
                )
                result = await producer.append(
                    replace(base.source(scope, identity="explicit-new-input"), actor="alice"),
                    fresh, sequence=1, actor="alice",
                )
                assert result["receipt"]["epoch"] == 1
                assert (await restorer(backup, scope, clock).export())["checkpoint"] == snapshot[
                    "checkpoint"
                ]

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["missing", "signature", "entries", "epoch", "scope", "pin"])
def test_incomplete_tampered_or_stale_authority_cannot_mutate_backup(store, tmp_path, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await seed(engine, kernel, scope, clock, count=1)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, source_id(scope, "1"))
                operator = restorer(backup, scope, clock)
                snapshot = await restorer(engine.repository, scope, clock).export()
                expected = json.loads(json.dumps(snapshot["checkpoint"]))
                if damage == "missing":
                    snapshot["entries"] = []
                elif damage == "signature":
                    snapshot["signature"] = "0" * 64
                elif damage == "entries":
                    snapshot["entries"][0]["source_event_id"] = "another"
                elif damage == "epoch":
                    snapshot["checkpoint"]["scope_epoch"] = 1
                elif damage == "scope":
                    snapshot["checkpoint"]["scope_key"] = "another"
                else:
                    expected["head"] = 2
                with pytest.raises(RetentionError):
                    await operator.replay(snapshot, expected_checkpoint=expected,
                                          restore_id="restore", reason="offline-backup")
                async with backup.unit_of_work() as uow:
                    assert await uow.get_source_event(scope, source_id(scope, "1")) is not None
                    assert await uow.purge_head(scope) == 0
                    assert await uow.purge_restore_count(scope) == 0

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["gap", "rollback", "epoch", "fork", "capacity"])
def test_export_and_replay_reject_damaged_local_history_without_skipping_gaps(store, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await erase(kernel, scope, "one")
            operator = restorer(engine.repository, scope, clock)
            old = await operator.export()
            await erase(kernel, scope, "two")
            if damage == "rollback":
                with pytest.raises(RetentionError, match="history_conflict"):
                    await replay(operator, old)
                return
            cls = type(engine.repository.unit_of_work())
            if damage in {"gap", "fork"}:
                if damage == "fork":
                    old = await operator.export()
                original = cls.purge_page

                async def damaged(uow, scope, after, limit):
                    rows = list(await original(uow, scope, after, limit))
                    if damage == "gap":
                        return rows[1:]
                    rows[0]["source_event_id"] = "forked"
                    return tuple(rows)

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(cls, "purge_page", damaged)
                    if damage == "gap":
                        with pytest.raises(RetentionError, match="history_unavailable"):
                            await operator.export()
                    else:
                        with pytest.raises(RetentionError, match="history_conflict"):
                            await replay(operator, old)
            else:
                method = "retention_epoch" if damage == "epoch" else "purge_head"

                async def invalid(uow, scope):
                    return 10 if damage == "epoch" else 4097

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(cls, method, invalid)
                    with pytest.raises(RetentionError):
                        await operator.export()
            async with engine.repository.unit_of_work() as uow:
                assert await uow.purge_restore_count(scope) == 0

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["delete", "journal", "receipt"])
def test_replay_rolls_back_content_epoch_tasks_and_journal_together(store, tmp_path, boundary):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await seed(engine, kernel, scope, clock, count=1)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "", all_in_scope=True)
                snapshot = await restorer(engine.repository, scope, clock).export()
                operator = restorer(backup, scope, clock)
                cls = type(backup.unit_of_work())
                method = {"delete": "forget_for_restore", "journal": "purge_import",
                          "receipt": "purge_restore_put"}[boundary]
                original = getattr(cls, method)

                async def failure(uow, *args):
                    await original(uow, *args)
                    raise RuntimeError("injected")

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(cls, method, failure)
                    with pytest.raises(RuntimeError, match="injected"):
                        await replay(operator, snapshot)
                async with backup.unit_of_work() as uow:
                    assert await uow.retention_epoch(scope) == 0
                    assert await uow.purge_head(scope) == 0
                    assert await uow.get_source_event(scope, source_id(scope, "1")) is not None
                    assert await uow.purge_restore_count(scope) == 0
                assert (await replay(operator, snapshot))["replayed_entries"] == 1

    asyncio.run(run())


def test_replay_concurrency_duplicate_identity_and_normal_deletion_serializes(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await seed(engine, kernel, scope, clock, count=2)
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, source_id(scope, "1"))
                snapshot = await restorer(engine.repository, scope, clock).export()
                operator = restorer(backup, scope, clock)
                receipts = await asyncio.gather(*[replay(operator, snapshot) for _ in range(4)])
                assert all(r == receipts[0] for r in receipts)
                async with backup.unit_of_work() as uow:
                    assert await uow.purge_restore_count(scope) == 1
                await erase(restored_kernel, scope, source_id(scope, "2"))
                with pytest.raises(RetentionError, match="history_conflict"):
                    await replay(operator, snapshot)
                assert (await operator.export())["checkpoint"]["head"] == 2

    asyncio.run(run())


def test_checkpoint_is_independent_and_empty_history_does_not_forge_deletion(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            operator = restorer(engine.repository, scope, clock)
            old = await operator.export()
            assert old["entries"] == [] and old["checkpoint"]["entries_sha256"] == digest([])
            receipt = await replay(operator, old)
            assert receipt["replayed_entries"] == 0 and receipt["affected"]["events"] == 0
            await erase(kernel, scope, "one")
            current = await operator.export()
            with pytest.raises(RetentionError, match="checkpoint_mismatch"):
                await operator.replay(old, expected_checkpoint=current["checkpoint"],
                                      restore_id="stale", reason="offline-backup")
            wrong = restorer(engine.repository, replace(scope, user_id="other"), clock)
            with pytest.raises(RetentionError, match="authority_mismatch"):
                await replay(wrong, current)

    asyncio.run(run())


def test_restore_is_not_a_model_dispatch_operation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await service(engine, kernel, scope, clock)
            _, session, _, api, _, _, _, _ = h
            for operation in ("purge_restore", "purge_export"):
                with pytest.raises(RetentionError, match="unsupported_durable_operation"):
                    await api.call(operation, {"session": {
                        "producer_id": session.producer_id, "epoch": session.epoch,
                        "token": session.token,
                        "configuration_sha256": session.configuration_sha256,
                    }}, MCPRequestContext(scope, actor="alice"))

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_real_process_kill_replays_old_backup_atomically_and_retries_same_receipt(
    store, tmp_path, boundary
):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, kernel, scope, clock):
            await seed(engine, kernel, scope, clock, count=1)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "", all_in_scope=True)
                snapshot = await restorer(engine.repository, scope, clock).export()
                await kill_at_boundary(
                    base.AdmissionEngine(backup), scope, clock, tmp_path,
                    "purge_restore_" + boundary,
                    extra={"snapshot": snapshot, "test_secret": SECRET.decode()},
                )
                committed = boundary == "after_commit"
                async with backup.unit_of_work() as uow:
                    assert await uow.purge_head(scope) == int(committed)
                    assert await uow.retention_epoch(scope) == int(committed)
                    assert await uow.purge_restore_count(scope) == int(committed)
                    assert (await uow.get_source_event(scope, source_id(scope, "1")) is None
                            ) == committed
                operator = restorer(backup, scope, clock)
                receipt = await replay(operator, snapshot)
                assert receipt["replayed_entries"] == 1
                assert await replay(operator, snapshot) == receipt
                assert (await operator.export())["checkpoint"] == snapshot["checkpoint"]

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["cursor", "mode", "epoch", "scope_identity", "extra"])
def test_even_signed_malformed_history_cannot_claim_complete_recovery(store, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await erase(kernel, scope, "one")
            operator = restorer(engine.repository, scope, clock)
            snapshot = await operator.export()
            entry = snapshot["entries"][0]
            if damage == "cursor":
                entry["cursor"] = True
            elif damage == "mode":
                entry["mode"] = "retract"
            elif damage == "epoch":
                entry["epoch"] = 1
            elif damage == "scope_identity":
                entry["all_in_scope"] = True
                entry["epoch"] = 1
                snapshot["checkpoint"]["scope_epoch"] = 1
            else:
                entry["content"] = "forbidden-body"
            snapshot["checkpoint"]["entries_sha256"] = digest(snapshot["entries"])
            body = {k: snapshot[k] for k in ("schema", "checkpoint", "entries")}
            snapshot["signature"] = operator._sign(body)
            with pytest.raises(RetentionError, match="history_unavailable"):
                await replay(operator, snapshot)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.purge_restore_count(scope) == 0

    asyncio.run(run())


def test_multiple_pages_epoch_order_and_existing_prefix_do_not_duplicate_scope_deletion(
    store, tmp_path
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await erase(kernel, scope, "", all_in_scope=True)
            await kernel.forget(ForgetRequest(
                scope, memory_ids=tuple(f"object-{i}" for i in range(129)), mode=ForgetMode.ERASE
            ))
            h = await service(engine, kernel, scope, clock)
            _, session, client, _, _, worker, _, indexer = h
            assert session.epoch == 1
            await client.durable_append(envelope(scope, "post-erase", clock), session, 1)
            assert await worker.run_once() and await indexer.run_once()
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "last")
                snapshot = await restorer(engine.repository, scope, clock).export()
                assert len(snapshot["entries"]) == 131
                receipt = await replay(restorer(backup, scope, clock), snapshot)
                assert receipt["from_cursor"] == 130 and receipt["replayed_entries"] == 1
                async with backup.unit_of_work() as uow:
                    assert await uow.retention_epoch(scope) == 1
                    # The historical scope erase is not reapplied to legitimate later-epoch input.
                    assert await uow.get_source_event(scope, source_id(scope, "post-erase"))
                    assert await uow.purge_head(scope) == 131

    asyncio.run(run())


def test_cross_connection_replay_competes_with_deletion_without_losing_either_operation(
    store, tmp_path
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await seed(engine, kernel, scope, clock, count=2)
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, source_id(scope, "1"))
                snapshot = await restorer(engine.repository, scope, clock).export()
                if hasattr(backup, "pool"):
                    other = type(backup).from_dsn(backup.pool.conninfo, max_size=2)
                else:
                    other = type(backup)(backup._path)
                try:
                    await other.initialize()
                    result, _ = await asyncio.gather(
                        replay(restorer(other, scope, clock), snapshot),
                        erase(restored_kernel, scope, source_id(scope, "2")),
                        return_exceptions=True,
                    )
                    assert _ is None
                    async with backup.unit_of_work() as uow:
                        assert await uow.get_source_event(scope, source_id(scope, "2")) is None
                        entries = await uow.purge_page(scope, 0, 128)
                        if isinstance(result, RetentionError):
                            assert result.code == "purge_restore_history_conflict"
                            assert len(entries) == 1
                            assert await uow.get_source_event(scope, source_id(scope, "1"))
                        else:
                            assert not isinstance(result, BaseException), result
                            assert result["state"] == "replayed" and len(entries) == 2
                            assert await uow.get_source_event(scope, source_id(scope, "1")) is None
                    await other.initialize()
                    async with other.unit_of_work() as uow:
                        assert await uow.purge_restore_count(scope) == int(isinstance(result, dict))
                finally:
                    if hasattr(other, "close"):
                        await other.close()

    asyncio.run(run())


def test_key_rotation_unsupported_ports_and_bounded_audit_fail_explicitly(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await erase(kernel, scope, "one")
            operator = restorer(engine.repository, scope, clock)
            snapshot = await operator.export()
            different_key = PurgeRestore(engine.repository, scope, authority_id="authority",
                                        secret=b"another-key" * 4, actor="operator")
            with pytest.raises(RetentionError, match="integrity_unavailable"):
                await replay(different_key, snapshot)
            cls = type(engine.repository.unit_of_work())
            with monkeypatch.context() as patch:
                patch.setattr(cls, "purge_import", None)
                with pytest.raises(RetentionError, match="restore_unsupported"):
                    await replay(operator, snapshot)
            async def full(uow, scope):
                return 1000
            with monkeypatch.context() as patch:
                patch.setattr(cls, "purge_restore_count", full)
                with pytest.raises(RetentionError, match="restore_capacity"):
                    await replay(operator, snapshot)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.purge_restore_count(scope) == 0

    asyncio.run(run())


@pytest.mark.parametrize("scope_erase", [False, True])
def test_offline_outbox_uses_replayed_original_purge_cursor_before_body_dispatch(
    store, tmp_path, scope_erase
):
    from test_durable_purge import client_for

    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, _ = await client_for(engine, kernel, scope, clock)
            path = tmp_path / "outbox.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True)
            outbox.append(envelope(scope, "erased-offline", clock))
            outbox.append(envelope(scope, "unrelated-new", clock))
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, source_id(scope, "erased-offline"),
                            all_in_scope=scope_erase)
                snapshot = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), snapshot)
                # Existing session is carried by the old backup, not registered with a new epoch.
                from agent_memory.capture.durable_api import DurableCaptureAPI
                from agent_memory.capture.producer import DurableProducer
                from agent_memory.lifecycle import LifecycleOrigin

                producer = DurableProducer(DurableReceiver(backup, clock=lambda: clock[0]))
                api = DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER)
                client = sdk.EmbeddedMemoryClient(
                    restored_kernel, MCPRequestContext(scope, actor="alice"), durable_capture=api
                )
                delivered = []

                class Observe:
                    def __getattr__(self, name):
                        return getattr(client, name)

                    async def durable_append(self, event, *args):
                        delivered.append(event["event_id"])
                        return await client.durable_append(event, *args)

                if scope_erase:
                    with pytest.raises(ValueError, match="purged"):
                        await outbox.flush_one(Observe())
                    assert delivered == []
                else:
                    assert (await outbox.flush_one(Observe()))["sequence"] == 2
                    assert delivered == ["unrelated-new"]
                with closing(sqlite3.connect(path)) as connection:
                    cursor = connection.execute(
                        "SELECT purge_cursor FROM durable_sessions"
                    ).fetchone()[0]
                    assert cursor == 1
                    assert all(row[0] == "null" for row in connection.execute(
                        "SELECT event_json FROM durable_pending"
                    ))

    asyncio.run(run())
