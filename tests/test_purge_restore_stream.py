"""Complete, bounded-memory journals beyond the legacy JSON capacity boundary."""

import asyncio
import io
import json
import signal
import sys
from dataclasses import replace
from pathlib import Path

import pytest
import test_atom_admission as base
from test_purge_restore import SECRET, backup_copy, erase, restorer

from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.operations.purge_restore import MAX_BYTES, PurgeRestore
from agent_memory.operations.purge_restore_stream import MAX_FRAME_BYTES, PAGE_ENTRIES
from agent_memory.operations.retention import RetentionError
from agent_memory.serialization import to_jsonable

store = base.store


async def exported(operator):
    output = io.BytesIO()
    checkpoint = await operator.export_stream(output)
    return output.getvalue(), checkpoint


async def replayed(operator, raw, checkpoint, **options):
    return await operator.replay_stream(
        io.BytesIO(raw), expected_checkpoint=checkpoint,
        restore_id=options.pop("restore_id", "stream-restore"), reason="offline-backup", **options,
    )


def test_stream_restores_over_4096_real_deletions_and_over_legacy_bytes(store, tmp_path, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await kernel.ingest_event(base.source(scope, identity="erase-this"))
            await kernel.ingest_event(base.source(scope, identity="keep-this"))
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                # Real committed deletion requests, not a mocked head or injected SQL journal.
                identities = tuple(f"deleted-{i:04d}-" + "\U0001f539" * 50 for i in range(4096))
                await kernel.forget(ForgetRequest(scope, memory_ids=identities, mode=ForgetMode.ERASE))
                await erase(kernel, scope, "erase-this")
                operator = restorer(engine.repository, scope, clock)
                with pytest.raises(RetentionError, match="capacity"):
                    await operator.export()
                cls = type(engine.repository.unit_of_work())
                original = cls.purge_page
                sizes = []

                async def bounded(uow, scope, after, limit):
                    assert 1 <= limit <= PAGE_ENTRIES
                    page = await original(uow, scope, after, limit)
                    sizes.append(len(page))
                    return page

                with monkeypatch.context() as patch:
                    patch.setattr(cls, "purge_page", bounded)
                    path = tmp_path / "large-journal.jsonl"
                    with path.open("wb") as output:
                        checkpoint = await operator.export_stream(output)
                    assert checkpoint["head"] == 4097 and path.stat().st_size > MAX_BYTES
                    with path.open("rb") as source:
                        receipt = await restorer(backup, scope, clock).replay_stream(
                            source, expected_checkpoint=checkpoint,
                            restore_id="stream-restore", reason="offline-backup",
                        )
                assert max(sizes) == PAGE_ENTRIES
                assert receipt["replayed_entries"] == 4097
                assert receipt["affected"]["events"] == 1
                # Close/reopen a separate connection; signed output and idempotent receipt survive.
                if hasattr(backup, "pool"):
                    peer = type(backup).from_dsn(backup.pool.conninfo, max_size=2)
                else:
                    peer = type(backup)(backup._path)
                try:
                    await peer.initialize()
                    restored = restorer(peer, scope, clock)
                    with path.open("rb") as source:
                        assert await restored.replay_stream(
                            source, expected_checkpoint=checkpoint,
                            restore_id="stream-restore", reason="offline-backup",
                        ) == receipt
                    with (tmp_path / "restored.jsonl").open("wb") as output:
                        assert await restored.export_stream(output) == checkpoint
                    async with peer.unit_of_work() as uow:
                        assert await uow.purge_head(scope) == 4097
                        assert await uow.get_source_event(scope, "erase-this") is None
                        assert await uow.get_source_event(scope, "keep-this") is not None
                        assert await uow.source_erased(scope, identities[0])
                        assert await uow.source_erased(scope, identities[-1])
                finally:
                    if hasattr(peer, "close"):
                        await peer.close()
    asyncio.run(run())


@pytest.mark.parametrize("damage", [
    "truncated", "missing_page", "reordered", "duplicate", "tampered_last", "trailing",
    "signature", "wrong_pin", "wrong_scope", "wrong_authority", "wrong_key", "oversize",
])
def test_bad_stream_is_fully_rejected_before_any_write(store, tmp_path, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await kernel.ingest_event(base.source(scope, identity="erase-this"))
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await kernel.forget(ForgetRequest(
                    scope, memory_ids=("erase-this", *(f"missing-{i}" for i in range(256))),
                    mode=ForgetMode.ERASE,
                ))
                async with engine.repository.unit_of_work() as uow:
                    await uow.refresh_scheduler_observe_clock(scope, now=clock[0].isoformat())
                raw, checkpoint = await exported(restorer(engine.repository, scope, clock))
                lines = raw.splitlines(keepends=True)
                operator = restorer(backup, scope, clock)
                if damage == "truncated":
                    lines[-1] = lines[-1][:-2]
                elif damage == "missing_page":
                    del lines[-1]
                elif damage == "reordered":
                    lines[1], lines[2] = lines[2], lines[1]
                elif damage == "duplicate":
                    lines[2] = lines[1]
                elif damage == "tampered_last":
                    page = json.loads(lines[-1])
                    page[-1]["source_event_id"] = "different-source"
                    lines[-1] = json.dumps(page).encode() + b"\n"
                elif damage == "trailing":
                    lines.append(b"[]\n")
                elif damage == "signature":
                    manifest = json.loads(lines[0])
                    manifest["signature"] = "0" * 64
                    lines[0] = json.dumps(manifest).encode() + b"\n"
                elif damage == "wrong_pin":
                    checkpoint = {**checkpoint, "head": 258}
                elif damage == "wrong_scope":
                    operator = restorer(backup, replace(scope, user_id="other"), clock)
                elif damage == "wrong_authority":
                    operator = PurgeRestore(backup, scope, authority_id="other", secret=SECRET,
                                            actor="operator")
                elif damage == "wrong_key":
                    operator = PurgeRestore(backup, scope, authority_id="authority", secret=b"x" * 32,
                                            actor="operator")
                else:
                    lines[-1] = b" " * MAX_FRAME_BYTES + b"\n"
                with pytest.raises(RetentionError):
                    await replayed(operator, b"".join(lines), checkpoint)
                async with backup.unit_of_work() as uow:
                    assert await uow.get_source_event(scope, "erase-this")
                    assert await uow.purge_head(scope) == 0
                    assert await uow.purge_restore_count(scope) == 0
                    assert await uow.refresh_scheduler_clock(scope) is None
    asyncio.run(run())


def test_matching_receipt_does_not_bypass_full_stream_validation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await erase(kernel, scope, "one")
            operator = restorer(engine.repository, scope, clock)
            raw, checkpoint = await exported(operator)
            receipt = await replayed(operator, raw, checkpoint)
            for corrupted in (raw[:-1], raw + b"[]\n", raw.replace(b'"one"', b'"two"')):
                with pytest.raises(RetentionError):
                    await replayed(operator, corrupted, checkpoint)
            assert await replayed(operator, raw, checkpoint) == receipt
    asyncio.run(run())


def test_stream_prefix_scope_epoch_legacy_compatibility_and_clock_floor(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await kernel.forget(ForgetRequest(
                scope, memory_ids=tuple(f"missing-{i}" for i in range(129)), mode=ForgetMode.ERASE,
            ))
            await erase(kernel, scope, "", all_in_scope=True)
            legacy = await restorer(engine.repository, scope, clock).export()
            await kernel.ingest_event(base.source(scope, identity="later-epoch-survivor"))
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "last", mode=ForgetMode.ARCHIVE)
                async with engine.repository.unit_of_work() as uow:
                    await uow.refresh_scheduler_observe_clock(scope, now=clock[0].isoformat())
                raw, checkpoint = await exported(restorer(engine.repository, scope, clock))
                operator = restorer(backup, scope, clock)
                receipt = await replayed(operator, raw, checkpoint)
                assert receipt["from_cursor"] == 130 and receipt["replayed_entries"] == 1
                async with backup.unit_of_work() as uow:
                    assert await uow.retention_epoch(scope) == 1
                    assert await uow.get_source_event(scope, "later-epoch-survivor")
                    assert await uow.refresh_scheduler_clock(scope) == checkpoint["scheduler_clock_floor"]
                with pytest.raises(RetentionError):
                    await operator.replay(legacy, expected_checkpoint=legacy["checkpoint"],
                                          restore_id="old", reason="offline-backup")
                # Legacy formats and their hashes remain usable on small current journals.
                current_legacy = await operator.export()
                assert current_legacy["schema"] == "purge-restore-journal/2"
                assert current_legacy["checkpoint"]["entries_sha256"] == checkpoint["entries_sha256"]
                await operator.replay(current_legacy, expected_checkpoint=current_legacy["checkpoint"],
                                      restore_id="legacy-current", reason="offline-backup")
    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["delete", "import", "receipt"])
def test_stream_late_replay_failure_rolls_back_all_state(store, tmp_path, monkeypatch, boundary):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await kernel.ingest_event(base.source(scope, identity="erase-this"))
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "", all_in_scope=True)
                await kernel.forget(ForgetRequest(
                    scope, memory_ids=tuple(f"missing-{i}" for i in range(130)), mode=ForgetMode.ERASE,
                ))
                async with engine.repository.unit_of_work() as uow:
                    await uow.refresh_scheduler_observe_clock(scope, now=clock[0].isoformat())
                raw, checkpoint = await exported(restorer(engine.repository, scope, clock))
                operator = restorer(backup, scope, clock)
                cls = type(backup.unit_of_work())
                method = {"delete": "forget_for_restore", "import": "purge_import",
                          "receipt": "purge_restore_put"}[boundary]
                original = getattr(cls, method)
                calls = 0

                async def fail_late(uow, *args):
                    nonlocal calls
                    result = await original(uow, *args)
                    calls += 1
                    if calls == (1 if boundary == "receipt" else 131):
                        raise RuntimeError("late failure")
                    return result

                with monkeypatch.context() as patch:
                    patch.setattr(cls, method, fail_late)
                    with pytest.raises(RuntimeError, match="late failure"):
                        await replayed(operator, raw, checkpoint)
                async with backup.unit_of_work() as uow:
                    assert await uow.get_source_event(scope, "erase-this")
                    assert await uow.purge_head(scope) == 0
                    assert await uow.retention_epoch(scope) == 0
                    assert await uow.purge_restore_count(scope) == 0
                    assert await uow.refresh_scheduler_clock(scope) is None
                assert (await replayed(operator, raw, checkpoint))["replayed_entries"] == 131
    asyncio.run(run())


def test_independent_checkpoint_is_copied_before_stream_can_mutate_it(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            operator = restorer(engine.repository, scope, clock)
            old, old_pin = await exported(operator)
            await erase(kernel, scope, "one")
            _, latest = await exported(operator)

            class MutatingInput(io.BytesIO):
                def readline(self, limit):
                    latest.clear()
                    latest.update(old_pin)
                    return super().readline(limit)

            with pytest.raises(RetentionError, match="checkpoint_mismatch"):
                await operator.replay_stream(MutatingInput(old), expected_checkpoint=latest,
                                             restore_id="old", reason="offline-backup")
            async with engine.repository.unit_of_work() as uow:
                assert await uow.purge_restore_count(scope) == 0
    asyncio.run(run())


@pytest.mark.parametrize("damage", ["fork", "gap", "hidden_tail", "overflow"])
def test_stream_export_and_replay_reject_broken_local_prefix(store, monkeypatch, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await erase(kernel, scope, "one")
            await erase(kernel, scope, "two")
            operator = restorer(engine.repository, scope, clock)
            raw, checkpoint = await exported(operator)
            cls = type(engine.repository.unit_of_work())
            original = cls.purge_page

            async def damaged_page(uow, scope, after, limit):
                rows = list(await original(uow, scope, after, limit))
                if rows:
                    if damage == "gap":
                        return rows[1:]
                    if damage == "fork":
                        rows[0]["source_event_id"] = "forked"
                return tuple(rows)

            async def damaged_head(uow, scope):
                return 1 if damage == "hidden_tail" else 2**63

            with monkeypatch.context() as patch:
                patch.setattr(cls, "purge_page", damaged_page)
                if damage in {"hidden_tail", "overflow"}:
                    patch.setattr(cls, "purge_head", damaged_head)
                if damage != "fork":
                    with pytest.raises(RetentionError):
                        await exported(operator)
                with pytest.raises(RetentionError):
                    await replayed(operator, raw, checkpoint)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.purge_restore_count(scope) == 0
    asyncio.run(run())


@pytest.mark.parametrize("damage", ["cursor", "mode", "epoch", "identity", "extra", "duplicate_key"])
def test_even_signed_malformed_stream_cannot_claim_complete_recovery(store, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await erase(kernel, scope, "one")
            operator = restorer(engine.repository, scope, clock)
            raw, _ = await exported(operator)
            manifest, page = [json.loads(line) for line in raw.splitlines()]
            if damage == "duplicate_key":
                raw = raw.replace(b'"cursor":1', b'"cursor":1,"cursor":1')
            else:
                if damage == "cursor":
                    page[0]["cursor"] = True
                elif damage == "mode":
                    page[0]["mode"] = "retract"
                elif damage == "epoch":
                    page[0]["epoch"] = 1
                elif damage == "identity":
                    page[0]["source_event_id"] = "x" * 257
                else:
                    page[0]["content"] = "forbidden"
                from agent_memory.operations.purge_restore import digest

                manifest["checkpoint"]["entries_sha256"] = digest(page)
                manifest["signature"] = operator._sign(
                    {key: manifest[key] for key in ("schema", "checkpoint")})
                raw = (json.dumps(manifest) + "\n" + json.dumps(page) + "\n").encode()
            with pytest.raises(RetentionError):
                await replayed(operator, raw, manifest["checkpoint"])
            async with engine.repository.unit_of_work() as uow:
                assert await uow.purge_restore_count(scope) == 0
    asyncio.run(run())


def test_cancellation_during_stream_replay_rolls_back_the_prefix(store, tmp_path, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await kernel.ingest_event(base.source(scope, identity="erase-this"))
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "", all_in_scope=True)
                raw, checkpoint = await exported(restorer(engine.repository, scope, clock))
                cls = type(backup.unit_of_work())
                original = cls.purge_import

                async def cancel(uow, *args):
                    await original(uow, *args)
                    raise asyncio.CancelledError

                with monkeypatch.context() as patch:
                    patch.setattr(cls, "purge_import", cancel)
                    with pytest.raises(asyncio.CancelledError):
                        await replayed(restorer(backup, scope, clock), raw, checkpoint)
                async with backup.unit_of_work() as uow:
                    assert await uow.get_source_event(scope, "erase-this")
                    assert await uow.purge_head(scope) == 0
                    assert await uow.retention_epoch(scope) == 0
                    assert await uow.purge_restore_count(scope) == 0
    asyncio.run(run())


def test_replay_uses_private_copy_after_source_changes(store, tmp_path, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await kernel.ingest_event(base.source(scope, identity="erase-this"))
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "erase-this")
                raw, checkpoint = await exported(restorer(engine.repository, scope, clock))
                source = io.BytesIO(raw)
                operator = restorer(backup, scope, clock)
                original = operator._open

                async def mutate_source(uow):
                    # Opening the write transaction happens after complete validation.
                    source.seek(0)
                    source.truncate(0)
                    source.write(b"replaced-after-validation")
                    await original(uow)

                monkeypatch.setattr(operator, "_open", mutate_source)
                receipt = await operator.replay_stream(
                    source, expected_checkpoint=checkpoint,
                    restore_id="stream-restore", reason="offline-backup",
                )
                assert receipt["replayed_entries"] == 1
                assert source.getvalue() == b"replaced-after-validation"
                async with backup.unit_of_work() as uow:
                    assert await uow.get_source_event(scope, "erase-this") is None
                    assert await uow.source_erased(scope, "erase-this")
    asyncio.run(run())


def test_export_second_pass_drift_never_returns_a_pin(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await erase(kernel, scope, "one")
            operator = restorer(engine.repository, scope, clock)
            _, trusted_pin = await exported(operator)
            cls = type(engine.repository.unit_of_work())
            original = cls.purge_page
            first_pages = 0

            async def inconsistent(uow, scope, after, limit):
                nonlocal first_pages
                rows = list(await original(uow, scope, after, limit))
                if after == 0:
                    first_pages += 1
                    if first_pages == 2:
                        rows[0]["source_event_id"] = "different"
                return tuple(rows)

            output = io.BytesIO()
            with monkeypatch.context() as patch:
                patch.setattr(cls, "purge_page", inconsistent)
                with pytest.raises(RetentionError, match="history_conflict"):
                    await operator.export_stream(output)
            with pytest.raises(RetentionError, match="integrity_unavailable"):
                await replayed(operator, output.getvalue(), trusted_pin)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.purge_restore_count(scope) == 0
                assert await uow.source_erased(scope, "one")
                assert not await uow.source_erased(scope, "different")
    asyncio.run(run())


def test_stream_empty_history_and_partial_write_sink(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            class PartialSink(io.BytesIO):
                def write(self, data):
                    return super().write(data[:7])

            operator = restorer(engine.repository, scope, clock)
            output = PartialSink()
            checkpoint = await operator.export_stream(output)
            assert checkpoint["head"] == 0
            assert (await replayed(operator, output.getvalue(), checkpoint))["replayed_entries"] == 0
    asyncio.run(run())


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="requires real SIGKILL")
@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_large_stream_process_kill_and_retry_preserve_atomic_recovery(store, tmp_path, boundary):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await kernel.ingest_event(base.source(scope, identity="erase-this"))
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "", all_in_scope=True)
                await kernel.forget(ForgetRequest(
                    scope, memory_ids=tuple(f"missing-{i}" for i in range(4096)),
                    mode=ForgetMode.ERASE,
                ))
                async with engine.repository.unit_of_work() as uow:
                    await uow.refresh_scheduler_observe_clock(scope, now=clock[0].isoformat())
                journal = tmp_path / "journal.jsonl"
                with journal.open("wb") as output:
                    checkpoint = await restorer(engine.repository, scope, clock).export_stream(output)
                config = {
                    "backend": "postgres" if hasattr(backup, "pool") else "sqlite",
                    "database": backup.pool.conninfo if hasattr(backup, "pool") else str(backup._path),
                    "scope": to_jsonable(scope), "clock": clock[0].isoformat(),
                    "boundary": boundary, "journal": str(journal), "checkpoint": checkpoint,
                    "test_secret": SECRET.decode(),
                }
                path = tmp_path / "child.json"
                path.write_text(json.dumps(config))
                child = Path(__file__).parent / "fixtures" / "purge_stream_crash_child.py"
                process = await asyncio.create_subprocess_exec(
                    sys.executable, str(child), str(path),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), timeout=90)
                    if line != b"ready\n":
                        error = await process.stderr.read()
                        pytest.fail(error.decode())
                    process.kill()
                    assert await asyncio.wait_for(process.wait(), timeout=10) == -signal.SIGKILL
                finally:
                    if process.returncode is None:
                        process.kill()
                        await process.wait()
                committed = boundary == "after_commit"
                async with backup.unit_of_work() as uow:
                    assert await uow.purge_head(scope) == (4097 if committed else 0)
                    assert await uow.retention_epoch(scope) == int(committed)
                    assert await uow.purge_restore_count(scope) == int(committed)
                    assert (await uow.get_source_event(scope, "erase-this") is None) == committed
                    assert await uow.refresh_scheduler_clock(scope) == (
                        checkpoint["scheduler_clock_floor"] if committed else None)
                operator = restorer(backup, scope, clock)
                with journal.open("rb") as source:
                    receipt = await operator.replay_stream(
                        source, expected_checkpoint=checkpoint,
                        restore_id="stream-restore", reason="offline-backup",
                    )
                with journal.open("rb") as source:
                    assert await operator.replay_stream(
                        source, expected_checkpoint=checkpoint,
                        restore_id="stream-restore", reason="offline-backup",
                    ) == receipt
                assert receipt["replayed_entries"] == 4097
    asyncio.run(run())
