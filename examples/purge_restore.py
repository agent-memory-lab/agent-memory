"""Keep a restored backup offline; replay a separately retained, pinned deletion journal.

Run: python examples/purge_restore.py
The host owns the signing key, journal retention and the service promotion decision.
"""

import asyncio
import json
import secrets
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory.domain import ForgetMode, ForgetRequest, MemoryEvent, MemoryScope
from agent_memory.kernel import MemoryKernel
from agent_memory.operations.purge_restore import PurgeRestore
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


def memory(repository):
    return MemoryKernel(repository, MetadataClaimExtractor(), TrustedMemoryPolicy(),
                        ReciprocalRankFusionReranker())


async def main():
    with TemporaryDirectory(prefix="memory-purge-restore-example-") as directory:
        root = Path(directory)
        content, backup = root / "live.db", root / "old-backup.db"
        # Control-plane files are outside the content backup. Protect them in deployment.
        control = root / "control-plane"
        control.mkdir()
        secret = secrets.token_bytes(32)
        scope = MemoryScope("example", user_id="alice", session_id="session")
        repository = SQLiteMemoryRepository(content)
        live = memory(repository)
        restored = None
        try:
            await live.initialize()
            for identity in ("erase-this", "keep-this"):
                await live.ingest_event(MemoryEvent(
                    scope, "message", "Private source " + identity, id=identity,
                    idempotency_key=identity,
                ))
            with closing(sqlite3.connect(content)) as connection:
                with closing(sqlite3.connect(backup)) as destination:
                    connection.backup(destination)
            authority = PurgeRestore(repository, scope, authority_id="host-journal", secret=secret,
                                     actor="host-operator")
            await live.forget(ForgetRequest(
                scope, memory_ids=("erase-this",), mode=ForgetMode.ERASE
            ))
            journal = await authority.export()
            (control / "journal.json").write_text(json.dumps(journal))
            (control / "latest-checkpoint.json").write_text(json.dumps(journal["checkpoint"]))

            target = SQLiteMemoryRepository(backup)
            restored = memory(target)
            await restored.initialize()  # Do not register this provider with a serving host yet.
            operator = PurgeRestore(target, scope, authority_id="host-journal", secret=secret,
                                    actor="host-operator")
            pinned = json.loads((control / "latest-checkpoint.json").read_text())
            snapshot = json.loads((control / "journal.json").read_text())
            options = dict(expected_checkpoint=pinned, restore_id="restore-1",
                           reason="offline-backup-recovery")
            receipt = await operator.replay(snapshot, **options)
            assert await operator.replay(snapshot, **options) == receipt
            async with target.unit_of_work() as uow:
                assert await uow.get_source_event(scope, "erase-this") is None
                assert await uow.get_source_event(scope, "keep-this") is not None
                assert await uow.purge_head(scope) == 1
            assert (await operator.export())["checkpoint"] == pinned
            # Host now rechecks the latest authority checkpoint and its deployment gates.
            print("Pinned journal replayed; deleted source absent; survivor retained; cursor 1.")
        finally:
            if restored is not None:
                await restored.close()
            await live.close()


if __name__ == "__main__":
    asyncio.run(main())
