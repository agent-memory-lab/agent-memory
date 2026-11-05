"""The parent SIGKILLs this process at a streamed-restore commit boundary."""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

# Exercise this checkout, never an unrelated editable installation.
ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "src"), *(str(path) for path in (ROOT / "packages").glob("*/src"))]

from agent_memory.domain import MemoryScope
from agent_memory.operations.purge_restore import PurgeRestore


async def pause():
    print("ready", flush=True)
    await asyncio.Event().wait()


async def main(config):
    if config["backend"] == "postgres":
        from agent_memory_postgres.repository import PostgresMemoryRepository

        repository = PostgresMemoryRepository.from_dsn(config["database"], max_size=2)
    else:
        from agent_memory.sqlite import SQLiteMemoryRepository

        repository = SQLiteMemoryRepository(config["database"])
    await repository.initialize()
    operator = PurgeRestore(
        repository, MemoryScope(**config["scope"]), authority_id="authority", actor="operator",
        secret=config["test_secret"].encode(), clock=lambda: datetime.fromisoformat(config["clock"]),
    )
    cls = type(repository.unit_of_work())
    original_import = cls.purge_import

    async def import_with_progress(uow, scope, entry):
        result = await original_import(uow, scope, entry)
        if entry["cursor"] % 128 == 0:
            print(f"progress {entry['cursor']}", flush=True)
        return result

    cls.purge_import = import_with_progress
    if config["boundary"] == "before_commit":
        original = cls.purge_restore_put

        async def before_commit(uow, *args):
            await original(uow, *args)
            await pause()

        cls.purge_restore_put = before_commit
    with Path(config["journal"]).open("rb") as source:
        await operator.replay_stream(
            source, expected_checkpoint=config["checkpoint"], restore_id="stream-restore",
            reason="offline-backup",
        )
    await pause()


if __name__ == "__main__":
    asyncio.run(main(json.loads(Path(sys.argv[1]).read_text())))
