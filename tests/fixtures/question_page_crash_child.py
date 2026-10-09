"""Independent real RefreshHost process, killed at a page publication COMMIT."""

# ruff: noqa: E402 -- standalone child must select this checkout before imports

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for path in (ROOT / "src", ROOT / "packages/postgres/src", ROOT / "tests"):
    sys.path.insert(0, str(path))

import test_atom_admission as base
import test_project_admission_v7 as project
from fixtures.refresh_crash_child import emit, gate

from agent_memory.derived.question_model import QuestionContext
from agent_memory.derived.question_service import QuestionService
from agent_memory.domain import MemoryScope
from agent_memory.operations.refresh_host import RefreshHost
from agent_memory.operations.worker_tasks import WorkerLimits


async def main(config):
    if config["backend"] == "postgres":
        from agent_memory_postgres.repository import PostgresMemoryRepository

        repository = PostgresMemoryRepository.from_dsn(config["database"], max_size=2)
    else:
        repository = base.SQLiteMemoryRepository(config["database"])
    await repository.initialize()
    scope = MemoryScope(**config["scope"])
    clock = [datetime.fromisoformat(config["clock"])]
    service = QuestionService(
        project.service(base.AdmissionEngine(repository), scope, clock),
        QuestionContext.from_payload(config["context"]),
    )
    runner = RefreshHost(
        service.queue,
        worker_id="page-process",
        limits=WorkerLimits(max_concurrency=1, lease_seconds=5),
    )
    leases = []
    original_claim = service.queue.claim

    async def claim(*args, **kwargs):
        lease = await original_claim(*args, **kwargs)
        if lease:
            assert lease.task.payload["unit"]["schema"] == "question-page-refresh-unit/1"
            leases.append(lease)
            if config["mode"] != "drain":
                emit("claimed", lease=lease)
                await gate()
        return lease

    service.queue.claim = claim
    unit_type = type(repository.unit_of_work())
    original_put, original_exit = unit_type.derived_put, unit_type.__aexit__

    async def put(uow, item_scope, kind, key, row):
        await original_put(uow, item_scope, kind, key, row)
        if kind == "refresh_publication":
            uow._page_publication = True

    async def boundary():
        emit("boundary")
        await asyncio.Event().wait()

    async def at_commit(uow, *args):
        publishing = args[0] is None and getattr(uow, "_page_publication", False)
        if publishing and config["mode"] == "before_publication_commit":
            await boundary()
        result = await original_exit(uow, *args)
        if publishing and config["mode"] == "after_publication_commit":
            await boundary()
        return result

    unit_type.derived_put, unit_type.__aexit__ = put, at_commit
    try:
        for _ in range(8):
            result = await runner.run_once()
            if result.idle:
                emit("drained", leases=leases)
                return
        raise AssertionError("finite page workload did not drain")
    finally:
        runner.stop()
        close = getattr(repository, "close", None)
        if close:
            await close()


if __name__ == "__main__":
    asyncio.run(main(json.loads(Path(sys.argv[1]).read_text())))
