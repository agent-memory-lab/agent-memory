"""Independent QuestionService worker stopped at an actual publication COMMIT."""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_atom_admission as base
import test_project_admission_v7 as project
from fixtures.refresh_crash_child import emit, gate

from agent_memory.derived.question_model import QuestionContext
from agent_memory.derived.question_service import QuestionService
from agent_memory.domain import MemoryScope


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
    queue = service.queue
    try:
        await queue.initialize()
        if config["mode"] == "drain":
            leases = []
            for _ in range(8):
                lease = await queue.claim("new-question-process", lease_seconds=60)
                if lease is None:
                    emit("drained", leases=leases)
                    return
                await queue.apply(lease.task)
                await queue.complete(lease)
                leases.append(lease)
            raise AssertionError("finite question workload did not drain")
        lease = await queue.claim("crashed-question-process", lease_seconds=5)
        assert lease is not None
        emit("claimed", lease=lease)
        await gate()
        unit_type = type(repository.unit_of_work())
        original_put, original_exit = unit_type.derived_put, unit_type.__aexit__

        async def put(uow, item_scope, kind, key, row):
            await original_put(uow, item_scope, kind, key, row)
            if kind == "question_head":
                uow._question_compute_mode = row["compute_mode"]
            if kind == "refresh_publication":
                uow._question_publication = True

        async def boundary(uow):
            emit("boundary", compute_mode=uow._question_compute_mode)
            await asyncio.Event().wait()

        async def at_commit(uow, *args):
            publishing = args[0] is None and getattr(uow, "_question_publication", False)
            if publishing and config["mode"] == "before_publication_commit":
                await boundary(uow)
            result = await original_exit(uow, *args)
            if publishing and config["mode"] == "after_publication_commit":
                await boundary(uow)
            return result

        unit_type.derived_put, unit_type.__aexit__ = put, at_commit
        await queue.apply(lease.task)
        raise AssertionError("question publication missed its COMMIT boundary")
    finally:
        close = getattr(repository, "close", None)
        if close:
            await close()


if __name__ == "__main__":
    asyncio.run(main(json.loads(Path(sys.argv[1]).read_text())))
