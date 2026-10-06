"""Real-process recovery fixture. Parent kills us only after the named SQL boundary."""

import asyncio
import json
import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import test_atom_admission as base
from test_durable_execution import Generator

from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import MemoryScope
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.retention import DurableReceiver


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
    scope = MemoryScope(**config["scope"])
    now = datetime.fromisoformat(config["clock"])
    receiver = DurableReceiver(repository, clock=lambda: now)
    phase = config["phase"]
    unit_type = type(repository.unit_of_work())
    if phase.startswith("receive_"):
        producer = DurableProducer(receiver)
        session = await producer.open(
            scope, producer_id="crash-device", actor="alice", configuration_sha256="a" * 64
        )
        event = replace(
            base.source(scope, identity="crash-receive", idempotency="crash-receive"), actor="alice"
        )
        if phase == "receive_before_commit":
            original = unit_type.producer_put

            async def before_commit(uow, *args):
                await original(uow, *args)
                await pause()

            unit_type.producer_put = before_commit
        await producer.append(event, session, sequence=1, actor="alice")
        await pause()
    elif phase.startswith("index_"):
        from agent_memory.operations.indexing import CandidateIndexChannel, CandidateIndexQueue

        queue = CandidateIndexQueue(
            repository, scope, CandidateIndexChannel("local"), clock=lambda: now
        )
        lease = await queue.claim("crashed-indexer", lease_seconds=5)
        if phase == "index_before_commit":
            original = unit_type.index_job_put

            async def before_commit(uow, scope, row):
                await original(uow, scope, row)
                if row["status"] == "completed":
                    await pause()

            unit_type.index_job_put = before_commit
        await queue.apply(lease.task, None)
        await pause()
    else:
        generator = Generator()
        pipeline = AtomExtractionPipeline(generator, generator)
        configuration = processing_configuration_sha256(pipeline, base.POLICY, base.SELF)
        queue = ExtractionQueue(
            repository, scope, configuration, clock=lambda: now, retry_seconds=0
        )
        handler = DurableAtomHandler(queue, pipeline, base.POLICY, base.SELF, local_only=True)
        lease = await queue.claim("crashed-worker", lease_seconds=5)
        if phase == "worker_after_claim":
            await pause()
        original_generate = generator.generate_atoms

        async def tracked_generation(event):
            with open(config["calls"], "a") as log:
                log.write("generate\n")
                log.flush()
            return await original_generate(event)

        generator.generate_atoms = tracked_generation
        if phase == "worker_before_publication_commit":
            original = unit_type.retention_update

            async def before_commit(uow, scope, identity, row):
                await original(uow, scope, identity, row)
                if row["status"] == "completed":
                    await pause()

            unit_type.retention_update = before_commit

        async def checkpoint(value):
            await queue.checkpoint(lease, value)
            if phase == "worker_after_checkpoint":
                await pause()

        await handler(lease.task, checkpoint)
        await pause()  # Publication committed; worker completion acknowledgment not sent.


if __name__ == "__main__":
    asyncio.run(main(json.loads(Path(sys.argv[1]).read_text())))
