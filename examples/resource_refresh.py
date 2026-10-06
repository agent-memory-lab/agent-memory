"""Coalesce host-owned refresh work and commit a local block with finite receipts."""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory.capture.producer import DurableProducer
from agent_memory.domain import MemoryBlock, MemoryEvent, MemoryScope, Provenance
from agent_memory.kernel import MemoryKernel
from agent_memory.operations.resource_refresh import ResourceRefreshQueue
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


async def main():
    with TemporaryDirectory(prefix="agent-memory-refresh-demo-") as directory:
        repo = SQLiteMemoryRepository(Path(directory) / "memory.db")
        kernel = MemoryKernel(
            repo, MetadataClaimExtractor(), TrustedMemoryPolicy(), ReciprocalRankFusionReranker()
        )
        await kernel.initialize()
        try:
            scope = MemoryScope("demo", user_id="alice", session_id="session")
            producer = DurableProducer(DurableReceiver(repo))
            session = await producer.open(
                scope, producer_id="host", actor="alice", configuration_sha256="a" * 64
            )
            sources = []
            for sequence in (1, 2):
                response = await producer.append(
                    MemoryEvent(scope, "message", f"Source {sequence}", actor="alice"),
                    session,
                    sequence=sequence,
                    actor="alice",
                )
                sources.append(response["receipt"]["source_event_id"])
            queue = ResourceRefreshQueue(repo, scope)
            first = await queue.submit(
                dedupe_key="first",
                serialization_key="source-inventory",
                definition_sha256="b" * 64,
                units={"one": [sources[0]]},
            )
            later = []

            async def handler(task, checkpoint):
                # Preparation is outside the publication transaction. This block is an
                # operational source inventory, not a generated fact or a persona.
                previous = await repo.read_block(scope, "source-inventory")
                if task.payload["claimed_through"] == ["one"]:
                    later.append(
                        await queue.submit(
                            dedupe_key="later",
                            serialization_key="source-inventory",
                            definition_sha256="b" * 64,
                            units={"two": [sources[1]]},
                        )
                    )
                inputs = tuple(
                    sorted(
                        set(previous.event_ids if previous else ())
                        | {source for values in task.payload["units"].values() for source in values}
                    )
                )
                block = MemoryBlock(
                    scope,
                    "Source inventory",
                    f"Processed sources: {len(inputs)}",
                    inputs,
                    id="source-inventory",
                    provenance=Provenance(source_event_ids=inputs),
                )

                async def write(uow, fixed_task):
                    await uow.save_block(
                        block, expected_version=previous.version if previous else 0
                    )
                    return fixed_task.payload["claimed_through"]

                await queue.commit(task, write)

            worker = BoundedWorker(queue, {"memory.refresh": handler}, worker_id="host")
            assert await worker.run_once()
            assert (await queue.status(first["request_id"]))["state"] == "reached"
            assert (await queue.status(later[0]["request_id"]))["state"] == "processing"
            assert await worker.run_once()
            assert (await queue.status(later[0]["request_id"]))["state"] == "reached"
            block = await repo.read_block(scope, "source-inventory")
            assert block.version == 2 and len(block.event_ids) == 2
            assert (await queue.status(first["request_id"]))["completed_units"] == ["one"]
            print("Fixed receipts; two serialized refresh commits; block version: 2.")
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
