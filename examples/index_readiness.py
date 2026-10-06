"""Opt-in local candidate locator: L1 publication precedes actual index visibility."""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_sdk import EmbeddedMemoryClient
from durable_readiness import LocalCityAdapter

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import MemoryScope, PredicateSpec, SourceAuthority, utc_now
from agent_memory.kernel import MemoryKernel
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.indexing import CandidateIndexChannel, CandidateIndexQueue
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


async def main():
    with TemporaryDirectory(prefix="agent-memory-index-demo-") as temporary:
        repository = SQLiteMemoryRepository(Path(temporary) / "memory.db")
        kernel = MemoryKernel(
            repository,
            MetadataClaimExtractor(),
            TrustedMemoryPolicy(),
            ReciprocalRankFusionReranker(),
        )
        await kernel.initialize()
        try:
            scope = MemoryScope("demo", user_id="alice", session_id="session")
            adapter = LocalCityAdapter()
            pipeline = AtomExtractionPipeline(adapter, adapter)
            policy = AdmissionPolicy([PredicateSpec("city")])
            authority = SourceAuthority("user:alice", subjects=("alice",), predicates=("city",))
            channel = CandidateIndexChannel("local")
            configuration = processing_configuration_sha256(
                pipeline, policy, authority, index_channel=channel
            )
            producer = DurableProducer(DurableReceiver(repository), index_channel=channel)
            session = await producer.open(
                scope, producer_id="device", actor="alice", configuration_sha256=configuration
            )
            client = EmbeddedMemoryClient(
                kernel,
                MCPRequestContext(scope, actor="alice"),
                durable_capture=DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER),
            )
            event = LifecycleEvent(
                scope,
                "one",
                LifecycleEventType.MESSAGE_RECEIVED,
                LifecycleOrigin.USER,
                utc_now(),
                "run",
                content="Alice lives in Hangzhou",
                actor="alice",
            )
            await client.durable_append(event.to_dict(), session, 1)
            target = await client.durable_freeze_target(session, [1])
            extraction = ExtractionQueue(repository, scope, configuration)
            handler = DurableAtomHandler(
                extraction, pipeline, policy, authority, local_only=True, index_channel=channel
            )
            assert await BoundedWorker(
                extraction, {"memory.extract": handler}, worker_id="extract"
            ).run_once()
            pending = await client.durable_readiness(
                session, target["target_id"], stage="index_visible"
            )
            assert pending["state"] == "processing"
            index = CandidateIndexQueue(repository, scope, channel)
            assert await BoundedWorker(
                index, {"memory.index": index.apply}, worker_id="index"
            ).run_once()
            ready = await client.durable_wait_until(
                session, target["target_id"], stage="index_visible", timeout=0
            )
            assert ready["state"] == "reached" and ready["continuous_visible_through"] == 1
            async with repository.unit_of_work() as uow:
                slot = (await uow.list_admission_records(scope))[0]["slot_key"]
            assert len(await index.lookup(slot)) == 1
            print(
                "L1 published; candidate locator visible; finite coverage: 1; continuous prefix: 1."
            )
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
