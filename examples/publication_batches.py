"""One immutable source, two L1 publications, explicit closure and exact index coverage."""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_sdk import EmbeddedMemoryClient

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import AtomReview, MemoryScope, PredicateSpec, SourceAuthority, utc_now
from agent_memory.kernel import MemoryKernel
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.indexing import CandidateIndexChannel, CandidateIndexQueue
from agent_memory.operations.publication_batches import PublicationPolicy
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


class LocalFactsAdapter:
    version = "local-facts-example/1"

    async def generate_atoms(self, event):
        return [
            dict(
                subject_id="alice",
                predicate=predicate,
                value=value,
                kind="fact",
                modality="asserted",
                source_quote=event.content,
            )
            for predicate, value in (("city", "Hangzhou"), ("locale", "zh-CN"))
        ]

    async def review_atoms(self, event, candidates):
        return [
            AtomReview(i, "supported", "durable", ("example",)) for i, _ in enumerate(candidates)
        ]


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
            adapter = LocalFactsAdapter()
            pipeline = AtomExtractionPipeline(adapter, adapter)
            policy = AdmissionPolicy([PredicateSpec("city"), PredicateSpec("locale")])
            authority = SourceAuthority(
                "user:alice", subjects=("alice",), predicates=("city", "locale")
            )
            channel = CandidateIndexChannel("local")
            publication = PublicationPolicy(batch_size=1)
            configuration = processing_configuration_sha256(
                pipeline, policy, authority, index_channel=channel, publication_policy=publication
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
                content="Alice lives in Hangzhou and prefers zh-CN",
                actor="alice",
            )
            await client.durable_append(event.to_dict(), session, 1)
            target = await client.durable_freeze_target(session, [1])
            extraction = ExtractionQueue(repository, scope, configuration)
            handler = DurableAtomHandler(
                extraction,
                pipeline,
                policy,
                authority,
                local_only=True,
                index_channel=channel,
                publication_policy=publication,
            )
            assert await BoundedWorker(
                extraction, {"memory.extract": handler}, worker_id="extract"
            ).run_once()
            pending = await client.durable_readiness(
                session, target["target_id"], stage="index_visible"
            )
            assert pending["state"] == "processing"
            index = CandidateIndexQueue(repository, scope, channel)
            indexer = BoundedWorker(index, {"memory.index": index.apply}, worker_id="index")
            assert await indexer.run_once()
            partial = await client.durable_readiness(
                session, target["target_id"], stage="index_visible"
            )
            assert partial["state"] == "processing"
            assert len(partial["covered_publication_ids"]) == 1
            assert await indexer.run_once()
            ready = await client.durable_wait_until(
                session, target["target_id"], stage="index_visible", timeout=0
            )
            assert ready["state"] == "reached" and ready["continuous_visible_through"] == 2
            async with repository.unit_of_work() as uow:
                slot = (await uow.list_admission_records(scope))[0]["slot_key"]
            assert len(await index.lookup(slot)) == 1
            print(
                "L1 published; candidate locator visible; finite coverage: 2; continuous prefix: 2."
            )
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
