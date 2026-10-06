"""Host submits a new interpretation; SDK waits for that fixed processing target."""

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
from agent_memory.operations.reprocessing import ReprocessingService
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


class NewLocalCityAdapter(LocalCityAdapter):
    version = "local-city-example/2"


async def main():
    with TemporaryDirectory(prefix="agent-memory-reprocessing-demo-") as temporary:
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
            policy = AdmissionPolicy([PredicateSpec("city")])
            authority = SourceAuthority("user:alice", subjects=("alice",), predicates=("city",))
            channel = CandidateIndexChannel("local")
            receiver = DurableReceiver(repository)

            def processor(adapter):
                pipeline = AtomExtractionPipeline(adapter, adapter)
                config = processing_configuration_sha256(
                    pipeline, policy, authority, index_channel=channel
                )
                queue = ExtractionQueue(repository, scope, config)
                handler = DurableAtomHandler(
                    queue, pipeline, policy, authority, local_only=True, index_channel=channel
                )
                return config, BoundedWorker(queue, {"memory.extract": handler}, worker_id="host")

            config, initial = processor(LocalCityAdapter())
            producer = DurableProducer(receiver, index_channel=channel)
            session = await producer.open(
                scope, producer_id="device", actor="alice", configuration_sha256=config
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
            received = await client.durable_append(event.to_dict(), session, 1)
            assert await initial.run_once()
            index = CandidateIndexQueue(repository, scope, channel)
            indexer = BoundedWorker(index, {"memory.index": index.apply}, worker_id="indexer")
            assert await indexer.run_once()

            # Host-owned submission uses the unchanged source and an explicit head/configuration.
            reprocess = ReprocessingService(receiver, producer_id="device", actor="alice")
            source_id = received["receipt"]["source_event_id"]
            head = await reprocess.snapshot(scope, source_id)
            new_config, worker = processor(NewLocalCityAdapter())
            assert new_config != config
            receipt = await reprocess.submit(
                scope,
                source_event_id=source_id,
                request_id="interpretation-2",
                mode="replace_interpretation",
                configuration_sha256=new_config,
                expected_head_generation=head["generation"],
            )
            target = await client.durable_freeze_reprocessing_target(session, [receipt.request_id])
            timed = await client.durable_wait_until(session, target["target_id"], timeout=0)
            assert timed["state"] == "timed_out"
            assert await worker.run_once()
            decided = await client.durable_wait_until(session, target["target_id"], timeout=0)
            assert decided["state"] == "reached"
            assert decided["processing_commit_tokens"][0]["kind"] == "processing"
            assert "capture_commit_tokens" not in decided
            lagging = await client.durable_readiness(
                session, target["target_id"], stage="index_visible"
            )
            assert lagging["state"] == "processing"
            assert await indexer.run_once()
            visible = await client.durable_wait_until(
                session, target["target_id"], stage="index_visible", timeout=0
            )
            assert visible["state"] == "reached" and visible["target_visible_through"] == 2
            assert (await client.durable_cursor(session))["acked_through"] == 1
            print(
                "Fixed reprocessing target: decided and indexed; capture cursor: 1."
            )
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
