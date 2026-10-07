"""Repair a failed locator write, then explicitly switch around a deleted-source gap."""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_sdk import EmbeddedMemoryClient
from durable_readiness import LocalCityAdapter

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import (
    ForgetMode,
    ForgetRequest,
    MemoryScope,
    PredicateSpec,
    SourceAuthority,
    utc_now,
)
from agent_memory.kernel import MemoryKernel
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.index_recovery import CandidateIndexRecovery
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
    with TemporaryDirectory(prefix="agent-memory-index-recovery-demo-") as directory:
        repo = SQLiteMemoryRepository(Path(directory) / "memory.db")
        kernel = MemoryKernel(
            repo, MetadataClaimExtractor(), TrustedMemoryPolicy(), ReciprocalRankFusionReranker()
        )
        await kernel.initialize()
        try:
            scope = MemoryScope("demo", user_id="alice", session_id="session")
            adapter = LocalCityAdapter()
            pipeline = AtomExtractionPipeline(adapter, adapter)
            policy = AdmissionPolicy([PredicateSpec("city")])
            authority = SourceAuthority("user:alice", subjects=("alice",), predicates=("city",))
            channel = CandidateIndexChannel("local")
            config = processing_configuration_sha256(
                pipeline, policy, authority, index_channel=channel
            )
            producer = DurableProducer(DurableReceiver(repo), index_channel=channel)
            session = await producer.open(
                scope, producer_id="device", actor="alice", configuration_sha256=config
            )
            client = EmbeddedMemoryClient(
                kernel,
                MCPRequestContext(scope, actor="alice"),
                durable_capture=DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER),
            )
            extraction = ExtractionQueue(repo, scope, config)
            handler = DurableAtomHandler(
                extraction, pipeline, policy, authority, local_only=True, index_channel=channel
            )
            worker = BoundedWorker(extraction, {"memory.extract": handler}, worker_id="extract")
            queue = CandidateIndexQueue(repo, scope, channel, max_attempts=1)
            indexer = BoundedWorker(queue, {"memory.index": queue.apply}, worker_id="index")
            recovery = CandidateIndexRecovery(repo, scope, channel, actor="operator")

            async def append(sequence):
                event = LifecycleEvent(
                    scope,
                    str(sequence),
                    LifecycleEventType.MESSAGE_RECEIVED,
                    LifecycleOrigin.USER,
                    utc_now(),
                    "run",
                    content="Alice lives in Hangzhou",
                    actor="alice",
                )
                receipt = await client.durable_append(event.to_dict(), session, sequence)
                assert await worker.run_once()
                return receipt

            first = await append(1)
            first_target = await client.durable_freeze_target(session, [1])
            lease = await queue.claim("failed", lease_seconds=60)
            await queue.fail(lease, RuntimeError("transient locator failure"))
            snap = await recovery.inspect(lease.task.id)
            await recovery.repair(
                lease.task.id,
                recovery_id="repair-1",
                stream=snap["stream"],
                expected_job_sha256=snap["job_sha256"],
                reason="transient-failure",
            )
            assert (
                await client.durable_readiness(
                    session, first_target["target_id"], stage="index_visible"
                )
            )["state"] == "reached"
            await append(2)
            assert await indexer.run_once()
            old = await client.durable_freeze_target(session, [2])
            await kernel.forget(
                ForgetRequest(
                    scope, memory_ids=(first["receipt"]["source_event_id"],), mode=ForgetMode.ERASE
                )
            )
            assert (
                await client.durable_readiness(session, old["target_id"], stage="index_visible")
            )["state"] == "failed"
            switched = await recovery.rollover(
                recovery_id="switch-1", expected_generation=0, reason="deleted-source-prefix"
            )
            fresh = await client.durable_freeze_target(session, [2])
            assert (
                fresh["index_stream"] == switched["stream"]
                and fresh["target_id"] != old["target_id"]
            )
            assert (
                await client.durable_readiness(session, fresh["target_id"], stage="index_visible")
            )["state"] == "reached"
            assert (
                await client.durable_readiness(session, old["target_id"], stage="index_visible")
            )["reason"] == "index_stream_retired"
            print(
                "Actual repair; stream 1 target visible; capture epoch unchanged."
            )
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
