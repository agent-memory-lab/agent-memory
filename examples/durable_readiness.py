"""Cancel an erased offline sequence, then wait for a fixed L1 processing target."""

import asyncio
import json
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_sdk import DurableOutbox, EmbeddedMemoryClient

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import (
    AtomReview,
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
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


class LocalCityAdapter:
    """A deliberately narrow local example, not a general fact verifier."""

    version = "local-city-example/1"

    async def generate_atoms(self, event):
        if event.content != "Alice lives in Hangzhou":
            return []
        return [
            {
                "subject_id": "alice",
                "predicate": "city",
                "value": "Hangzhou",
                "kind": "fact",
                "modality": "asserted",
                "source_quote": event.content,
            }
        ]

    async def review_atoms(self, event, candidates):
        return [
            AtomReview(i, "supported", "durable", ("exact_example_sentence",))
            for i, _ in enumerate(candidates)
        ]


async def main():
    with TemporaryDirectory(prefix="agent-memory-readiness-demo-") as temporary:
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
            configuration = processing_configuration_sha256(pipeline, policy, authority)
            producer = DurableProducer(DurableReceiver(repository), max_gap=2)
            session = await producer.open(
                scope,
                producer_id="device",
                actor="alice",
                configuration_sha256=configuration,
                sync_purges=True,
                sequence_dispositions=True,
            )
            client = EmbeddedMemoryClient(
                kernel,
                MCPRequestContext(scope, actor="alice"),
                durable_capture=DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER),
            )
            outbox = DurableOutbox(
                Path(temporary) / "outbox.db", session, sync_purges=True, settle_purges=True
            )
            for identity, city in (("old", "Shanghai"), ("fresh", "Hangzhou")):
                outbox.append(
                    LifecycleEvent(
                        scope,
                        identity,
                        LifecycleEventType.MESSAGE_RECEIVED,
                        LifecycleOrigin.USER,
                        utc_now(),
                        "run",
                        content="Alice lives in " + city,
                        actor="alice",
                    ).to_dict()
                )
            erased = (
                "source:"
                + sha256(
                    json.dumps([scope.partition_key(), "old"], separators=(",", ":")).encode()
                ).hexdigest()
            )
            await kernel.forget(ForgetRequest(scope, memory_ids=(erased,), mode=ForgetMode.ERASE))
            received = await outbox.flush_one(client)
            assert (
                received["cursor"]["received_count"] == received["cursor"]["cancelled_count"] == 1
            )
            assert received["cursor"]["settled_through"] == 2
            target = await client.durable_freeze_target(session, [2])
            assert (await client.durable_wait_until(session, target["target_id"], timeout=0))[
                "state"
            ] == "timed_out"
            queue = ExtractionQueue(repository, scope, configuration)
            handler = DurableAtomHandler(queue, pipeline, policy, authority, local_only=True)
            assert await BoundedWorker(
                queue, {"memory.extract": handler}, worker_id="demo"
            ).run_once()
            ready = await client.durable_wait_until(session, target["target_id"], timeout=1)
            assert ready["state"] == "reached" and ready["publication_manifest_closed"]
            print("Received: 1; cancelled: 1; settled through: 2; fixed L1 target: reached.")
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
