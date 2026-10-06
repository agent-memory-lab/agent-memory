"""Run a local, deterministic durable memory round trip (requires Python SDK)."""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_sdk import DurableOutbox, EmbeddedMemoryClient

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.extraction_rules import RuleBasedAtomAdapter
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
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


async def main():
    with TemporaryDirectory(prefix="agent-memory-durable-demo-") as temporary:
        root = Path(temporary)
        repository = SQLiteMemoryRepository(root / "memory.db")
        kernel = MemoryKernel(
            repository,
            MetadataClaimExtractor(),
            TrustedMemoryPolicy(),
            ReciprocalRankFusionReranker(),
        )
        await kernel.initialize()
        try:
            scope = MemoryScope("demo", user_id="alice", session_id="session")
            rules = RuleBasedAtomAdapter("alice")
            pipeline = AtomExtractionPipeline(rules, rules)
            policy = AdmissionPolicy([PredicateSpec("home_city")])
            authority = SourceAuthority(
                "user:alice", subjects=("alice",), predicates=("home_city",)
            )
            configuration = processing_configuration_sha256(pipeline, policy, authority)
            producer = DurableProducer(DurableReceiver(repository))
            session = await producer.open(
                scope, producer_id="demo-host", actor="alice", configuration_sha256=configuration
            )
            client = EmbeddedMemoryClient(
                kernel,
                MCPRequestContext(scope, actor="alice"),
                durable_capture=DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER),
            )
            outbox = DurableOutbox(root / "pending.db", session)
            # Real hosts sanitize before local persistence; this example contains no secrets.
            event = LifecycleEvent(
                scope=scope,
                event_id="user-message-1",
                event_type=LifecycleEventType.MESSAGE_RECEIVED,
                origin=LifecycleOrigin.USER,
                occurred_at=utc_now(),
                run_id="demo",
                content="我住在杭州",
            ).to_dict()
            sequence = outbox.append(event)
            await outbox.flush_one(client)
            queue = ExtractionQueue(repository, scope, configuration)
            handler = DurableAtomHandler(queue, pipeline, policy, authority, local_only=True)
            await BoundedWorker(queue, {"memory.extract": handler}, worker_id="local").run_once()
            status = await client.durable_status(session, sequence)
            print(
                json.dumps(
                    {
                        "status": status["receipt"]["status"],
                        "source_persisted": status["source_persisted"],
                        "l1_decided": status["l1_decided"],
                        "claim_ids": status["result"]["claim_ids"],
                    },
                    ensure_ascii=False,
                )
            )
            outbox.purge()
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
