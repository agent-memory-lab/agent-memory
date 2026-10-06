"""Synchronize deletion before an offline outbox can deliver source bodies."""

import asyncio
import json
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_sdk import DurableOutbox, EmbeddedMemoryClient

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.domain import ForgetMode, ForgetRequest, MemoryScope, utc_now
from agent_memory.kernel import MemoryKernel
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.retention import DurableReceiver
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


async def main():
    with TemporaryDirectory(prefix="agent-memory-purge-demo-") as temporary:
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
            producer = DurableProducer(DurableReceiver(repository))
            session = await producer.open(
                scope,
                producer_id="device",
                actor="alice",
                configuration_sha256="a" * 64,
                sync_purges=True,
            )
            client = EmbeddedMemoryClient(
                kernel,
                MCPRequestContext(scope, actor="alice"),
                durable_capture=DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER),
            )
            outbox = DurableOutbox(Path(temporary) / "outbox.db", session, sync_purges=True)
            for identity in ("old", "fresh"):
                outbox.append(
                    LifecycleEvent(
                        scope,
                        identity,
                        LifecycleEventType.MESSAGE_RECEIVED,
                        LifecycleOrigin.USER,
                        utc_now(),
                        "run",
                        content="Private input " + identity,
                        actor="alice",
                    ).to_dict()
                )
            # The offline source has never reached the server. Its stable identity is sufficient.
            source_id = (
                "source:"
                + sha256(
                    json.dumps([scope.partition_key(), "old"], separators=(",", ":")).encode()
                ).hexdigest()
            )
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id,), mode=ForgetMode.ERASE)
            )
            response = await outbox.flush_one(client)
            assert response["sequence"] == 2 and response["acked_through"] == 0
            assert await outbox.flush_one(client) is None
            print("Old body purged; fresh sequence 2 received; sequence 1 remains an actual gap.")
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
