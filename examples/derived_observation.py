"""Run: python examples/derived_observation.py (core + Python SDK installed).

The host registers definitions and source permissions. Agent-facing SDK calls
can only read current guarded output. This fixture demonstrates protocol behavior,
not production extraction quality or externally verified preferences.
"""

import asyncio
import json
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_sdk import EmbeddedMemoryClient

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.derived import (
    FacetDefinition,
    HostGrantAuthority,
    ObservationService,
    ProcessingGrant,
    QueryDefinition,
)
from agent_memory.domain import (
    AtomReview,
    MemoryEvent,
    MemoryScope,
    PredicateSpec,
    SourceAuthority,
    utc_now,
)
from agent_memory.kernel import MemoryKernel
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.facet_refresh import FacetRefreshQueue
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


class LocaleExample:
    version = "locale-example/1"

    async def generate_atoms(self, event):
        return [
            dict(
                subject_id="alice",
                predicate="locale",
                value="zh-CN",
                kind="preference",
                modality="asserted",
                source_quote=event.content,
            )
        ]

    async def review_atoms(self, event, candidates):
        return [
            AtomReview(i, "supported", "durable", ("explicit_example",))
            for i, _ in enumerate(candidates)
        ]


async def main(*, host_controls=False, history=False, continuous=False, parents=False):
    if parents and (history or continuous):
        raise ValueError("Derived parent views currently require current-time inputs")
    history = history or continuous
    host_controls = host_controls or history
    history_mode = (
        "published-interval/1" if continuous else ("published-point/1" if history else None)
    )
    with TemporaryDirectory(prefix="memory-observation-") as directory:
        repository = SQLiteMemoryRepository(Path(directory) / "memory.db")
        kernel = MemoryKernel(
            repository,
            MetadataClaimExtractor(),
            TrustedMemoryPolicy(),
            ReciprocalRankFusionReranker(),
        )
        await kernel.initialize()
        try:
            scope = MemoryScope("example", user_id="alice", session_id="demo")
            policy = AdmissionPolicy([PredicateSpec("locale")])
            authority = SourceAuthority("user:alice", subjects=("alice",), predicates=("locale",))
            adapter = LocaleExample()
            pipeline = AtomExtractionPipeline(adapter, adapter)
            configuration = processing_configuration_sha256(pipeline, policy, authority)
            source = MemoryEvent(scope, "message", "Alice prefers zh-CN", actor="alice")
            receiver = DurableReceiver(repository)
            ticket = await receiver.issue_ticket(
                source, request_id="example", producer_id="host", configuration_sha256=configuration
            )
            await receiver.submit(
                source, ticket=ticket, producer_id="host", configuration_sha256=configuration
            )
            extraction = ExtractionQueue(repository, scope, configuration)
            handler = DurableAtomHandler(extraction, pipeline, policy, authority, local_only=True)
            assert await BoundedWorker(
                extraction, {"memory.extract": handler}, worker_id="extract"
            ).run_once()
            authority_id = "example-host" if host_controls else None
            query_id = "language-inputs" if host_controls else None
            derived = ObservationService(
                repository,
                scope,
                policy,
                authority_id=authority_id,
                authority_min_version=0 if host_controls else None,
                history_mode=history_mode,
            )
            if host_controls:
                await derived.set_authority(
                    HostGrantAuthority(authority_id, ("alice",), utc_now() + timedelta(hours=1))
                )
                await derived.register_query(QueryDefinition(query_id, scope, "alice", ("locale",)))
            await derived.register(
                FacetDefinition(
                    "language",
                    "alice",
                    query_id=query_id,
                    authority_id=authority_id,
                    history_mode=history_mode,
                )
            )
            await derived.grant(ProcessingGrant(source.id, ("alice",)))
            queue = FacetRefreshQueue(derived)
            target = await queue.request("language", dedupe_key="example")
            assert await BoundedWorker(
                queue, {"memory.facet_refresh": derived.apply}, worker_id="derive"
            ).run_once()
            client = EmbeddedMemoryClient(
                kernel, MCPRequestContext(scope, actor="alice"), derived=derived
            )
            assert (await client.derived_status(target["target_id"]))["complete"]
            print(
                json.dumps(await client.derived_context("language"), ensure_ascii=False, indent=2)
            )
            if parents:
                for key, parent in (("scenario-language", "language"),
                                    ("agent-language", "scenario-language")):
                    await derived.register(FacetDefinition(
                        key, "alice", template_version="locale-parents/1",
                        parent_facets=(parent,), authority_id=authority_id,
                    ))
                    await queue.request(key, dedupe_key=key)
                    assert await BoundedWorker(
                        queue, {"memory.facet_refresh": derived.apply}, worker_id=key
                    ).run_once()
                print(json.dumps(await client.derived_context("agent-language"),
                                 ensure_ascii=False, indent=2))
                await derived.grant(
                    ProcessingGrant(source.id, ("alice",), revoked=True), expected_version=1
                )
                assert (await client.derived_context("agent-language"))["observations"] == []
            if history:
                points = await client.derived_history_points("language")
                known_at = points["points"][0]["known_at"]
                for valid_at in (source.occurred_at, source.occurred_at - timedelta(seconds=1)):
                    print(
                        json.dumps(
                            await client.derived_read(
                                "language", known_at=known_at, valid_at=valid_at.isoformat()
                            ),
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
            if continuous:
                print(
                    json.dumps(
                        await client.derived_context(
                            "language",
                            known_at=utc_now().isoformat(),
                            valid_at=source.occurred_at.isoformat(),
                        ),
                        ensure_ascii=False,
                        indent=2,
                    )
                )
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
