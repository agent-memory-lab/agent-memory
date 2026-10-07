"""Run with core + SDK installed; a synthetic, source-grounded conditional example."""

import asyncio
import json
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_sdk import EmbeddedMemoryClient

from agent_memory.conditions import Condition, ContextAttribute, ProjectionPolicy, QueryContext
from agent_memory.consolidation.admission import AdmissionPolicy, draft_from_payload
from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.qualification import ContextualMemory, target_fingerprint
from agent_memory.derived import FacetContext, FacetDefinition, ObservationService, ProcessingGrant
from agent_memory.domain import (
    AtomReview,
    MemoryEvent,
    MemoryScope,
    PredicateSpec,
    SourceAuthority,
    utc_now,
)
from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange
from agent_memory.fact_qualification import SourceSpan
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


class LanguageExample:
    version = "qualified-locale-example/1"

    async def generate_atoms(self, event):
        return [
            dict(
                subject_id="alice",
                predicate="locale",
                value="zh-CN",
                kind="preference",
                modality="asserted",
                source_quote=event.content,
                conditions=("仅限项目 A",),
                exceptions=("节假日除外",),
            )
        ]

    async def review_atoms(self, event, candidates):
        return [
            AtomReview(i, "supported", "durable", ("synthetic_example",))
            for i, _ in enumerate(candidates)
        ]


async def main():
    with TemporaryDirectory(prefix="memory-contextual-observation-") as directory:
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
            adapter = LanguageExample()
            pipeline = AtomExtractionPipeline(adapter, adapter)
            config = processing_configuration_sha256(pipeline, policy, authority)
            source = MemoryEvent(scope, "message", "项目 A 请使用中文，节假日除外。", actor="alice")
            receiver = DurableReceiver(repository)
            ticket = await receiver.issue_ticket(
                source, request_id="example", producer_id="host", configuration_sha256=config
            )
            await receiver.submit(
                source, ticket=ticket, producer_id="host", configuration_sha256=config
            )
            extraction = ExtractionQueue(repository, scope, config)
            handler = DurableAtomHandler(extraction, pipeline, policy, authority, local_only=True)
            assert await BoundedWorker(
                extraction, {"memory.extract": handler}, worker_id="extract"
            ).run_once()
            async with repository.unit_of_work() as uow:
                row = (await uow.list_admission_records(scope))[0]
            draft = draft_from_payload(row["payload"]["draft"])
            projection = ProjectionPolicy("locale-context/1", "agent_context")
            fields = ("subject_id", "predicate", "value", "conditions", "exceptions")
            link = EvidenceLink(
                "literal",
                fields,
                target_fingerprint(draft),
                SourceSpan(source.id, 0, len(source.content), source.content),
                authority,
                SupportRange(source.occurred_at),
                source.id,
            )
            await ContextualMemory(
                AdmissionEngine(repository), scope, principal="host:alice"
            ).qualify(
                row["id"],
                expected_version=row["version"],
                admission_policy=policy,
                projection_policy=projection,
                applicability_id="project-A",
                conditions=(Condition("eq", "project", "A"),),
                exceptions=(Condition("eq", "holiday", True),),
                links=(link,),
                field_support=tuple(FieldSupport(field, (("literal",),)) for field in fields),
            )
            now = utc_now()
            context = QueryContext(
                "host:alice",
                scope,
                "alice",
                "agent_context",
                now,
                now,
                (
                    ContextAttribute("project", "A", "host-route"),
                    ContextAttribute("holiday", False, "host-calendar"),
                ),
                snapshot_token="project-A-route",
            )
            derived = ObservationService(
                repository, scope, policy, context_token=context.snapshot_token
            )
            await derived.register(
                FacetDefinition(
                    "language-A",
                    "alice",
                    template_version="locale-context/1",
                    context=FacetContext(context, projection, now + timedelta(minutes=30)),
                )
            )
            await derived.grant(ProcessingGrant(source.id, ("alice",)))
            queue = FacetRefreshQueue(derived)
            target = await queue.request("language-A", dedupe_key="example")
            assert await BoundedWorker(
                queue, {"memory.facet_refresh": derived.apply}, worker_id="derive"
            ).run_once()
            client = EmbeddedMemoryClient(
                kernel, MCPRequestContext(scope, actor="alice"), derived=derived
            )
            assert (await client.derived_status(target["target_id"]))["complete"]
            result = await client.derived_context("language-A")
            block = result["observations"][0]["body"]["blocks"][0]
            assert (
                block["value"] == "zh-CN"
                and block["qualified"]
                and block["conditions"]
                and block["exceptions"]
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
